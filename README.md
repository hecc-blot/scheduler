# hecc-blot-scheduler

定时任务模块：cron 表达式调度，支持时区、重叠控制、多副本守卫与优雅停止。

## 安装

```bash
go get github.com/hecc-blot/scheduler
```

## 快速开始

```go
import (
	"context"

	logContract "github.com/hecc-blot/core/contract/log"
	schedulerContract "github.com/hecc-blot/scheduler/contract"
	scheduler "github.com/hecc-blot/scheduler/service"
	traceContract "github.com/hecc-blot/trace/contract"
)

func main() {
	// 1. 构造（返回 实例 / 错误；无 cleanup）
	//    logger / traceSvc / guard 均可为 nil（guard 为 nil 表示每个任务都执行）
	schedulerSvc, err := scheduler.NewScheduler(&config.Scheduler, logSvc, traceSvc, nil)
	if err != nil {
		panic(err)
	}

	// 2. 注册任务（Add 的错误不要丢弃）
	if err := schedulerSvc.Add("cleanup", "0 */5 * * * *", &CleanupJob{}); err != nil {
		panic(err)
	}

	// 3. 启动 → defer 停止
	schedulerSvc.Start()
	defer schedulerSvc.Stop()
}
```

完整的应用组装顺序见[组织主页](https://github.com/hecc-blot)。

## 接口定义

```go
import schedulerContract "github.com/hecc-blot/scheduler/contract"

// 任务抽象
type Job interface {
	Run(ctx context.Context) error
}

// 函数式适配
type JobFunc func(ctx context.Context) error

type IScheduler interface {
	// Add 注册任务。spec 为 cron 表达式（6 段含秒 / 5 段标准），name 全局唯一。
	// 同名重复注册、nil 任务或非法表达式返回错误。
	Add(name string, spec string, job Job) error
	// AddFunc 等价于 Add(name, spec, JobFunc(fn))
	AddFunc(name string, spec string, fn func(ctx context.Context) error) error
	// Start 启动调度循环。幂等。
	Start()
	// Stop 优雅停止：停止触发新任务，等待在途任务完成后返回。幂等。
	Stop()
}

// 多副本执行守卫：决定本节点是否应执行某任务
type IJobGuard interface {
	// TryAcquire 尝试获取执行权，返回释放闭包（任务结束后调用）。
	// acquired=false 表示其他节点正在执行，本次跳过。
	TryAcquire(ctx context.Context, name string) (release func(), acquired bool)
}
```

## 使用

### 注册任务

```go
type CleanupJob struct {
	Db dbContract.IDb `inject:""`
}

func (j *CleanupJob) Run(ctx context.Context) error {
	return j.Db.WithContext(ctx).Where("created_at < ?", cutoff).Remove(&LogModel{})
}

// 结构化任务
if err := schedulerSvc.Add("cleanup", "0 */5 * * * *", &CleanupJob{}); err != nil {
	return err
}

// 函数式任务
err := schedulerSvc.AddFunc("ping", "*/30 * * * * *", func(ctx context.Context) error {
	logSvc.Info(ctx, "ping")
	return nil
})
```

> **`Add` 的错误要处理**：同名重复注册、任务为 nil、cron 表达式非法都会返回 error。写成 `_ = Add(...)` 会让配置错误静默失效。

### cron 表达式

框架使用 `SecondOptional | Minute | Hour | Dom | Month | Dow` 六字段解析器：

```
0 */5 * * * *      6 段（含秒）：每 5 分钟（第 0 秒）触发
*/30 * * * * *     6 段：每 30 秒
*/5 * * * *        5 段（标准）：每 5 分钟
0 0 3 * * *        每天 03:00
0 30 9 * * 1-5     工作日 09:30
```

**不支持**的写法：`@every 5m` / `@daily` 等描述符、`?` 占位符、`JAN` / `MON` 之类的月份与星期名称。

### 多副本守卫

单实例部署传 `nil`（每个任务都会执行）。多副本部署时注入 `IJobGuard`，让同一任务在集群中只跑一次：

```go
type LockGuard struct {
	Locker lockContract.ILocker
}

func (g *LockGuard) TryAcquire(ctx context.Context, name string) (func(), bool) {
	lk, err := g.Locker.TryLock(ctx, "scheduler:"+name, 30*time.Second)
	if err != nil {
		return func() {}, false
	}
	return func() { _ = lk.Unlock(context.Background()) }, true
}

schedulerSvc, err := scheduler.NewScheduler(&config.Scheduler, logSvc, traceSvc, &LockGuard{Locker: locker})
```

## 配置

```yaml
scheduler:
  location: Asia/Shanghai   # 时区，默认 Local；支持 IANA 名 / UTC / Local
  allow_overlap: false      # 同名任务上次未结束时是否允许重叠，默认 false（跳过本次）
```

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `location` | string | `Local` | cron 解析时区，非法时区名会让 `NewScheduler` 返回 error |
| `allow_overlap` | bool | false | `false` 时跳过与上次未结束执行重叠的触发 |

## 执行语义

| 行为 | 说明 |
|------|------|
| 重叠 | `allow_overlap: false`（默认）时，上次未结束则**跳过本次触发**；`true` 则并发执行 |
| 并发 | 不同任务之间互不影响，各自独立触发 |
| 停止 | `Stop()` 停止触发新任务并**阻塞等待在途任务完成**后才返回 |
| 幂等 | `Start()` / `Stop()` 均可重复调用 |

> ⚠️ **任务没有超时与取消机制**。传给 `Job.Run` 的 ctx 是 `context.Background()`，没有 deadline，任务期间也不会被取消。因此：
>
> - 长任务会**阻塞 `Stop()`**，拖慢进程退出；
> - 需要超时请在 `Run` 内部自行用 `context.WithTimeout` 包一层。
>
> 另：`guard` 的 `TryAcquire` 也是用 `context.Background()` 调用的。

## 语义与约定

- **`NewScheduler` 的 `cfg` 不能为 nil**（返回 error）；`logger` / `traceSvc` / `guard` 可以为 nil。
- **panic 条件**：`Add` 在调度器启动后再调用不会 panic，但新任务同样能被调度；`Stop` 之后能否再 `Start` 未定义。
- **无释放语义**：调度器不持有需关闭的资源，构造器不返回 cleanup。

## 测试与 mock

```go
import "github.com/hecc-blot/scheduler/mocks"

mock := &mocks.MockScheduler{
	AddFn:   func(name, spec string, job schedulerContract.Job) error { return nil },
	StartFn: func() {},
	StopFn:  func() {},
}
// mock.Added 记录已注册的任务，便于断言
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [core](https://github.com/hecc-blot/core) | `log.ILog` 日志契约 |
| [trace](https://github.com/hecc-blot/trace) | `ITrace`，任务执行链路 |
| [lock](https://github.com/hecc-blot/lock) | 多副本守卫 `IJobGuard` 的常用实现基础 |
