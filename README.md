# hecc-blot-scheduler

面向接口的定时任务调度组件：解析 cron 表达式、按计划触发任务、优雅停止，自动接入链路追踪与统一日志。覆盖超时处理、周期对账、批量清理等场景。

## 安装

```bash
go get github.com/hecc-blot/scheduler
```

## 接口定义

```go
import (
    "context"
    schedulerContract "github.com/hecc-blot/scheduler/contract"
)

type Job interface {
    Run(ctx context.Context) error
}

type IScheduler interface {
    Add(name, spec string, job Job) error   // 6 段含秒 / 5 段标准 cron，name 唯一
    AddFunc(name, spec string, fn func(context.Context) error) error
    Start() // 幂等
    Stop()  // 优雅停止：停触发 + 等在途
}
```

## 初始化

```go
import (
    scheduler "github.com/hecc-blot/scheduler/service"
)

s, err := scheduler.NewScheduler(&config.Scheduler, logSvc, traceSvc, nil)
if err != nil {
    panic(err)
}
```

`logSvc` 为 `log.ILog`（framework），`traceSvc` 为 `trace.ITrace`（trace），`guard` 为多副本执行守卫（单实例传 nil），均可为 nil。

## 注册任务

```go
// 结构体任务（依赖在构造时注入）
_ = s.Add("cleanup", "0 */5 * * * *", NewCleanupJob(dbFactory))

// 闭包任务
_ = s.AddFunc("reconcile", "0 0 2 * * *", func(ctx context.Context) error {
    return reconcile(ctx)
})

s.Start()
defer s.Stop()
```

## cron 表达式

同时兼容 5 段（标准 `分 时 日 月 周`）与 6 段（含秒 `秒 分 时 日 月 周`），支持 `*`、`*/step`、`a-b`、`a-b/step`、列表 `a,b,c`。

## 执行语义

| 行为 | 说明 |
|------|------|
| 重叠控制 | 默认同名任务上一次未结束、下一次触发时跳过；`allow_overlap: true` 放开 |
| panic 恢复 | 任务 panic 被捕获并记录，不拖垮调度循环 |
| 多副本守卫 | 注入 `IJobGuard`（分布式锁）后，同任务仅一个节点执行；单实例传 nil |
| 链路追踪 | 每次执行新建独立根 span，日志自动携带 traceId |

## 配置

```yaml
scheduler:
  location: Asia/Shanghai   # 时区，默认 Local
  allow_overlap: false      # 是否允许同名任务重叠执行，默认 false
```

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `location` | string | 时区，IANA 名 / `UTC` / `Local`，默认 `Local` |
| `allow_overlap` | bool | 是否允许重叠执行，默认 `false` |

## 测试与 mock

业务单测中 mock 掉调度器，见 `mocks/`：

```go
import "github.com/hecc-blot/scheduler/mocks"

mockScheduler := &mocks.MockScheduler{}
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [framework](https://github.com/hecc-blot/framework) | `log.ILog` |
| [trace](https://github.com/hecc-blot/trace) | `trace.ITrace` 链路追踪 |
| [cache](https://github.com/hecc-blot/cache) | 多副本守卫依赖的 SetNX 原子原语（规划中） |
