package scheduler

import "context"

// Job 定时任务抽象。业务方实现 Run 定义任务逻辑。
type Job interface {
	Run(ctx context.Context) error
}

// JobFunc 函数式适配，便于用闭包直接注册。
type JobFunc func(ctx context.Context) error

// Run 实现 Job 接口。
func (f JobFunc) Run(ctx context.Context) error { return f(ctx) }

// IJobGuard 多副本执行守卫：决定本节点是否应执行某任务。
// 单实例部署传 nil（每个任务都执行）；多副本部署时注入基于分布式锁的实现
// （依赖 cache 原子原语 SetNX，待 feature.md #1 落地后提供）。
type IJobGuard interface {
	// TryAcquire 尝试获取执行权，返回释放闭包（任务结束后调用）。
	// acquired=false 表示其他节点正在执行，本次跳过。
	TryAcquire(ctx context.Context, name string) (release func(), acquired bool)
}

// IScheduler 定时任务调度器：解析 cron 表达式、按计划触发任务、优雅停止。
type IScheduler interface {
	// Add 注册任务。spec 为 cron 表达式（6 段含秒 / 5 段标准），name 全局唯一。
	// 同名重复注册、nil 任务或非法表达式返回错误。
	Add(name string, spec string, job Job) error
	// AddFunc 注册函数式任务，等价于 Add(name, spec, JobFunc(fn))。
	AddFunc(name string, spec string, fn func(ctx context.Context) error) error
	// Start 启动调度循环。幂等：重复调用无副作用。
	Start()
	// Stop 优雅停止：停止触发新任务，等待在途任务完成后返回。幂等。
	Stop()
}
