package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/hecc-blot/core/contract/log"
	schedulerConf "github.com/hecc-blot/scheduler/config"
	schedulerContract "github.com/hecc-blot/scheduler/contract"
	trace "github.com/hecc-blot/trace/contract"
)

// noopSpan 是 trace.Span 的空实现，避免到处判空。
type noopSpan struct{}

func (noopSpan) End()                             {}
func (noopSpan) SetAttribute(string, interface{}) {}
func (noopSpan) RecordError(error)                {}
func (noopSpan) Name() string                     { return "" }

// schedulerSvc 基于 robfig/cron 的调度器实现。
type schedulerSvc struct {
	cron         *cron.Cron
	logger       log.ILog
	traceSvc     trace.ITrace
	guard        schedulerContract.IJobGuard
	allowOverlap bool

	started atomic.Bool

	mu    sync.Mutex
	names map[string]struct{}
}

// NewScheduler 创建调度器。logger / traceSvc / guard 均可为 nil。
func NewScheduler(cfg *schedulerConf.Config, logger log.ILog, traceSvc trace.ITrace, guard schedulerContract.IJobGuard) (schedulerContract.IScheduler, error) {
	if cfg == nil {
		return nil, fmt.Errorf("scheduler: 配置不能为空")
	}
	c := schedulerConf.Normalize(*cfg)

	loc, err := loadLocation(c.Location)
	if err != nil {
		return nil, err
	}

	// SecondOptional：同时兼容 6 段（含秒）与 5 段（标准）cron 表达式。
	parser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)

	return &schedulerSvc{
		cron:         cron.New(cron.WithLocation(loc), cron.WithParser(parser)),
		logger:       logger,
		traceSvc:     traceSvc,
		guard:        guard,
		allowOverlap: c.AllowOverlap,
		names:        make(map[string]struct{}),
	}, nil
}

// Add 注册任务。
func (s *schedulerSvc) Add(name string, spec string, job schedulerContract.Job) error {
	if name == "" {
		return fmt.Errorf("scheduler: 任务名不能为空")
	}
	if job == nil {
		return fmt.Errorf("scheduler: 任务 %q 不能为 nil", name)
	}

	s.mu.Lock()
	if _, exists := s.names[name]; exists {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: 任务 %q 已存在", name)
	}
	s.names[name] = struct{}{}
	s.mu.Unlock()

	entry := &jobEntry{name: name, job: job, scheduler: s}
	if _, err := s.cron.AddJob(spec, entry); err != nil {
		// 表达式非法，回滚名字登记
		s.mu.Lock()
		delete(s.names, name)
		s.mu.Unlock()
		return fmt.Errorf("scheduler: 任务 %q 的 cron 表达式非法: %w", name, err)
	}
	return nil
}

// AddFunc 注册函数式任务。
func (s *schedulerSvc) AddFunc(name string, spec string, fn func(ctx context.Context) error) error {
	return s.Add(name, spec, schedulerContract.JobFunc(fn))
}

// Start 启动调度循环（幂等）。
func (s *schedulerSvc) Start() {
	if s.started.CompareAndSwap(false, true) {
		s.cron.Start()
	}
}

// Stop 优雅停止（幂等）：停止触发新任务，等待在途任务完成。
func (s *schedulerSvc) Stop() {
	if s.started.CompareAndSwap(true, false) {
		ctx := s.cron.Stop()
		<-ctx.Done()
	}
}

// loadLocation 解析时区配置。
func loadLocation(name string) (*time.Location, error) {
	switch name {
	case "", "Local":
		return time.Local, nil
	default:
		loc, err := time.LoadLocation(name)
		if err != nil {
			return nil, fmt.Errorf("scheduler: 无效时区 %q: %w", name, err)
		}
		return loc, nil
	}
}

// jobEntry 适配 cron.Job，注入 trace / 日志 / panic 恢复 / 重叠控制 / 执行守卫。
type jobEntry struct {
	name      string
	job       schedulerContract.Job
	scheduler *schedulerSvc
	running   atomic.Bool
}

// Run 实现 cron.Job 接口。
func (e *jobEntry) Run() {
	// 1) 重叠控制（默认跳过）：上一次执行未结束时，本次触发直接跳过
	if !e.scheduler.allowOverlap {
		if !e.running.CompareAndSwap(false, true) {
			e.logWarn("任务仍在执行，跳过本次触发")
			return
		}
		defer e.running.Store(false)
	}

	// 2) 多副本执行守卫（可选）：未获取执行权则跳过
	if e.scheduler.guard != nil {
		release, acquired := e.scheduler.guard.TryAcquire(context.Background(), e.name)
		if !acquired {
			e.logWarn("其他节点正在执行，本次跳过")
			return
		}
		defer release()
	}

	// 3) 独立根 span：定时任务无父请求，新建追踪根
	ctx := context.Background()
	var span trace.Span = noopSpan{}
	if e.scheduler.traceSvc != nil {
		ctx, span = e.scheduler.traceSvc.Start(ctx, "scheduler."+e.name, "job.name", e.name)
	}
	defer span.End()

	// 4) panic 恢复：任务 panic 不拖垮调度循环
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			span.RecordError(err)
			e.logError("任务 panic 已捕获", zap.Any("panic", r))
		}
	}()

	// 5) 执行任务
	if err := e.job.Run(ctx); err != nil {
		span.RecordError(err)
		e.logError("任务执行失败", zap.Error(err))
	}
}

// withJob 在日志字段前统一追加任务名。
func (e *jobEntry) withJob(fields ...interface{}) []interface{} {
	return append([]interface{}{zap.String("job", e.name)}, fields...)
}

func (e *jobEntry) logWarn(msg string, fields ...interface{}) {
	if e.scheduler.logger == nil {
		return
	}
	e.scheduler.logger.Warn(context.Background(), msg, e.withJob(fields...)...)
}

func (e *jobEntry) logError(msg string, fields ...interface{}) {
	if e.scheduler.logger == nil {
		return
	}
	e.scheduler.logger.Error(context.Background(), msg, e.withJob(fields...)...)
}
