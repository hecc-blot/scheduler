package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	schedulerConf "github.com/hecc-blot/scheduler/config"
	schedulerContract "github.com/hecc-blot/scheduler/contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestScheduler(t *testing.T) *schedulerSvc {
	t.Helper()
	s, err := NewScheduler(&schedulerConf.Config{Location: "Local"}, nil, nil, nil)
	require.NoError(t, err)
	return s.(*schedulerSvc)
}

func TestNewSchedulerNilConfig(t *testing.T) {
	_, err := NewScheduler(nil, nil, nil, nil)
	assert.Error(t, err)
}

func TestNewSchedulerInvalidLocation(t *testing.T) {
	_, err := NewScheduler(&schedulerConf.Config{Location: "Mars/Olympus"}, nil, nil, nil)
	assert.Error(t, err)
}

func TestAddValidSpecs(t *testing.T) {
	s := newTestScheduler(t)
	noop := schedulerContract.JobFunc(func(ctx context.Context) error { return nil })
	// 5 段标准
	assert.NoError(t, s.Add("five", "*/5 * * * *", noop))
	// 6 段含秒
	assert.NoError(t, s.Add("six", "*/10 * * * * *", noop))
}

func TestAddInvalidSpec(t *testing.T) {
	s := newTestScheduler(t)
	err := s.Add("bad", "not a cron", schedulerContract.JobFunc(func(ctx context.Context) error { return nil }))
	assert.Error(t, err)
}

func TestAddDuplicateName(t *testing.T) {
	s := newTestScheduler(t)
	job := schedulerContract.JobFunc(func(ctx context.Context) error { return nil })
	require.NoError(t, s.Add("dup", "* * * * *", job))
	assert.Error(t, s.Add("dup", "* * * * *", job))
}

func TestAddNilJob(t *testing.T) {
	s := newTestScheduler(t)
	assert.Error(t, s.Add("nil", "* * * * *", nil))
}

func TestAddEmptyName(t *testing.T) {
	s := newTestScheduler(t)
	assert.Error(t, s.Add("", "* * * * *", schedulerContract.JobFunc(func(ctx context.Context) error { return nil })))
}

func TestAddFunc(t *testing.T) {
	s := newTestScheduler(t)
	assert.NoError(t, s.AddFunc("fn", "* * * * *", func(ctx context.Context) error { return nil }))
	// AddFunc 与 Add 共用名字空间
	assert.Error(t, s.Add("fn", "* * * * *", schedulerContract.JobFunc(func(ctx context.Context) error { return nil })))
}

func TestStartStopIdempotent(t *testing.T) {
	s := newTestScheduler(t)
	require.NoError(t, s.Add("a", "* * * * *", schedulerContract.JobFunc(func(ctx context.Context) error { return nil })))
	s.Start()
	s.Start() // 幂等
	s.Stop()
	s.Stop() // 幂等
}

func TestJobEntrySkipIfRunning(t *testing.T) {
	s := newTestScheduler(t)
	var started, finished atomic.Int32
	release := make(chan struct{})

	e := &jobEntry{
		name:      "skip",
		scheduler: s,
		job: schedulerContract.JobFunc(func(ctx context.Context) error {
			started.Add(1)
			<-release
			finished.Add(1)
			return nil
		}),
	}

	// 第一次触发：进入执行并阻塞
	go e.Run()
	require.Eventually(t, func() bool { return started.Load() == 1 }, time.Second, 10*time.Millisecond)

	// 第二次触发：allowOverlap=false，应被跳过
	e.Run()
	assert.Equal(t, int32(1), started.Load())

	// 放行第一次执行
	close(release)
	require.Eventually(t, func() bool { return finished.Load() == 1 }, time.Second, 10*time.Millisecond)

	// 结束后再次触发应正常执行
	e.Run()
	assert.Equal(t, int32(2), started.Load())
}

func TestJobEntryGuardSkips(t *testing.T) {
	var ran atomic.Int32
	guard := &fakeGuard{acquired: false}
	s, err := NewScheduler(&schedulerConf.Config{Location: "Local"}, nil, nil, guard)
	require.NoError(t, err)
	svc := s.(*schedulerSvc)

	e := &jobEntry{
		name:      "guard",
		scheduler: svc,
		job:       schedulerContract.JobFunc(func(ctx context.Context) error { ran.Add(1); return nil }),
	}
	e.Run()
	assert.Equal(t, int32(0), ran.Load())
}

func TestJobEntryRecoverPanic(t *testing.T) {
	s := newTestScheduler(t)
	e := &jobEntry{
		name:      "panic",
		scheduler: s,
		job:       schedulerContract.JobFunc(func(ctx context.Context) error { panic("boom") }),
	}
	assert.NotPanics(t, e.Run)
}

// fakeGuard 是 IJobGuard 的测试替身。
type fakeGuard struct{ acquired bool }

func (g *fakeGuard) TryAcquire(_ context.Context, _ string) (func(), bool) {
	return func() {}, g.acquired
}
