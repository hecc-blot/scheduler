package mocks

import (
	"context"

	schedulerContract "github.com/hecc-blot/scheduler/contract"
)

// AddedJob 记录一次已注册任务，供测试断言。
type AddedJob struct {
	Name string
	Spec string
	Job  schedulerContract.Job
}

// MockScheduler 是 IScheduler 接口的 mock 实现。
// 通过 AddFn / AddFuncFn / StartFn / StopFn 定制行为，未设置时记录到 Added。
type MockScheduler struct {
	AddFn     func(name, spec string, job schedulerContract.Job) error
	AddFuncFn func(name, spec string, fn func(ctx context.Context) error) error
	StartFn   func()
	StopFn    func()

	// Added 记录经 Add / AddFunc 注册的任务，便于断言。
	Added []AddedJob
}

func (m *MockScheduler) Add(name, spec string, job schedulerContract.Job) error {
	if m.AddFn != nil {
		return m.AddFn(name, spec, job)
	}
	m.Added = append(m.Added, AddedJob{Name: name, Spec: spec, Job: job})
	return nil
}

func (m *MockScheduler) AddFunc(name, spec string, fn func(ctx context.Context) error) error {
	if m.AddFuncFn != nil {
		return m.AddFuncFn(name, spec, fn)
	}
	m.Added = append(m.Added, AddedJob{Name: name, Spec: spec, Job: schedulerContract.JobFunc(fn)})
	return nil
}

func (m *MockScheduler) Start() {
	if m.StartFn != nil {
		m.StartFn()
	}
}

func (m *MockScheduler) Stop() {
	if m.StopFn != nil {
		m.StopFn()
	}
}

// 编译期断言：确保 mock 完整实现接口。
var _ schedulerContract.IScheduler = (*MockScheduler)(nil)
