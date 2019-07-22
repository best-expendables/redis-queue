package redisqueue

import "context"

type HandlerMock struct {
	HandleFn                func(ctx context.Context, job Job) error
	ShouldRetryOnErrorFn    func(err error) bool
	ShouldRejectOnFailureFn func(err error) bool
}

func (handler HandlerMock) Handle(ctx context.Context, job Job) error {
	return handler.HandleFn(ctx, job)
}

func (handler HandlerMock) ShouldRetryOnError(err error) bool {
	return handler.ShouldRetryOnErrorFn(err)
}

func (handler HandlerMock) ShouldRejectOnFailure(err error) bool {
	return handler.ShouldRejectOnFailureFn(err)
}
