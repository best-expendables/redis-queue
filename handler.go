package redisqueue

import "context"

type Handler interface {
	// Handle job return nil error mean job was processed successfully, otherwise job
	// was failed and method FailJob will be called
	Handle(ctx context.Context, job Job) error
	// Should retry on error
	ShouldRetryOnError(err error) bool
	// Should be move to rejected queue in case of fail
	ShouldRejectOnFailure(err error) bool
}

// Make handler from context
// Brand new handler is created for every request
type HandlerFactory func(ctx context.Context) Handler

// Base handler, all handler should embed this one
type BaseHandler struct{}

// Handle job
func (handler *BaseHandler) Handle(_ context.Context, _ Job) error {
	return nil
}

// Determine if which this error job should retry or fail
func (handler *BaseHandler) ShouldRetryOnError(err error) bool {
	return false
}

// Depend on error, we can move job to rejected queue OR just skip, ignore the job
func (handler *BaseHandler) ShouldRejectOnFailure(err error) bool {
	return true
}
