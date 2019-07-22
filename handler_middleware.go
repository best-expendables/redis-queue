package redisqueue

import (
	"bitbucket.org/snapmartinc/logger"
	"context"
)

type HandleFunc func(context.Context, Job) error
type HandlerMiddleWare func(next HandleFunc) HandleFunc

//Log message middleware
func LogJob(next HandleFunc) HandleFunc {
	return func(ctx context.Context, job Job) error {
		logEntry := logger.EntryFromContext(ctx)
		meta := logger.Fields{
			"source": "ConsumerLogger",
			"job":    getJobStringFormat(job),
		}
		logEntry.WithFields(meta).Info("Log consuming job")

		return next(ctx, job)
	}
}

func chainHandlerMiddleWares(middlewares []HandlerMiddleWare, h HandleFunc) HandleFunc {
	if len(middlewares) == 0 {
		return h
	}

	// Wrap the end handler with the middleware chain
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return h
}
