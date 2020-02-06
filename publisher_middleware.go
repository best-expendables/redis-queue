package redisqueue

import (
	"context"
	"fmt"

	newrelic "github.com/newrelic/go-agent"
)

type PublisherHandlerFunc func(ctx context.Context, job Job) error

type PublisherHandlerMiddleWare func(next PublisherHandlerFunc) PublisherHandlerFunc

func WithNewRelicTransaction() PublisherHandlerMiddleWare {
	return func(next PublisherHandlerFunc) PublisherHandlerFunc {
		return func(ctx context.Context, job Job) error {
			txn := newrelic.FromContext(ctx)
			if txn != nil {
				segment := newrelic.StartSegment(txn, fmt.Sprintf("%s-%s", job.GetQueue(), job.GetID()))
				defer func() {
					_ = segment.End()
				}()
			}
			return next(ctx, job)
		}
	}
}
