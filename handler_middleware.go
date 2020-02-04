package redisqueue

import (
	"context"
	"fmt"

	"bitbucket.org/snapmartinc/logger"
	nrcontext "bitbucket.org/snapmartinc/newrelic-context"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	newrelic "github.com/newrelic/go-agent"
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

func WithNewRelicForConsumer(nrApp newrelic.Application) func(next HandleFunc) HandleFunc {
	return func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, job Job) error {
			nrTxn := nrApp.StartTransaction(fmt.Sprintf("%s:%s", "redis-queue-", job.GetQueue()), nil, nil)
			_ = nrTxn.AddAttribute(logger.FieldTraceId, job.GetTraceID())
			_ = nrTxn.AddAttribute(logger.FieldUserId, job.GetUserID())
			_ = nrTxn.AddAttribute("entityId", job.GetID())
			defer func() {
				_ = nrTxn.End()
			}()
			ctx = newrelic.NewContext(ctx, nrTxn)
			return next(ctx, job)
		}
	}
}

func NewRelicToGorm(dbConn *gorm.DB) func(next HandleFunc) HandleFunc {
	return func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, job Job) error {
			dbConn = nrcontext.SetTxnToGorm(ctx, dbConn)
			ctx = SetGormToContext(ctx, dbConn)
			return next(ctx, job)
		}
	}
}

func NewRelicGormWithTransaction(dbConn *gorm.DB) func(next HandleFunc) HandleFunc {
	return func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, job Job) error {
			txn := dbConn.Begin()
			if txn.Error != nil {
				return txn.Error
			}
			SetGormToContext(ctx, nrcontext.SetTxnToGorm(ctx, txn))

			err := next(ctx, job)
			if err != nil {
				return txn.Rollback().Error
			}

			return txn.Commit().Error
		}
	}
}

func NewRelicToRedis(c *redis.Client) func(next HandleFunc) HandleFunc {
	return func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, job Job) error {
			redisClientWithNR := nrcontext.WrapRedisClient(ctx, c)
			ctx = SetRedisClientToContext(ctx, redisClientWithNR)
			return next(ctx, job)
		}
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
