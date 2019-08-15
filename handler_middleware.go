package redisqueue

import (
	"context"

	"bitbucket.org/snapmartinc/logger"
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

// func Gorm(dbConn *gorm.DB) func(next HandleFunc) HandleFunc {
// 	return func(next HandleFunc) HandleFunc {
// 		return func(ctx context.Context, job Job) error {
// 			dbConn = nrcontext.SetTxnToGorm(ctx, dbConn)
// 			ctx = SetGormToContext(ctx, dbConn)

// 			return next(ctx, job)
// 		}
// 	}
// }

// func WithTransaction(dbConn *gorm.DB) func(next HandleFunc) HandleFunc {
// 	return func(next HandleFunc) HandleFunc {
// 		return func(ctx context.Context, job Job) error {
// 			txn := dbConn.Begin()
// 			if txn.Error != nil {
// 				return txn.Error
// 			}
// 			SetGormToContext(ctx, nrcontext.SetTxnToGorm(ctx, txn))

// 			err := next(ctx, job)
// 			if err != nil {
// 				return txn.Rollback().Error
// 			}

// 			return txn.Commit().Error
// 		}
// 	}
// }

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
