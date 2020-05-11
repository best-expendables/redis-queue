package redisqueue

import (
	"github.com/best-expendables/logger"
	"context"
)

const (
	FieldJobPayload = "job"
)

type FailHandler func(ctx context.Context, j Job, err error)

func LogFailedJob(ctx context.Context, j Job, err error) {
	meta := logger.Fields{
		"source": "ConsumerFailedLogger",
		"job":    getJobStringFormat(j),
		"error":  err.Error(),
	}

	logger.EntryFromContext(ctx).WithFields(meta).Info("Log failed job")
}
