package redisqueue

import "errors"

var (
	ErrFailedWithUnknownData = errors.New("consume failed with unknown data")
	ErrJobExceedRetryTimes   = errors.New("job exceeds retry times")
	ErrorInValidJobModel     = errors.New("invalid job struct")
	ErrDbEmpty               = errors.New("db connection invalid")
	ErrRedisConnectionEmpty  = errors.New("Redis connection invalid")
)
