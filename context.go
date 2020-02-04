package redisqueue

import (
	"context"

	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
)

type contextKey int

const gormContextKey contextKey = 0
const redisContextKey contextKey = 0

func SetGormToContext(ctx context.Context, dbConn *gorm.DB) context.Context {
	return context.WithValue(ctx, gormContextKey, dbConn)
}

func GetGormFromContext(ctx context.Context) *gorm.DB {
	if db := ctx.Value(gormContextKey); db != nil {
		return db.(*gorm.DB)
	}
	panic(ErrDbEmpty)
}

func SetRedisClientToContext(ctx context.Context, c *redis.Client) context.Context {
	return context.WithValue(ctx, redisContextKey, c)
}

func GetRedisClientFromContext(ctx context.Context) *redis.Client {
	if db := ctx.Value(redisContextKey); db != nil {
		return db.(*redis.Client)
	}
	panic(ErrRedisConnectionEmpty)
}
