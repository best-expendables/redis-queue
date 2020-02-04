package redisqueue

import (
	"context"

	"github.com/jinzhu/gorm"
)

type contextKey int

const gormContextKey contextKey = 0

func SetGormToContext(ctx context.Context, dbConn *gorm.DB) context.Context {
	return context.WithValue(ctx, gormContextKey, dbConn)
}

func GetGormFromContext(ctx context.Context) *gorm.DB {
	if db := ctx.Value(gormContextKey); db != nil {
		return db.(*gorm.DB)
	}
	panic(ErrDbEmpty)
}
