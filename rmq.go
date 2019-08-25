package redisqueue

import (
	"bitbucket.org/snapmartinc/rmq"
	"github.com/go-redis/redis"
	"time"
)

// NewRmqConn returns a connection to RedisQueue with redisClient
func NewRmqConn(redisConn *redis.Client) (rmq.Connection, error) {
	connection := rmq.OpenConnectionWithRedisClient("redisQueue", redisConn)

	cleaner := rmq.NewCleaner(connection)
	if err := cleaner.Clean(); err != nil {
		return nil, err
	}
	return connection, nil
}

// NewRmqConnFromRedisConfig returns a connection to RedisQueue using redisConfig
func NewRmqConnFromRedisConfig(redisConfig *RedisConfig) (rmq.Connection, error) {
	options := redis.Options{
		Addr:               redisConfig.GetSentinelAddress(),
		MaxRetries:         10,
		DialTimeout:        time.Minute,
		ReadTimeout:        time.Minute,
		WriteTimeout:       time.Minute,
		PoolSize:           redisConfig.RedisMaxActiveConnection,
		PoolTimeout:        time.Minute,
		IdleTimeout:        time.Duration(redisConfig.MaxIdle) * time.Millisecond,
		IdleCheckFrequency: time.Second * 10,
	}

	redisClient := redis.NewClient(&options)

	return NewRmqConn(redisClient)
}
