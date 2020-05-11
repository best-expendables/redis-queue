package redisqueue

import (
	"github.com/best-expendables/rmq"
	"github.com/go-redis/redis/v8"
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
	if redisConfig.SentinelPort == "26379" {
		options := redis.FailoverOptions{
			MasterName:         redisConfig.RedisMaster,
			SentinelAddrs:      []string{redisConfig.GetSentinelAddress()},
			MaxRetries:         10,
			DialTimeout:        time.Minute,
			ReadTimeout:        time.Minute,
			WriteTimeout:       time.Minute,
			PoolSize:           1000,
			PoolTimeout:        time.Minute,
			IdleTimeout:        time.Minute,
			IdleCheckFrequency: time.Second * 10,
		}
		redisClient := redis.NewFailoverClient(&options)

		return NewRmqConn(redisClient)
	}

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
