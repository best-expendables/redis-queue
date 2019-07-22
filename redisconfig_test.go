package redisqueue_test

import (
	"os"
	redisqueue "redis-queue"
	"testing"
)

func TestRedisConfig_GetSentinelAddress(t *testing.T) {
	redisConfig := &redisqueue.RedisConfig{
		RedisMaster:              "master",
		SentinelHost:             "localhost",
		SentinelPort:             "1234",
		RedisMaxActiveConnection: 10000,
		MaxIdle:                  10,
	}

	if redisConfig.GetSentinelAddress() != "localhost:1234" {
		t.Error("expect localhost:1234 but got ", redisConfig.GetSentinelAddress())
	}
}

func TestGetConfigFromEnv(t *testing.T) {
	os.Setenv("REDIS_MASTER", "master")
	os.Setenv("SENTINEL_HOST", "localhost")
	os.Setenv("SENTINEL_PORT", "1234")
	os.Setenv("REDIS_MAX_ACTIVE", "10000")
	os.Setenv("REDIS_MAX_IDLE", "10")

	config := redisqueue.GetConfigFromEnv()

	expect := redisqueue.RedisConfig{
		RedisMaster:              "master",
		SentinelHost:             "localhost",
		SentinelPort:             "1234",
		RedisMaxActiveConnection: 10000,
		MaxIdle:                  10,
	}

	if config != expect {
		t.Error("should load config from env success")
	}
}
