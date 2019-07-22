package redisqueue

import (
	"github.com/kelseyhightower/envconfig"
)

type RedisConfig struct {
	RedisMaster              string `envconfig:"REDIS_MASTER" required:"true"`
	SentinelHost             string `envconfig:"SENTINEL_HOST" required:"true"`
	SentinelPort             string `envconfig:"SENTINEL_PORT" required:"true"`
	RedisMaxActiveConnection int    `envconfig:"REDIS_MAX_ACTIVE" required:"false"`
	MaxIdle                  int    `envconfig:"REDIS_MAX_IDLE" required:"false"`
}

func (redisConfig *RedisConfig) GetSentinelAddress() string {
	return redisConfig.SentinelHost + ":" + redisConfig.SentinelPort
}

func GetConfigFromEnv() RedisConfig {
	var conf RedisConfig
	envconfig.MustProcess("", &conf)
	return conf
}
