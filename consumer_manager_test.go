package redisqueue_test

import (
	"github.com/best-expendables/rmq"
	redisqueue "redis-queue"
	"testing"
	"time"
)

func TestConsumerManager(t *testing.T) {
	testConnection := rmq.NewTestConnection()
	consumerManager := redisqueue.NewConsumerManagerWithConnection(testConnection)
	testConsumer := rmq.NewTestConsumer("test_consumer")

	consumerManager.Add("test_queue", testConsumer)
	consumerManager.StartConsuming("test_queue", 3, time.Second)
	consumerManager.StopConsuming("test_queue")
}

func TestConsumerManager_StartConsumingGetError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("should panic")
		}
	}()

	testConnection := rmq.NewTestConnection()
	consumerManager := redisqueue.NewConsumerManagerWithConnection(testConnection)

	consumerManager.StartConsuming("test_queue", 3, time.Second)
	consumerManager.StopConsuming("test_queue")
}
