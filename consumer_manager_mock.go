package redisqueue

import (
	"github.com/best-expendables/rmq"
	"time"
)

type ConsumerManagerMock struct {
	AddFn            func(queueName string, consumer rmq.Consumer)
	StartConsumingFn func(queueName string, replicas int, pollDuration time.Duration)
	StopConsumingFn  func(queueName string)
}

func (p ConsumerManagerMock) Add(queueName string, consumer rmq.Consumer) {
	p.AddFn(queueName, consumer)
}

func (p ConsumerManagerMock) StartConsuming(queueName string, replicas int, pollDuration time.Duration) {
	p.StartConsumingFn(queueName, replicas, pollDuration)
}

func (p ConsumerManagerMock) StopConsuming(queueName string) {
	p.StopConsumingFn(queueName)
}
