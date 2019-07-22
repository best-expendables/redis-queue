package redisqueue

import (
	"fmt"
	"time"

	"bitbucket.org/snapmartinc/rmq"
	"github.com/go-redis/redis"
)

type ConsumerManager interface {
	Add(queueName string, consumer rmq.Consumer)
	StartConsuming(queueName string, replicas int, pollDuration time.Duration)
	StopConsuming(queueName string)
}

type consumerManager struct {
	conn      rmq.Connection
	consumers map[string]rmq.Consumer
	queues    map[string]rmq.Queue
}

func init() {
	//Turn off go-redis log
	redis.SetLogger(nil)
}

// NewConsumerManager returns a ConsumerManager
func NewConsumerManager() (ConsumerManager, error) {
	conf := GetConfigFromEnv()
	return NewConsumerManagerFromConfig(&conf)
}

func NewConsumerManagerFromConfig(conf *RedisConfig) (ConsumerManager, error) {
	conn, err := NewRmqConnFromRedisConfig(conf)
	if err != nil {
		return nil, err
	}

	return &consumerManager{
			conn:      conn,
			consumers: make(map[string]rmq.Consumer),
			queues:    make(map[string]rmq.Queue),
		},
		nil
}

func NewConsumerManagerWithConnection(conn rmq.Connection) ConsumerManager {
	return &consumerManager{
		conn:      conn,
		consumers: make(map[string]rmq.Consumer),
		queues:    make(map[string]rmq.Queue),
	}
}

// NewConsumerManager.Add adds consumer to the passed queue
func (cm *consumerManager) Add(queueName string, consumer rmq.Consumer) {
	cm.consumers[queueName] = consumer
}

// NewConsumerManager.StartConsuming starts consuming the passed queueName
// replicas indicates how many consumers will be added
// pollDuration indicates how long should consumers wait before checking for tasks in the queue
func (cm *consumerManager) StartConsuming(queueName string, replicas int, pollDuration time.Duration) {
	consumer, exists := cm.consumers[queueName]
	if !exists {
		panic(fmt.Errorf("there is no consumer for queue `%s`", queueName))
	}

	cm.queues[queueName] = cm.conn.OpenQueue(queueName)

	//Open failed queue then we can call delivery.Push() to push to it
	//Jobs in failed queue can be retried if needed
	//Jobs in rejected queue consider to be deleted
	failedQueue := cm.conn.OpenQueue(fmt.Sprintf("%s_failed", queueName))
	cm.queues[queueName].SetPushQueue(failedQueue)

	prefetchLimit := replicas + 1
	cm.queues[queueName].StartConsuming(prefetchLimit, pollDuration)

	consumerName := fmt.Sprintf("%s_consumer", queueName)
	for i := 0; i < replicas; i++ {
		cm.queues[queueName].AddConsumer(fmt.Sprintf("%v_%d", consumerName, i), consumer)
	}
}

// NewConsumerManager.StopConsuming stops consume on the queue
func (cm *consumerManager) StopConsuming(queueName string) {
	cm.queues[queueName].StopConsuming()
}
