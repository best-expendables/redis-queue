package main

import (
	"context"
	"errors"
	"fmt"
	redisqueue "redis-queue"
	sample "redis-queue/examples"
	"time"
)

var (
	ErrToBeRetried = errors.New("retry on me")
)

type SimpleHandler struct {
	redisqueue.BaseHandler
}

func (handler *SimpleHandler) Handle(ctx context.Context, job redisqueue.Job) error {
	simpleJob, ok := job.(*sample.SimpleJob)
	if !ok {
		return redisqueue.ErrorInValidJobModel
	}

	fmt.Printf("Job: %s process: %d time\n", simpleJob.GetID(), simpleJob.GetAttempts())
	time.Sleep(10 * time.Second)
	fmt.Printf("Job: %s finished\n\n", simpleJob.GetID())

	return nil
}

func (handler *SimpleHandler) ShouldRetryOnError(err error) bool {
	if err == ErrToBeRetried {
		return true
	}

	return false
}

func main() {

	conf := &redisqueue.RedisConfig{
		RedisMaster:              "mymaster",
		SentinelHost:             "localhost",
		SentinelPort:             "26379",
		RedisMaxActiveConnection: 1,
		MaxIdle:                  1,
	}

	//Create consumer manager
	cm, err := redisqueue.NewConsumerManagerFromConfig(conf)
	panicOnError(err)

	//Create 1 default publisher for handlers
	publisher, err := redisqueue.NewPublisherFromConfig(conf)
	panicOnError(err)

	//Create handler for job
	handler := &SimpleHandler{}

	panicOnError(err)

	//Add handler to consumer manager
	cm.Add(sample.SimpleQueue, redisqueue.MakeConsumer(
		func() redisqueue.Job { return &sample.SimpleJob{} },
		redisqueue.HandlerFactory(func(ctx context.Context) redisqueue.Handler {
			return handler
		}),
		publisher,
		[]redisqueue.FailHandler{redisqueue.LogFailedJob},
		[]redisqueue.HandlerMiddleWare{
			redisqueue.LogJob,
			//redisqueue.NewRelicToGorm(gorm.Db)
		},
	),
	)

	//Start consuming on the Queue with 1 replicas and the poll duration is 1 second
	go cm.StartConsuming(sample.SimpleQueue, 3, time.Second)

	//Stop consuming on queue
	//cm.StopConsuming(sample.SimpleQueue)

	fmt.Println("Done")

	//Keep the process continue otherwise consumer will be killed
	select {}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
