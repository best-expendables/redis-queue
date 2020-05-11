package redisqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/best-expendables/rmq"
)

var (
	emptyQueueOnJob       = errors.New("job's queue is empty")
	emptyDelayedTimeOnJob = errors.New("job's delay time is not set")
)

type Publisher interface {
	Publish(ctx context.Context, job Job) error
	PublishOnDelay(ctx context.Context, job Job) error
	PublishRejected(ctx context.Context, job Job) error
	UseMiddlewares(m ...PublisherHandlerMiddleWare)
}

type publisher struct {
	rmqConn     rmq.Connection
	middleWares []PublisherHandlerMiddleWare
}

func NewPublisher() (Publisher, error) {
	conf := GetConfigFromEnv()
	return NewPublisherFromConfig(&conf)
}

func NewPublisherWithConnection(conn rmq.Connection) (Publisher, error) {
	return &publisher{rmqConn: conn, middleWares: make([]PublisherHandlerMiddleWare, 0)}, nil
}

func NewPublisherFromConfig(conf *RedisConfig) (Publisher, error) {
	rmqConn, err := NewRmqConnFromRedisConfig(conf)
	if err != nil {
		return nil, err
	}
	return &publisher{rmqConn: rmqConn, middleWares: make([]PublisherHandlerMiddleWare, 0)}, nil
}

// Publish a Payload to the passed Queue
func (p *publisher) UseMiddlewares(m ...PublisherHandlerMiddleWare) {
	p.middleWares = append(p.middleWares, m...)
}

// Publish a Payload to the passed Queue
func (p *publisher) Publish(ctx context.Context, job Job) error {
	processFunc := func(ctx context.Context, job Job) error {
		if job.GetQueue() == "" {
			return emptyQueueOnJob
		}
		payload, err := p.encodeJob(job)
		if err != nil {
			return err
		}
		ok := p.rmqConn.OpenQueue(job.GetQueue()).Publish(payload)
		if !ok {
			return fmt.Errorf("could not publish payloads to `%s` Queue", job.GetQueue())
		}
		return nil
	}
	return p.processWithMiddleWare(ctx, job, processFunc)
}

// Publish a Payload to the passed Queue after the delayAt time
func (p *publisher) PublishOnDelay(ctx context.Context, job Job) error {
	processFunc := func(ctx context.Context, job Job) error {
		if job.GetQueue() == "" {
			return emptyQueueOnJob
		}
		if job.GetQueue() == "" {
			return emptyDelayedTimeOnJob
		}
		payload, err := p.encodeJob(job)
		if err != nil {
			return err
		}
		delayedTo := time.Now().Add(time.Duration(job.Delay()) * time.Second)
		if ok := p.rmqConn.OpenQueue(job.GetQueue()).PublishOnDelay(payload, delayedTo); !ok {
			return fmt.Errorf("could not publish payloads to `%s` Queue", job.GetQueue())
		}
		return nil
	}
	return p.processWithMiddleWare(ctx, job, processFunc)
}

func (p *publisher) PublishRejected(ctx context.Context, job Job) error {
	processFunc := func(ctx context.Context, job Job) error {
		if job.GetQueue() == "" {
			return emptyQueueOnJob
		}
		payload, err := p.encodeJob(job)
		if err != nil {
			return err
		}
		if ok := p.rmqConn.OpenQueue(job.GetQueue()).PublishRejected(payload); !ok {
			return fmt.Errorf("could not publish payloads to rejected queue")
		}
		return nil
	}
	return p.processWithMiddleWare(ctx, job, processFunc)
}

func (p *publisher) processWithMiddleWare(ctx context.Context, job Job, handlerFunc PublisherHandlerFunc) error {
	if p.middleWares == nil || len(p.middleWares) == 0 {
		return handlerFunc(ctx, job)
	}
	h := handlerFunc
	for i := len(p.middleWares) - 1; i >= 0; i-- {
		h = p.middleWares[i](h)
	}
	return h(ctx, job)
}

func (p *publisher) encodeJob(job Job) (string, error) {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return "", err
	}

	return string(jobBytes), nil
}
