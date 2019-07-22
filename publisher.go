package redisqueue

import (
	"bitbucket.org/snapmartinc/rmq"
	"encoding/json"
	"fmt"
	"time"
)

type Publisher interface {
	Publish(queue string, job Job) error
	PublishOnDelay(queue string, job Job, delayAt time.Time) error
	PublishRejected(job Job) error
}

type publisher struct {
	rmqConn rmq.Connection
}

func NewPublisher() (Publisher, error) {
	conf := GetConfigFromEnv()
	return NewPublisherFromConfig(&conf)
}

func NewPublisherWithConnection(conn rmq.Connection) (Publisher, error) {
	return &publisher{conn}, nil
}

func NewPublisherFromConfig(conf *RedisConfig) (Publisher, error) {
	rmqConn, err := NewRmqConnFromRedisConfig(conf)
	if err != nil {
		return nil, err
	}

	return &publisher{rmqConn}, nil
}

// Publish a Payload to the passed Queue
func (p *publisher) Publish(queue string, job Job) error {
	job.OnQueue(queue)

	payload, err := p.encodeJob(job)
	if err != nil {
		return err
	}

	ok := p.rmqConn.OpenQueue(queue).Publish(string(payload))
	if !ok {
		return fmt.Errorf("could not publish payloads to `%s` Queue", queue)
	}

	return nil
}

// Publish a Payload to the passed Queue after the delayAt time
func (p *publisher) PublishOnDelay(queue string, job Job, delayAt time.Time) error {
	job.OnQueue(queue)

	payload, err := p.encodeJob(job)
	if err != nil {
		return err
	}

	ok := p.rmqConn.OpenQueue(queue).PublishOnDelay(payload, delayAt)
	if !ok {
		return fmt.Errorf("could not publish payloads to `%s` Queue", queue)
	}

	return nil
}

func (p *publisher) PublishRejected(job Job) error {
	payload, err := p.encodeJob(job)
	if err != nil {
		return err
	}

	ok := p.rmqConn.
		OpenQueue(job.GetQueue()).
		PublishRejected(string(payload))
	if !ok {
		return fmt.Errorf("could not publish payloads to rejected queue")
	}

	return nil
}

func (p *publisher) encodeJob(job Job) (string, error) {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return "", err
	}

	return string(jobBytes), nil
}
