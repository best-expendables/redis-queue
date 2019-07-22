package redisqueue

import "time"

type PublisherMock struct {
	PublishFn         func(queue string, job Job) error
	PublishOnDelayFn  func(queue string, job Job, delayAt time.Time) error
	PublishRejectedFn func(job Job) error
}

func (p PublisherMock) Publish(queue string, job Job) error {
	return p.PublishFn(queue, job)
}

func (p PublisherMock) PublishOnDelay(queue string, job Job, delayAt time.Time) error {
	return p.PublishOnDelayFn(queue, job, delayAt)
}

func (p PublisherMock) PublishRejected(job Job) error {
	return p.PublishRejectedFn(job)
}
