package redisqueue_test

import (
	"bitbucket.org/snapmartinc/rmq"
	"context"
	"encoding/json"
	"errors"
	redisqueue "redis-queue"
	"testing"
	"time"
)

var handler *redisqueue.HandlerMock
var publisher *redisqueue.PublisherMock
var consumer rmq.Consumer
var mockFailHandler *redisqueue.FailHandler

func setUpConsumerTestCase(_ *testing.T) func(t *testing.T) {
	handler = &redisqueue.HandlerMock{}
	publisher = &redisqueue.PublisherMock{}
	tempFailHandler := redisqueue.FailHandler(func(ctx context.Context, j redisqueue.Job, err error) {})
	mockFailHandler = &tempFailHandler

	consumer = redisqueue.MakeConsumer(
		func() redisqueue.Job { return &redisqueue.RedisJob{} },
		redisqueue.HandlerFactory(func(ctx context.Context) redisqueue.Handler {
			return handler
		}),
		publisher,
		[]redisqueue.FailHandler{
			redisqueue.LogFailedJob,
			*mockFailHandler,
		},
		[]redisqueue.HandlerMiddleWare{
			redisqueue.LogJob,
		},
	)

	return func(t *testing.T) {
		handler = nil
		publisher = nil
		consumer = nil
		mockFailHandler = nil
	}
}

func TestConsumeSuccess(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	job := *redisqueue.NewRedisJob("user_id_here", "trace_id_here")
	jobBytes, _ := json.Marshal(job)
	delivery := rmq.NewTestDeliveryString(string(jobBytes))

	handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
		if job == nil {
			t.Error("should get job data")
		}

		return nil
	}

	consumer.Consume(delivery)

	if delivery.State != rmq.Acked {
		t.Error("job was processed then delivery should be acked")
	}
}

func TestConsumeWhenJobCanNotBeDecoded(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	delivery := rmq.NewTestDeliveryString(string("this is invalid json string"))

	consumer.Consume(delivery)

	if delivery.State != rmq.Rejected {
		t.Error("delivery should be rejected")
	}
}

func TestConsumeSuccessAfterRetrying(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	job := &redisqueue.RedisJob{}

	jobBytes, _ := json.Marshal(job)
	delivery := rmq.NewTestDeliveryString(string(jobBytes))
	const retryTimes = 3
	count := 1

	errToBeRetried := errors.New("should retry on me")

	handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
		//fail 3 times
		if count <= retryTimes {
			count++
			return errToBeRetried
		}

		//the 4th run -> handle succeeded
		return nil
	}
	handler.ShouldRetryOnErrorFn = func(err error) bool {
		if err == errToBeRetried {
			return true
		}

		return false
	}
	tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
		t.Error("this method should not be called")
	})
	mockFailHandler = &tempMockFailHandler
	publisher.PublishOnDelayFn = func(queue string, job redisqueue.Job, delayAt time.Time) error {
		return nil
	}

	consumer.Consume(delivery)

	if delivery.State != rmq.Acked {
		t.Error("delivery should be unacked")
	}
}

func TestConsumeFailedWithoutRetrying(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	t.Run("attempts > max retries", func(t *testing.T) {
		job := &redisqueue.RedisJob{}
		jobBytes, _ := json.Marshal(job)
		delivery := rmq.NewTestDeliveryString(string(jobBytes))

		errToBeRetried := errors.New("should retry on me")
		errNotRetried := errors.New("should not retry on me")

		handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
			return errNotRetried
		}
		handler.ShouldRetryOnErrorFn = func(err error) bool {
			if err == errToBeRetried {
				return true
			}

			return false
		}
		handler.ShouldRejectOnFailureFn = func(err error) bool {
			return true
		}
		tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
			if job == nil {
				t.Error("should fail with job data")
			}

			if err != errNotRetried {
				t.Error("should handle fail with correct error")
			}
		})
		mockFailHandler = &tempMockFailHandler
		publisher.PublishRejectedFn = func(job redisqueue.Job) error {
			return nil
		}

		consumer.Consume(delivery)
	})

	t.Run("attempts < max retries", func(t *testing.T) {
		job := &redisqueue.RedisJob{
			Attempts: 3,
		}
		jobBytes, _ := json.Marshal(job)
		delivery := rmq.NewTestDeliveryString(string(jobBytes))

		errNotRetried := errors.New("should not retry on me")

		handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
			return errNotRetried
		}
		handler.ShouldRetryOnErrorFn = func(err error) bool {
			t.Error("shoud not check to retry")
			return false
		}
		tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
			if job == nil {
				t.Error("should fail with job data")
			}

			if err != redisqueue.ErrJobExceedRetryTimes {
				t.Error("should handle fail with correct error")
			}
		})
		mockFailHandler = &tempMockFailHandler
		publisher.PublishRejectedFn = func(job redisqueue.Job) error {
			return nil
		}

		consumer.Consume(delivery)
	})
}

func TestConsumeHandlePanic(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	job := &redisqueue.RedisJob{}
	jobBytes, _ := json.Marshal(job)
	delivery := rmq.NewTestDeliveryString(string(jobBytes))

	handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
		panic("too bad, i'm panicking")
		return nil
	}
	handler.ShouldRetryOnErrorFn = func(err error) bool {
		t.Error("should not call me")
		return false
	}
	handler.ShouldRejectOnFailureFn = func(err error) bool {
		return true
	}
	tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
		if job == nil {
			t.Error("should fail with job data")
		}

		if err == nil {
			t.Error("should handle fail with error")
		}

		if err.Error() != "too bad, i'm panicking" {
			t.Error("invalid error messsage")
		}
	})
	mockFailHandler = &tempMockFailHandler
	publisher.PublishRejectedFn = func(job redisqueue.Job) error {
		return nil
	}

	consumer.Consume(delivery)
}

func TestConsumeHandlePanicError(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	job := &redisqueue.RedisJob{}
	jobBytes, _ := json.Marshal(job)
	delivery := rmq.NewTestDeliveryString(string(jobBytes))
	errorPanic := errors.New("panic error")

	handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
		panic(errorPanic)
		return nil
	}
	handler.ShouldRetryOnErrorFn = func(err error) bool {
		t.Error("should not call me")
		return false
	}
	handler.ShouldRejectOnFailureFn = func(err error) bool {
		return true
	}
	tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
		if job == nil {
			t.Error("should fail with job data")
		}
		if err == nil {
			t.Error("should handle fail with error")
		}
		if err != errorPanic {
			t.Error("invalid error")
		}
	})
	mockFailHandler = &tempMockFailHandler
	publisher.PublishRejectedFn = func(job redisqueue.Job) error {
		return nil
	}

	consumer.Consume(delivery)
}

func TestConsumeHandlePanicWithDataIsNotAStringer(t *testing.T) {
	teardownConsumerTestCase := setUpConsumerTestCase(t)
	defer teardownConsumerTestCase(t)

	job := &redisqueue.RedisJob{}
	jobBytes, _ := json.Marshal(job)
	delivery := rmq.NewTestDeliveryString(string(jobBytes))

	handler.HandleFn = func(ctx context.Context, job redisqueue.Job) error {
		panic(1234)
		return nil
	}
	handler.ShouldRetryOnErrorFn = func(err error) bool {
		t.Error("should not call me")
		return false
	}
	handler.ShouldRejectOnFailureFn = func(err error) bool {
		return true
	}
	tempMockFailHandler := redisqueue.FailHandler(func(ctx context.Context, job redisqueue.Job, err error) {
		if job == nil {
			t.Error("should fail with job data")
		}
		if err == nil {
			t.Error("should handle fail with error")
		}
		if err.Error() != "consume failed with unknown data" {
			t.Error("invalid error message")
		}
	})
	mockFailHandler = &tempMockFailHandler
	publisher.PublishRejectedFn = func(job redisqueue.Job) error {
		return nil
	}

	consumer.Consume(delivery)
}
