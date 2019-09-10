package redisqueue

import (
	"bitbucket.org/snapmartinc/logger"
	"bitbucket.org/snapmartinc/rmq"
	"bitbucket.org/snapmartinc/trace"
	"bitbucket.org/snapmartinc/user-service-client"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

type defaultConsumer struct {
	jobFactory      JobFactory
	handlerFactory  HandlerFactory
	publisher       Publisher
	failJobHandlers []FailHandler
	middlewareList  []HandlerMiddleWare
}

func MakeConsumer(
	jobFactory JobFactory,
	handlerFactory HandlerFactory,
	publisher Publisher,
	failJobHandlers []FailHandler,
	middlewareList []HandlerMiddleWare,
) *defaultConsumer {
	return &defaultConsumer{
		jobFactory:      jobFactory,
		handlerFactory:  handlerFactory,
		publisher:       publisher,
		failJobHandlers: failJobHandlers,
		middlewareList:  middlewareList,
	}
}

func (consumer *defaultConsumer) Consume(delivery rmq.Delivery) {
	payload := delivery.Payload()
	job := consumer.jobFactory()
	if err := json.Unmarshal([]byte(payload), job); err != nil {
		consumer.failDeliveryWithNoJob(delivery, err)
		return
	}

	ctx := contextFromJob(job)
	var handlerInstance Handler

	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic: %v; stack trace: %v", r, debug.Stack())
			consumer.recoverPanic(errMsg, handlerInstance, ctx, job, delivery)
		}
	}()

	defaultHandlerFunc := HandleFunc(func(ctx context.Context, j Job) error {
		handlerInstance = consumer.handlerFactory(ctx)

		return handlerInstance.Handle(ctx, j)
	})

	if job.GetAttempts() < job.GetMaxTries() {
		handleFunc := chainHandlerMiddleWares(consumer.middlewareList, defaultHandlerFunc)
		job.Attempt()
		err := handleFunc(ctx, job)
		if err == nil {
			delivery.Ack()
		} else {
			if handlerInstance.ShouldRetryOnError(err) {
				job.Retry(err)
				timeToProcess := consumer.getTimeToProcessJob(job.Delay())
				consumer.publisher.PublishOnDelay(job.GetQueue(), job, timeToProcess)
				//Need to call Ack() on delivery to remove the delivery from unacked queue
				//Because job will be repushed to redis again, current processing job need to be deleted
				delivery.Ack()
			} else {
				consumer.failDelivery(ctx, handlerInstance, job, delivery, err)
			}
		}
	} else {
		consumer.failDelivery(ctx, handlerInstance, job, delivery, ErrJobExceedRetryTimes)
	}
}

func (consumer *defaultConsumer) getTimeToProcessJob(timeDurationInSecond int) time.Time {
	return time.Now().Add(time.Duration(timeDurationInSecond) * time.Second)
}

func (consumer *defaultConsumer) failDelivery(ctx context.Context, h Handler, job Job, delivery rmq.Delivery, err error) {
	job.Fail(err)

	for _, failHandler := range consumer.failJobHandlers {
		failHandler(ctx, job, err)
	}

	// Handler can be nil when attempts > max_retries
	// In that case, the job will be push to rejected queue
	if h != nil {
		if h.ShouldRejectOnFailure(err) {
			consumer.publisher.PublishRejected(job)
		}
	} else {
		consumer.publisher.PublishRejected(job)
	}

	//Delete current processing job from redis
	delivery.Ack()
}

func (consumer *defaultConsumer) failDeliveryWithNoJob(delivery rmq.Delivery, err error) {
	delivery.Reject()
	meta := logger.Fields{
		"source":          "ConsumerJobDecodeFailedLogger",
		"deliveryPayload": delivery.Payload(),
		"error":           err.Error(),
	}

	logger.WithFields(meta).Info("Log failed job")
}

func contextFromJob(j Job) context.Context {
	ctx := trace.ContextWithRequestID(context.Background(), j.GetTraceID())
	ctx = userclient.ContextWithUser(ctx, &userclient.User{Id: j.GetUserID()})
	loggerFactory := logger.NewLoggerFactory(logger.InfoLevel)

	return logger.ContextWithEntry(loggerFactory.Logger(ctx), ctx)
}

func (consumer *defaultConsumer) recoverPanic(r interface{}, h Handler, ctx context.Context, job Job, delivery rmq.Delivery) {
	switch val := r.(type) {
	case string:
		consumer.failDelivery(ctx, h, job, delivery, errors.New(val))
	case error:
		consumer.failDelivery(ctx, h, job, delivery, val)
	default:
		consumer.failDelivery(ctx, h, job, delivery, ErrFailedWithUnknownData)
	}
}
