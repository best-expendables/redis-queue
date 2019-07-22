package redisqueue_test

import (
	"context"
	"errors"
	redisqueue "redis-queue"
	"testing"
)

var baseHandler redisqueue.Handler

func setupHandlerTestCase(t *testing.T) func(t *testing.T) {
	baseHandler = &redisqueue.BaseHandler{}

	return func(t *testing.T) {
		baseHandler = nil
	}
}

func TestBaseHandler_Handle(t *testing.T) {
	tearDownHandlerTestCase := setupHandlerTestCase(t)
	defer tearDownHandlerTestCase(t)

	j := redisqueue.RedisJob{}

	err := baseHandler.Handle(context.TODO(), &j)

	if err != nil {
		t.Error("expect no error")
	}
}

func TestBaseHandler_ShouldRetryOnError(t *testing.T) {
	tearDownHandlerTestCase := setupHandlerTestCase(t)
	defer tearDownHandlerTestCase(t)

	err := errors.New("test error")

	result := baseHandler.ShouldRetryOnError(err)

	if result != false {
		t.Error("by default handler does not retry on any errors")
	}
}
