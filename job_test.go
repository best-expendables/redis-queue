package redisqueue_test

import (
	"errors"
	redisqueue "redis-queue"
	"testing"
)

var jobTest redisqueue.Job

func init() {
	jobTest = redisqueue.NewRedisJob("user_id", "trace_id")
}

func TestRedisJob_GetID(t *testing.T) {
	if jobTest.GetID() == "" {
		t.Error("should set id correctly")
	}
}

func TestRedisJob_GetQueue(t *testing.T) {
	jobTest.OnQueue("test_queue")

	if jobTest.GetQueue() != "test_queue" {
		t.Error("should get correct queue")
	}
}

func TestRedisJob_Attempts(t *testing.T) {
	jobTest.Attempt()
	jobTest.Attempt()

	if jobTest.GetAttempts() != 2 {
		t.Error("should get correct attempt times")
	}
}

func TestRedisJob_HasFailed(t *testing.T) {
	jobTest.Fail(errors.New("test error"))

	if jobTest.HasFailed() != true {
		t.Error("job should be failed")
	}
}

func TestRedisJob_GetMaxTries(t *testing.T) {
	if jobTest.GetMaxTries() != 2 {
		t.Error("should get default retry time as 2")
	}
}

func TestRedisJob_Delay(t *testing.T) {
	if jobTest.Delay() != 60 {
		t.Error("should delay 60 seconds by default")
	}
}

func TestRedisJob_GetUserID(t *testing.T) {
	if jobTest.GetUserID() != "user_id" {
		t.Error("should get user id error")
	}
}

func TestRedisJob_GetTraceID(t *testing.T) {
	if jobTest.GetTraceID() != "trace_id" {
		t.Error("should get trace id")
	}
}

func TestRedisJob_GetFailedError(t *testing.T) {
	jobTest.Fail(errors.New("test error"))

	if jobTest.GetFailedError() != "test error" {
		t.Error("should get correct failed error")
	}
}

func TestRedisJob_OnQueue(t *testing.T) {
	jobTest.OnQueue("queue_name")

	if jobTest.GetQueue() != "queue_name" {
		t.Error("should be on correct queue")
	}
}

func TestRedisJob_Retry(t *testing.T) {
	jobTest.Retry(errors.New("test error"))
}
