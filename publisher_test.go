package redisqueue_test

import (
	"github.com/best-expendables/rmq"
	redisqueue "redis-queue"
	"testing"
	"time"
)

var connectionTest *rmq.TestConnection
var publisherTest redisqueue.Publisher

func setupPublisherTestCase(t *testing.T) func(t *testing.T) {
	temp := rmq.NewTestConnection()
	connectionTest = &temp
	var err error
	publisherTest, err = redisqueue.NewPublisherWithConnection(connectionTest)
	if err != nil {
		panic(err)
	}

	return func(t *testing.T) {
		connectionTest = nil
		publisherTest = nil
	}
}

func TestPublisher_Publish(t *testing.T) {
	teardownPublisherTestCase := setupPublisherTestCase(t)
	defer teardownPublisherTestCase(t)

	testJob := redisqueue.RedisJob{
		ID: "ID123",
	}
	publisherTest.Publish("test_queue", &testJob)

	if connectionTest.GetDelivery("test_queue", 0) == "" {
		t.Error("should get job data")
	}
}

func TestPublisher_PublishOnDelay(t *testing.T) {
	teardownPublisherTestCase := setupPublisherTestCase(t)
	defer teardownPublisherTestCase(t)

	testJob := redisqueue.RedisJob{}
	publisherTest.PublishOnDelay(
		"test_queue",
		&testJob,
		time.Now().Add(20*time.Second),
	)

	if connectionTest.GetDelivery("test_queue", 0) == "" {
		t.Error("should get job data")
	}
}

func TestPublisher_PublishRejected(t *testing.T) {
	teardownPublisherTestCase := setupPublisherTestCase(t)
	defer teardownPublisherTestCase(t)

	testJob := redisqueue.RedisJob{}
	testJob.OnQueue("test_queue")
	publisherTest.PublishRejected(&testJob)

	if connectionTest.GetDelivery("test_queue", 0) == "" {
		t.Error("should get job data")
	}
}
