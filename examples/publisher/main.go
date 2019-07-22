package main

import (
	"fmt"
	redisqueue "redis-queue"
	sample "redis-queue/examples"
)

func main() {
	conf := &redisqueue.RedisConfig{
		RedisMaster:              "mymaster",
		SentinelHost:             "127.0.0.1",
		SentinelPort:             "26379",
		RedisMaxActiveConnection: 1,
		MaxIdle:                  1,
	}

	publisher, err := redisqueue.NewPublisherFromConfig(conf)
	panicOnError(err)

	for i := 0; i < 9; i++ {
		testJob := sample.NewSimpleJob()
		err = publisher.Publish(sample.SimpleQueue, &testJob)
		fmt.Printf("Publish job id: %s\n", testJob.GetID())
		panicOnError(err)
	}

	// Or set set time to push after 20 seconds
	//err = publisher.PublishOnDelay(
	//	sample.SimpleQueue,
	//	&testJob,
	//	time.Now().Add(20*time.Second),
	//)
	fmt.Println("Done")
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
