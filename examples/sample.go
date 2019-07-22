package sample

import (
	redisqueue "redis-queue"
)

const SimpleQueue = "simple_queue_name"

type SimpleJob struct {
	redisqueue.RedisJob

	Order Order
}

type Order struct {
	ID      int
	Address string
	Items   []OrderItem
}

type OrderItem struct {
	ItemName string
	Price    float64
}

func NewSimpleJob() SimpleJob {
	order := Order{
		ID:      1,
		Address: "Customer order address",
		Items: []OrderItem{
			{
				ItemName: "Item 1",
				Price:    10.3,
			},
			{
				ItemName: "Item 2",
				Price:    1.13,
			},
			{
				ItemName: "Item 3",
				Price:    445.3,
			},
		},
	}

	return SimpleJob{
		RedisJob: *redisqueue.NewRedisJob("userId", "traceId"),
		Order:    order,
	}
}

//Override get delay time to retry
func (job SimpleJob) Delay() int {
	return job.GetAttempts() * 2
}

//Set max retry times to 10
func (job SimpleJob) GetMaxTries() int {
	return 10
}
