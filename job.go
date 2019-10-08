package redisqueue

import (
	"github.com/gofrs/uuid"
)

const (
	defaultMaxRetries    = 2
	defaultDelayInSecond = 60
)

// Return a new job instance for consumer to decode payload json
type JobFactory func() Job

type Job interface {
	// Get job ID
	GetID() string
	// Get user who triggered job
	GetUserID() string
	// Get trace id of request which triggered job
	GetTraceID() string
	// Set Queue
	OnQueue(queueName string)
	// Increase number of attempt times
	Attempt()
	// Get the number of job's attempt times
	GetAttempts() int
	// Mark job as failed
	Fail(err error)
	// Retry on error
	Retry(err error)
	// Determine if the job has been marked as a failure.
	HasFailed() bool
	// Get the number of times to attempt a job. Default is 1.
	GetMaxTries() int
	// Get job's Queue name
	GetQueue() string
	// Get delay time time in second before the job is retried again
	Delay() int
	// Return error string why job failed
	GetFailedError() string
}

type RedisJob struct {
	ID       string
	UserID   string
	TraceID  string
	Queue    string
	Attempts int
	Failed   bool
	Error    string
}

func createUuid() string {
	u, err := uuid.NewV4()

	if err == nil {
		return u.String()
	}
	return ""
}

func NewRedisJob(userID, traceID string) *RedisJob {
	return &RedisJob{
		ID:       createUuid(),
		UserID:   userID,
		TraceID:  traceID,
		Queue:    "",
		Attempts: 0,
		Failed:   false,
	}
}

func (job *RedisJob) GetID() string {
	return job.ID
}

func (job *RedisJob) GetUserID() string {
	return job.UserID
}

func (job *RedisJob) GetTraceID() string {
	return job.TraceID
}

func (job *RedisJob) OnQueue(queue string) {
	job.Queue = queue
}

func (job *RedisJob) Attempt() {
	job.Attempts++
}

func (job *RedisJob) GetAttempts() int {
	return job.Attempts
}

func (job *RedisJob) Fail(err error) {
	job.Attempts = 0
	job.Error = err.Error()
	job.Failed = true
}

func (job *RedisJob) Retry(err error) {
	job.Error = err.Error()
}

func (job *RedisJob) HasFailed() bool {
	return job.Failed
}

func (job *RedisJob) GetMaxTries() int {
	return defaultMaxRetries
}

func (job *RedisJob) GetQueue() string {
	return job.Queue
}

func (job *RedisJob) Delay() int {
	return defaultDelayInSecond
}

func (job *RedisJob) GetFailedError() string {
	return job.Error
}
