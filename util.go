package redisqueue

import "encoding/json"

func getJobStringFormat(j Job) string {
	data, _ := json.Marshal(j)

	return string(data)
}
