package configuration

type Config struct {
	Workdir           string `json:"workdir"`
	MaxWorkers        int    `json:"maxWorkers"`
	FlowCheckInterval int    `json:"flowCheckInterval"`
	FlowBatchSize     int    `json:"flowBatchSize"`
	MaxRetries        int    `json:"maxRetries"`
	Backoff           int    `json:"backoff"`
}
