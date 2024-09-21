package configuration

type Config struct {
	Workdir           string 			`json:"workdir"`
	MaxWorkers        int    			`json:"maxWorkers"`
	FlowCheckInterval int    			`json:"flowCheckInterval"`
	FlowBatchSize     int   			`json:"flowBatchSize"`
	BackOff			  time.Duration   	`json:"Backoff"`
	MaxAttempts       int	 			`json:"MaxAttempts"`
}
