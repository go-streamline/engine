package config

type Config struct {
	Workdir    string `json:"workdir"`
	MaxWorkers int    `json:"maxWorkers"`
}
