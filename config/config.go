package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka    KafkaConfig    `yaml:"kafka"`
	Pipeline PipelineConfig `yaml:"pipeline"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Health   HealthConfig   `yaml:"health"`
}

type KafkaConfig struct {
	Brokers       []string      `yaml:"brokers"`
	ConsumerGroup string        `yaml:"consumer_group"`
	InputTopics   []string      `yaml:"input_topics"`
	OutputTopic   string        `yaml:"output_topic"`
	DLQTopic      string        `yaml:"dlq_topic"`
	MaxRetries    int           `yaml:"max_retries"`
	RetryBackoff  time.Duration `yaml:"retry_backoff"`
}

type PipelineConfig struct {
	WindowSize    time.Duration `yaml:"window_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	BatchSize     int           `yaml:"batch_size"`
	Workers       int           `yaml:"workers"`
}

type MetricsConfig struct {
	Port int `yaml:"port"`
}

type HealthConfig struct {
	Port int `yaml:"port"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func Default() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			ConsumerGroup: "stream-processor",
			InputTopics:   []string{"user-events"},
			OutputTopic:   "aggregated-stats",
			DLQTopic:      "dead-letter",
			MaxRetries:    3,
			RetryBackoff:  100 * time.Millisecond,
		},
		Pipeline: PipelineConfig{
			WindowSize:    1 * time.Minute,
			FlushInterval: 10 * time.Second,
			BatchSize:     100,
			Workers:       4,
		},
		Metrics: MetricsConfig{Port: 9090},
		Health:  HealthConfig{Port: 8081},
	}
}
