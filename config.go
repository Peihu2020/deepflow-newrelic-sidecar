package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// NewRelic config
	NewRelicLicense   string
	NewRelicAccountID string
	NewRelicDebugMode bool

	// Sidecar config
	BatchSize        int
	FlushInterval    time.Duration
	LogLevel         string
	HTTPPort         string
	DebugDir         string
	SaveDebugData    bool
	MaxQueueSize     int
	WorkerCount      int
	RateLimitPerSec  int
	RateLimitBurst   int
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration

	// Log type filters
	EnableL7      bool
	EnableL4      bool
	EnableMetrics bool

	// HTTP mode config
	DeepFlowHTTPEndpoint string

	// Kafka Producer config
	KafkaProducerEnabled bool
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaProducerTimeout time.Duration

	// Kafka Consumer config
	KafkaConsumerEnabled bool
	KafkaConsumerGroup   string
	KafkaConsumerTopics  []string
	KafkaConsumerOffset  string
}

func LoadConfig() *Config {
	return &Config{
		NewRelicLicense:   getEnv("NEW_RELIC_LICENSE_KEY", ""),
		NewRelicAccountID: getEnv("NEW_RELIC_ACCOUNT_ID", ""),
		NewRelicDebugMode: getEnvBool("NEW_RELIC_DEBUG_MODE", false),

		BatchSize:        getEnvInt("BATCH_SIZE", 200),
		FlushInterval:    getEnvDuration("FLUSH_INTERVAL", 3*time.Second),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		HTTPPort:         getEnv("HTTP_PORT", "8080"),
		DebugDir:         getEnv("DEBUG_DIR", "./debug"),
		SaveDebugData:    getEnvBool("SAVE_DEBUG_DATA", true),
		MaxQueueSize:     getEnvInt("MAX_QUEUE_SIZE", 10000),
		WorkerCount:      getEnvInt("WORKER_COUNT", 4),
		RateLimitPerSec:  getEnvInt("RATE_LIMIT_PER_SEC", 20000),
		RateLimitBurst:   getEnvInt("RATE_LIMIT_BURST", 50000),
		HTTPReadTimeout:  getEnvDuration("HTTP_READ_TIMEOUT", 30*time.Second),
		HTTPWriteTimeout: getEnvDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),

		EnableL7:      getEnvBool("ENABLE_L7", true),
		EnableL4:      getEnvBool("ENABLE_L4", false),
		EnableMetrics: getEnvBool("ENABLE_METRICS", false),

		DeepFlowHTTPEndpoint: getEnv("DEEPFLOW_HTTP_ENDPOINT", "/api/deepflow-data"),

		// Kafka Producer
		KafkaProducerEnabled: getEnvBool("KAFKA_PRODUCER_ENABLED", false),
		KafkaBrokers:         strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:           getEnv("KAFKA_TOPIC", "deepflow-logs"),
		KafkaProducerTimeout: getEnvDuration("KAFKA_PRODUCER_TIMEOUT", 5*time.Second),

		// Kafka Consumer
		KafkaConsumerEnabled: getEnvBool("KAFKA_CONSUMER_ENABLED", true),
		KafkaConsumerGroup:   getEnv("KAFKA_CONSUMER_GROUP", "deepflow-sidecar"),
		KafkaConsumerTopics:  strings.Split(getEnv("KAFKA_CONSUMER_TOPICS", "deepflow-logs"), ","),
		KafkaConsumerOffset:  getEnv("KAFKA_CONSUMER_OFFSET", "oldest"),
	}
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return strings.ToLower(value) == "true" || value == "1"
	}
	return defaultValue
}
