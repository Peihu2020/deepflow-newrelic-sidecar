package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP 配置
	HTTPPort             string        `env:"HTTP_PORT" default:"8080"`
	HTTPReadTimeout      time.Duration `env:"HTTP_READ_TIMEOUT" default:"30s"`
	HTTPWriteTimeout     time.Duration `env:"HTTP_WRITE_TIMEOUT" default:"30s"`
	DeepFlowHTTPEndpoint string        `env:"DEEPFLOW_HTTP_ENDPOINT" default:"/api/deepflow-data"`

	// 处理配置
	BatchSize       int           `env:"BATCH_SIZE" default:"100"`
	FlushInterval   time.Duration `env:"FLUSH_INTERVAL" default:"3s"`
	WorkerCount     int           `env:"WORKER_COUNT" default:"4"`
	MaxQueueSize    int           `env:"MAX_QUEUE_SIZE" default:"10000"`
	RateLimitPerSec int           `env:"RATE_LIMIT_PER_SEC" default:"20000"`
	RateLimitBurst  int           `env:"RATE_LIMIT_BURST" default:"50000"`

	// 日志类型
	EnableL7      bool `env:"ENABLE_L7" default:"true"`
	EnableL4      bool `env:"ENABLE_L4" default:"true"`
	EnableMetrics bool `env:"ENABLE_METRICS" default:"false"`

	// New Relic 配置
	NewRelicLicense   string `env:"NEW_RELIC_LICENSE_KEY"`
	NewRelicAccountID string `env:"NEW_RELIC_ACCOUNT_ID"`
	NewRelicDebugMode bool   `env:"NEW_RELIC_DEBUG_MODE" default:"false"`

	// Kafka Producer 配置
	KafkaProducerEnabled bool          `env:"KAFKA_PRODUCER_ENABLED" default:"false"`
	KafkaBrokers         []string      `env:"KAFKA_BROKERS" default:"localhost:9092"`
	KafkaTopic           string        `env:"KAFKA_TOPIC" default:"deepflow-logs"`
	KafkaProducerTimeout time.Duration `env:"KAFKA_PRODUCER_TIMEOUT" default:"5s"`

	// Kafka Consumer 配置
	KafkaConsumerEnabled bool     `env:"KAFKA_CONSUMER_ENABLED" default:"false"`
	KafkaConsumerGroup   string   `env:"KAFKA_CONSUMER_GROUP" default:"deepflow-sidecar"`
	KafkaConsumerTopics  []string `env:"KAFKA_CONSUMER_TOPICS" default:"deepflow-logs"`
	KafkaConsumerOffset  string   `env:"KAFKA_CONSUMER_OFFSET" default:"oldest"`

	// ========== 新增：Consumer Only 模式 ==========
	ConsumerOnlyMode bool `env:"CONSUMER_ONLY_MODE" default:"false"`

	// 调试配置
	LogLevel      string `env:"LOG_LEVEL" default:"info"`
	SaveDebugData bool   `env:"SAVE_DEBUG_DATA" default:"false"`
	DebugDir      string `env:"DEBUG_DIR" default:"./debug"`

	// ========== 新增：Profiler 配置 ==========
	EnableProfiler   bool   `env:"ENABLE_PROFILER" default:"true"`
	ProfilerEndpoint string `env:"PROFILER_ENDPOINT" default:"/api/profiler"`
	ProfilerTopic    string `env:"PROFILER_TOPIC" default:"deepflow-profiler"` // Kafka topic for profiler data
	ProfileEventType string `env:"PROFILE_EVENT_TYPE" default:"DeepFlowProfiler"`
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
		ConsumerOnlyMode:     getEnvBool("CONSUMER_ONLY_MODE", false),

		EnableProfiler:   getEnvBool("ENABLE_PROFILER", true),
		ProfilerEndpoint: getEnv("PROFILER_ENDPOINT", "/api/profiler"),
		ProfilerTopic:    getEnv("PROFILER_TOPIC", "deepflow-profiler"),
		ProfileEventType: getEnv("PROFILE_EVENT_TYPE", "DeepFlowProfiler"),
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
