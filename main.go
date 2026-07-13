package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	config := LoadConfig()

	// ========== 打印 ConsumerOnlyMode 状态 ==========
	if config.ConsumerOnlyMode {
		log.Printf("⚠️  Running in CONSUMER-ONLY mode: HTTP requests will be rejected")
	}
	fmt.Fprintf(os.Stderr, "[DEBUG] Main - KafkaBrokers: %v\n", config.KafkaBrokers)
	if config.NewRelicLicense == "" || config.NewRelicAccountID == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY and NEW_RELIC_ACCOUNT_ID are required")
	}

	if config.SaveDebugData {
		os.MkdirAll(config.DebugDir, 0755)
	}

	log.Printf("Starting sidecar in HTTP mode")
	log.Printf("Enabled types - L7: %v, L4: %v, Metrics: %v, Profiler: %v",
		config.EnableL7, config.EnableL4, config.EnableMetrics, config.EnableProfiler)
	log.Printf("Workers: %d, Queue Size: %d, Rate Limit: %d/s", config.WorkerCount, config.MaxQueueSize, config.RateLimitPerSec)

	if config.KafkaProducerEnabled {
		log.Printf("Kafka Producer enabled - brokers: %v, topic: %s", config.KafkaBrokers, config.KafkaTopic)
	}
	if config.KafkaConsumerEnabled {
		log.Printf("Kafka Consumer enabled - brokers: %v, group: %s, topics: %v", config.KafkaBrokers, config.KafkaConsumerGroup, config.KafkaConsumerTopics)
	}

	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, config.NewRelicDebugMode)

	processor := NewProcessor(config, nrClient)

	// ========== 初始化 Kafka Producer（独立） ==========
	if config.KafkaProducerEnabled {
		kafkaProducer, err := NewKafkaProducer(config)
		if err != nil {
			log.Printf("[WARN] Failed to create Kafka producer: %v", err)
		} else {
			processor.kafkaProducer = kafkaProducer
		}
	}

	// ========== 初始化 Kafka Consumer（独立，不依赖 Producer） ==========
	if config.KafkaConsumerEnabled {
		kafkaConsumer, err := NewKafkaConsumer(config, processor)
		if err != nil {
			log.Printf("[WARN] Failed to create Kafka consumer: %v", err)
		} else {
			processor.kafkaConsumer = kafkaConsumer
			log.Printf("[INFO] 🚀 Starting Kafka Consumer...")
			processor.kafkaConsumer.Start()
		}
	}

	processor.Start()
	processor.StartHTTPServer()

	// GC Ticker
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runtime.GC()
			debug.FreeOSMemory()
			if config.LogLevel == "debug" {
				log.Printf("[DEBUG] Forced GC, memory released")
			}
		}
	}()

	// Stats Ticker
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped, queueLen := processor.GetStats()

			logStr := fmt.Sprintf("📊 Stats - L7: sent=%d dropped=%d, L4: sent=%d dropped=%d, Metrics: sent=%d dropped=%d, Queue: %d/%d",
				l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped,
				queueLen, config.MaxQueueSize)

			if processor.kafkaProducer != nil && processor.kafkaProducer.enabled {
				sent, errors := processor.kafkaProducer.GetStats()
				logStr += fmt.Sprintf(", Kafka Producer: sent=%d errors=%d", sent, errors)
			}

			// ========== 只获取 stats，不调用 Start() ==========
			if processor.kafkaConsumer != nil && processor.kafkaConsumer.enabled {
				received := processor.kafkaConsumer.GetStats()
				logStr += fmt.Sprintf(", Kafka Consumer: received=%d", received)
			}

			log.Println(logStr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	processor.Stop()
}
