package main

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	producer   sarama.AsyncProducer
	topic      string
	enabled    bool
	sentCount  int64
	errorCount int64
	closed     atomic.Bool
}

func NewKafkaProducer(config *Config) (*KafkaProducer, error) {
	if !config.KafkaProducerEnabled {
		return &KafkaProducer{enabled: false}, nil
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_6_0_0

	// ========== 压缩配置 ==========
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy

	// ========== 异步配置（关键）==========
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Return.Successes = false // 不返回成功确认（异步）
	kafkaConfig.Producer.Return.Errors = true     // 但要返回错误

	// ========== 批量发送配置 ==========
	kafkaConfig.Producer.Flush.Bytes = 1024 * 1024                // 1MB 批量
	kafkaConfig.Producer.Flush.Messages = 200                     // 200条消息批量
	kafkaConfig.Producer.Flush.Frequency = 100 * time.Millisecond // 100ms刷新

	// ========== 重试配置 ==========
	kafkaConfig.Producer.Retry.Max = 3
	kafkaConfig.Producer.Retry.Backoff = 100 * time.Millisecond

	// ========== 超时配置 ==========
	kafkaConfig.Producer.Timeout = config.KafkaProducerTimeout

	// ========== 元数据配置 ==========
	kafkaConfig.Metadata.Full = false
	kafkaConfig.Metadata.RefreshFrequency = 0
	kafkaConfig.ClientID = "deepflow-sidecar-producer"

	// ========== 最大消息大小 ==========
	kafkaConfig.Producer.MaxMessageBytes = 1024 * 1024 * 10 // 10MB

	// 创建异步 Producer
	producer, err := sarama.NewAsyncProducer(config.KafkaBrokers, kafkaConfig)
	if err != nil {
		return nil, err
	}

	kp := &KafkaProducer{
		producer: producer,
		topic:    config.KafkaTopic,
		enabled:  true,
	}

	// 启动错误处理协程
	go kp.handleErrors()

	// 可选：启动成功确认协程（如果需要记录成功）
	// go kp.handleSuccesses()

	log.Printf("[INFO] Kafka AsyncProducer initialized: brokers=%v, topic=%s, compression=snappy",
		config.KafkaBrokers, config.KafkaTopic)

	return kp, nil
}

// 处理异步发送的错误
func (kp *KafkaProducer) handleErrors() {
	for err := range kp.producer.Errors() {
		atomic.AddInt64(&kp.errorCount, 1)
		if atomic.LoadInt64(&kp.errorCount)%100 == 1 {
			log.Printf("[ERROR] Kafka producer error: %v (total errors: %d)",
				err.Err, atomic.LoadInt64(&kp.errorCount))
		}
	}
}

// 可选：处理成功确认（用于统计）
func (kp *KafkaProducer) handleSuccesses() {
	for range kp.producer.Successes() {
		atomic.AddInt64(&kp.sentCount, 1)
	}
}

// 异步发送消息
func (kp *KafkaProducer) Send(data []byte) error {
	if !kp.enabled || kp.closed.Load() {
		return nil
	}

	msg := &sarama.ProducerMessage{
		Topic:     kp.topic,
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	// 异步发送：将消息放入 channel，立即返回
	select {
	case kp.producer.Input() <- msg:
		atomic.AddInt64(&kp.sentCount, 1)
		return nil
	default:
		// 如果 channel 满了，记录错误
		atomic.AddInt64(&kp.errorCount, 1)
		return nil
	}
}

func (kp *KafkaProducer) GetStats() (sent, errors int64) {
	return atomic.LoadInt64(&kp.sentCount), atomic.LoadInt64(&kp.errorCount)
}

func (kp *KafkaProducer) Close() {
	if kp.enabled && kp.producer != nil {
		kp.closed.Store(true)
		kp.producer.Close()
	}
}
