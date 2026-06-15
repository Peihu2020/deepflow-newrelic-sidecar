package main

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	producer   sarama.SyncProducer
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

	// ========== 优化1：启用压缩（减少网络传输 70-80%）==========
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy // 或 GZIP

	// ========== 优化2：降低确认级别（减少等待）==========
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal // 只等待 leader 确认
	// 如果数据非常重要，保持 WaitForAll，但配合压缩使用

	// ========== 优化3：批量发送（减少网络往返）==========
	kafkaConfig.Producer.Flush.Bytes = 1024 * 1024                // 1MB 批量
	kafkaConfig.Producer.Flush.Messages = 200                     // 或 200 条消息
	kafkaConfig.Producer.Flush.Frequency = 100 * time.Millisecond // 或 100ms

	// ========== 优化4：异步发送（提高吞吐）==========
	// 改为 AsyncProducer 会更好，但需要修改代码结构
	// 保持 SyncProducer 的话，至少启用批量

	kafkaConfig.Producer.Retry.Max = 3
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Timeout = config.KafkaProducerTimeout

	// 禁用 metadata 获取
	kafkaConfig.Metadata.Full = false
	kafkaConfig.Metadata.RefreshFrequency = 0
	kafkaConfig.ClientID = "deepflow-sidecar-producer"

	producer, err := sarama.NewSyncProducer(config.KafkaBrokers, kafkaConfig)
	if err != nil {
		return nil, err
	}

	log.Printf("[INFO] Kafka Producer initialized: brokers=%v, topic=%s, compression=snappy",
		config.KafkaBrokers, config.KafkaTopic)

	return &KafkaProducer{
		producer: producer,
		topic:    config.KafkaTopic,
		enabled:  true,
	}, nil
}

func (kp *KafkaProducer) Send(data []byte) error {
	if !kp.enabled || kp.closed.Load() {
		return nil
	}

	msg := &sarama.ProducerMessage{
		Topic:     kp.topic,
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	_, _, err := kp.producer.SendMessage(msg)
	if err != nil {
		atomic.AddInt64(&kp.errorCount, 1)
		return err
	}

	atomic.AddInt64(&kp.sentCount, 1)
	return nil
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
