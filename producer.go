package main

import (
	"log"
	"strings"
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

	// ========== 🔥 重试配置（硬编码 - 增加重试次数和间隔）==========
	kafkaConfig.Producer.Retry.Max = 10                          // 从 3 改为 10
	kafkaConfig.Producer.Retry.Backoff = 1000 * time.Millisecond // 从 100ms 改为 1000ms (1秒)

	// ========== 🔥 超时配置（硬编码 - 增加超时时间）==========
	kafkaConfig.Producer.Timeout = 60 * time.Second // 从默认 30s 改为 60s

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

	log.Printf("[INFO] Kafka AsyncProducer initialized: brokers=%v, topics: l4/l7=%s, profiler=%s",
		config.KafkaBrokers, config.KafkaTopic, config.ProfilerTopic)
	log.Printf("[INFO] Kafka config: Retry.Max=10, Retry.Backoff=1s, Timeout=60s")

	return kp, nil
}

// 处理异步发送的错误 - 增强调试
func (kp *KafkaProducer) handleErrors() {
	for err := range kp.producer.Errors() {
		atomic.AddInt64(&kp.errorCount, 1)
		errorCount := atomic.LoadInt64(&kp.errorCount)
		sentCount := atomic.LoadInt64(&kp.sentCount)

		// ========== 详细错误信息 ==========
		log.Printf("[ERROR] 🔥 Kafka producer error #%d:", errorCount)
		log.Printf("[ERROR]   Error: %v", err.Err)
		log.Printf("[ERROR]   Topic: %s", err.Msg.Topic)
		log.Printf("[ERROR]   Partition: %d", err.Msg.Partition)

		// 打印消息内容前100字符
		if err.Msg.Value != nil {
			value, _ := err.Msg.Value.Encode()
			preview := string(value)
			if len(preview) > 100 {
				preview = preview[:100] + "..."
			}
			log.Printf("[ERROR]   Message size: %d bytes", len(value))
			log.Printf("[ERROR]   Message preview: %s", preview)
		}

		// 判断错误类型
		errMsg := err.Err.Error()
		switch {
		case err.Err == sarama.ErrMessageSizeTooLarge:
			log.Printf("[ERROR] ⚠️ 消息太大！检查 KAFKA_MAX_MESSAGE_BYTES 配置")
		case err.Err == sarama.ErrBrokerNotAvailable:
			log.Printf("[ERROR] ⚠️ Broker 不可用！检查 Kafka 连接")
		case err.Err == sarama.ErrUnknownTopicOrPartition:
			log.Printf("[ERROR] ⚠️ Topic 不存在！检查 topic: %s", kp.topic)
		case err.Err == sarama.ErrNotEnoughReplicas:
			log.Printf("[ERROR] ⚠️ 副本不足！检查 replication factor")
		case err.Err == sarama.ErrNotEnoughReplicasAfterAppend:
			log.Printf("[ERROR] ⚠️ 写入后副本不足！检查 ISR")
		case err.Err == sarama.ErrLeaderNotAvailable:
			log.Printf("[ERROR] ⚠️ Leader 不可用！检查 Kafka 集群状态")
		case err.Err == sarama.ErrTopicAuthorizationFailed:
			log.Printf("[ERROR] ⚠️ 授权失败！检查 Kafka ACL")
		default:
			// 检查是否包含 timeout 关键字
			if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "Timeout") {
				log.Printf("[ERROR] ⚠️ 超时错误！检查网络或增加 KAFKA_PRODUCER_TIMEOUT")
			} else {
				log.Printf("[ERROR] ⚠️ 未知错误类型: %v", err.Err)
			}
		}

		// 每50个错误打印统计
		if errorCount%50 == 0 {
			total := sentCount + errorCount
			errorRate := float64(errorCount) / float64(total) * 100
			log.Printf("[ERROR] 📊 错误统计: %d 错误 / %d 总消息 (%.2f%% 错误率)",
				errorCount, total, errorRate)
		}
	}
}

// 异步发送消息 - 添加日志
func (kp *KafkaProducer) Send(data []byte) error {
	if !kp.enabled || kp.closed.Load() {
		return nil
	}

	sent := atomic.AddInt64(&kp.sentCount, 1)
	if sent%1000 == 0 {
		log.Printf("[DEBUG] 📤 Producer sent %d messages (size: %d bytes)", sent, len(data))
	}

	msg := &sarama.ProducerMessage{
		Topic:     kp.topic,
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	kp.producer.Input() <- msg
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
