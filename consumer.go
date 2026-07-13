package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	consumerGroup  sarama.ConsumerGroup
	topics         []string
	ready          chan bool
	enabled        bool
	stopped        atomic.Bool
	wg             sync.WaitGroup
	processor      *Processor
	receivedCount  int64
	workerCount    int
	messageChannel chan *sarama.ConsumerMessage
}

type ConsumerGroupHandler struct {
	consumer *KafkaConsumer
}

func NewKafkaConsumer(config *Config, processor *Processor) (*KafkaConsumer, error) {
	if !config.KafkaConsumerEnabled {
		return &KafkaConsumer{enabled: false}, nil
	}

	brokers := config.KafkaBrokers
	log.Printf("[INFO] Connecting to Kafka brokers: %v", brokers)

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_6_0_0
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	if config.KafkaConsumerOffset == "newest" {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// 性能配置
	kafkaConfig.Consumer.Fetch.Default = 1024 * 1024 * 5
	kafkaConfig.Consumer.Fetch.Max = 1024 * 1024 * 20
	kafkaConfig.Consumer.MaxWaitTime = 500 * time.Millisecond
	kafkaConfig.Consumer.MaxProcessingTime = 60 * time.Second

	kafkaConfig.Metadata.Full = false
	kafkaConfig.Metadata.RefreshFrequency = 0
	kafkaConfig.ClientID = "deepflow-sidecar-consumer"

	consumerGroup, err := sarama.NewConsumerGroup(brokers, config.KafkaConsumerGroup, kafkaConfig)
	if err != nil {
		log.Printf("[WARN] Failed to create Kafka consumer group: %v", err)
		return &KafkaConsumer{enabled: false}, nil
	}

	workerCount := config.WorkerCount
	if workerCount < 4 {
		workerCount = 4
	}
	if workerCount > 64 {
		workerCount = 64
	}

	log.Printf("[INFO] Kafka Consumer initialized: brokers=%v, group=%s, topics=%v, workers=%d",
		brokers, config.KafkaConsumerGroup, config.KafkaConsumerTopics, workerCount)

	consumer := &KafkaConsumer{
		consumerGroup:  consumerGroup,
		topics:         config.KafkaConsumerTopics,
		ready:          make(chan bool),
		enabled:        true,
		processor:      processor,
		workerCount:    workerCount,
		messageChannel: make(chan *sarama.ConsumerMessage, workerCount*10),
	}

	return consumer, nil
}

func (c *KafkaConsumer) Start() {
	if !c.enabled {
		return
	}

	// 启动 worker 池
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.messageWorker(i)
	}

	// 启动 consumer group
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		handler := &ConsumerGroupHandler{consumer: c}

		for {
			if c.stopped.Load() {
				return
			}

			if err := c.consumerGroup.Consume(context.Background(), c.topics, handler); err != nil {
				if !c.stopped.Load() {
					log.Printf("[ERROR] Kafka consumer error: %v", err)
				}
				time.Sleep(5 * time.Second)
			}

			if c.stopped.Load() {
				return
			}
		}
	}()

	// 等待 consumer 就绪
	select {
	case <-c.ready:
		log.Printf("[INFO] Kafka consumer ready, processing messages")
	case <-time.After(30 * time.Second):
		log.Printf("[ERROR] Timeout waiting for Kafka consumer to be ready!")
	}
}

func (c *KafkaConsumer) messageWorker(workerID int) {
	defer c.wg.Done()

	for {
		select {
		case message, ok := <-c.messageChannel:
			if !ok || c.stopped.Load() {
				return
			}
			c.processMessage(message)
		case <-time.After(10 * time.Second):
			if c.stopped.Load() && len(c.messageChannel) == 0 {
				return
			}
		}
	}
}

func (c *KafkaConsumer) processMessage(message *sarama.ConsumerMessage) {
	// ========== 新增：检测 Profiler 数据 ==========
	// 检查是否有 profiler 相关的 header
	for _, header := range message.Headers {
		if string(header.Key) == "type" && string(header.Value) == "profiler" {
			c.processProfilerMessage(message.Value)
			return
		}
	}

	// 检查是否 JSON 格式的 profiler 数据
	var profilerPayload ProfilerPayload
	if err := json.Unmarshal(message.Value, &profilerPayload); err == nil && len(profilerPayload.Samples) > 0 {
		c.processProfilerMessage(message.Value)
		return
	}
	// ========== 新增结束 ==========
	shouldProcess := false
	parts := strings.SplitN(string(message.Value), "|", 2)
	if len(parts) == 2 {
		dataType := parts[0]
		switch dataType {
		case "l7":
			shouldProcess = c.processor.config.EnableL7
		case "l4":
			shouldProcess = c.processor.config.EnableL4
		case "metrics":
			shouldProcess = c.processor.config.EnableMetrics
		}
	}

	if shouldProcess {
		atomic.AddInt64(&c.receivedCount, 1)
		c.processor.ProcessKafkaMessage(message.Value)
	}
}

// ========== 新增：处理 Profiler 消息 ==========
func (c *KafkaConsumer) processProfilerMessage(data []byte) {
	var payload ProfilerPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("[ERROR] Failed to parse profiler data: %v", err)
		return
	}

	atomic.AddInt64(&c.receivedCount, int64(len(payload.Samples)))
	log.Printf("[INFO] Received %d profiler samples from %s",
		len(payload.Samples), payload.AgentID)

	// ========== Print first sample with readable stack ==========
	if len(payload.Samples) > 0 {
		sample := payload.Samples[0]
		log.Printf("[PROFILER] Sample: PID=%d Comm=%s CPU=%d Count=%d",
			sample.PID, sample.Comm, sample.CPU, sample.Count)
		log.Printf("[PROFILER]   Stack: %s", sample.StackDataString)
	}
	// =============================================================
}

func (c *KafkaConsumer) Stop() {
	if !c.enabled {
		return
	}

	c.stopped.Store(true)
	time.Sleep(2 * time.Second)
	close(c.messageChannel)

	if err := c.consumerGroup.Close(); err != nil {
		log.Printf("[WARN] Error closing consumer group: %v", err)
	}

	c.wg.Wait()
	log.Printf("[INFO] Kafka consumer stopped")
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	// Prevent closing already closed channel
	select {
	case <-h.consumer.ready:
		// Already closed, do nothing
	default:
		close(h.consumer.ready)
	}
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		select {
		case h.consumer.messageChannel <- message:
		default:
			h.consumer.processMessage(message)
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func (c *KafkaConsumer) GetStats() int64 {
	return atomic.LoadInt64(&c.receivedCount)
}
