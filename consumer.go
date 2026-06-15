package main

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
	ready         chan bool
	enabled       bool
	stopped       atomic.Bool
	wg            sync.WaitGroup
	processor     *Processor
	receivedCount int64
}

type ConsumerGroupHandler struct {
	consumer *KafkaConsumer
}

func NewKafkaConsumer(config *Config, processor *Processor) (*KafkaConsumer, error) {
	if !config.KafkaConsumerEnabled {
		return &KafkaConsumer{enabled: false}, nil
	}

	// 强制使用配置的 broker 地址
	brokers := config.KafkaBrokers
	log.Printf("[INFO] Connecting to Kafka brokers: %v", brokers)

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_6_0_0
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	if config.KafkaConsumerOffset == "newest" {
		kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// 禁用 metadata 自动刷新，使用配置的地址
	kafkaConfig.Metadata.Full = false
	kafkaConfig.Metadata.RefreshFrequency = 0
	kafkaConfig.ClientID = "deepflow-sidecar-consumer"

	consumerGroup, err := sarama.NewConsumerGroup(brokers, config.KafkaConsumerGroup, kafkaConfig)
	if err != nil {
		log.Printf("[WARN] Failed to create Kafka consumer group: %v", err)
		return &KafkaConsumer{enabled: false}, nil
	}

	log.Printf("[INFO] Kafka Consumer initialized: brokers=%v, group=%s, topics=%v",
		brokers, config.KafkaConsumerGroup, config.KafkaConsumerTopics)

	consumer := &KafkaConsumer{
		consumerGroup: consumerGroup,
		topics:        config.KafkaConsumerTopics,
		ready:         make(chan bool),
		enabled:       true,
		processor:     processor,
	}

	return consumer, nil
}

func (c *KafkaConsumer) Start() {
	if !c.enabled {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx := context.Background()
		handler := &ConsumerGroupHandler{consumer: c}

		for {
			if c.stopped.Load() {
				return
			}

			if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
				if !c.stopped.Load() {
					log.Printf("[WARN] Kafka consumer error: %v", err)
				}
				time.Sleep(5 * time.Second)
			}

			if c.stopped.Load() {
				return
			}
		}
	}()

	<-c.ready
	log.Printf("[INFO] Kafka consumer ready, processing messages")
}

func (c *KafkaConsumer) Stop() {
	if !c.enabled {
		return
	}
	c.stopped.Store(true)
	c.consumerGroup.Close()
	c.wg.Wait()
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(h.consumer.ready)
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 根据配置决定是否处理
		shouldProcess := false

		parts := strings.SplitN(string(message.Value), "|", 2)
		if len(parts) == 2 {
			dataType := parts[0]
			switch dataType {
			case "l7":
				shouldProcess = h.consumer.processor.config.EnableL7
			case "l4":
				shouldProcess = h.consumer.processor.config.EnableL4
			case "metrics":
				shouldProcess = h.consumer.processor.config.EnableMetrics
			}
		}

		if shouldProcess {
			atomic.AddInt64(&h.consumer.receivedCount, 1)
			h.consumer.processor.ProcessKafkaMessage(message.Value)
		}
		// 无论是否处理，都标记消息已消费
		session.MarkMessage(message, "")
	}
	return nil
}

func (c *KafkaConsumer) GetStats() int64 {
	return atomic.LoadInt64(&c.receivedCount)
}
