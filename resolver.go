package main

import (
	"sync"

	"github.com/IBM/sarama"
)

// CustomBrokerResolver 强制使用配置的 broker 地址，忽略 Kafka 返回的 metadata
type CustomBrokerResolver struct {
	brokers []string
	mu      sync.RWMutex
}

func NewCustomBrokerResolver(brokers []string) *CustomBrokerResolver {
	return &CustomBrokerResolver{
		brokers: brokers,
	}
}

func (r *CustomBrokerResolver) BrokerList() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.brokers
}

// 自定义 Client 包装器
type customClient struct {
	sarama.Client
	brokers []string
}

func (c *customClient) Brokers() []*sarama.Broker {
	// 返回自定义的 broker 地址，而不是从 metadata 获取
	brokers := make([]*sarama.Broker, len(c.brokers))
	for i, addr := range c.brokers {
		broker := sarama.NewBroker(addr)
		brokers[i] = broker
	}
	return brokers
}

func (c *customClient) Controller() (*sarama.Broker, error) {
	// 返回第一个 broker 作为 controller
	if len(c.brokers) == 0 {
		return nil, sarama.ErrBrokerNotFound
	}
	return sarama.NewBroker(c.brokers[0]), nil
}
