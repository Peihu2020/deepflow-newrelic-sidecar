package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// Profiler data structures (matching DeepFlow agent format)
type StackTraceData struct {
	PID             uint32 `json:"pid"`
	TID             uint32 `json:"tid"`
	CPU             uint32 `json:"cpu"`
	Count           uint64 `json:"count"`
	Stime           uint64 `json:"stime"`
	Timestamp       uint64 `json:"timestamp"`
	Comm            string `json:"comm"`
	ProcessName     string `json:"process_name"`
	UStackID        int32  `json:"u_stack_id"`
	KStackID        int32  `json:"k_stack_id"`
	ProfilerType    uint8  `json:"profiler_type"`
	StackData       []byte `json:"stack_data"`
	StackDataLen    uint32 `json:"stack_data_len"`
	StackDataString string `json:"stack_data_string"`
}

type ProfilerPayload struct {
	AgentID   string           `json:"agent_id"`
	Hostname  string           `json:"hostname"`
	Timestamp uint64           `json:"timestamp"`
	Samples   []StackTraceData `json:"samples"`
}

// Process profiler data from HTTP
func (p *Processor) processProfilerData(data []byte) error {
	var payload ProfilerPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	// ========== CONVERT stack_data to readable string ==========
	for i := range payload.Samples {
		payload.Samples[i].StackDataString = string(payload.Samples[i].StackData)
	}
	// =========================================================

	// Send to Kafka if enabled
	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		topic := p.config.ProfilerTopic
		if topic == "" {
			topic = "deepflow-profiler"
		}

		// Re-marshal with converted data
		newData, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(newData),
			Timestamp: time.Now(),
			Headers: []sarama.RecordHeader{
				{Key: []byte("type"), Value: []byte("profiler")},
				{Key: []byte("agent_id"), Value: []byte(payload.AgentID)},
				{Key: []byte("hostname"), Value: []byte(payload.Hostname)},
				{Key: []byte("samples"), Value: []byte(fmt.Sprintf("%d", len(payload.Samples)))},
			},
		}

		p.kafkaProducer.producer.Input() <- msg

		atomic.AddInt64(&p.profilerSent, 1)
		atomic.AddInt64(&p.profilerSamples, int64(len(payload.Samples)))
	}

	return nil
}

// Process profiler data from Kafka consumer
func (p *Processor) processProfilerDataFromKafka(data []byte) {
	var payload ProfilerPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("[ERROR] Failed to parse profiler data: %v", err)
		return
	}

	if len(payload.Samples) > 0 {
		log.Printf("[DEBUG] Received %d profiler samples from agent %s",
			len(payload.Samples), payload.AgentID)
	}
}
