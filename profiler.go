package main

import (
	"encoding/base64"
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
	StackDataString string `json:"stack_data_string"` // Decoded stack trace string
}

type ProfilerPayload struct {
	AgentID   string           `json:"agent_id"`
	Hostname  string           `json:"hostname"`
	Timestamp uint64           `json:"timestamp"`
	Samples   []StackTraceData `json:"samples"`
}

// decodeStackTrace decodes Base64 encoded stack trace if needed
func decodeStackTrace(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// Try Base64 decode (DeepFlow sends stack_data as Base64)
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		return string(decoded)
	}

	// Not Base64, return as-is
	return string(data)
}

// Process profiler data from HTTP
func (p *Processor) processProfilerData(data []byte) error {
	var payload ProfilerPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	// ===== DECODE STACK DATA & FILTER =====
	var realSamples []StackTraceData
	for _, sample := range payload.Samples {
		// Decode stack trace
		sample.StackDataString = decodeStackTrace(sample.StackData)

		// Only keep real processes (PID != 0)
		if sample.PID != 0 {
			realSamples = append(realSamples, sample)
		}
	}

	// Skip if no real samples
	if len(realSamples) == 0 {
		return nil
	}

	// Create filtered payload with decoded stack data
	filteredPayload := ProfilerPayload{
		AgentID:   payload.AgentID,
		Hostname:  payload.Hostname,
		Timestamp: payload.Timestamp,
		Samples:   realSamples,
	}
	// =============================================

	// Send filtered data to Kafka
	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		topic := p.config.ProfilerTopic
		if topic == "" {
			topic = "deepflow-profiler"
		}

		filteredData, err := json.Marshal(filteredPayload)
		if err != nil {
			return err
		}

		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(filteredData),
			Timestamp: time.Now(),
			Headers: []sarama.RecordHeader{
				{Key: []byte("type"), Value: []byte("profiler")},
				{Key: []byte("agent_id"), Value: []byte(payload.AgentID)},
				{Key: []byte("hostname"), Value: []byte(payload.Hostname)},
				{Key: []byte("samples"), Value: []byte(fmt.Sprintf("%d", len(realSamples)))},
			},
		}

		p.kafkaProducer.producer.Input() <- msg
		atomic.AddInt64(&p.profilerSent, 1)
		atomic.AddInt64(&p.profilerSamples, int64(len(realSamples)))
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
