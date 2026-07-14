package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type Processor struct {
	config        Config
	nrClient      *NewRelicClient
	l7Sender      *BatchSender
	l4Sender      *BatchSender
	metricsSender *BatchSender
	stopCh        chan struct{}
	wg            sync.WaitGroup
	httpServer    *http.Server

	// Worker pool
	workQueue       chan []byte
	workerWg        sync.WaitGroup
	limiter         *rate.Limiter
	droppedRequests int64

	// Kafka
	kafkaProducer *KafkaProducer
	kafkaConsumer *KafkaConsumer

	// Profiler stats
	profilerSent    int64
	profilerSamples int64
	profilerDropped int64

	profilerSender *BatchSender
}

func NewProcessor(config *Config, nrClient *NewRelicClient) *Processor {
	return &Processor{
		config:          *config,
		nrClient:        nrClient,
		stopCh:          make(chan struct{}),
		workQueue:       make(chan []byte, config.MaxQueueSize),
		limiter:         rate.NewLimiter(rate.Limit(config.RateLimitPerSec), config.RateLimitBurst),
		l7Sender:        NewBatchSender(nrClient, "DeepFlowL7Log", config, config.EnableL7),
		l4Sender:        NewBatchSender(nrClient, "DeepFlowL4Log", config, config.EnableL4),
		metricsSender:   NewBatchSender(nrClient, "DeepFlowMetrics", config, config.EnableMetrics),
		profilerSender:  NewBatchSender(nrClient, "DeepFlowProfiler", config, config.EnableProfiler),
		profilerSent:    0,
		profilerSamples: 0,
		profilerDropped: 0,
	}
}

func (p *Processor) GetProfilerStats() (sent, samples, dropped int64) {
	return atomic.LoadInt64(&p.profilerSent),
		atomic.LoadInt64(&p.profilerSamples),
		atomic.LoadInt64(&p.profilerDropped)
}

func (p *Processor) Start() {
	p.l7Sender.Start()
	p.l4Sender.Start()
	p.metricsSender.Start()
	p.profilerSender.Start()

	for i := 0; i < p.config.WorkerCount; i++ {
		p.workerWg.Add(1)
		go p.worker(i)
	}
}

func (p *Processor) Stop() {
	close(p.stopCh)
	close(p.workQueue)
	p.workerWg.Wait()

	p.l7Sender.Stop()
	p.l4Sender.Stop()
	p.metricsSender.Stop()
	p.profilerSender.Stop()

	if p.kafkaProducer != nil {
		p.kafkaProducer.Close()
	}

	if p.kafkaConsumer != nil {
		p.kafkaConsumer.Stop()
	}

	if p.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.httpServer.Shutdown(ctx)
	}

	p.wg.Wait()
	log.Println("Sidecar stopped")
}

func (p *Processor) ProcessKafkaMessage(data []byte) {
	// 按行分割
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}

		dataType := parts[0]
		dataStr := parts[1]

		hostname, _ := os.Hostname()

		switch dataType {
		case "l7":
			if p.config.EnableL7 {
				if jsonBytes := p.processL7Line(dataStr, hostname); jsonBytes != nil {
					p.l7Sender.Add(jsonBytes)
				}
			}
		case "l4":
			if p.config.EnableL4 {
				if jsonBytes := p.processL4Line(dataStr, hostname); jsonBytes != nil {
					p.l4Sender.Add(jsonBytes)
				}
			}
		case "metrics":
			if p.config.EnableMetrics {
				if jsonBytes := p.processMetricsLine(dataStr, hostname); jsonBytes != nil {
					p.metricsSender.Add(jsonBytes)
				}
			}
		}
	}
}

// Send profiler data to NewRelic
func (p *Processor) sendProfilerToNewRelic(payload *ProfilerPayload) {
	if p.nrClient == nil || !p.config.EnableProfiler {
		return
	}

	maxSamples := 100
	samples := payload.Samples
	if len(samples) > maxSamples {
		samples = samples[:maxSamples]
	}

	// Use DeepFlowL7Log (known working event type)
	eventType := p.config.ProfileEventType

	var logs []json.RawMessage
	for _, sample := range samples {
		event := map[string]interface{}{
			"eventType":        eventType,
			"profiler_pid":     sample.PID,
			"profiler_tid":     sample.TID,
			"profiler_cpu":     sample.CPU,
			"profiler_count":   sample.Count,
			"profiler_comm":    sample.Comm,
			"profiler_process": sample.ProcessName,
			"profiler_trace":   sample.StackDataString,
			"agent_id":         payload.AgentID,
			"hostname":         payload.Hostname,
			"timestamp":        sample.Timestamp / 1000000,
			"profiler_type":    sample.ProfilerType,
		}

		// Convert to JSON and append to logs
		jsonBytes, err := json.Marshal(event)
		if err != nil {
			continue
		}
		logs = append(logs, json.RawMessage(jsonBytes))
	}

	if len(logs) > 0 {
		log.Printf("[PROFILER] Sending %d profiler events as %s", len(logs), eventType)
		if err := p.nrClient.SendLogs(logs); err != nil {
			log.Printf("[ERROR] Failed to send profiler events to NewRelic: %v", err)
		} else {
			log.Printf("[PROFILER] Successfully sent %d profiler events as %s", len(logs), eventType)
		}
	}
}

func (p *Processor) GetStats() (l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped int64, queueLen int) {
	l7Sent, l7Dropped = p.l7Sender.GetStats()
	l4Sent, l4Dropped = p.l4Sender.GetStats()
	metricsSent, metricsDropped = p.metricsSender.GetStats()
	queueLen = len(p.workQueue)
	return
}

func (p *Processor) GetDroppedRequests() int64 {
	return atomic.LoadInt64(&p.droppedRequests)
}
