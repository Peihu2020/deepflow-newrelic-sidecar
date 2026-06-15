package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type BatchSender struct {
	logs         []json.RawMessage
	mu           sync.Mutex
	ticker       *time.Ticker
	nrClient     *NewRelicClient
	eventType    string
	stopCh       chan struct{}
	wg           sync.WaitGroup
	debugFile    *os.File
	debugMu      sync.Mutex
	config       *Config
	totalSent    int64
	dropped      int64
	maxQueueSize int
	enabled      bool
	errorCount   int64
}

func NewBatchSender(nrClient *NewRelicClient, eventType string, config *Config, enabled bool) *BatchSender {
	if !enabled {
		return &BatchSender{
			eventType:    eventType,
			config:       config,
			enabled:      false,
			maxQueueSize: config.MaxQueueSize,
		}
	}

	sender := &BatchSender{
		logs:         make([]json.RawMessage, 0, config.BatchSize),
		ticker:       time.NewTicker(config.FlushInterval),
		nrClient:     nrClient,
		eventType:    eventType,
		stopCh:       make(chan struct{}),
		config:       config,
		totalSent:    0,
		dropped:      0,
		maxQueueSize: config.MaxQueueSize,
		enabled:      true,
	}

	if config.SaveDebugData {
		var filename string
		switch eventType {
		case "DeepFlowL7Log":
			filename = "l7_debug.jsonl"
		case "DeepFlowL4Log":
			filename = "l4_debug.jsonl"
		case "DeepFlowMetrics":
			filename = "metrics_debug.jsonl"
		}
		filepath := config.DebugDir + "/" + filename
		file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			sender.debugFile = file
		}
	}

	return sender
}

func (b *BatchSender) Add(log json.RawMessage) bool {
	if !b.enabled {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.logs) >= b.maxQueueSize {
		atomic.AddInt64(&b.dropped, 1)
		if atomic.LoadInt64(&b.dropped)%1000 == 0 {
			fmt.Printf("[WARN] [%s] Queue full, dropped %d events", b.eventType, atomic.LoadInt64(&b.dropped))
		}
		return false
	}

	b.logs = append(b.logs, log)

	if len(b.logs) >= b.config.BatchSize {
		go b.flush()
	}
	return true
}

func (b *BatchSender) saveToDebugFile(data []byte) {
	if b.debugFile == nil || !b.enabled {
		return
	}
	b.debugMu.Lock()
	defer b.debugMu.Unlock()
	b.debugFile.Write(data)
	b.debugFile.Write([]byte("\n"))
}

func (b *BatchSender) Start() {
	if !b.enabled {
		log.Printf("[INFO] [%s] Disabled, skipping start", b.eventType)
		return
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.stopCh:
				b.flush()
				b.ticker.Stop()
				if b.debugFile != nil {
					b.debugFile.Close()
				}
				return
			case <-b.ticker.C:
				b.flush()
			}
		}
	}()
}

func (b *BatchSender) Stop() {
	if !b.enabled {
		return
	}
	close(b.stopCh)
	b.wg.Wait()
}

func (b *BatchSender) flush() {
	if !b.enabled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *BatchSender) flushLocked() {
	if len(b.logs) == 0 {
		return
	}

	err := b.nrClient.SendLogs(b.logs)
	if err != nil {
		atomic.AddInt64(&b.errorCount, 1)
		if atomic.LoadInt64(&b.errorCount)%100 == 1 {
			log.Printf("[ERROR] Failed to send %s events: %v (total errors: %d)",
				b.eventType, err, atomic.LoadInt64(&b.errorCount))
		}
		return
	}

	b.totalSent += int64(len(b.logs))
	if b.config.LogLevel == "debug" {
		log.Printf("[DEBUG] Sent %d %s events (total: %d)", len(b.logs), b.eventType, b.totalSent)
	}
	b.logs = b.logs[:0]
}

func (b *BatchSender) GetStats() (sent, dropped int64) {
	return atomic.LoadInt64(&b.totalSent), atomic.LoadInt64(&b.dropped)
}
