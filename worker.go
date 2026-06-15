package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"
)

func (p *Processor) worker(id int) {
	defer p.workerWg.Done()
	for body := range p.workQueue {
		start := time.Now()
		p.processHTTPData(body)
		if p.config.LogLevel == "debug" {
			log.Printf("[DEBUG] [Worker %d] Processed batch in %v", id, time.Since(start))
		}
	}
}

func (p *Processor) processHTTPData(body []byte) {
	lines := strings.Split(string(body), "\n")
	hostname, _ := os.Hostname()

	l7Batch := make([]json.RawMessage, 0, len(lines)/3)
	l4Batch := make([]json.RawMessage, 0, len(lines)/3)
	metricsBatch := make([]json.RawMessage, 0, len(lines)/3)

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

		switch dataType {
		case "l7":
			if p.config.EnableL7 {
				if jsonBytes := p.processL7Line(dataStr, hostname); jsonBytes != nil {
					l7Batch = append(l7Batch, jsonBytes)
				}
			}
		case "l4":
			if p.config.EnableL4 {
				if jsonBytes := p.processL4Line(dataStr, hostname); jsonBytes != nil {
					l4Batch = append(l4Batch, jsonBytes)
				}
			}
		case "metrics":
			if p.config.EnableMetrics {
				if jsonBytes := p.processMetricsLine(dataStr, hostname); jsonBytes != nil {
					metricsBatch = append(metricsBatch, jsonBytes)
				}
			}
		}
	}

	for _, item := range l7Batch {
		p.l7Sender.Add(item)
	}
	for _, item := range l4Batch {
		p.l4Sender.Add(item)
	}
	for _, item := range metricsBatch {
		p.metricsSender.Add(item)
	}
}
