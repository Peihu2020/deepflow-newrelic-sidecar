package main

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
)

func (p *Processor) StartHTTPServer() {
	// 强制使用 IPv4 监听
	addr := "0.0.0.0:" + p.config.HTTPPort
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Printf("[ERROR] Failed to create IPv4 listener on %s: %v", addr, err)
		return
	}

	p.httpServer = &http.Server{
		Handler:      p.buildHTTPHandler(),
		ReadTimeout:  p.config.HTTPReadTimeout,
		WriteTimeout: p.config.HTTPWriteTimeout,
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		log.Printf("HTTP server listening on %s (IPv4)", addr)
		if err := p.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP error: %v", err)
		}
	}()
}

func (p *Processor) buildHTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(p.config.DeepFlowHTTPEndpoint, p.handleDataAsync)
	mux.HandleFunc("/health", p.handleHealth)
	mux.HandleFunc("/stats", p.handleStats)
	mux.HandleFunc("/api/flush", p.handleFlush)
	return mux
}

func (p *Processor) handleDataAsync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !p.limiter.Allow() {
		atomic.AddInt64(&p.droppedRequests, 1)
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// ========== 根据配置决定是否写入 Kafka ==========
	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		// 检查数据类型，根据配置决定是否保存到 Kafka
		shouldSaveToKafka := true

		parts := strings.SplitN(string(body), "|", 2)
		if len(parts) == 2 {
			dataType := parts[0]
			switch dataType {
			case "l7":
				shouldSaveToKafka = p.config.EnableL7
			case "l4":
				shouldSaveToKafka = p.config.EnableL4
			case "metrics":
				shouldSaveToKafka = p.config.EnableMetrics
			default:
				// 未知类型，根据配置决定
				shouldSaveToKafka = p.config.EnableL7 || p.config.EnableL4 || p.config.EnableMetrics
			}
		}

		if shouldSaveToKafka {
			if err := p.kafkaProducer.Send(body); err != nil {
				log.Printf("[WARN] Failed to send to Kafka: %v", err)
			}
		}
	}

	// 总是放入 workQueue 处理（实时发送到 New Relic）
	select {
	case p.workQueue <- body:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"accepted"}`))
	default:
		atomic.AddInt64(&p.droppedRequests, 1)
		http.Error(w, "Queue full", http.StatusServiceUnavailable)
	}
}

func (p *Processor) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped, queueLen := p.GetStats()

	response := map[string]interface{}{
		"status": "healthy",
		"enabled_types": map[string]bool{
			"l7":      p.config.EnableL7,
			"l4":      p.config.EnableL4,
			"metrics": p.config.EnableMetrics,
		},
		"stats": map[string]interface{}{
			"l7_sent":          l7Sent,
			"l7_dropped":       l7Dropped,
			"l4_sent":          l4Sent,
			"l4_dropped":       l4Dropped,
			"metrics_sent":     metricsSent,
			"metrics_dropped":  metricsDropped,
			"queue_length":     queueLen,
			"requests_dropped": p.GetDroppedRequests(),
		},
	}

	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		sent, errors := p.kafkaProducer.GetStats()
		response["kafka_producer"] = map[string]interface{}{
			"enabled": true,
			"sent":    sent,
			"errors":  errors,
		}
	}

	if p.kafkaConsumer != nil && p.kafkaConsumer.enabled {
		response["kafka_consumer"] = map[string]interface{}{
			"enabled":  true,
			"received": p.kafkaConsumer.GetStats(),
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (p *Processor) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped, queueLen := p.GetStats()

	response := map[string]interface{}{
		"enabled_types": map[string]bool{
			"l7":      p.config.EnableL7,
			"l4":      p.config.EnableL4,
			"metrics": p.config.EnableMetrics,
		},
		"l7_sent":         l7Sent,
		"l7_dropped":      l7Dropped,
		"l4_sent":         l4Sent,
		"l4_dropped":      l4Dropped,
		"metrics_sent":    metricsSent,
		"metrics_dropped": metricsDropped,
		"queue_length":    queueLen,
	}

	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		sent, errors := p.kafkaProducer.GetStats()
		response["kafka_producer"] = map[string]interface{}{
			"enabled": true,
			"sent":    sent,
			"errors":  errors,
		}
	}

	if p.kafkaConsumer != nil && p.kafkaConsumer.enabled {
		response["kafka_consumer"] = map[string]interface{}{
			"enabled":  true,
			"received": p.kafkaConsumer.GetStats(),
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (p *Processor) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.l7Sender.flush()
	p.l4Sender.flush()
	p.metricsSender.flush()
	w.Write([]byte(`{"status":"flushed"}`))
}
