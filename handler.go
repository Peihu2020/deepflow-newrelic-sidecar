package main

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
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
	mux.HandleFunc(p.config.ProfilerEndpoint, p.handleProfilerData)
	mux.HandleFunc("/health", p.handleHealth)
	mux.HandleFunc("/stats", p.handleStats)
	mux.HandleFunc("/api/flush", p.handleFlush)
	return mux
}

// handleProfilerData handles profiler data from DeepFlow agent
func (p *Processor) handleProfilerData(w http.ResponseWriter, r *http.Request) {
	// Consumer-only mode: reject HTTP requests
	if p.config.ConsumerOnlyMode {
		http.Error(w, "This instance only consumes from Kafka", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !p.config.EnableProfiler {
		http.Error(w, "Profiler disabled", http.StatusForbidden)
		return
	}

	if !p.limiter.Allow() {
		atomic.AddInt64(&p.droppedRequests, 1)
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Read body (50MB limit for profiler)
	body, err := io.ReadAll(io.LimitReader(r.Body, 50*1024*1024))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Process profiler data
	if err := p.processProfilerData(body); err != nil {
		log.Printf("[ERROR] Failed to process profiler data: %v", err)
		http.Error(w, "Failed to process data", http.StatusInternalServerError)
		return
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "accepted",
		"type":   "profiler",
		"size":   len(body),
	})
}

// handler.go
func (p *Processor) handleDataAsync(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > 1000*1024*1024 { // 1000MB limit
		http.Error(w, "Memory pressure", http.StatusServiceUnavailable)
		return
	}
	// ========== Consumer-only 模式：拒绝 HTTP 请求 ==========
	if p.config.ConsumerOnlyMode {
		http.Error(w, "This instance only consumes from Kafka", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !p.limiter.Allow() {
		atomic.AddInt64(&p.droppedRequests, 1)
		dropped := atomic.LoadInt64(&p.droppedRequests)
		if dropped%1000 == 0 {
			log.Printf("[WARN] Rate limit exceeded, rejected %d requests total", dropped)
		}
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// 写入 Kafka（如果启用）
	if p.kafkaProducer != nil && p.kafkaProducer.enabled {
		if err := p.kafkaProducer.Send(body); err != nil {
			log.Printf("[WARN] Failed to send to Kafka: %v", err)
		}
	}

	// 直接处理（放入 workQueue）
	select {
	case p.workQueue <- body:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"accepted"}`))
	case <-time.After(50 * time.Millisecond): // ← Add timeout
		atomic.AddInt64(&p.droppedRequests, 1)
		http.Error(w, "Queue full", http.StatusServiceUnavailable)
	}
}

func (p *Processor) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped, queueLen := p.GetStats()
	profilerSent, profilerSamples, profilerDropped := p.GetProfilerStats()
	response := map[string]interface{}{
		"status": "healthy",
		"enabled_types": map[string]bool{
			"l7":       p.config.EnableL7,
			"l4":       p.config.EnableL4,
			"metrics":  p.config.EnableMetrics,
			"profiler": p.config.EnableProfiler, // NEW
		},
		"stats": map[string]interface{}{
			"l7_sent":          l7Sent,
			"l7_dropped":       l7Dropped,
			"l4_sent":          l4Sent,
			"l4_dropped":       l4Dropped,
			"metrics_sent":     metricsSent,
			"metrics_dropped":  metricsDropped,
			"profiler_sent":    profilerSent,    // NEW
			"profiler_samples": profilerSamples, // NEW
			"profiler_dropped": profilerDropped, // NEW
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
