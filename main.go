package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

// ============================================================================
// Configuration
// ============================================================================

type Config struct {
	// NewRelic config
	NewRelicLicense   string
	NewRelicAccountID string
	NewRelicDebugMode bool

	// Sidecar config
	BatchSize        int
	FlushInterval    time.Duration
	LogLevel         string
	HTTPPort         string
	DebugDir         string
	SaveDebugData    bool
	MaxQueueSize     int
	WorkerCount      int
	RateLimitPerSec  int
	RateLimitBurst   int
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration

	// Log type filters (which types to process)
	EnableL7      bool
	EnableL4      bool
	EnableMetrics bool

	// HTTP mode config
	DeepFlowHTTPEndpoint string
}

// ============================================================================
// NewRelic Client
// ============================================================================

type NewRelicClient struct {
	license    string
	accountID  string
	client     *http.Client
	debug      bool
	errorCount int64
}

func NewNewRelicClient(license, accountID string, debug bool) *NewRelicClient {
	return &NewRelicClient{
		license:   license,
		accountID: accountID,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		debug: debug,
	}
}

func (c *NewRelicClient) SendLogs(logs []json.RawMessage) error {
	if len(logs) == 0 {
		return nil
	}

	var eventList []map[string]interface{}
	for _, log := range logs {
		var event map[string]interface{}
		if err := json.Unmarshal(log, &event); err != nil {
			continue
		}
		eventList = append(eventList, event)
	}

	if len(eventList) == 0 {
		return nil
	}

	payload, err := json.Marshal(eventList)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", c.accountID)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Insert-Key", c.license)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		return nil
	}

	// 413 错误只每 100 次打印一次
	if resp.StatusCode == 413 {
		atomic.AddInt64(&c.errorCount, 1)
		if atomic.LoadInt64(&c.errorCount)%100 == 1 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("[WARN] HTTP 413 (payload too large) - errors: %d", atomic.LoadInt64(&c.errorCount))
			if len(body) > 0 && c.debug {
				log.Printf("[DEBUG] Response: %s", string(body))
			}
		}
		return fmt.Errorf("HTTP 413")
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
}

// ============================================================================
// Optimized BatchSender with Bounded Queue
// ============================================================================

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
	// If not enabled, return a disabled sender (no ticker, no processing)
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
		// Silently ignore if not enabled
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
		// 每 100 次错误打印一次
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

// ============================================================================
// Optimized Processor with Worker Pool
// ============================================================================

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
}

func (p *Processor) startHTTPServer() {
	p.httpServer = &http.Server{
		Addr:         ":" + p.config.HTTPPort,
		Handler:      p.buildHTTPHandler(),
		ReadTimeout:  p.config.HTTPReadTimeout,
		WriteTimeout: p.config.HTTPWriteTimeout,
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		log.Printf("HTTP server listening on port %s", p.config.HTTPPort)
		if err := p.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
		// 节流打印限流警告
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

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"accepted"}`))

	select {
	case p.workQueue <- body:
	default:
		// 节流打印队列满警告
		atomic.AddInt64(&p.droppedRequests, 1)
		dropped := atomic.LoadInt64(&p.droppedRequests)
		if dropped%1000 == 0 {
			log.Printf("[WARN] Work queue full, dropped %d requests total", dropped)
		}
	}
}

func (p *Processor) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	l7Sent, l7Dropped := p.l7Sender.GetStats()
	l4Sent, l4Dropped := p.l4Sender.GetStats()
	metricsSent, metricsDropped := p.metricsSender.GetStats()

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
			"queue_length":     len(p.workQueue),
			"requests_dropped": atomic.LoadInt64(&p.droppedRequests),
		},
	}
	json.NewEncoder(w).Encode(response)
}

func (p *Processor) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	l7Sent, l7Dropped := p.l7Sender.GetStats()
	l4Sent, l4Dropped := p.l4Sender.GetStats()
	metricsSent, metricsDropped := p.metricsSender.GetStats()

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
		"queue_length":    len(p.workQueue),
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

func (p *Processor) processL7Line(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	if attrs, ok := item["attributes"].(map[string]interface{}); ok {
		for k, v := range attrs {
			item["attr_"+k] = v
		}
		delete(item, "attributes")
	}

	if traceIds, ok := item["trace_ids"].([]interface{}); ok && len(traceIds) > 0 {
		if traceIdStr, ok := traceIds[0].(string); ok {
			item["trace_id"] = traceIdStr
		}
		delete(item, "trace_ids")
	}

	item["eventType"] = "DeepFlowL7Log"
	item["hostname"] = hostname
	item["ingest_timestamp"] = time.Now().UnixMilli()

	if duration, ok := item["response_duration"].(float64); ok {
		item["response_duration_ms"] = duration / 1000.0
	}

	jsonBytes, err := json.Marshal(item)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.l7Sender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func (p *Processor) processL4Line(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	if duration, ok := item["duration"].(float64); ok {
		item["duration_ms"] = duration / 1000.0
	}

	if rtt, ok := item["rtt"].(float64); ok {
		item["rtt_ms"] = rtt / 1000.0
	}
	if rttClientMax, ok := item["rtt_client_max"].(float64); ok {
		item["rtt_client_max_ms"] = rttClientMax / 1000.0
	}
	if rttServerMax, ok := item["rtt_server_max"].(float64); ok {
		item["rtt_server_max_ms"] = rttServerMax / 1000.0
	}

	if byteTx, ok := item["byte_tx"].(float64); ok {
		if duration, ok := item["duration"].(float64); ok && duration > 0 {
			throughputMbps := (byteTx * 8) / (duration / 1000000.0) / 1000000.0
			item["throughput_tx_mbps"] = throughputMbps
		}
	}
	if byteRx, ok := item["byte_rx"].(float64); ok {
		if duration, ok := item["duration"].(float64); ok && duration > 0 {
			throughputMbps := (byteRx * 8) / (duration / 1000000.0) / 1000000.0
			item["throughput_rx_mbps"] = throughputMbps
		}
	}

	if packetTx, ok := item["packet_tx"].(float64); ok {
		if retransTx, ok := item["retrans_tx"].(float64); ok && packetTx > 0 {
			retransRate := (retransTx / packetTx) * 100
			item["retransmission_rate_tx_pct"] = retransRate
		}
	}
	if packetRx, ok := item["packet_rx"].(float64); ok {
		if retransRx, ok := item["retrans_rx"].(float64); ok && packetRx > 0 {
			retransRate := (retransRx / packetRx) * 100
			item["retransmission_rate_rx_pct"] = retransRate
		}
	}

	item["eventType"] = "DeepFlowL4Log"
	item["hostname"] = hostname
	item["ingest_timestamp"] = time.Now().UnixMilli()

	jsonBytes, err := json.Marshal(item)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.l4Sender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func (p *Processor) processMetricsLine(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	flattened := flattenMetrics(item)
	flattened["eventType"] = "DeepFlowMetrics"
	flattened["hostname"] = hostname
	flattened["ingest_timestamp"] = time.Now().UnixMilli()

	jsonBytes, err := json.Marshal(flattened)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.metricsSender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func (p *Processor) Stop() {
	close(p.stopCh)
	close(p.workQueue)
	p.workerWg.Wait()

	p.l7Sender.Stop()
	p.l4Sender.Stop()
	p.metricsSender.Stop()

	if p.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.httpServer.Shutdown(ctx)
	}

	p.wg.Wait()
	log.Println("Sidecar stopped")
}

// ============================================================================
// Helper Functions
// ============================================================================

func flattenMetrics(data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})

	if ts, ok := data["timestamp"]; ok {
		flattened["timestamp"] = ts
	}

	if tagger, ok := data["tagger"].(map[string]interface{}); ok {
		for k, v := range tagger {
			if k == "code" {
				if codeMap, ok := v.(map[string]interface{}); ok {
					if bits, ok := codeMap["bits"]; ok {
						flattened["tagger_code_bits"] = bits
					}
				}
				continue
			}
			if k == "mac" || k == "mac1" {
				if macArr, ok := v.([]interface{}); ok && len(macArr) >= 6 {
					flattened["tagger_"+k] = fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
						toByte(macArr[0]), toByte(macArr[1]), toByte(macArr[2]),
						toByte(macArr[3]), toByte(macArr[4]), toByte(macArr[5]))
					continue
				}
			}
			flattened["tagger_"+k] = v
		}
	}

	if meter, ok := data["meter"].(map[string]interface{}); ok {
		if app, ok := meter["App"].(map[string]interface{}); ok {
			if traffic, ok := app["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["app_traffic_"+k] = v
				}
			}
			if latency, ok := app["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
					if (k == "rrt_max" || k == "rrt_sum") && v != nil {
						if rrtVal, ok := v.(float64); ok {
							flattened["app_latency_"+k] = rrtVal
							flattened["app_latency_"+k+"_ms"] = rrtVal / 1000.0
						} else {
							flattened["app_latency_"+k] = v
						}
					} else {
						flattened["app_latency_"+k] = v
					}
				}
			}
			if anomaly, ok := app["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["app_anomaly_"+k] = v
				}
			}
		}

		if flow, ok := meter["Flow"].(map[string]interface{}); ok {
			if traffic, ok := flow["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["flow_traffic_"+k] = v
				}
			}
			if latency, ok := flow["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
					flattened["flow_latency_"+k] = v
				}
			}
			if performance, ok := flow["performance"].(map[string]interface{}); ok {
				for k, v := range performance {
					flattened["flow_performance_"+k] = v
				}
			}
			if anomaly, ok := flow["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["flow_anomaly_"+k] = v
				}
			}
		}
	}

	if flags, ok := data["flags"].(map[string]interface{}); ok {
		if bits, ok := flags["bits"]; ok {
			flattened["flags_bits"] = bits
		}
	}

	if appTrafficRequest, ok := flattened["app_traffic_request"].(float64); ok {
		flattened["has_requests"] = appTrafficRequest > 0
	}
	if appLatencyRrtMaxMs, ok := flattened["app_latency_rrt_max_ms"].(float64); ok {
		flattened["app_response_time_ms"] = appLatencyRrtMaxMs
	}
	if appAnomalyClientError, ok := flattened["app_anomaly_client_error"].(float64); ok {
		flattened["has_client_errors"] = appAnomalyClientError > 0
	}
	if appAnomalyServerError, ok := flattened["app_anomaly_server_error"].(float64); ok {
		flattened["has_server_errors"] = appAnomalyServerError > 0
	}
	if appAnomalyTimeout, ok := flattened["app_anomaly_timeout"].(float64); ok {
		flattened["has_timeouts"] = appAnomalyTimeout > 0
	}

	return flattened
}

func toByte(v interface{}) byte {
	switch val := v.(type) {
	case float64:
		return byte(val)
	case int:
		return byte(val)
	default:
		return 0
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		fmt.Sscanf(value, "%d", &intVal)
		return intVal
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return strings.ToLower(value) == "true" || value == "1"
	}
	return defaultValue
}

// ============================================================================
// Main
// ============================================================================

func main() {
	godotenv.Load()

	config := Config{
		NewRelicLicense:   getEnv("NEW_RELIC_LICENSE_KEY", ""),
		NewRelicAccountID: getEnv("NEW_RELIC_ACCOUNT_ID", ""),
		NewRelicDebugMode: getEnvBool("NEW_RELIC_DEBUG_MODE", false),

		BatchSize:        getEnvInt("BATCH_SIZE", 200),
		FlushInterval:    getEnvDuration("FLUSH_INTERVAL", 3*time.Second),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		HTTPPort:         getEnv("HTTP_PORT", "8080"),
		DebugDir:         getEnv("DEBUG_DIR", "./debug"),
		SaveDebugData:    getEnvBool("SAVE_DEBUG_DATA", true),
		MaxQueueSize:     getEnvInt("MAX_QUEUE_SIZE", 10000),
		WorkerCount:      getEnvInt("WORKER_COUNT", 4),
		RateLimitPerSec:  getEnvInt("RATE_LIMIT_PER_SEC", 20000),
		RateLimitBurst:   getEnvInt("RATE_LIMIT_BURST", 50000),
		HTTPReadTimeout:  getEnvDuration("HTTP_READ_TIMEOUT", 30*time.Second),
		HTTPWriteTimeout: getEnvDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),

		// Log type filters (set to true/false based on your needs)
		EnableL7:      getEnvBool("ENABLE_L7", true),
		EnableL4:      getEnvBool("ENABLE_L4", false),
		EnableMetrics: getEnvBool("ENABLE_METRICS", false),

		DeepFlowHTTPEndpoint: getEnv("DEEPFLOW_HTTP_ENDPOINT", "/api/deepflow-data"),
	}

	if config.NewRelicLicense == "" || config.NewRelicAccountID == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY and NEW_RELIC_ACCOUNT_ID are required")
	}

	if config.SaveDebugData {
		os.MkdirAll(config.DebugDir, 0755)
	}

	log.Printf("Starting sidecar in HTTP mode")
	log.Printf("Enabled types - L7: %v, L4: %v, Metrics: %v", config.EnableL7, config.EnableL4, config.EnableMetrics)
	log.Printf("Workers: %d, Queue Size: %d, Rate Limit: %d/s", config.WorkerCount, config.MaxQueueSize, config.RateLimitPerSec)

	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, config.NewRelicDebugMode)

	processor := &Processor{
		config:    config,
		nrClient:  nrClient,
		stopCh:    make(chan struct{}),
		workQueue: make(chan []byte, config.MaxQueueSize),
		limiter:   rate.NewLimiter(rate.Limit(config.RateLimitPerSec), config.RateLimitBurst),
	}

	processor.l7Sender = NewBatchSender(nrClient, "DeepFlowL7Log", &config, config.EnableL7)
	processor.l4Sender = NewBatchSender(nrClient, "DeepFlowL4Log", &config, config.EnableL4)
	processor.metricsSender = NewBatchSender(nrClient, "DeepFlowMetrics", &config, config.EnableMetrics)

	processor.l7Sender.Start()
	processor.l4Sender.Start()
	processor.metricsSender.Start()

	for i := 0; i < config.WorkerCount; i++ {
		processor.workerWg.Add(1)
		go processor.worker(i)
	}

	processor.startHTTPServer()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			l7Sent, l7Dropped := processor.l7Sender.GetStats()
			l4Sent, l4Dropped := processor.l4Sender.GetStats()
			metricsSent, metricsDropped := processor.metricsSender.GetStats()
			log.Printf("📊 Stats - L7: sent=%d dropped=%d, L4: sent=%d dropped=%d, Metrics: sent=%d dropped=%d, Queue: %d/%d",
				l7Sent, l7Dropped, l4Sent, l4Dropped, metricsSent, metricsDropped,
				len(processor.workQueue), config.MaxQueueSize)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	processor.Stop()
}
