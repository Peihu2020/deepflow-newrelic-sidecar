package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	NewRelicLicense   string
	NewRelicAccountID string
	BatchSize         int
	FlushInterval     time.Duration
	LogLevel          string
	HTTPPort          string
	DebugDir          string
	SaveDebugData     bool
}

type BatchSender struct {
	logs      []json.RawMessage
	mu        sync.Mutex
	ticker    *time.Ticker
	nrClient  *NewRelicClient
	eventType string
	stopCh    chan struct{}
	wg        sync.WaitGroup
	debugFile *os.File
	debugMu   sync.Mutex
	config    *Config
	totalSent int64
}

type Processor struct {
	config        Config
	nrClient      *NewRelicClient
	l7Sender      *BatchSender
	l4Sender      *BatchSender
	metricsSender *BatchSender
	stopCh        chan struct{}
	wg            sync.WaitGroup
	httpServer    *http.Server
}

func main() {
	godotenv.Load()

	config := Config{
		NewRelicLicense:   getEnv("NEW_RELIC_LICENSE_KEY", ""),
		NewRelicAccountID: getEnv("NEW_RELIC_ACCOUNT_ID", ""),
		BatchSize:         getEnvInt("BATCH_SIZE", 10),
		FlushInterval:     getEnvDuration("FLUSH_INTERVAL", 2*time.Second),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
		HTTPPort:          getEnv("HTTP_PORT", "8080"),
		DebugDir:          getEnv("DEBUG_DIR", "./debug"),
		SaveDebugData:     getEnvBool("SAVE_DEBUG_DATA", true),
	}

	if config.NewRelicLicense == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY is required")
	}

	if config.SaveDebugData {
		os.MkdirAll(config.DebugDir, 0755)
	}

	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, false)

	processor := &Processor{
		config:   config,
		nrClient: nrClient,
		stopCh:   make(chan struct{}),
	}

	processor.l7Sender = NewBatchSender(nrClient, "DeepFlowL7Log", &config)
	processor.l4Sender = NewBatchSender(nrClient, "DeepFlowL4Log", &config)
	processor.metricsSender = NewBatchSender(nrClient, "DeepFlowMetrics", &config)

	processor.l7Sender.Start()
	processor.l4Sender.Start()
	processor.metricsSender.Start()

	processor.startHTTPServer()

	// Print stats every 10 seconds (only httpbin.org counts)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// log.Printf("📊 Total sent - L7: %d, L4: %d, Metrics: %d",
			// processor.l7Sender.totalSent, processor.l4Sender.totalSent, processor.metricsSender.totalSent)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	processor.Stop()
}

func (p *Processor) startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/deepflow-data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		p.handleData(w, r)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{
			"l7_sent": %d,
			"l4_sent": %d,
			"metrics_sent": %d
		}`,
			p.l7Sender.totalSent, p.l4Sender.totalSent, p.metricsSender.totalSent)))
	})

	p.httpServer = &http.Server{Addr: ":" + p.config.HTTPPort, Handler: mux}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP error: %v", err)
		}
	}()
}

func (p *Processor) handleData(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	lines := strings.Split(string(body), "\n")

	hostname, _ := os.Hostname()
	l7Count := 0
	l4Count := 0
	metricsCount := 0
	httpbinL7Count := 0

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
			var item map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
				continue
			}

			// FLATTEN attributes object
			if attrs, ok := item["attributes"].(map[string]interface{}); ok {
				for k, v := range attrs {
					item["attr_"+k] = v
				}
				delete(item, "attributes")
			}

			// FLATTEN trace_ids array
			if traceIds, ok := item["trace_ids"].([]interface{}); ok && len(traceIds) > 0 {
				if traceIdStr, ok := traceIds[0].(string); ok {
					item["trace_id"] = traceIdStr
				}
				delete(item, "trace_ids")
			}

			// Check if this is httpbin.org request
			isHttpBin := false
			if requestDomain, ok := item["request_domain"]; ok {
				if domainStr, ok := requestDomain.(string); ok && domainStr == "httpbin.org" {
					isHttpBin = true
					httpbinL7Count++
				}
			}

			item["eventType"] = "DeepFlowL7Log"
			item["hostname"] = hostname
			item["ingest_timestamp"] = time.Now().UnixMilli()

			jsonBytes, err := json.Marshal(item)
			if err != nil {
				continue
			}

			if p.config.SaveDebugData {
				p.l7Sender.saveToDebugFile(jsonBytes)
			}
			p.l7Sender.Add(json.RawMessage(jsonBytes))
			l7Count++

			if isHttpBin && p.config.LogLevel == "info" {
				// Print flattened httpbin event
				itemJSON, _ := json.MarshalIndent(item, "", "  ")
				log.Printf("🔍 httpbin.org event (flattened):\n%s", string(itemJSON))
			}

		case "l4":
			var item map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
				continue
			}

			item["eventType"] = "DeepFlowL4Log"
			item["hostname"] = hostname
			item["ingest_timestamp"] = time.Now().UnixMilli()

			jsonBytes, err := json.Marshal(item)
			if err != nil {
				continue
			}

			if p.config.SaveDebugData {
				p.l4Sender.saveToDebugFile(jsonBytes)
			}
			p.l4Sender.Add(json.RawMessage(jsonBytes))
			l4Count++

		case "metrics":
			var item map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
				continue
			}

			flattened := flattenMetrics(item)
			flattened["eventType"] = "DeepFlowMetrics"
			flattened["hostname"] = hostname
			flattened["ingest_timestamp"] = time.Now().UnixMilli()

			jsonBytes, err := json.Marshal(flattened)
			if err != nil {
				continue
			}

			if p.config.SaveDebugData {
				p.metricsSender.saveToDebugFile(jsonBytes)
			}
			p.metricsSender.Add(json.RawMessage(jsonBytes))
			metricsCount++
		}
	}

	if httpbinL7Count > 0 {
		log.Printf("✅ Processed %d httpbin.org L7 events (total L7: %d)", httpbinL7Count, l7Count)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"status":"ok","l7":%d,"l4":%d,"metrics":%d}`,
		l7Count, l4Count, metricsCount)))
}

func flattenMetrics(data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})

	if ts, ok := data["timestamp"]; ok {
		flattened["timestamp"] = ts
	}

	if tagger, ok := data["tagger"].(map[string]interface{}); ok {
		for k, v := range tagger {
			if k == "code" {
				continue
			}
			if k == "mac" || k == "mac1" {
				if macArr, ok := v.([]interface{}); ok && len(macArr) >= 3 {
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
					flattened["app_latency_"+k] = v
				}
			}
			if anomaly, ok := app["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["app_anomaly_"+k] = v
				}
			}
		}
	}

	if flags, ok := data["flags"].(map[string]interface{}); ok {
		if bits, ok := flags["bits"]; ok {
			flattened["flags_bits"] = bits
		}
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

func (p *Processor) Stop() {
	close(p.stopCh)

	if p.l7Sender != nil {
		p.l7Sender.Stop()
	}
	if p.l4Sender != nil {
		p.l4Sender.Stop()
	}
	if p.metricsSender != nil {
		p.metricsSender.Stop()
	}

	if p.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.httpServer.Shutdown(ctx)
	}

	p.wg.Wait()
	log.Println("Sidecar stopped")
}

func NewBatchSender(nrClient *NewRelicClient, eventType string, config *Config) *BatchSender {
	sender := &BatchSender{
		logs:      make([]json.RawMessage, 0, config.BatchSize),
		ticker:    time.NewTicker(config.FlushInterval),
		nrClient:  nrClient,
		eventType: eventType,
		stopCh:    make(chan struct{}),
		config:    config,
		totalSent: 0,
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
		filepath := filepath.Join(config.DebugDir, filename)
		file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			sender.debugFile = file
		}
	}

	return sender
}

func (b *BatchSender) saveToDebugFile(data []byte) {
	if b.debugFile == nil {
		return
	}
	b.debugMu.Lock()
	defer b.debugMu.Unlock()
	b.debugFile.Write(data)
	b.debugFile.Write([]byte("\n"))
}

func (b *BatchSender) Start() {
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
	close(b.stopCh)
	b.wg.Wait()
}

func (b *BatchSender) Add(log json.RawMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logs = append(b.logs, log)
	if len(b.logs) >= b.config.BatchSize {
		b.flushLocked()
	}
}

func (b *BatchSender) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *BatchSender) flushLocked() {
	if len(b.logs) == 0 {
		return
	}

	// Only log if this is L7 and might contain httpbin.org
	if b.eventType == "DeepFlowL7Log" && b.config.LogLevel == "info" {
		// Check if any event in batch is httpbin.org
		hasHttpBin := false
		for _, log := range b.logs {
			var item map[string]interface{}
			if err := json.Unmarshal(log, &item); err == nil {
				if requestDomain, ok := item["request_domain"]; ok {
					if domainStr, ok := requestDomain.(string); ok && domainStr == "httpbin.org" {
						hasHttpBin = true
						break
					}
				}
			}
		}
		if hasHttpBin {
			log.Printf("📤 Sending batch with %d L7 events (including httpbin.org)", len(b.logs))
		}
	}

	err := b.nrClient.SendLogs(b.logs)
	if err != nil {
		log.Printf("❌ Failed to send %s events: %v", b.eventType, err)
	} else {
		b.totalSent += int64(len(b.logs))
		// Only log for L7 events that might be httpbin.org
		if b.eventType == "DeepFlowL7Log" {
			// log.Printf("✅ Sent %d L7 events (total L7 sent: %d)", len(b.logs), b.totalSent)
		}
	}

	b.logs = b.logs[:0]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
