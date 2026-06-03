package main

import (
	"bufio"
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
	// NewRelic config
	NewRelicLicense   string
	NewRelicAccountID string
	NewRelicDebugMode bool

	// Sidecar config
	BatchSize     int
	FlushInterval time.Duration
	LogLevel      string
	HTTPPort      string
	DebugDir      string
	SaveDebugData bool

	// DeepFlow source mode
	DeepFlowSourceMode string // "http" or "file"

	// File mode config
	DeepFlowLogDir       string
	DeepFlowPollInterval time.Duration
	DeepFlowStartFromEnd bool

	// HTTP mode config
	DeepFlowHTTPEndpoint string
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
	fileWatcher   *FileWatcher
}

type FileWatcher struct {
	config          *Processor
	stopCh          chan struct{}
	wg              sync.WaitGroup
	fileOffsets     map[string]int64  // Last read offset for each file
	fileInodes      map[string]uint64 // Inode number to detect rotation
	fileLastSize    map[string]int64  // Last file size to detect truncation
	processedInodes map[uint64]bool   // Track which inodes we've processed
	mu              sync.Mutex
}

func main() {
	godotenv.Load()

	config := Config{
		// NewRelic config
		NewRelicLicense:   getEnv("NEW_RELIC_LICENSE_KEY", ""),
		NewRelicAccountID: getEnv("NEW_RELIC_ACCOUNT_ID", ""),
		NewRelicDebugMode: getEnvBool("NEW_RELIC_DEBUG_MODE", false),

		// Sidecar config
		BatchSize:     getEnvInt("BATCH_SIZE", 100),
		FlushInterval: getEnvDuration("FLUSH_INTERVAL", 5*time.Second),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		HTTPPort:      getEnv("HTTP_PORT", "8080"),
		DebugDir:      getEnv("DEBUG_DIR", "./debug"),
		SaveDebugData: getEnvBool("SAVE_DEBUG_DATA", true),

		// DeepFlow source mode
		DeepFlowSourceMode: getEnv("DEEPFLOW_SOURCE_MODE", "http"),

		// File mode config
		DeepFlowLogDir:       getEnv("DEEPFLOW_LOG_DIR", "/var/log/deepflow-agent/"),
		DeepFlowPollInterval: getEnvDuration("DEEPFLOW_POLL_INTERVAL", 5*time.Second),
		DeepFlowStartFromEnd: getEnvBool("DEEPFLOW_START_FROM_END", true),

		// HTTP mode config
		DeepFlowHTTPEndpoint: getEnv("DEEPFLOW_HTTP_ENDPOINT", "/api/deepflow-data"),
	}

	if config.NewRelicLicense == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY is required")
	}

	if config.NewRelicAccountID == "" {
		log.Fatal("NEW_RELIC_ACCOUNT_ID is required")
	}

	if config.SaveDebugData {
		if err := os.MkdirAll(config.DebugDir, 0755); err != nil {
			log.Printf("Warning: Failed to create debug directory: %v", err)
		}
	}

	log.Printf("Starting sidecar in %s mode", config.DeepFlowSourceMode)
	log.Printf("Log level: %s", config.LogLevel)
	log.Printf("Start from end: %v", config.DeepFlowStartFromEnd)

	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, config.NewRelicDebugMode)

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

	// Start appropriate data source based on mode
	if config.DeepFlowSourceMode == "http" {
		processor.startHTTPServer()
	} else if config.DeepFlowSourceMode == "file" {
		processor.startFileWatcher()
	} else {
		log.Fatalf("Invalid DEEPFLOW_SOURCE_MODE: %s. Must be 'http' or 'file'", config.DeepFlowSourceMode)
	}

	// Print stats periodically
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if config.LogLevel == "info" {
				log.Printf("📊 Total Sent - L7: %d, L4: %d, Metrics: %d",
					processor.l7Sender.totalSent,
					processor.l4Sender.totalSent,
					processor.metricsSender.totalSent)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	processor.Stop()
}

func (p *Processor) startFileWatcher() {
	p.fileWatcher = &FileWatcher{
		config:          p,
		stopCh:          make(chan struct{}),
		fileOffsets:     make(map[string]int64),
		fileInodes:      make(map[string]uint64),
		fileLastSize:    make(map[string]int64),
		processedInodes: make(map[uint64]bool),
	}

	p.fileWatcher.Start()
	log.Printf("File watcher started. Monitoring directory: %s", p.config.DeepFlowLogDir)
	log.Printf("Monitoring files: l7_flow_log, l4_flow_log, flow_metrics (NOT .pre files)")
}

func (p *Processor) startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc(p.config.DeepFlowHTTPEndpoint, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		p.handleData(w, r)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status":"healthy","mode":"%s"}`, p.config.DeepFlowSourceMode)))
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		offsetInfo := make(map[string]interface{})
		if p.fileWatcher != nil {
			p.fileWatcher.mu.Lock()
			for k, v := range p.fileWatcher.fileOffsets {
				offsetInfo[k] = v
			}
			p.fileWatcher.mu.Unlock()
		}

		response := map[string]interface{}{
			"mode":         p.config.DeepFlowSourceMode,
			"l7_sent":      p.l7Sender.totalSent,
			"l4_sent":      p.l4Sender.totalSent,
			"metrics_sent": p.metricsSender.totalSent,
			"offsets":      offsetInfo,
		}

		json.NewEncoder(w).Encode(response)
	})

	mux.HandleFunc("/api/flush", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		p.l7Sender.flush()
		p.l4Sender.flush()
		p.metricsSender.flush()

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"flushed"}`))
	})

	mux.HandleFunc("/api/reset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if p.fileWatcher != nil {
			p.fileWatcher.mu.Lock()
			p.fileWatcher.fileOffsets = make(map[string]int64)
			p.fileWatcher.fileInodes = make(map[string]uint64)
			p.fileWatcher.fileLastSize = make(map[string]int64)
			p.fileWatcher.processedInodes = make(map[uint64]bool)
			p.fileWatcher.mu.Unlock()

			log.Printf("🔄 File watcher state reset")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"reset","message":"File watcher reset, will re-read files on next poll"}`))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"error","message":"Not in file mode"}`))
		}
	})

	p.httpServer = &http.Server{Addr: ":" + p.config.HTTPPort, Handler: mux}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		log.Printf("HTTP server listening on port %s", p.config.HTTPPort)
		if err := p.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP error: %v", err)
		}
	}()
}

// FileWatcher Methods
func (f *FileWatcher) Start() {
	f.wg.Add(1)
	go f.watchFiles()
}

func (f *FileWatcher) watchFiles() {
	defer f.wg.Done()

	// Initial scan to set up file positions
	f.initializeFilePositions()

	ticker := time.NewTicker(f.config.config.DeepFlowPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
			f.scanAndProcessFiles()
		}
	}
}

func (f *FileWatcher) initializeFilePositions() {
	filesToWatch := []string{
		"l7_flow_log",
		"l4_flow_log",
		"flow_metrics",
	}

	for _, fileName := range filesToWatch {
		filePath := filepath.Join(f.config.config.DeepFlowLogDir, fileName)
		f.initializeFile(filePath, fileName)
	}
}

func (f *FileWatcher) initializeFile(filePath, fileName string) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if f.config.config.LogLevel == "debug" {
			log.Printf("File %s does not exist yet, will wait for it", filePath)
		}
		return
	}

	// Get inode
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		log.Printf("Cannot get inode for %s, skipping", filePath)
		return
	}
	inode := stat.Ino

	// Check if we've processed this inode before
	f.mu.Lock()
	alreadyProcessed := f.processedInodes[inode]
	f.mu.Unlock()

	var initialOffset int64
	if alreadyProcessed {
		// Already processed this file, start from end
		initialOffset = fileInfo.Size()
		if f.config.config.LogLevel == "info" {
			log.Printf("Already processed %s (inode: %d), starting from end (offset: %d)",
				fileName, inode, initialOffset)
		}
	} else {
		// First time seeing this file
		if f.config.config.DeepFlowStartFromEnd {
			initialOffset = fileInfo.Size()
			log.Printf("Starting to watch %s from end (offset: %d bytes, inode: %d, size: %d)",
				fileName, initialOffset, inode, fileInfo.Size())
		} else {
			initialOffset = 0
			log.Printf("Starting to watch %s from beginning (offset: %d bytes, inode: %d, size: %d)",
				fileName, initialOffset, inode, fileInfo.Size())
		}
	}

	f.mu.Lock()
	f.fileOffsets[fileName] = initialOffset
	f.fileInodes[fileName] = inode
	f.fileLastSize[fileName] = fileInfo.Size()
	f.processedInodes[inode] = true
	f.mu.Unlock()

	// Read if starting from beginning
	if initialOffset == 0 && fileInfo.Size() > 0 {
		log.Printf("📖 Reading existing %d bytes from %s", fileInfo.Size(), fileName)
		f.readNewLines(filePath, fileName, 0)
	}
}

func (f *FileWatcher) scanAndProcessFiles() {
	filesToWatch := []string{
		"l7_flow_log",
		"l4_flow_log",
		"flow_metrics",
	}

	for _, fileName := range filesToWatch {
		filePath := filepath.Join(f.config.config.DeepFlowLogDir, fileName)
		f.processFile(filePath, fileName)
	}
}

func (f *FileWatcher) processFile(filePath, fileName string) {
	// Check if file exists
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		// File doesn't exist, clean up state
		f.mu.Lock()
		delete(f.fileOffsets, fileName)
		delete(f.fileInodes, fileName)
		delete(f.fileLastSize, fileName)
		f.mu.Unlock()
		return
	}

	// Get current inode
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		log.Printf("Cannot get inode for %s, skipping", filePath)
		return
	}
	currentInode := stat.Ino

	// Get current state
	f.mu.Lock()
	lastInode, inodeExists := f.fileInodes[fileName]
	lastOffset, offsetExists := f.fileOffsets[fileName]
	lastSize, sizeExists := f.fileLastSize[fileName]
	// alreadyProcessed := f.processedInodes[currentInode]
	f.mu.Unlock()

	// Case 1: File was rotated (inode changed)
	if inodeExists && currentInode != lastInode {
		f.handleFileRotation(filePath, fileName, fileInfo, currentInode)
		return
	}

	// Case 2: First time seeing this file
	if !inodeExists {
		f.initializeFile(filePath, fileName)
		return
	}

	// Case 3: File was truncated (size decreased but inode same)
	if sizeExists && fileInfo.Size() < lastSize {
		f.handleFileTruncation(filePath, fileName, fileInfo)
		return
	}

	// Case 4: No new data
	if offsetExists && lastOffset >= fileInfo.Size() {
		return
	}

	// Case 5: Read only new data from lastOffset to EOF
	if offsetExists && lastOffset < fileInfo.Size() {
		f.readNewLines(filePath, fileName, lastOffset)
	}
}

func (f *FileWatcher) handleFileRotation(filePath, fileName string, fileInfo os.FileInfo, newInode uint64) {
	f.mu.Lock()
	oldInode := f.fileInodes[fileName]
	alreadyProcessed := f.processedInodes[newInode]
	f.mu.Unlock()

	log.Printf("🔄 File %s was rotated (inode: %d -> %d)", fileName, oldInode, newInode)

	var newOffset int64

	// If we've already processed this inode, start from end
	if alreadyProcessed {
		newOffset = fileInfo.Size()
		log.Printf("  Already processed inode %d, starting from end (offset: %d)", newInode, newOffset)
	} else {
		// First time seeing this inode - decide based on config
		if f.config.config.DeepFlowStartFromEnd {
			newOffset = fileInfo.Size()
			log.Printf("  New file %s size: %d bytes, starting at offset: %d (skip existing)",
				fileName, fileInfo.Size(), newOffset)
		} else {
			newOffset = 0
			log.Printf("  New file %s size: %d bytes, starting at offset: %d (will read existing)",
				fileName, fileInfo.Size(), newOffset)
		}
	}

	// Update state for new file
	f.mu.Lock()
	f.fileOffsets[fileName] = newOffset
	f.fileInodes[fileName] = newInode
	f.fileLastSize[fileName] = fileInfo.Size()
	f.processedInodes[newInode] = true
	f.mu.Unlock()

	// Read existing data if starting from beginning
	if newOffset == 0 && fileInfo.Size() > 0 {
		log.Printf("📖 Reading %d bytes from newly rotated %s", fileInfo.Size(), fileName)
		f.readNewLines(filePath, fileName, 0)
	}
}

func (f *FileWatcher) handleFileTruncation(filePath, fileName string, fileInfo os.FileInfo) {
	f.mu.Lock()
	oldSize := f.fileLastSize[fileName]
	f.mu.Unlock()

	log.Printf("⚠️ File %s was truncated (was %d bytes, now %d bytes)", fileName, oldSize, fileInfo.Size())

	// Reset offset based on config
	var newOffset int64
	if f.config.config.DeepFlowStartFromEnd {
		newOffset = fileInfo.Size()
	} else {
		newOffset = 0
	}

	f.mu.Lock()
	f.fileOffsets[fileName] = newOffset
	f.fileLastSize[fileName] = fileInfo.Size()
	f.mu.Unlock()

	// Read existing data if starting from beginning
	if newOffset == 0 && fileInfo.Size() > 0 {
		f.readNewLines(filePath, fileName, 0)
	}
}

func (f *FileWatcher) readNewLines(filePath, fileName string, offset int64) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open %s: %v", filePath, err)
		return
	}
	defer file.Close()

	// Seek to last position
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		log.Printf("Failed to seek in %s: %v", filePath, err)
		return
	}

	// Read new lines
	scanner := bufio.NewScanner(file)
	// Handle large lines (up to 10MB per line)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	var lines []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading %s: %v", filePath, err)
		return
	}

	// Get new offset (current position in file)
	newOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("Failed to get new offset for %s: %v", filePath, err)
		return
	}

	// Update file size
	fileInfo, _ := os.Stat(filePath)

	// CRITICAL: Only update offset if we actually read new data
	if newOffset > offset {
		f.mu.Lock()
		f.fileOffsets[fileName] = newOffset
		f.fileLastSize[fileName] = fileInfo.Size()
		f.mu.Unlock()

		if f.config.config.LogLevel == "debug" || len(lines) > 0 {
			log.Printf("📖 Read %d new lines from %s (offset: %d -> %d, bytes read: %d)",
				len(lines), fileName, offset, newOffset, newOffset-offset)
		}
	}

	// Process the lines
	if len(lines) > 0 {
		f.processLines(lines, fileName)
	}
}

func (f *FileWatcher) processLines(lines []string, source string) {
	var l7Lines, l4Lines, metricsLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Route based on source file name
		switch source {
		case "l7_flow_log":
			l7Lines = append(l7Lines, line)
		case "l4_flow_log":
			l4Lines = append(l4Lines, line)
		case "flow_metrics":
			metricsLines = append(metricsLines, line)
		default:
			// Auto-detect by content (fallback)
			if strings.Contains(line, "request_domain") || strings.Contains(line, "response_code") {
				l7Lines = append(l7Lines, line)
			} else if strings.Contains(line, "byte_tx") && strings.Contains(line, "rtt") {
				l4Lines = append(l4Lines, line)
			} else if strings.Contains(line, "tagger") && strings.Contains(line, "meter") {
				metricsLines = append(metricsLines, line)
			}
		}
	}

	// Process each type
	if len(l7Lines) > 0 {
		f.config.processL7Logs(l7Lines)
		if f.config.config.LogLevel == "info" {
			log.Printf("📝 Queued %d L7 logs from file", len(l7Lines))
		}
	}
	if len(l4Lines) > 0 {
		f.config.processL4Logs(l4Lines)
		if f.config.config.LogLevel == "info" {
			log.Printf("🔌 Queued %d L4 logs from file", len(l4Lines))
		}
	}
	if len(metricsLines) > 0 {
		f.config.processMetricsLogs(metricsLines)
		if f.config.config.LogLevel == "info" {
			log.Printf("📊 Queued %d metrics logs from file", len(metricsLines))
		}
	}
}

// Log Processing Methods
func (p *Processor) processL7Logs(lines []string) {
	hostname, _ := os.Hostname()

	for _, dataStr := range lines {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
			if p.config.LogLevel == "debug" {
				log.Printf("Failed to parse L7 log: %v", err)
			}
			continue
		}

		// Flatten attributes
		if attrs, ok := item["attributes"].(map[string]interface{}); ok {
			for k, v := range attrs {
				item["attr_"+k] = v
			}
			delete(item, "attributes")
		}

		// Flatten trace_ids
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
			continue
		}

		if p.config.SaveDebugData {
			p.l7Sender.saveToDebugFile(jsonBytes)
		}
		p.l7Sender.Add(json.RawMessage(jsonBytes))
	}
}

func (p *Processor) processL4Logs(lines []string) {
	hostname, _ := os.Hostname()

	for _, dataStr := range lines {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
			if p.config.LogLevel == "debug" {
				log.Printf("Failed to parse L4 log: %v", err)
			}
			continue
		}

		// Add duration_ms if duration field exists (duration is in microseconds)
		if duration, ok := item["duration"].(float64); ok {
			item["duration_ms"] = duration / 1000.0
		}

		// Add rtt_ms if rtt fields exist (rtt is in microseconds)
		if rtt, ok := item["rtt"].(float64); ok {
			item["rtt_ms"] = rtt / 1000.0
		}
		if rttClientMax, ok := item["rtt_client_max"].(float64); ok {
			item["rtt_client_max_ms"] = rttClientMax / 1000.0
		}
		if rttServerMax, ok := item["rtt_server_max"].(float64); ok {
			item["rtt_server_max_ms"] = rttServerMax / 1000.0
		}

		// Add throughput in Mbps for better analysis
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

		// Add retransmission rate if packet counts exist
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
			continue
		}

		if p.config.SaveDebugData {
			p.l4Sender.saveToDebugFile(jsonBytes)
		}
		p.l4Sender.Add(json.RawMessage(jsonBytes))
	}
}

func (p *Processor) processMetricsLogs(lines []string) {
	hostname, _ := os.Hostname()
	count := 0
	httpBinCount := 0

	for _, dataStr := range lines {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
			if p.config.LogLevel == "debug" {
				log.Printf("Failed to parse metrics log: %v", err)
			}
			continue
		}

		flattened := flattenMetrics(item)
		flattened["eventType"] = "DeepFlowMetrics"
		flattened["hostname"] = hostname
		flattened["ingest_timestamp"] = time.Now().UnixMilli()

		// Check if this is httpbin.org related for debugging
		if endpoint, ok := flattened["tagger_endpoint"]; ok {
			if endpointStr, ok := endpoint.(string); ok && strings.Contains(endpointStr, "httpbin") {
				httpBinCount++
				if p.config.LogLevel == "debug" {
					log.Printf("🔍 httpbin.org metrics: %v", flattened)
				}
			}
		}

		jsonBytes, err := json.Marshal(flattened)
		if err != nil {
			continue
		}

		if p.config.SaveDebugData {
			p.metricsSender.saveToDebugFile(jsonBytes)
		}
		p.metricsSender.Add(json.RawMessage(jsonBytes))
		count++
	}

	if count > 0 && p.config.LogLevel == "info" {
		log.Printf("📊 Processed %d metrics logs from file (httpbin: %d)", count, httpBinCount)
	}
}

// HTTP Data Handler
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

			// Flatten attributes
			if attrs, ok := item["attributes"].(map[string]interface{}); ok {
				for k, v := range attrs {
					item["attr_"+k] = v
				}
				delete(item, "attributes")
			}

			// Flatten trace_ids
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
				continue
			}

			if p.config.SaveDebugData {
				p.l7Sender.saveToDebugFile(jsonBytes)
			}
			p.l7Sender.Add(json.RawMessage(jsonBytes))
			l7Count++

		case "l4":
			var item map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
				continue
			}

			// Add duration_ms if duration field exists (duration is in microseconds)
			if duration, ok := item["duration"].(float64); ok {
				item["duration_ms"] = duration / 1000.0
			}

			// Add rtt_ms if rtt fields exist
			if rtt, ok := item["rtt"].(float64); ok {
				item["rtt_ms"] = rtt / 1000.0
			}
			if rttClientMax, ok := item["rtt_client_max"].(float64); ok {
				item["rtt_client_max_ms"] = rttClientMax / 1000.0
			}
			if rttServerMax, ok := item["rtt_server_max"].(float64); ok {
				item["rtt_server_max_ms"] = rttServerMax / 1000.0
			}

			// Add throughput in Mbps
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

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"status":"ok","l7":%d,"l4":%d,"metrics":%d}`,
		l7Count, l4Count, metricsCount)))
}

// Helper Functions
func flattenMetrics(data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})

	// Add timestamp
	if ts, ok := data["timestamp"]; ok {
		flattened["timestamp"] = ts
	}

	// Flatten tagger fields
	if tagger, ok := data["tagger"].(map[string]interface{}); ok {
		for k, v := range tagger {
			if k == "code" {
				// Handle code.bits specially
				if codeMap, ok := v.(map[string]interface{}); ok {
					if bits, ok := codeMap["bits"]; ok {
						flattened["tagger_code_bits"] = bits
					}
				}
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

	// Flatten meter fields - handle both Flow and App metrics
	if meter, ok := data["meter"].(map[string]interface{}); ok {
		// Handle App metrics (L7 application metrics)
		if app, ok := meter["App"].(map[string]interface{}); ok {
			// Flatten App traffic
			if traffic, ok := app["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["app_traffic_"+k] = v
				}
			}
			// Flatten App latency
			if latency, ok := app["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
					// Convert RRT from microseconds to milliseconds if needed
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
			// Flatten App anomaly
			if anomaly, ok := app["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["app_anomaly_"+k] = v
				}
			}
		}

		// Handle Flow metrics (L4 network metrics)
		if flow, ok := meter["Flow"].(map[string]interface{}); ok {
			// Flatten Flow traffic
			if traffic, ok := flow["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["flow_traffic_"+k] = v
				}
			}
			// Flatten Flow latency
			if latency, ok := flow["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
					flattened["flow_latency_"+k] = v
				}
			}
			// Flatten Flow performance
			if performance, ok := flow["performance"].(map[string]interface{}); ok {
				for k, v := range performance {
					flattened["flow_performance_"+k] = v
				}
			}
			// Flatten Flow anomaly
			if anomaly, ok := flow["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["flow_anomaly_"+k] = v
				}
			}
		}
	}

	// Add flags
	if flags, ok := data["flags"].(map[string]interface{}); ok {
		if bits, ok := flags["bits"]; ok {
			flattened["flags_bits"] = bits
		}
	}

	// Add calculated fields for App metrics
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

func (p *Processor) Stop() {
	close(p.stopCh)

	if p.fileWatcher != nil {
		close(p.fileWatcher.stopCh)
		p.fileWatcher.wg.Wait()
	}

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

// BatchSender Methods
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

	err := b.nrClient.SendLogs(b.logs)
	if err != nil {
		log.Printf("❌ Failed to send %s events: %v", b.eventType, err)
	} else {
		b.totalSent += int64(len(b.logs))
		if b.config.LogLevel == "debug" {
			log.Printf("✅ Sent %d %s events (total: %d)", len(b.logs), b.eventType, b.totalSent)
		}
	}

	b.logs = b.logs[:0]
}

// Helper functions
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
