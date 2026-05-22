package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
)

// 日志类型常量
const (
	LogTypeL7Flow      = "l7_flow_log"
	LogTypeL4Flow      = "l4_flow_log"
	LogTypeFlowMetrics = "flow_metrics"
)

// 配置
type Config struct {
	LogDir            string
	NewRelicAppName   string
	NewRelicLicense   string
	NewRelicAccountID string
	BatchSize         int
	FlushInterval     time.Duration
	MetricsEnabled    bool
	LogLevel          string
	EventTypeL7       string
	EventTypeL4       string
	EventTypeMetrics  string
}

// 日志处理器
type LogProcessor struct {
	config        Config
	logWatchers   map[string]*LogWatcher
	offsetManager *OffsetManager
	nrClient      *NewRelicClient
	mu            sync.RWMutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// 文件监听器
type LogWatcher struct {
	filePath      string
	file          *os.File
	reader        *bufio.Reader
	lastOffset    int64
	processor     *LogProcessor
	offsetManager *OffsetManager
	mu            sync.Mutex
	logType       string
}

// 批量发送器
type BatchSender struct {
	logs      []json.RawMessage
	mu        sync.Mutex
	ticker    *time.Ticker
	processor *LogProcessor
	logType   string
}

var errTimeout = fmt.Errorf("timeout")

func main() {
	// 加载 .env 文件
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: No .env file found, using environment variables")
	}

	// 从环境变量读取配置
	config := Config{
		LogDir:            getEnv("DEEPFLOW_LOG_DIR", "/var/log/deepflow-agent"),
		NewRelicAppName:   getEnv("NEW_RELIC_APP_NAME", "deepflow-agent"),
		NewRelicLicense:   getEnv("NEW_RELIC_LICENSE_KEY", ""),
		NewRelicAccountID: getEnv("NEW_RELIC_ACCOUNT_ID", ""),
		BatchSize:         getEnvInt("BATCH_SIZE", 100),
		FlushInterval:     getEnvDuration("FLUSH_INTERVAL", 5*time.Second),
		MetricsEnabled:    getEnvBool("METRICS_ENABLED", true),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
		EventTypeL7:       getEnv("EVENT_TYPE_L7", "DeepFlowL7Log"),
		EventTypeL4:       getEnv("EVENT_TYPE_L4", "DeepFlowL4Log"),
		EventTypeMetrics:  getEnv("EVENT_TYPE_METRICS", "DeepFlowFlowMetrics"),
	}

	// 验证必需配置
	if config.NewRelicLicense == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY is required (set in .env or environment variable)")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  New Relic License: %s...", config.NewRelicLicense[:8])
	log.Printf("  New Relic Account ID: %s", config.NewRelicAccountID)
	log.Printf("  Log Directory: %s", config.LogDir)
	log.Printf("  Batch Size: %d", config.BatchSize)
	log.Printf("  Flush Interval: %v", config.FlushInterval)
	log.Printf("  Log Level: %s", config.LogLevel)
	log.Printf("  Event Types: L7=%s, L4=%s, Metrics=%s", config.EventTypeL7, config.EventTypeL4, config.EventTypeMetrics)

	// 确保日志目录存在
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	// 创建偏移量管理器 - 使用 /tmp 目录存储 .offsets.json
	offsetFilePath := filepath.Join("/tmp", "deepflow-sidecar-offsets.json")
	offsetManager := NewOffsetManager(offsetFilePath)
	defer offsetManager.Stop()

	log.Printf("Offset file stored at: %s", offsetFilePath)

	// 创建 New Relic 客户端
	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, false)

	// 创建日志处理器
	processor := &LogProcessor{
		config:        config,
		logWatchers:   make(map[string]*LogWatcher),
		offsetManager: offsetManager,
		nrClient:      nrClient,
		stopCh:        make(chan struct{}),
	}

	// 初始化批量发送器
	batchSenders := map[string]*BatchSender{
		LogTypeL7Flow:      NewBatchSender(processor, LogTypeL7Flow),
		LogTypeL4Flow:      NewBatchSender(processor, LogTypeL4Flow),
		LogTypeFlowMetrics: NewBatchSender(processor, LogTypeFlowMetrics),
	}

	// 启动文件监听
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}
	defer watcher.Close()

	if err := watcher.Add(config.LogDir); err != nil {
		log.Fatalf("Failed to watch directory: %v", err)
	}

	// 处理现有文件
	processor.wg.Add(1)
	go func() {
		defer processor.wg.Done()
		processor.processExistingFiles(batchSenders)
	}()

	// 监听新文件和文件变化
	processor.wg.Add(1)
	go func() {
		defer processor.wg.Done()
		processor.watchEvents(watcher, batchSenders)
	}()

	// 启动批量发送
	for _, sender := range batchSenders {
		sender.Start()
	}

	// 监听信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Println("DeepFlow New Relic Sidecar started")
	log.Printf("Watching directory: %s", config.LogDir)

	// 等待停止信号
	<-sigCh
	log.Println("Shutting down...")
	processor.Stop()
}

// getBaseName removes .pre suffix only, keeps .log files
func getBaseName(fileName string) string {
	baseName := fileName

	// Only remove .pre suffix (with dot)
	// Do NOT remove .log - we want to process .log files
	baseName = strings.TrimSuffix(baseName, ".pre")

	return baseName
}

// deletePreFile deletes a .pre file
func deletePreFile(logDir, fileName string) {
	filePath := filepath.Join(logDir, fileName)
	if err := os.Remove(filePath); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to delete .pre file %s: %v", filePath, err)
		}
	} else {
		log.Printf("Deleted .pre file: %s", filePath)
	}
}

// shouldSkipFile checks if the file should be ignored and deletes .pre files
func shouldSkipFile(logDir, fileName string) bool {
	// Skip offset files
	if fileName == ".offsets.json" || fileName == ".offsets.json.tmp" {
		return true
	}

	// Skip and DELETE .pre files
	if strings.HasSuffix(fileName, ".pre") {
		log.Printf("Found .pre file, deleting: %s", fileName)
		go deletePreFile(logDir, fileName)
		return true
	}

	// Keep .log files - they will be processed
	return false
}

func (p *LogProcessor) processExistingFiles(batchSenders map[string]*BatchSender) {
	entries, err := os.ReadDir(p.config.LogDir)
	if err != nil {
		log.Printf("Error reading directory: %v", err)
		return
	}

	log.Printf("Scanning directory %s for existing files...", p.config.LogDir)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()

		// Skip unwanted files (.pre, .offsets, etc.) and delete .pre files
		if shouldSkipFile(p.config.LogDir, fileName) {
			continue
		}

		baseName := getBaseName(fileName)
		log.Printf("File: %s -> BaseName: %s", fileName, baseName)

		// Check for both .log and non-.log files
		if baseName == "l7_flow_log" || baseName == "l4_flow_log" || baseName == "flow_metrics" {
			filePath := filepath.Join(p.config.LogDir, fileName)
			log.Printf("Found existing file: %s (type: %s)", filePath, baseName)
			p.watchFile(filePath, batchSenders)
		}
	}
}

func (p *LogProcessor) watchEvents(watcher *fsnotify.Watcher, batchSenders map[string]*BatchSender) {
	for {
		select {
		case <-p.stopCh:
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				time.Sleep(100 * time.Millisecond)

				fileName := filepath.Base(event.Name)

				// Skip unwanted files (.pre, .offsets, etc.) and delete .pre files
				if shouldSkipFile(p.config.LogDir, fileName) {
					continue
				}

				baseName := getBaseName(fileName)
				log.Printf("New file detected: %s -> BaseName: %s", fileName, baseName)

				if baseName == "l7_flow_log" || baseName == "l4_flow_log" || baseName == "flow_metrics" {
					log.Printf("Detected new file: %s", event.Name)
					p.watchFile(event.Name, batchSenders)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher error: %v", err)
		}
	}
}

func (p *LogProcessor) watchFile(filePath string, batchSenders map[string]*BatchSender) {
	fileName := filepath.Base(filePath)
	baseName := getBaseName(fileName)

	var logType string
	switch baseName {
	case "l7_flow_log":
		logType = LogTypeL7Flow
	case "l4_flow_log":
		logType = LogTypeL4Flow
	case "flow_metrics":
		logType = LogTypeFlowMetrics
	default:
		log.Printf("Unknown log type for file: %s (baseName: %s)", filePath, baseName)
		return
	}

	p.mu.Lock()
	if _, exists := p.logWatchers[filePath]; exists {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	log.Printf("Watching file: %s (type: %s)", filePath, logType)

	watcher := &LogWatcher{
		filePath:      filePath,
		processor:     p,
		offsetManager: p.offsetManager,
		logType:       logType,
	}

	p.mu.Lock()
	p.logWatchers[filePath] = watcher
	p.mu.Unlock()

	p.wg.Add(1)
	go watcher.tailFile(batchSenders[logType])
}

func (p *LogProcessor) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	log.Println("DeepFlow New Relic Sidecar stopped")
}

func (w *LogWatcher) tailFile(sender *BatchSender) {
	defer w.processor.wg.Done()

	for {
		select {
		case <-w.processor.stopCh:
			if w.file != nil {
				offset, _ := w.file.Seek(0, io.SeekCurrent)
				w.offsetManager.SetOffset(w.filePath, offset)
				w.file.Close()
			}
			return
		default:
		}

		if w.file == nil {
			file, err := os.Open(w.filePath)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Printf("Error opening file %s: %v", w.filePath, err)
				}
				time.Sleep(1 * time.Second)
				continue
			}

			savedOffset := w.offsetManager.GetOffset(w.filePath)

			var offset int64
			if savedOffset > 0 {
				offset = savedOffset
				log.Printf("Resuming file %s from offset %d", w.filePath, offset)
			} else {
				offset = 0
				log.Printf("Starting from BEGINNING of file: %s", w.filePath)
			}

			if _, err := file.Seek(offset, io.SeekStart); err != nil {
				log.Printf("Error seeking to offset %d: %v", offset, err)
				file.Close()
				time.Sleep(1 * time.Second)
				continue
			}

			w.lastOffset = offset
			w.file = file
			w.reader = bufio.NewReader(file)

			if stat, err := file.Stat(); err == nil {
				log.Printf("File %s opened, size: %d bytes, starting at offset: %d", w.filePath, stat.Size(), offset)
			}
		}

		if err := w.readWithTimeout(sender); err != nil {
			if err == errTimeout {
				currentOffset, _ := w.file.Seek(0, io.SeekCurrent)
				if currentOffset != w.lastOffset {
					w.offsetManager.SetOffset(w.filePath, currentOffset)
					w.lastOffset = currentOffset
				}
				continue
			}
			if err == io.EOF {
				if w.checkFileRotation() {
					continue
				}
				currentOffset, _ := w.file.Seek(0, io.SeekCurrent)
				if currentOffset != w.lastOffset {
					w.offsetManager.SetOffset(w.filePath, currentOffset)
					w.lastOffset = currentOffset
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Printf("Error reading file: %v", err)
			w.file.Close()
			w.file = nil
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

func (w *LogWatcher) readWithTimeout(sender *BatchSender) error {
	type result struct {
		line []byte
		err  error
	}

	ch := make(chan result, 1)
	go func() {
		line, err := w.reader.ReadBytes('\n')
		ch <- result{line, err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		w.processLine(res.line, sender)
		return nil
	case <-time.After(5 * time.Second):
		return errTimeout
	}
}

func (w *LogWatcher) checkFileRotation() bool {
	newFile, err := os.Open(w.filePath)
	if err != nil {
		return false
	}
	defer newFile.Close()

	oldInfo, err1 := w.file.Stat()
	newInfo, err2 := newFile.Stat()

	if err1 != nil || err2 != nil {
		return false
	}

	if os.SameFile(oldInfo, newInfo) {
		return false
	}

	currentOffset, _ := w.file.Seek(0, io.SeekCurrent)
	w.offsetManager.SetOffset(w.filePath, currentOffset)
	w.file.Close()
	w.file = nil

	log.Printf("File rotated: %s, saved offset %d", w.filePath, currentOffset)
	return true
}

func (w *LogWatcher) processLine(line []byte, sender *BatchSender) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}

	// 解析原始 JSON
	var rawData map[string]interface{}
	if err := json.Unmarshal(line, &rawData); err != nil {
		log.Printf("Failed to parse JSON: %v", err)
		return
	}

	// 确定事件类型
	var eventTypeName string
	switch w.logType {
	case LogTypeL7Flow:
		eventTypeName = w.processor.config.EventTypeL7
	case LogTypeL4Flow:
		eventTypeName = w.processor.config.EventTypeL4
	case LogTypeFlowMetrics:
		eventTypeName = w.processor.config.EventTypeMetrics
	default:
		eventTypeName = getEnv("EVENT_TYPE_DEFAULT", "DeepFlowLog")
	}

	// 构建 New Relic 事件 - 展开所有字段，不保存 raw_log
	nrEvent := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"eventType": eventTypeName,
		"log_type":  w.logType,
	}

	// ========== 展开 rawData 中的所有字段到顶层 ==========
	for key, value := range rawData {
		// 跳过已经处理过的字段
		if key == "eventType" || key == "timestamp" {
			continue
		}

		// 处理嵌套的 meter 结构
		if key == "meter" {
			if meter, ok := value.(map[string]interface{}); ok {
				// 处理 Flow 指标
				if flow, ok := meter["Flow"].(map[string]interface{}); ok {
					// 展开 traffic 子字段
					if traffic, ok := flow["traffic"].(map[string]interface{}); ok {
						for k, v := range traffic {
							nrEvent["traffic_"+k] = v
						}
					}
					// 展开 latency 子字段
					if latency, ok := flow["latency"].(map[string]interface{}); ok {
						for k, v := range latency {
							nrEvent["latency_"+k] = v
						}
					}
					// 展开 performance 子字段
					if perf, ok := flow["performance"].(map[string]interface{}); ok {
						for k, v := range perf {
							nrEvent["performance_"+k] = v
						}
					}
					// 展开 anomaly 子字段
					if anomaly, ok := flow["anomaly"].(map[string]interface{}); ok {
						for k, v := range anomaly {
							nrEvent["anomaly_"+k] = v
						}
					}
					// flow_load 指标
					if flowLoad, ok := flow["flow_load"].(map[string]interface{}); ok {
						for k, v := range flowLoad {
							nrEvent["flow_load_"+k] = v
						}
					}
				}
				// 处理 App 指标
				if app, ok := meter["App"].(map[string]interface{}); ok {
					if traffic, ok := app["traffic"].(map[string]interface{}); ok {
						for k, v := range traffic {
							nrEvent["app_traffic_"+k] = v
						}
					}
					if latency, ok := app["latency"].(map[string]interface{}); ok {
						for k, v := range latency {
							nrEvent["app_latency_"+k] = v
						}
					}
					if anomaly, ok := app["anomaly"].(map[string]interface{}); ok {
						for k, v := range anomaly {
							nrEvent["app_anomaly_"+k] = v
						}
					}
				}
			}
			continue
		}

		// 处理 tagger 嵌套结构
		if key == "tagger" {
			if tagger, ok := value.(map[string]interface{}); ok {
				for k, v := range tagger {
					// 跳过 code 字段（太大且不常用）
					if k == "code" {
						continue
					}
					// 处理 mac 数组
					if k == "mac" || k == "mac1" {
						if macArr, ok := v.([]interface{}); ok && len(macArr) >= 3 {
							nrEvent[k] = fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
								toByte(macArr[0]), toByte(macArr[1]), toByte(macArr[2]),
								toByte(macArr[3]), toByte(macArr[4]), toByte(macArr[5]))
						}
						continue
					}
					nrEvent["tagger_"+k] = v
				}
			}
			continue
		}

		// 普通字段直接添加
		nrEvent[key] = value
	}

	// 序列化事件
	eventJSON, err := json.Marshal(nrEvent)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}

	sender.Add(eventJSON)
}

// 辅助函数：将 interface{} 转换为 byte
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

func NewBatchSender(processor *LogProcessor, logType string) *BatchSender {
	return &BatchSender{
		logs:      make([]json.RawMessage, 0, processor.config.BatchSize),
		ticker:    time.NewTicker(processor.config.FlushInterval),
		processor: processor,
		logType:   logType,
	}
}

func (b *BatchSender) Start() {
	b.processor.wg.Add(1)
	go func() {
		defer b.processor.wg.Done()
		for {
			select {
			case <-b.processor.stopCh:
				b.flush()
				b.ticker.Stop()
				return
			case <-b.ticker.C:
				b.flush()
			}
		}
	}()
}

func (b *BatchSender) Add(log json.RawMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logs = append(b.logs, log)
	if len(b.logs) >= b.processor.config.BatchSize {
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

	log.Printf("Sending %d events to New Relic for %s", len(b.logs), b.logType)

	if err := b.processor.nrClient.SendLogs(b.logs); err != nil {
		log.Printf("Failed to send logs to New Relic: %v", err)
		return
	}

	b.logs = b.logs[:0]
}

// 辅助函数
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
