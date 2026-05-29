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
	Hostname          string
}

// 日志处理器
type LogProcessor struct {
	config      Config
	logWatchers map[string]*LogWatcher
	nrClient    *NewRelicClient
	mu          sync.RWMutex
	stopCh      chan struct{}
	wg          sync.WaitGroup
	hostname    string
}

// 文件监听器
type LogWatcher struct {
	filePath  string
	file      *os.File
	processor *LogProcessor
	logType   string
}

// 批量发送器
type BatchSender struct {
	logs      []json.RawMessage
	mu        sync.Mutex
	ticker    *time.Ticker
	processor *LogProcessor
	logType   string
}

func main() {
	// 加载 .env 文件
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: No .env file found, using environment variables")
	}

	// 获取主机名
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Warning: Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	log.Printf("Hostname: %s", hostname)

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
		Hostname:          hostname,
	}

	// 验证必需配置
	if config.NewRelicLicense == "" {
		log.Fatal("NEW_RELIC_LICENSE_KEY is required (set in .env or environment variable)")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  Hostname: %s", config.Hostname)
	log.Printf("  New Relic License: %s...", config.NewRelicLicense[:8])
	log.Printf("  New Relic Account ID: %s", config.NewRelicAccountID)
	log.Printf("  Log Directory: %s", config.LogDir)
	log.Printf("  Batch Size: %d", config.BatchSize)
	log.Printf("  Flush Interval: %v", config.FlushInterval)

	// 确保日志目录存在
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	// 创建 New Relic 客户端
	nrClient := NewNewRelicClient(config.NewRelicLicense, config.NewRelicAccountID, false)

	// 创建日志处理器
	processor := &LogProcessor{
		config:      config,
		logWatchers: make(map[string]*LogWatcher),
		nrClient:    nrClient,
		stopCh:      make(chan struct{}),
		hostname:    hostname,
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

	// 启动 .pre 文件清理协程（每分钟扫描并删除）
	processor.wg.Add(1)
	go processor.cleanupPreFiles()

	// 监听退出信号（支持 Ctrl+C）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Println("========================================")
	log.Println("DeepFlow New Relic Sidecar started")
	log.Printf("Watching directory: %s", config.LogDir)
	log.Println("Press Ctrl+C to stop")
	log.Println("========================================")

	// 等待停止信号
	<-sigCh
	log.Println("\nReceived shutdown signal, stopping...")
	processor.Stop()
	log.Println("Sidecar stopped successfully")
}

// shouldSkipFile 检查是否应该跳过文件
func shouldSkipFile(fileName string) bool {
	// 跳过 .pre 文件（不监控，让清理线程删除）
	if strings.HasSuffix(fileName, ".pre") {
		return true
	}
	return false
}

// cleanupPreFiles 定期清理 .pre 文件（每分钟扫描，发现即删除）
func (p *LogProcessor) cleanupPreFiles() {
	defer p.wg.Done()

	// 每 1 分钟检查一次
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// 启动时立即执行一次清理
	p.doCleanupPreFiles()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.doCleanupPreFiles()
		}
	}
}

// doCleanupPreFiles 执行实际的清理操作（发现 .pre 文件立即删除）
func (p *LogProcessor) doCleanupPreFiles() {
	// 查找所有 .pre 文件
	pattern := filepath.Join(p.config.LogDir, "*.pre")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("Error finding .pre files: %v", err)
		return
	}

	if len(files) == 0 {
		return
	}

	deletedCount := 0
	for _, filePath := range files {
		if err := os.Remove(filePath); err != nil {
			if !os.IsNotExist(err) {
				log.Printf("Failed to delete .pre file %s: %v", filePath, err)
			}
		} else {
			log.Printf("Deleted .pre file: %s", filePath)
			deletedCount++
		}
	}

	if deletedCount > 0 {
		log.Printf("Cleaned up %d .pre files", deletedCount)
	}
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
		if shouldSkipFile(fileName) {
			continue
		}

		// 只处理 .log 文件
		if fileName == "l7_flow_log" || fileName == "l4_flow_log" || fileName == "flow_metrics" {
			filePath := filepath.Join(p.config.LogDir, fileName)
			log.Printf("Found existing file: %s", filePath)
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
				if shouldSkipFile(fileName) {
					continue
				}

				if fileName == "l7_flow_log" || fileName == "l4_flow_log" || fileName == "flow_metrics" {
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
	// 跳过 .pre 文件
	if strings.HasSuffix(filePath, ".pre") {
		log.Printf("Skipping .pre file: %s", filePath)
		return
	}

	p.mu.Lock()
	if _, exists := p.logWatchers[filePath]; exists {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	var logType string
	switch filepath.Base(filePath) {
	case "l7_flow_log":
		logType = LogTypeL7Flow
	case "l4_flow_log":
		logType = LogTypeL4Flow
	case "flow_metrics":
		logType = LogTypeFlowMetrics
	default:
		log.Printf("Unknown log type for file: %s", filePath)
		return
	}

	log.Printf("Watching file: %s (type: %s)", filePath, logType)

	watcher := &LogWatcher{
		filePath:  filePath,
		processor: p,
		logType:   logType,
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
	log.Println("All goroutines stopped")
}

// tailFile - 首次启动从开头读取，然后切换到 tail 模式
func (w *LogWatcher) tailFile(sender *BatchSender) {
	defer w.processor.wg.Done()

	// 标记是否是第一次运行
	isFirstRun := true

	for {
		select {
		case <-w.processor.stopCh:
			if w.file != nil {
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

			if isFirstRun {
				// 第一次启动：从文件开头读取所有现有内容
				if _, err := file.Seek(0, io.SeekStart); err != nil {
					log.Printf("Error seeking to start: %v", err)
					file.Close()
					time.Sleep(1 * time.Second)
					continue
				}
				log.Printf("First run: reading %s from beginning", w.filePath)
				isFirstRun = false
			} else {
				// 后续：从文件末尾开始 tail
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					log.Printf("Error seeking to end: %v", err)
					file.Close()
					time.Sleep(1 * time.Second)
					continue
				}
				log.Printf("Tailing file: %s", w.filePath)
			}

			w.file = file
		}

		// 读取新行
		reader := bufio.NewReader(w.file)
		for {
			line, err := reader.ReadBytes('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading file %s: %v", w.filePath, err)
				w.file.Close()
				w.file = nil
				break
			}
			w.processLine(line, sender)
		}

		time.Sleep(100 * time.Millisecond)
	}
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

	// 构建 New Relic 事件
	nrEvent := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"eventType": eventTypeName,
		"log_type":  w.logType,
		"hostname":  w.processor.hostname,
	}

	// 展开 rawData 中的所有字段
	for key, value := range rawData {
		if key == "eventType" || key == "timestamp" {
			continue
		}

		// 处理嵌套的 meter 结构
		if key == "meter" {
			if meter, ok := value.(map[string]interface{}); ok {
				// 处理 Flow 指标
				if flow, ok := meter["Flow"].(map[string]interface{}); ok {
					if traffic, ok := flow["traffic"].(map[string]interface{}); ok {
						for k, v := range traffic {
							nrEvent["traffic_"+k] = v
						}
					}
					if latency, ok := flow["latency"].(map[string]interface{}); ok {
						for k, v := range latency {
							nrEvent["latency_"+k] = v
						}
					}
					if perf, ok := flow["performance"].(map[string]interface{}); ok {
						for k, v := range perf {
							nrEvent["performance_"+k] = v
						}
					}
					if anomaly, ok := flow["anomaly"].(map[string]interface{}); ok {
						for k, v := range anomaly {
							nrEvent["anomaly_"+k] = v
						}
					}
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
					if k == "code" {
						continue
					}
					if (k == "mac" || k == "mac1") && len(v.([]interface{})) >= 3 {
						macArr := v.([]interface{})
						nrEvent[k] = fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
							toByte(macArr[0]), toByte(macArr[1]), toByte(macArr[2]),
							toByte(macArr[3]), toByte(macArr[4]), toByte(macArr[5]))
						continue
					}
					nrEvent["tagger_"+k] = v
				}
			}
			continue
		}

		// 处理 trace_ids 数组
		if key == "trace_ids" {
			if traceIDs, ok := value.([]interface{}); ok && len(traceIDs) > 0 {
				if traceID, ok := traceIDs[0].(string); ok {
					nrEvent["trace_id"] = traceID
				}
				nrEvent["trace_ids"] = value
			}
			continue
		}

		// 处理 attributes 嵌套对象
		if key == "attributes" {
			if attrs, ok := value.(map[string]interface{}); ok {
				if apmTraceID, ok := attrs["apm_trace_id"].(string); ok {
					nrEvent["apm_trace_id"] = apmTraceID
				}
				if traceparent, ok := attrs["traceparent"].(string); ok {
					nrEvent["traceparent"] = traceparent
				}
				for k, v := range attrs {
					switch k {
					case "apm_trace_id", "traceparent":
						continue
					default:
						nrEvent["attr_"+k] = v
					}
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
