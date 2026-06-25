package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// New Relic limits
	MAX_PAYLOAD_SIZE_COMPRESSED   = 950 * 1024      // 950KB (safe under 1MB)
	MAX_PAYLOAD_SIZE_UNCOMPRESSED = 9 * 1024 * 1024 // 9MB (safe under 10MB)
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

	// Dynamic batching fields
	currentBatchSize  int
	minBatchSize      int
	maxBatchSize      int
	batchAdjustStep   int
	avgLogSize        int64
	sizeSampleCount   int64
	consecutiveErrors int
	errorThreshold    int
	lastAdjustTime    time.Time
	adjustCooldown    time.Duration
	successfulFlushes int64
	totalLogSize      int64

	// Enhanced metrics
	flushCount       int64
	lastFlushTime    time.Time
	flushDurations   []time.Duration
	maxFlushDuration time.Duration
	minFlushDuration time.Duration
	avgFlushDuration time.Duration

	// Size tracking
	lastFailedSize     int64
	lastSuccessfulSize int64
	isCompressed       bool
	enableDynamicBatch bool
}

// NewBatchSender creates a new batch sender with dynamic batching
func NewBatchSender(nrClient *NewRelicClient, eventType string, config *Config, enabled bool) *BatchSender {
	if !enabled {
		return &BatchSender{
			eventType:    eventType,
			config:       config,
			enabled:      false,
			maxQueueSize: config.MaxQueueSize,
		}
	}

	// Initialize batch sizes from config or defaults
	initialBatchSize := config.BatchSize
	if initialBatchSize <= 0 {
		initialBatchSize = 100
	}

	// Set min and max batch sizes
	minBatchSize := 10
	maxBatchSize := config.MaxQueueSize
	if maxBatchSize > 10000 {
		maxBatchSize = 10000 // Cap at 10000 for safety
	}
	if initialBatchSize < minBatchSize {
		initialBatchSize = minBatchSize
	}
	if initialBatchSize > maxBatchSize {
		initialBatchSize = maxBatchSize
	}

	sender := &BatchSender{
		logs:               make([]json.RawMessage, 0, initialBatchSize),
		ticker:             time.NewTicker(config.FlushInterval),
		nrClient:           nrClient,
		eventType:          eventType,
		stopCh:             make(chan struct{}),
		config:             config,
		totalSent:          0,
		dropped:            0,
		maxQueueSize:       config.MaxQueueSize,
		enabled:            true,
		currentBatchSize:   initialBatchSize,
		minBatchSize:       minBatchSize,
		maxBatchSize:       maxBatchSize,
		batchAdjustStep:    10,   // Adjust by 10 events at a time
		avgLogSize:         1024, // Initial estimate: 1KB
		sizeSampleCount:    0,
		consecutiveErrors:  0,
		errorThreshold:     3,
		lastAdjustTime:     time.Now(),
		adjustCooldown:     30 * time.Second,
		successfulFlushes:  0,
		totalLogSize:       0,
		flushCount:         0,
		lastFlushTime:      time.Now(),
		flushDurations:     make([]time.Duration, 0, 100),
		isCompressed:       true, // Assume compression is enabled
		enableDynamicBatch: true, // Enabled by default
	}

	// Setup debug file if enabled
	if config.SaveDebugData {
		var filename string
		switch eventType {
		case "DeepFlowL7Log":
			filename = "l7_debug.jsonl"
		case "DeepFlowL4Log":
			filename = "l4_debug.jsonl"
		case "DeepFlowMetrics":
			filename = "metrics_debug.jsonl"
		default:
			filename = fmt.Sprintf("%s_debug.jsonl", eventType)
		}
		filepath := config.DebugDir + "/" + filename
		file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			sender.debugFile = file
		} else {
			log.Printf("[WARN] Failed to open debug file %s: %v", filepath, err)
		}
	}

	log.Printf("[INFO] [%s] BatchSender initialized with batch size: %d (min: %d, max: %d, dynamic: %v)",
		eventType, initialBatchSize, minBatchSize, maxBatchSize, sender.enableDynamicBatch)

	return sender
}

// Add adds a log to the batch
func (b *BatchSender) Add(log json.RawMessage) bool {
	if !b.enabled {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Sample log size for dynamic adjustment
	logSize := len(log)
	b.sampleLogSize(logSize)

	if len(b.logs) >= b.maxQueueSize {
		atomic.AddInt64(&b.dropped, 1)
		dropped := atomic.LoadInt64(&b.dropped)
		if dropped%1000 == 0 {
			fmt.Printf("[WARN] [%s] Queue full (%d/%d), dropped %d events total",
				b.eventType, len(b.logs), b.maxQueueSize, dropped)
		}
		return false
	}

	b.logs = append(b.logs, log)
	b.totalLogSize += int64(logSize)

	// Check if we should trigger flush based on size (dynamic batching)
	if b.enableDynamicBatch && len(b.logs) > 0 {
		estimatedSize := b.estimateBatchSize(len(b.logs))
		maxPayload := b.getMaxPayloadSize()

		// Trigger flush if we're approaching size limit (90% of max)
		if estimatedSize >= int64(float64(maxPayload)*0.9) {
			if b.config.LogLevel == "debug" {
				fmt.Printf("[DEBUG] [%s] Size-based flush triggered: %d logs, estimated %d bytes",
					b.eventType, len(b.logs), estimatedSize)
			}
			go b.flush()
			return true
		}
	}

	// Traditional count-based trigger
	if len(b.logs) >= b.currentBatchSize {
		go b.flush()
	}

	return true
}

// sampleLogSize samples log size for average calculation
func (b *BatchSender) sampleLogSize(size int) {
	if !b.enableDynamicBatch {
		return
	}

	b.sizeSampleCount++
	// Sample every 100 logs
	if b.sizeSampleCount%100 == 0 {
		if b.avgLogSize == 0 {
			b.avgLogSize = int64(size)
		} else {
			// Exponential moving average for better reactivity
			b.avgLogSize = (b.avgLogSize*9 + int64(size)) / 10
		}

		if b.config.LogLevel == "debug" && b.sizeSampleCount%1000 == 0 {
			log.Printf("[DEBUG] [%s] Average log size: %d bytes, sample count: %d",
				b.eventType, b.avgLogSize, b.sizeSampleCount)
		}
	}
}

// estimateBatchSize estimates the total size of a batch
func (b *BatchSender) estimateBatchSize(count int) int64 {
	if b.avgLogSize == 0 {
		// If we don't have data, assume 1KB per log
		return int64(count * 1024)
	}
	// Add overhead for JSON array structure (~2 bytes per item)
	overhead := int64(count * 2)
	return (b.avgLogSize * int64(count)) + overhead
}

// getMaxPayloadSize returns the max payload size based on compression
func (b *BatchSender) getMaxPayloadSize() int64 {
	if b.isCompressed {
		return MAX_PAYLOAD_SIZE_COMPRESSED
	}
	return MAX_PAYLOAD_SIZE_UNCOMPRESSED
}

// calculateOptimalBatchSize calculates the optimal batch size based on average log size
func (b *BatchSender) calculateOptimalBatchSize() int {
	if !b.enableDynamicBatch || b.avgLogSize == 0 {
		return b.currentBatchSize
	}

	maxSize := b.getMaxPayloadSize()
	// Use 80% of max for safety margin
	safeMaxSize := int64(float64(maxSize) * 0.8)
	idealCount := int(safeMaxSize / b.avgLogSize)

	// Apply bounds
	if idealCount < b.minBatchSize {
		return b.minBatchSize
	}
	if idealCount > b.maxBatchSize {
		return b.maxBatchSize
	}

	// Round to nearest multiple of batchAdjustStep
	step := b.batchAdjustStep
	if step <= 0 {
		step = 10
	}
	return ((idealCount + step/2) / step) * step
}

// adjustBatchSize adjusts batch size based on success/failure
func (b *BatchSender) adjustBatchSize(errorOccurred bool, errorSize int64) {
	if !b.enableDynamicBatch {
		return
	}

	now := time.Now()
	if now.Sub(b.lastAdjustTime) < b.adjustCooldown {
		return // Cooldown period to prevent rapid fluctuations
	}
	b.lastAdjustTime = now

	if errorOccurred {
		b.consecutiveErrors++
		b.lastFailedSize = errorSize

		if b.consecutiveErrors >= b.errorThreshold {
			// Reduce batch size significantly on errors
			newSize := b.currentBatchSize / 2
			if newSize < b.minBatchSize {
				newSize = b.minBatchSize
			}

			// If we have size info, calculate a more precise safe size
			if b.lastFailedSize > 0 && b.avgLogSize > 0 {
				// Use 50% of what would have fit
				maxSafeCount := int((b.lastFailedSize / b.avgLogSize) * 1 / 2)
				if maxSafeCount > 0 && maxSafeCount < newSize {
					newSize = maxSafeCount
				}
			}

			if newSize != b.currentBatchSize {
				log.Printf("[WARN] [%s] Reducing batch size from %d to %d due to %d consecutive errors",
					b.eventType, b.currentBatchSize, newSize, b.consecutiveErrors)
				b.currentBatchSize = newSize
				b.consecutiveErrors = 0
			}
		}
	} else {
		// Success - reset error counter and gradually increase batch size
		b.consecutiveErrors = 0
		b.successfulFlushes++

		// Try to increase after every 10 successful flushes
		if b.successfulFlushes%10 == 0 {
			optimalSize := b.calculateOptimalBatchSize()
			if optimalSize > b.currentBatchSize {
				newSize := b.currentBatchSize + b.batchAdjustStep
				if newSize > optimalSize {
					newSize = optimalSize
				}
				if newSize != b.currentBatchSize && newSize <= b.maxBatchSize {
					log.Printf("[INFO] [%s] Increasing batch size from %d to %d (optimal: %d)",
						b.eventType, b.currentBatchSize, newSize, optimalSize)
					b.currentBatchSize = newSize
				}
			}
		}
	}
}

// saveToDebugFile saves data to debug file
func (b *BatchSender) saveToDebugFile(data []byte) {
	if b.debugFile == nil || !b.enabled {
		return
	}
	b.debugMu.Lock()
	defer b.debugMu.Unlock()
	b.debugFile.Write(data)
	b.debugFile.Write([]byte("\n"))
}

// Start starts the batch sender
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
				log.Printf("[INFO] [%s] BatchSender stopped. Total sent: %d, dropped: %d, final batch size: %d",
					b.eventType, atomic.LoadInt64(&b.totalSent), atomic.LoadInt64(&b.dropped), b.currentBatchSize)
				return
			case <-b.ticker.C:
				b.flush()
			}
		}
	}()
}

// Stop stops the batch sender
func (b *BatchSender) Stop() {
	if !b.enabled {
		return
	}
	close(b.stopCh)
	b.wg.Wait()
}

// flush flushes the current batch
func (b *BatchSender) flush() {
	if !b.enabled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked performs the actual flush with lock held
func (b *BatchSender) flushLocked() {
	if len(b.logs) == 0 {
		return
	}

	startTime := time.Now()
	batchSize := len(b.logs)
	batchBytes := b.totalLogSize

	// Check payload size before sending
	if b.enableDynamicBatch {
		estimatedSize := b.estimateBatchSize(batchSize)
		maxPayload := b.getMaxPayloadSize()

		if estimatedSize > maxPayload && batchSize > 1 {
			if b.config.LogLevel == "debug" || true {
				log.Printf("[WARN] [%s] Batch too large: %d logs, estimated %d bytes (max: %d), splitting",
					b.eventType, batchSize, estimatedSize, maxPayload)
			}
			b.splitAndFlush()
			return
		}
	}

	err := b.nrClient.SendLogs(b.logs)
	if err != nil {
		atomic.AddInt64(&b.errorCount, 1)
		errorCount := atomic.LoadInt64(&b.errorCount)

		if errorCount%100 == 1 || errorCount < 5 {
			log.Printf("[ERROR] [%s] Failed to send %d events: %v (total errors: %d)",
				b.eventType, batchSize, err, errorCount)
		}

		// Check if it's a 413 error (payload too large)
		if isSizeLimitError(err) {
			log.Printf("[WARN] [%s] 413 error detected for %d events, adjusting batch size",
				b.eventType, batchSize)
			b.adjustBatchSize(true, b.estimateBatchSize(batchSize))

			// Try to split and retry if batch is large enough
			if batchSize > 1 {
				// Clear current logs and retry with smaller batches
				b.splitAndFlush()
			}
			return
		}

		// Other errors - still adjust but don't retry immediately
		b.adjustBatchSize(true, b.estimateBatchSize(batchSize))
		return
	}

	// Success - update metrics
	b.adjustBatchSize(false, 0)
	b.totalSent += int64(batchSize)
	b.flushCount++
	b.successfulFlushes++

	// Update flush performance metrics
	flushDuration := time.Since(startTime)
	b.updateFlushMetrics(flushDuration, batchSize, batchBytes)

	if b.config.LogLevel == "debug" {
		log.Printf("[DEBUG] [%s] Sent %d events (batch size: %d, avg size: %d bytes, duration: %v)",
			b.eventType, batchSize, b.currentBatchSize, b.avgLogSize, flushDuration)
	}

	// Clear the batch
	b.logs = b.logs[:0]
	b.totalLogSize = 0
}

// splitAndFlush splits a large batch and flushes it in parts
func (b *BatchSender) splitAndFlush() {
	if len(b.logs) <= 1 {
		// Can't split further, try to send anyway
		log.Printf("[WARN] [%s] Cannot split single event, attempting to send directly", b.eventType)
		err := b.nrClient.SendLogs(b.logs)
		if err != nil {
			log.Printf("[ERROR] [%s] Failed to send single event: %v", b.eventType, err)
			atomic.AddInt64(&b.dropped, int64(len(b.logs)))
		} else {
			b.totalSent += int64(len(b.logs))
		}
		b.logs = b.logs[:0]
		b.totalLogSize = 0
		return
	}

	// Save current logs
	currentLogs := b.logs
	// currentSize := b.totalLogSize

	// Calculate split point (half)
	half := len(currentLogs) / 2
	if half < 1 {
		half = 1
	}

	log.Printf("[INFO] [%s] Splitting batch: %d logs into two batches of ~%d logs",
		b.eventType, len(currentLogs), half)

	// Process first half
	firstHalf := currentLogs[:half]
	b.logs = firstHalf
	b.totalLogSize = 0
	for _, log := range firstHalf {
		b.totalLogSize += int64(len(log))
	}
	b.flushLocked()

	// Process second half
	secondHalf := currentLogs[half:]
	b.logs = secondHalf
	b.totalLogSize = 0
	for _, log := range secondHalf {
		b.totalLogSize += int64(len(log))
	}
	b.flushLocked()
}

// updateFlushMetrics updates performance metrics
func (b *BatchSender) updateFlushMetrics(duration time.Duration, count int, bytes int64) {
	b.lastFlushTime = time.Now()

	// Keep last 100 flush durations
	b.flushDurations = append(b.flushDurations, duration)
	if len(b.flushDurations) > 100 {
		b.flushDurations = b.flushDurations[1:]
	}

	// Update stats
	if b.maxFlushDuration == 0 || duration > b.maxFlushDuration {
		b.maxFlushDuration = duration
	}
	if b.minFlushDuration == 0 || duration < b.minFlushDuration {
		b.minFlushDuration = duration
	}

	// Calculate average
	var total time.Duration
	for _, d := range b.flushDurations {
		total += d
	}
	b.avgFlushDuration = total / time.Duration(len(b.flushDurations))
}

// isSizeLimitError checks if error is due to size limit
func isSizeLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	lower := strings.ToLower(errStr)

	// Check for common size limit error patterns
	sizeLimitPatterns := []string{
		"413",
		"payload too large",
		"request entity too large",
		"size limit exceeded",
		"max body size",
		"too large",
		"exceeds limit",
		"request too large",
	}

	for _, pattern := range sizeLimitPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// GetStats returns current stats
func (b *BatchSender) GetStats() (sent, dropped int64) {
	return atomic.LoadInt64(&b.totalSent), atomic.LoadInt64(&b.dropped)
}

// GetBatchSize returns current batch size
func (b *BatchSender) GetBatchSize() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentBatchSize
}

// GetMetrics returns detailed metrics
func (b *BatchSender) GetMetrics() map[string]interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	return map[string]interface{}{
		"event_type":              b.eventType,
		"current_batch_size":      b.currentBatchSize,
		"min_batch_size":          b.minBatchSize,
		"max_batch_size":          b.maxBatchSize,
		"avg_log_size_bytes":      b.avgLogSize,
		"total_sent":              atomic.LoadInt64(&b.totalSent),
		"total_dropped":           atomic.LoadInt64(&b.dropped),
		"total_errors":            atomic.LoadInt64(&b.errorCount),
		"successful_flushes":      b.successfulFlushes,
		"queue_size":              len(b.logs),
		"max_queue_size":          b.maxQueueSize,
		"consecutive_errors":      b.consecutiveErrors,
		"avg_flush_duration_ms":   b.avgFlushDuration.Milliseconds(),
		"max_flush_duration_ms":   b.maxFlushDuration.Milliseconds(),
		"last_flush_time":         b.lastFlushTime,
		"enable_dynamic_batch":    b.enableDynamicBatch,
		"adjust_cooldown_seconds": int(b.adjustCooldown.Seconds()),
	}
}
