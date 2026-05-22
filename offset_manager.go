package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type OffsetManager struct {
	offsets    map[string]int64
	offsetFile string
	mu         sync.RWMutex
	saveTicker *time.Ticker
	stopCh     chan struct{}
}

func NewOffsetManager(offsetFilePath string) *OffsetManager {
	if dir := filepath.Dir(offsetFilePath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Failed to create offset directory: %v", err)
		}
	}

	om := &OffsetManager{
		offsets:    make(map[string]int64),
		offsetFile: offsetFilePath,
		stopCh:     make(chan struct{}),
		saveTicker: time.NewTicker(30 * time.Second),
	}

	om.load()
	go om.periodicSave()

	return om
}

func (om *OffsetManager) load() {
	om.mu.Lock()
	defer om.mu.Unlock()

	data, err := os.ReadFile(om.offsetFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to load offset file: %v", err)
		}
		return
	}

	if err := json.Unmarshal(data, &om.offsets); err != nil {
		log.Printf("Failed to parse offset file: %v", err)
		return
	}

	log.Printf("Loaded %d file offsets from %s", len(om.offsets), om.offsetFile)
}

func (om *OffsetManager) save() {
	om.mu.RLock()
	defer om.mu.RUnlock()

	data, err := json.MarshalIndent(om.offsets, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal offsets: %v", err)
		return
	}

	tmpFile := om.offsetFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		log.Printf("Failed to write offset file: %v", err)
		return
	}

	if err := os.Rename(tmpFile, om.offsetFile); err != nil {
		log.Printf("Failed to rename offset file: %v", err)
	}
}

func (om *OffsetManager) periodicSave() {
	for {
		select {
		case <-om.saveTicker.C:
			om.save()
		case <-om.stopCh:
			om.saveTicker.Stop()
			om.save()
			return
		}
	}
}

func (om *OffsetManager) GetOffset(filePath string) int64 {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.offsets[filePath]
}

func (om *OffsetManager) SetOffset(filePath string, offset int64) {
	om.mu.Lock()
	defer om.mu.Unlock()
	om.offsets[filePath] = offset
}

func (om *OffsetManager) RemoveOffset(filePath string) {
	om.mu.Lock()
	defer om.mu.Unlock()
	delete(om.offsets, filePath)
}

func (om *OffsetManager) Stop() {
	close(om.stopCh)
}
