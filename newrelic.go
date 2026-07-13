package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

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

// SendEvent sends a single event to NewRelic Insights API
func (c *NewRelicClient) SendEvent(event map[string]interface{}) error {
	if c == nil {
		return nil
	}

	// Wrap in array (NewRelic expects array of events)
	events := []map[string]interface{}{event}

	payload, err := json.Marshal(events)
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

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
}
