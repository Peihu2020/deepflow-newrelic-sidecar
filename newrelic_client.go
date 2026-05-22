package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type NewRelicClient struct {
	licenseKey string
	endpoint   string
	accountID  string
	client     *http.Client
	debugMode  bool
}

func NewNewRelicClient(licenseKey, accountID string, debugMode bool) *NewRelicClient {
	endpoint := fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", accountID)

	// 如果是 EU 区域，使用不同的端点
	// endpoint := fmt.Sprintf("https://insights-collector.eu01.nr-data.net/v1/accounts/%s/events", accountID)

	client := &NewRelicClient{
		licenseKey: licenseKey,
		endpoint:   endpoint,
		accountID:  accountID,
		debugMode:  debugMode,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}

	log.Printf("✅ New Relic client initialized")
	log.Printf("✅ Endpoint: %s", endpoint)
	log.Printf("✅ Account ID: %s", accountID)

	return client
}

func (c *NewRelicClient) SendLogs(logs []json.RawMessage) error {
	if len(logs) == 0 {
		return nil
	}

	// 解析所有事件
	var events []map[string]interface{}
	for _, logEntry := range logs {
		var event map[string]interface{}
		if err := json.Unmarshal(logEntry, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			continue
		}
		events = append(events, event)
	}

	if len(events) == 0 {
		return nil
	}

	// Insights API 期望的格式：{"eventType": "...", ...}
	var payload bytes.Buffer
	for _, event := range events {
		eventBytes, err := json.Marshal(event)
		if err != nil {
			continue
		}
		payload.Write(eventBytes)
		payload.WriteByte('\n')
	}

	if c.debugMode {
		log.Printf("Sending %d events (%d bytes) to New Relic", len(events), payload.Len())
	}

	req, err := http.NewRequest("POST", c.endpoint, &payload)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Insert-Key", c.licenseKey)

	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("HTTP request failed: %v", err)
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		log.Printf("New Relic API error: status=%d, body=%s", resp.StatusCode, string(body))
		return fmt.Errorf("API error: %d - %s", resp.StatusCode, string(body))
	}

	if c.debugMode {
		log.Printf("Successfully sent %d events, response: %s", len(events), string(body))
	}

	return nil
}
