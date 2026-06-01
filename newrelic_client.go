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

func (c *NewRelicClient) SendLogs(events []json.RawMessage) error {
	if len(events) == 0 {
		return nil
	}

	url := fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", c.accountID)

	var payload []map[string]interface{}
	for _, event := range events {
		var eventMap map[string]interface{}
		if err := json.Unmarshal(event, &eventMap); err != nil {
			continue
		}
		payload = append(payload, eventMap)
	}

	// Check if this batch contains any httpbin.org events
	hasHttpBin := false
	httpBinEvents := []map[string]interface{}{}
	for _, event := range payload {
		if requestDomain, ok := event["request_domain"]; ok {
			if domainStr, ok := requestDomain.(string); ok && domainStr == "httpbin.org" {
				hasHttpBin = true
				httpBinEvents = append(httpBinEvents, event)
			}
		}
	}

	// Only log if this batch has httpbin.org events
	if hasHttpBin {
		log.Printf("📤 Sending batch with %d L7 events (including %d httpbin.org events)", len(payload), len(httpBinEvents))

		// Print each httpbin.org event
		for i, event := range httpBinEvents {
			eventJSON, _ := json.MarshalIndent(event, "", "  ")
			log.Printf("🔍 httpbin.org event #%d:\n%s", i+1, string(eventJSON))
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", c.licenseKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		if hasHttpBin {
			log.Printf("❌ HTTP request failed for httpbin.org batch: %v", err)
		}
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if hasHttpBin {
		log.Printf("📬 New Relic response for httpbin.org batch: status=%d, body=%s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		if hasHttpBin {
			log.Printf("❌ New Relic rejected httpbin.org events: %d - %s", resp.StatusCode, string(respBody))
		}
		return fmt.Errorf("New Relic returned %d: %s", resp.StatusCode, string(respBody))
	}

	var nrResponse struct {
		Success bool `json:"success"`
	}
	if err := json.Unmarshal(respBody, &nrResponse); err == nil {
		if nrResponse.Success && hasHttpBin {
			log.Printf("✅ New Relic accepted %d events (including %d httpbin.org events)", len(payload), len(httpBinEvents))
		} else if !nrResponse.Success && hasHttpBin {
			log.Printf("⚠️ New Relic rejected httpbin.org events: %s", string(respBody))
		}
	}

	return nil
}
