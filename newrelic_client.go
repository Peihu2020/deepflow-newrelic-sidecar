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

	// Check if this batch contains any httpbin.org events (only if debug mode)
	hasHttpBin := false
	httpBinEvents := []map[string]interface{}{}
	if c.debugMode {
		for _, event := range payload {
			if requestDomain, ok := event["request_domain"]; ok {
				if domainStr, ok := requestDomain.(string); ok && domainStr == "httpbin.org" {
					hasHttpBin = true
					httpBinEvents = append(httpBinEvents, event)
				}
			}
		}
	}

	// Only log if this batch has httpbin.org events and debug mode is on
	if hasHttpBin && c.debugMode {
		log.Printf("📤 Sending batch with %d events (including %d httpbin.org events)", len(payload), len(httpBinEvents))
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

	resp, err := c.client.Do(req)
	if err != nil {
		if hasHttpBin && c.debugMode {
			log.Printf("❌ HTTP request failed for httpbin.org batch: %v", err)
		}
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if hasHttpBin && c.debugMode {
		log.Printf("📬 New Relic response: status=%d, body=%s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		if hasHttpBin && c.debugMode {
			log.Printf("❌ New Relic rejected events: %d - %s", resp.StatusCode, string(respBody))
		}
		return fmt.Errorf("New Relic returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
