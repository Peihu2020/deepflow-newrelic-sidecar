package main

/*
#cgo LDFLAGS: -lm

#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

type PluginConfig struct {
	NewRelicAccountID string
	NewRelicInsertKey string
	BatchSize         int
	FlushInterval     int
	LogLevel          string
	HTTPTimeout       int
	RetryLimit        int
}

type DeepFlowOutput struct {
	config     *PluginConfig
	httpClient *http.Client
	batch      []json.RawMessage
	mu         chan int
	totalSent  int64
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "deepflow_newrelic", "Send DeepFlow logs to New Relic as Custom Events")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	config := &PluginConfig{
		NewRelicAccountID: output.FLBPluginConfigKey(plugin, "accountId"),
		NewRelicInsertKey: output.FLBPluginConfigKey(plugin, "insertKey"),
		BatchSize:         getConfigInt(plugin, "batchSize", 100),
		FlushInterval:     getConfigInt(plugin, "flushInterval", 5),
		LogLevel:          getConfigString(plugin, "logLevel", "info"),
		HTTPTimeout:       getConfigInt(plugin, "httpTimeout", 30),
		RetryLimit:        getConfigInt(plugin, "retryLimit", 5),
	}

	if config.NewRelicAccountID == "" {
		fmt.Fprintln(os.Stderr, "[ERROR] accountId is required")
		return output.FLB_ERROR
	}
	if config.NewRelicInsertKey == "" {
		fmt.Fprintln(os.Stderr, "[ERROR] insertKey is required")
		return output.FLB_ERROR
	}

	httpClient := &http.Client{
		Timeout: time.Duration(config.HTTPTimeout) * time.Second,
	}

	instance := &DeepFlowOutput{
		config:     config,
		httpClient: httpClient,
		batch:      make([]json.RawMessage, 0, config.BatchSize),
		mu:         make(chan int, 1),
	}
	instance.mu <- 1

	output.FLBPluginSetContext(plugin, instance)

	fmt.Fprintf(os.Stderr, "[INFO] DeepFlow New Relic plugin initialized\n")
	return output.FLB_OK
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func decodeIfBase64(s string) string {
	if len(s) == 0 {
		return s
	}

	isBase64 := true
	for _, c := range s {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
			isBase64 = false
			break
		}
	}

	if !isBase64 {
		return s
	}

	if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
		decodedStr := string(decoded)
		readable := 0
		for _, c := range decodedStr {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '.' || c == '-' ||
				c == ':' || c == '/' || c == ' ' || c == '_' || c == '=' {
				readable++
			}
		}
		if len(decodedStr) > 0 && float64(readable)/float64(len(decodedStr)) > 0.7 {
			return decodedStr
		}
	}
	return s
}

func convertAndDecode(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, val := range data {
		switch v := val.(type) {
		case string:
			result[key] = decodeIfBase64(v)
		case []byte:
			result[key] = decodeIfBase64(string(v))
		case map[string]interface{}:
			result[key] = convertAndDecode(v)
		case map[interface{}]interface{}:
			nested := make(map[string]interface{})
			for k, nestedVal := range v {
				nested[fmt.Sprintf("%v", k)] = nestedVal
			}
			result[key] = convertAndDecode(nested)
		case []interface{}:
			newArray := make([]interface{}, len(v))
			for i, item := range v {
				switch itemVal := item.(type) {
				case string:
					newArray[i] = decodeIfBase64(itemVal)
				case []byte:
					newArray[i] = decodeIfBase64(string(itemVal))
				case map[string]interface{}:
					newArray[i] = convertAndDecode(itemVal)
				case map[interface{}]interface{}:
					nested := make(map[string]interface{})
					for k, nestedVal := range itemVal {
						nested[fmt.Sprintf("%v", k)] = nestedVal
					}
					newArray[i] = convertAndDecode(nested)
				default:
					newArray[i] = itemVal
				}
			}
			result[key] = newArray
		default:
			result[key] = v
		}
	}

	return result
}

func flattenJSON(data map[string]interface{}, prefix string, result map[string]interface{}) {
	for key, value := range data {
		newKey := key
		if prefix != "" {
			newKey = prefix + "_" + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			flattenJSON(v, newKey, result)
		case []interface{}:
			if key == "trace_ids" && len(v) > 0 {
				result[newKey] = toString(v[0])
			} else {
				jsonBytes, _ := json.Marshal(v)
				result[newKey] = string(jsonBytes)
			}
		default:
			result[newKey] = v
		}
	}
}

func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case uint64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var instance *DeepFlowOutput
	if val := output.FLBPluginGetContext(ctx); val != nil {
		instance = val.(*DeepFlowOutput)
	}
	if instance == nil {
		return output.FLB_ERROR
	}

	tagStr := C.GoString(tag)

	var sourceType string
	switch {
	case strings.Contains(tagStr, "l7"):
		sourceType = "l7"
	case strings.Contains(tagStr, "l4"):
		sourceType = "l4"
	case strings.Contains(tagStr, "metrics"):
		sourceType = "metrics"
	default:
		sourceType = "unknown"
	}

	dec := output.NewDecoder(data, int(length))

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		item := make(map[string]interface{})
		for k, v := range record {
			key := fmt.Sprintf("%v", k)
			item[key] = v
		}

		item = convertAndDecode(item)

		flattened := make(map[string]interface{})
		flattenJSON(item, "", flattened)

		// Extract trace fields for L7 logs
		if sourceType == "l7" {
			if traceIds, ok := item["trace_ids"].([]interface{}); ok && len(traceIds) > 0 {
				traceIdStr := toString(traceIds[0])
				if traceIdStr != "" {
					flattened["trace_id"] = traceIdStr
					flattened["trace_ids"] = traceIdStr
				}
			}

			if attrs, ok := item["attributes"].(map[string]interface{}); ok {
				if apmTraceId, ok := attrs["apm_trace_id"]; ok {
					apmStr := toString(apmTraceId)
					if apmStr != "" {
						flattened["apm_trace_id"] = apmStr
						flattened["attributes_apm_trace_id"] = apmStr
						if _, exists := flattened["trace_id"]; !exists {
							flattened["trace_id"] = apmStr
						}
					}
				}
				if traceparent, ok := attrs["traceparent"]; ok {
					tpStr := toString(traceparent)
					if tpStr != "" {
						flattened["traceparent"] = tpStr
						flattened["attributes_traceparent"] = tpStr
					}
				}
			}
		}

		// Calculated fields
		if duration, ok := flattened["response_duration"]; ok {
			flattened["response_duration_ms"] = toFloat64(duration) / 1000.0
		}
		if duration, ok := flattened["duration"]; ok {
			flattened["duration_ms"] = toFloat64(duration) / 1000.0
		}
		if rtt, ok := flattened["rtt"]; ok {
			flattened["rtt_ms"] = toFloat64(rtt) / 1000.0
		}
		if rttClientMax, ok := flattened["rtt_client_max"]; ok {
			flattened["rtt_client_max_ms"] = toFloat64(rttClientMax) / 1000.0
		}
		if rttServerMax, ok := flattened["rtt_server_max"]; ok {
			flattened["rtt_server_max_ms"] = toFloat64(rttServerMax) / 1000.0
		}

		if byteTx, ok := flattened["byte_tx"]; ok {
			if duration, ok := flattened["duration"]; ok {
				txBytes := toFloat64(byteTx)
				durVal := toFloat64(duration)
				if durVal > 0 {
					flattened["throughput_tx_mbps"] = (txBytes * 8) / (durVal / 1000000.0) / 1000000.0
				}
			}
		}
		if byteRx, ok := flattened["byte_rx"]; ok {
			if duration, ok := flattened["duration"]; ok {
				rxBytes := toFloat64(byteRx)
				durVal := toFloat64(duration)
				if durVal > 0 {
					flattened["throughput_rx_mbps"] = (rxBytes * 8) / (durVal / 1000000.0) / 1000000.0
				}
			}
		}

		if packetTx, ok := flattened["packet_tx"]; ok {
			if retransTx, ok := flattened["retrans_tx"]; ok {
				packets := toFloat64(packetTx)
				retrans := toFloat64(retransTx)
				if packets > 0 {
					flattened["retransmission_rate_tx_pct"] = (retrans / packets) * 100
				}
			}
		}
		if packetRx, ok := flattened["packet_rx"]; ok {
			if retransRx, ok := flattened["retrans_rx"]; ok {
				packets := toFloat64(packetRx)
				retrans := toFloat64(retransRx)
				if packets > 0 {
					flattened["retransmission_rate_rx_pct"] = (retrans / packets) * 100
				}
			}
		}

		if appTrafficRequest, ok := flattened["app_traffic_request"]; ok {
			flattened["has_requests"] = toFloat64(appTrafficRequest) > 0
		}
		if appLatencyRrtMaxMs, ok := flattened["app_latency_rrt_max_ms"]; ok {
			flattened["app_response_time_ms"] = toFloat64(appLatencyRrtMaxMs)
		}
		if appAnomalyClientError, ok := flattened["app_anomaly_client_error"]; ok {
			flattened["has_client_errors"] = toFloat64(appAnomalyClientError) > 0
		}
		if appAnomalyServerError, ok := flattened["app_anomaly_server_error"]; ok {
			flattened["has_server_errors"] = toFloat64(appAnomalyServerError) > 0
		}
		if appAnomalyTimeout, ok := flattened["app_anomaly_timeout"]; ok {
			flattened["has_timeouts"] = toFloat64(appAnomalyTimeout) > 0
		}

		var eventType string
		switch sourceType {
		case "l7":
			eventType = "DeepFlowL7Log"
		case "l4":
			eventType = "DeepFlowL4Log"
		case "metrics":
			eventType = "DeepFlowMetrics"
		default:
			continue
		}

		hostname, _ := os.Hostname()
		flattened["eventType"] = eventType
		flattened["hostname"] = hostname
		flattened["source_file"] = sourceType
		flattened["ingest_timestamp"] = time.Now().UnixMilli()

		finalBytes, err := json.Marshal(flattened)
		if err != nil {
			continue
		}

		instance.addToBatch(finalBytes)

		if len(instance.batch) >= instance.config.BatchSize {
			instance.flush()
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func (d *DeepFlowOutput) addToBatch(event []byte) {
	<-d.mu
	defer func() { d.mu <- 1 }()
	d.batch = append(d.batch, json.RawMessage(event))
}

func (d *DeepFlowOutput) flush() int {
	<-d.mu
	batch := d.batch
	d.batch = d.batch[:0]
	d.mu <- 1

	if len(batch) == 0 {
		return output.FLB_OK
	}

	err := d.sendToNewRelic(batch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to send: %v\n", err)
		return output.FLB_RETRY
	}

	d.totalSent += int64(len(batch))
	fmt.Fprintf(os.Stderr, "[INFO] Sent %d events (total: %d)\n", len(batch), d.totalSent)
	return output.FLB_OK
}

func (d *DeepFlowOutput) sendToNewRelic(events []json.RawMessage) error {
	var eventList []map[string]interface{}
	for _, event := range events {
		var eventMap map[string]interface{}
		if err := json.Unmarshal(event, &eventMap); err != nil {
			continue
		}
		eventList = append(eventList, eventMap)
	}

	if len(eventList) == 0 {
		return nil
	}

	payload, err := json.Marshal(eventList)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", d.config.NewRelicAccountID)

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Insert-Key", d.config.NewRelicInsertKey)

	resp, err := d.httpClient.Do(req)
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

func getConfigString(plugin unsafe.Pointer, key, defaultValue string) string {
	val := output.FLBPluginConfigKey(plugin, key)
	if val == "" {
		return defaultValue
	}
	return val
}

func getConfigInt(plugin unsafe.Pointer, key string, defaultValue int) int {
	val := output.FLBPluginConfigKey(plugin, key)
	if val == "" {
		return defaultValue
	}
	var result int
	fmt.Sscanf(val, "%d", &result)
	return result
}

func main() {}
