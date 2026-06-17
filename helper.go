package main

import (
	"encoding/json"
	"fmt"
	"time"
)

func (p *Processor) processL7Line(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	if attrs, ok := item["attributes"].(map[string]interface{}); ok {
		for k, v := range attrs {
			item["attr_"+k] = v
		}
		delete(item, "attributes")
	}

	if traceIds, ok := item["trace_ids"].([]interface{}); ok && len(traceIds) > 0 {
		if traceIdStr, ok := traceIds[0].(string); ok {
			item["trace_id"] = traceIdStr
		}
		delete(item, "trace_ids")
	}

	item["eventType"] = "DeepFlowL7Log"
	if _, exists := item["hostname"]; !exists {
		item["hostname"] = hostname
	}
	item["ingest_timestamp"] = time.Now().UnixMilli()
	if startTime, ok := item["start_time"].(float64); ok {
		item["start_timestamp"] = int64(startTime / 1000)
	}
	if endTime, ok := item["end_time"].(float64); ok {
		item["end_timestamp"] = int64(endTime / 1000)
	}

	if duration, ok := item["response_duration"].(float64); ok {
		item["response_duration_ms"] = duration / 1000.0
	}

	jsonBytes, err := json.Marshal(item)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.l7Sender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func (p *Processor) processL4Line(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	if duration, ok := item["duration"].(float64); ok {
		item["duration_ms"] = duration / 1000.0
	}

	if rtt, ok := item["rtt"].(float64); ok {
		item["rtt_ms"] = rtt / 1000.0
	}
	if rttClientMax, ok := item["rtt_client_max"].(float64); ok {
		item["rtt_client_max_ms"] = rttClientMax / 1000.0
	}
	if rttServerMax, ok := item["rtt_server_max"].(float64); ok {
		item["rtt_server_max_ms"] = rttServerMax / 1000.0
	}

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
	if _, exists := item["hostname"]; !exists {
		item["hostname"] = hostname
	}
	item["ingest_timestamp"] = time.Now().UnixMilli()
	if startTime, ok := item["start_time"].(float64); ok {
		item["start_timestamp"] = int64(startTime / 1000)
	}
	if endTime, ok := item["end_time"].(float64); ok {
		item["end_timestamp"] = int64(endTime / 1000)
	}
	jsonBytes, err := json.Marshal(item)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.l4Sender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func (p *Processor) processMetricsLine(dataStr, hostname string) json.RawMessage {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &item); err != nil {
		return nil
	}

	flattened := flattenMetrics(item)
	flattened["eventType"] = "DeepFlowMetrics"
	flattened["hostname"] = hostname
	flattened["ingest_timestamp"] = time.Now().UnixMilli()

	jsonBytes, err := json.Marshal(flattened)
	if err != nil {
		return nil
	}

	if p.config.SaveDebugData {
		p.metricsSender.saveToDebugFile(jsonBytes)
	}
	return json.RawMessage(jsonBytes)
}

func flattenMetrics(data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})

	if ts, ok := data["timestamp"]; ok {
		flattened["timestamp"] = ts
	}

	if tagger, ok := data["tagger"].(map[string]interface{}); ok {
		for k, v := range tagger {
			if k == "code" {
				if codeMap, ok := v.(map[string]interface{}); ok {
					if bits, ok := codeMap["bits"]; ok {
						flattened["tagger_code_bits"] = bits
					}
				}
				continue
			}
			if k == "mac" || k == "mac1" {
				if macArr, ok := v.([]interface{}); ok && len(macArr) >= 6 {
					flattened["tagger_"+k] = fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
						toByte(macArr[0]), toByte(macArr[1]), toByte(macArr[2]),
						toByte(macArr[3]), toByte(macArr[4]), toByte(macArr[5]))
					continue
				}
			}
			flattened["tagger_"+k] = v
		}
	}

	if meter, ok := data["meter"].(map[string]interface{}); ok {
		if app, ok := meter["App"].(map[string]interface{}); ok {
			if traffic, ok := app["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["app_traffic_"+k] = v
				}
			}
			if latency, ok := app["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
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
			if anomaly, ok := app["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["app_anomaly_"+k] = v
				}
			}
		}

		if flow, ok := meter["Flow"].(map[string]interface{}); ok {
			if traffic, ok := flow["traffic"].(map[string]interface{}); ok {
				for k, v := range traffic {
					flattened["flow_traffic_"+k] = v
				}
			}
			if latency, ok := flow["latency"].(map[string]interface{}); ok {
				for k, v := range latency {
					flattened["flow_latency_"+k] = v
				}
			}
			if performance, ok := flow["performance"].(map[string]interface{}); ok {
				for k, v := range performance {
					flattened["flow_performance_"+k] = v
				}
			}
			if anomaly, ok := flow["anomaly"].(map[string]interface{}); ok {
				for k, v := range anomaly {
					flattened["flow_anomaly_"+k] = v
				}
			}
		}
	}

	if flags, ok := data["flags"].(map[string]interface{}); ok {
		if bits, ok := flags["bits"]; ok {
			flattened["flags_bits"] = bits
		}
	}

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
