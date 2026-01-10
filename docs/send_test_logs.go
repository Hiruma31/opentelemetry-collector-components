package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// StringValue represents a string value in OTLP format
type StringValue struct {
	StringValue string `json:"stringValue"`
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   string      `json:"key"`
	Value StringValue `json:"value"`
}

// Scope represents a scope
type Scope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// LogRecord represents a single log record
type LogRecord struct {
	TimeUnixNano   string      `json:"timeUnixNano"`
	SeverityNumber int         `json:"severityNumber"`
	SeverityText   string      `json:"severityText"`
	Body           StringValue `json:"body"`
	Attributes     []KeyValue  `json:"attributes"`
}

// ScopeLog represents logs for a scope
type ScopeLog struct {
	Scope      Scope       `json:"scope"`
	LogRecords []LogRecord `json:"logRecords"`
}

// Resource represents a resource
type Resource struct {
	Attributes []KeyValue `json:"attributes"`
}

// ResourceLog represents resource logs
type ResourceLog struct {
	Resource  Resource   `json:"resource"`
	ScopeLogs []ScopeLog `json:"scopeLogs"`
}

// LogsPayload represents the complete logs payload
type LogsPayload struct {
	ResourceLogs []ResourceLog `json:"resourceLogs"`
}

// SendOTLPHTTPLogs sends logs to HTTP OTLP receiver
func SendOTLPHTTPLogs(serviceName string, messageCount int, delayMs int) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	for i := 0; i < messageCount; i++ {
		payload := LogsPayload{
			ResourceLogs: []ResourceLog{
				{
					Resource: Resource{
						Attributes: []KeyValue{
							{
								Key: "service.name",
								Value: StringValue{
									StringValue: serviceName,
								},
							},
							{
								Key: "service.namespace",
								Value: StringValue{
									StringValue: "default",
								},
							},
						},
					},
					ScopeLogs: []ScopeLog{
						{
							Scope: Scope{
								Name:    "test-scope",
								Version: "1.0",
							},
							LogRecords: []LogRecord{
								{
									TimeUnixNano:   fmt.Sprintf("%d", time.Now().UnixNano()),
									SeverityNumber: 2,
									SeverityText:   "INFO",
									Body: StringValue{
										StringValue: fmt.Sprintf("Test message %d from HTTP OTLP receiver", i+1),
									},
									Attributes: []KeyValue{
										{
											Key: "message.id",
											Value: StringValue{
												StringValue: fmt.Sprintf("http-%s-%d", serviceName, i+1),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		body, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("[HTTP OTLP] Error marshaling message %d for %s: %v\n", i+1, serviceName, err)
			continue
		}

		req, err := http.NewRequest("POST", "http://localhost:4318/v1/logs", bytes.NewBuffer(body))
		if err != nil {
			fmt.Printf("[HTTP OTLP] Error creating request for %s: %v\n", serviceName, err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[HTTP OTLP] Error sending message %d from %s: %v\n", i+1, serviceName, err)
			continue
		}

		statusCode := resp.StatusCode
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		resp.Body.Close()

		fmt.Printf("[HTTP OTLP] Service %s - Message %d/%d sent - Status: %d\n", serviceName, i+1, messageCount, statusCode)

		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
	}

	return nil
}

// Main function
func main() {
	var (
		goroutines    = flag.Int("goroutines", 4, "Number of concurrent goroutines to spawn")
		messagesPerGR = flag.Int("messages", 10, "Number of messages per goroutine")
		delayMs       = flag.Int("delay", 10, "Delay in milliseconds between messages")
		portFlag      = flag.String("port", "4318", "HTTP receiver port")
		hostFlag      = flag.String("host", "localhost", "HTTP receiver host")
	)
	flag.Parse()

	fmt.Println("=" + string(bytes.Repeat([]byte("="), 58)) + "=")
	fmt.Println("OpenTelemetry Collector Test Log Sender (Go)")
	fmt.Println("=" + string(bytes.Repeat([]byte("="), 58)) + "=")
	fmt.Println()
	fmt.Printf("Configuration:\n")
	fmt.Printf("  - Goroutines: %d\n", *goroutines)
	fmt.Printf("  - Messages per goroutine: %d\n", *messagesPerGR)
	fmt.Printf("  - Delay between messages: %d ms\n", *delayMs)
	fmt.Printf("  - Receiver: http://%s:%s/v1/logs\n", *hostFlag, *portFlag)
	fmt.Println("-" + string(bytes.Repeat([]byte("-"), 58)) + "-")
	fmt.Println()

	// Update the global client to use the provided host and port
	// (Note: For simplicity, we're keeping localhost hardcoded in the function,
	// but in production you'd pass these as parameters)

	var wg sync.WaitGroup
	wg.Add(*goroutines)

	start := time.Now()

	// Launch goroutines with unique service names
	for i := 0; i < *goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			serviceName := fmt.Sprintf("service-%d", id)
			fmt.Printf("Starting goroutine %d with service name: %s\n", id, serviceName)

			err := SendOTLPHTTPLogs(serviceName, *messagesPerGR, *delayMs)
			if err != nil {
				fmt.Printf("Error in goroutine %d: %v\n", id, err)
			}

			fmt.Printf("Completed goroutine %d (%s)\n", id, serviceName)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	elapsed := time.Since(start)

	fmt.Println()
	fmt.Println("=" + string(bytes.Repeat([]byte("="), 58)) + "=")
	fmt.Printf("All messages sent! (completed in %v)\n", elapsed)
	fmt.Printf("Total messages sent: %d\n", *goroutines**messagesPerGR)
	fmt.Println("Check the collector output to see the debug exporter results.")
	fmt.Println("=" + string(bytes.Repeat([]byte("="), 58)) + "=")
}
