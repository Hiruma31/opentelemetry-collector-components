package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WriteLogsToFile writes log lines to a file
func WriteLogsToFile(serviceName string, messageCount int, delayMs int, outputDir string) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create filename for this service
	filename := filepath.Join(outputDir, fmt.Sprintf("logs-%s.log", serviceName))

	// Open file for writing (create if doesn't exist, append if exists)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	for i := 0; i < messageCount; i++ {
		// Create a simple log line
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		logLine := fmt.Sprintf("%s [%s] %s - Test message %d (id: file-%s-%d)\n",
			timestamp, "INFO", serviceName, i+1, serviceName, i+1)

		// Write log line to file
		if _, err := file.WriteString(logLine); err != nil {
			fmt.Printf("[File Writer] Error writing message %d for %s: %v\n", i+1, serviceName, err)
			continue
		}

		fmt.Printf("[File Writer] Service %s - Message %d/%d written to %s\n", serviceName, i+1, messageCount, filename)

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
		delayMs       = flag.Int("delay", 1, "Delay in milliseconds between messages")
		outputDir     = flag.String("output", "./logs", "Output directory for log files")
	)
	flag.Parse()

	fmt.Println("=" + string(repeat('=', 58)) + "=")
	fmt.Println("OpenTelemetry Collector Test Log Writer (Go)")
	fmt.Println("=" + string(repeat('=', 58)) + "=")
	fmt.Println()
	fmt.Printf("Configuration:\n")
	fmt.Printf("  - Goroutines: %d\n", *goroutines)
	fmt.Printf("  - Messages per goroutine: %d\n", *messagesPerGR)
	fmt.Printf("  - Delay between messages: %d ms\n", *delayMs)
	fmt.Printf("  - Output directory: %s\n", *outputDir)
	fmt.Println("-" + string(repeat('-', 58)) + "-")
	fmt.Println()

	var wg sync.WaitGroup
	wg.Add(*goroutines)

	start := time.Now()

	// Launch goroutines with unique service names
	for i := 0; i < *goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			serviceName := fmt.Sprintf("service-%d", id)
			fmt.Printf("Starting goroutine %d with service name: %s\n", id, serviceName)

			err := WriteLogsToFile(serviceName, *messagesPerGR, *delayMs, *outputDir)
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
	fmt.Println("=" + string(repeat('=', 58)) + "=")
	fmt.Printf("All messages written! (completed in %v)\n", elapsed)
	fmt.Printf("Total messages written: %d\n", *goroutines**messagesPerGR)
	fmt.Printf("Log files location: %s\n", *outputDir)
	fmt.Println("=" + string(repeat('=', 58)) + "=")
}

// Helper function to repeat a character
func repeat(ch rune, count int) []rune {
	result := make([]rune, count)
	for i := 0; i < count; i++ {
		result[i] = ch
	}
	return result
}
