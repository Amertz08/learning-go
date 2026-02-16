package main

import "time"

func main() {
	// start log generator
	// pipeline steps
	// 1. read from a stream and parse bytes to a structured log object
	// 2. apply filters to a structured log object
	// 3. enrich logs with metadata (IP lookup -- add fake delay)
	// 4. serialize to JSON
	// 5. write to persistent storage
}

type LogLevel string

const (
	DEBUG   LogLevel = "DEBUG"
	WARNING LogLevel = "WARNING"
	INFO    LogLevel = "INFO"
	ERROR   LogLevel = "ERROR"
)

type LogRecord struct {
	Timestamp time.Time
	Level     LogLevel
	Message   string
}
