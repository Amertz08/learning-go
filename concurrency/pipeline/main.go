package main

import (
	"regexp"
	"time"
)

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

// ParseLog parse a log line into a structured log object
// Logs are assumed to be in the format <timestamp>|<level>|<message>
func ParseLog(b []byte) (*LogRecord, error) {
	logString := string(b)
	re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z)\|([A-Z]+)\|(.*)$`)
	matches := re.FindStringSubmatch(logString)
	if len(matches) != 4 {
		// TODO log
		return nil, nil
	}
	record := &LogRecord{}
	ts, err := time.Parse(time.RFC3339Nano, matches[1])
	if err != nil {
		// TODO custom erro
		return nil, err
	}
	record.Timestamp = ts
	record.Level = LogLevel(matches[2])
	record.Message = matches[3]

	return record, nil
}
