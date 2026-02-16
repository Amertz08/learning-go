package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

func main() {
	fmt.Println("starting pipeline")
	ctx := context.Background()
	// pipeline steps
	// 1. read from a stream
	readChan := ReadStreamStage(ctx, 100)
	// 2. parse bytes to a structured log object
	parsedChan := ParseStreamStage(ctx, readChan)
	// 3. apply filters to a structured log object
	filteredChan := FilterLogStage(ctx, parsedChan, logPriorities[WARNING])
	// 4. enrich logs with metadata (IP lookup -- add fake delay)
	enrichedChan := EnrichLogStage(ctx, filteredChan)
	// 5. serialize to JSON
	jsonChan := SerializeLogStage(ctx, enrichedChan)
	// 6. write to persistent storage
	PersistLogStage(ctx, jsonChan)
}

type LogLevel string
type LogPriority int

const (
	DEBUG   LogLevel = "DEBUG"
	WARNING LogLevel = "WARNING"
	INFO    LogLevel = "INFO"
	ERROR   LogLevel = "ERROR"
)

var logPriorities = map[LogLevel]LogPriority{
	DEBUG:   0,
	WARNING: 1,
	INFO:    2,
	ERROR:   3,
}

type LogRecord struct {
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	Message   string    `json:"message"`
	IP        string    `json:"ip"`
}

// ParseLog parse a log line into a structured log object
// Logs are assumed to be in the format <timestamp>|<level>|<message>
func ParseLog(b []byte) (*LogRecord, error) {
	logString := string(b)
	re := regexp.MustCompile(`^([^|]+)\|([^|]+)\|(.*)$`)
	matches := re.FindStringSubmatch(logString)
	if len(matches) != 4 {
		// TODO log
		return nil, nil
	}
	record := &LogRecord{}
	ts, err := time.Parse(time.RFC3339Nano, matches[1])
	if err != nil {
		// TODO custom error
		return nil, err
	}
	record.Timestamp = ts
	record.Level = LogLevel(matches[2])
	record.Message = matches[3]

	return record, nil
}

// ReadStreamStage generates a stream of log lines
func ReadStreamStage(ctx context.Context, count int) <-chan []byte {
	out := make(chan []byte)
	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			logLine := time.Now().Format(time.RFC3339Nano) + "|INFO|Sample log message"
			select {
			case <-ctx.Done():
				return
			case out <- []byte(logLine):
				fmt.Println("sending log line:", logLine)
				time.Sleep(100 * time.Millisecond)
			}

		}
	}()
	return out
}

// ParseStreamStage parses log lines into structured log objects
func ParseStreamStage(ctx context.Context, in <-chan []byte) <-chan *LogRecord {
	out := make(chan *LogRecord)
	go func() {
		defer close(out)
		for bytes := range in {
			record, err := ParseLog(bytes)
			fmt.Println("parsing log line:", string(bytes))
			if err != nil || record == nil {
				// TODO: should probably log
				fmt.Println("error parsing log line:", err, record)
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- record:
			}
		}
	}()
	return out
}

// FilterLogStage filters log records greater than or equal to the given priority.
func FilterLogStage(ctx context.Context, in <-chan *LogRecord, priority LogPriority) <-chan *LogRecord {
	out := make(chan *LogRecord)
	go func() {
		defer close(out)
		for record := range in {
			if lp, ok := logPriorities[record.Level]; ok && lp >= priority {
				fmt.Println("sending filtered log line:", record.Message)
				select {
				case <-ctx.Done():
					return
				case out <- record:
				}
			} else {
				fmt.Println("skipping log line:", record.Message)
			}
		}
	}()
	return out
}

// EnrichLogStage adds metadata to log records.
func EnrichLogStage(ctx context.Context, in <-chan *LogRecord) <-chan *LogRecord {
	// TODO: since this is network bound, we can consider using a worker pool to parallelize the enrichment process
	out := make(chan *LogRecord)
	go func() {
		defer close(out)
		for record := range in {
			time.Sleep(100 * time.Millisecond)
			// We need to make a copy of the record to avoid mutating the original
			recordVal := *record
			recordVal.IP = "127.0.0.1"

			select {
			case <-ctx.Done():
				return
			case out <- &recordVal:
			}
		}
	}()
	return out
}

// SerializeLogStage serializes LogRecord into JSON
func SerializeLogStage(ctx context.Context, in <-chan *LogRecord) <-chan []byte {
	out := make(chan []byte)
	go func() {
		defer close(out)
		for record := range in {
			jsonData, err := json.Marshal(record)
			if err != nil {
				// TODO: log the error
				continue
			}
			fmt.Println("sending json log line:", string(jsonData))

			select {
			case <-ctx.Done():
				return
			case out <- jsonData:
			}
		}
	}()
	return out
}

// PersistLogStage writes the JSON record
func PersistLogStage(ctx context.Context, in <-chan []byte) {
	// TODO: since this is disk bound, we can consider using a worker pool to parallelize the write process.
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-in:
			if !ok {
				return // Channel closed
			}
			_ = data // No Op
			fmt.Println("persisting log line:", string(data))
			time.Sleep(1 * time.Second)
		}
	}
}
