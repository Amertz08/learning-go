package main

import (
	"regexp"
	"time"
)

func main() {
	// pipeline steps
	// 1. read from a stream
	readChan := ReadStreamStage(100)
	// 2. parse bytes to a structured log object
	parsedChan := ParseStreamStage(readChan)
	// 3. apply filters to a structured log object
	filteredChan := FilterLogStage(parsedChan, logPriorities[WARNING])
	// 4. enrich logs with metadata (IP lookup -- add fake delay)
	enrichedChan := EnrichLogStage(filteredChan)
	// 5. serialize to JSON
	// 6. write to persistent storage
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
	Timestamp time.Time
	Level     LogLevel
	Message   string
	IP        string
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
		// TODO custom error
		return nil, err
	}
	record.Timestamp = ts
	record.Level = LogLevel(matches[2])
	record.Message = matches[3]

	return record, nil
}

// ReadStreamStage generates a stream of log lines
func ReadStreamStage(count int) <-chan []byte {
	out := make(chan []byte)
	go func() {
		for i := 0; i < count; i++ {
			logLine := time.Now().Format(time.RFC3339Nano) + "|INFO|Sample log message"
			out <- []byte(logLine)
		}
		close(out)
	}()
	return out
}

// ParseStreamStage parses log lines into structured log objects
func ParseStreamStage(in <-chan []byte) <-chan *LogRecord {
	// TODO: pointer or value?
	out := make(chan *LogRecord)
	go func() {
		for bytes := range in {
			record, err := ParseLog(bytes)
			if err != nil {
				// TODO: should probably log
				continue
			}
			out <- record
		}
		close(out)
	}()
	return out
}

// FilterLogStage filters log records greater than or equal to the given priority.
func FilterLogStage(in <-chan *LogRecord, priority LogPriority) <-chan *LogRecord {
	out := make(chan *LogRecord)
	go func() {
		for record := range in {
			if lp, ok := logPriorities[record.Level]; ok && lp >= priority {
				out <- record
			}
		}
		close(out)
	}()
	return out
}

// EnrichLogStage adds metadata to log records.
func EnrichLogStage(in <-chan *LogRecord) <-chan *LogRecord {
	out := make(chan *LogRecord)
	go func() {
		for record := range in {
			time.Sleep(100 * time.Millisecond)
			// We need to make a copy of the record to avoid mutating the original
			recordVal := *record
			recordVal.IP = "127.0.0.1"
			out <- &recordVal
		}
		close(out)
	}()
	return out
}
