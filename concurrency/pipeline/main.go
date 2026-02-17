package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"time"
)

func main() {
	fmt.Println("starting pipeline")
	ctx := context.Background()

	NewPipeline(ctx).
		SourceBytes(NewReadStreamStage(100)).
		TransformBytesToRecord(NewParseStreamStage()).
		TransformRecord(NewFilterLogStage(logPriorities[WARNING])).
		TransformRecord(NewEnrichLogStage(5)). // 5 concurrent workers
		TransformRecordToBytes(NewSerializeLogStage()).
		SinkBytes(NewPersistToFileStage("logs.json")).
		Run()

	fmt.Println("pipeline complete - logs written to logs.json")
}

// ============================================================================
// Domain Types
// ============================================================================

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

// ============================================================================
// Pipeline Framework
// ============================================================================

// Pipeline represents a data processing pipeline with multiple stages
type Pipeline struct {
	ctx context.Context
}

// NewPipeline creates a new pipeline with the given context
func NewPipeline(ctx context.Context) *Pipeline {
	return &Pipeline{ctx: ctx}
}

// Stage function types

// SourceBytesStage represents a stage that produces []byte values
type SourceBytesStage func(context.Context) <-chan []byte

// TransformBytesToRecordStage transforms []byte to *LogRecord
type TransformBytesToRecordStage func(context.Context, <-chan []byte) <-chan *LogRecord

// TransformRecordStage transforms *LogRecord to *LogRecord
type TransformRecordStage func(context.Context, <-chan *LogRecord) <-chan *LogRecord

// TransformRecordToBytesStage transforms *LogRecord to []byte
type TransformRecordToBytesStage func(context.Context, <-chan *LogRecord) <-chan []byte

// SinkBytesStage consumes []byte values and returns acknowledgments
type SinkBytesStage func(context.Context, <-chan []byte) <-chan struct{}

// PipelineStage represents a complete pipeline stage configuration
type PipelineStage struct {
	sourceBytesStage            SourceBytesStage
	transformBytesToRecordStage TransformBytesToRecordStage
	transformRecordStages       []TransformRecordStage
	transformRecordToBytesStage TransformRecordToBytesStage
	sinkBytesStage              SinkBytesStage
}

// Pipeline builder types and methods

// SourceBytes sets the source stage that produces []byte values
func (p *Pipeline) SourceBytes(stage SourceBytesStage) *PipelineWithBytes {
	return &PipelineWithBytes{
		ctx:    p.ctx,
		stages: PipelineStage{sourceBytesStage: stage},
		ch:     stage(p.ctx),
	}
}

// PipelineWithBytes represents a pipeline with a []byte channel
type PipelineWithBytes struct {
	ctx    context.Context
	stages PipelineStage
	ch     <-chan []byte
}

// TransformBytesToRecord transforms []byte to *LogRecord
func (p *PipelineWithBytes) TransformBytesToRecord(stage TransformBytesToRecordStage) *PipelineWithRecord {
	return &PipelineWithRecord{
		ctx:    p.ctx,
		stages: p.stages,
		ch:     stage(p.ctx, p.ch),
	}
}

// SinkBytes consumes []byte values
func (p *PipelineWithBytes) SinkBytes(stage SinkBytesStage) *PipelineComplete {
	return &PipelineComplete{
		ctx:    p.ctx,
		stages: p.stages,
		ch:     stage(p.ctx, p.ch),
	}
}

// PipelineWithRecord represents a pipeline with a *LogRecord channel
type PipelineWithRecord struct {
	ctx    context.Context
	stages PipelineStage
	ch     <-chan *LogRecord
}

// TransformRecord applies a transformation to *LogRecord
func (p *PipelineWithRecord) TransformRecord(stage TransformRecordStage) *PipelineWithRecord {
	return &PipelineWithRecord{
		ctx:    p.ctx,
		stages: p.stages,
		ch:     stage(p.ctx, p.ch),
	}
}

// TransformRecordToBytes transforms *LogRecord to []byte
func (p *PipelineWithRecord) TransformRecordToBytes(stage TransformRecordToBytesStage) *PipelineWithBytes {
	return &PipelineWithBytes{
		ctx:    p.ctx,
		stages: p.stages,
		ch:     stage(p.ctx, p.ch),
	}
}

// PipelineComplete represents a complete pipeline ready to run
type PipelineComplete struct {
	ctx    context.Context
	stages PipelineStage
	ch     <-chan struct{}
}

// Run executes the pipeline and waits for completion
func (p *PipelineComplete) Run() {
	for range p.ch {
	}
}

// ============================================================================
// Pipeline Stages - Source
// ============================================================================

// NewReadStreamStage creates a source stage that generates log lines
func NewReadStreamStage(count int) SourceBytesStage {
	return func(ctx context.Context) <-chan []byte {
		return ReadStreamStage(ctx, count)
	}
}

// ReadStreamStage generates a stream of log lines
func ReadStreamStage(ctx context.Context, count int) <-chan []byte {
	out := make(chan []byte)
	go func() {
		defer close(out)
		levels := []LogLevel{DEBUG, WARNING, INFO, ERROR}
		for i := 0; i < count; i++ {
			randomLevel := levels[rand.Intn(len(levels))]
			logLine := time.Now().Format(time.RFC3339Nano) + "|" + string(randomLevel) + "|Sample log message"
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

// ============================================================================
// Pipeline Stages - Transforms
// ============================================================================

// NewParseStreamStage creates a transform stage that parses log lines
func NewParseStreamStage() TransformBytesToRecordStage {
	return ParseStreamStage
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

// NewFilterLogStage creates a transform stage that filters log records
func NewFilterLogStage(priority LogPriority) TransformRecordStage {
	return func(ctx context.Context, in <-chan *LogRecord) <-chan *LogRecord {
		return FilterLogStage(ctx, in, priority)
	}
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

// NewEnrichLogStage creates a transform stage that enriches log records
// workers parameter specifies the maximum number of concurrent enrichment operations
func NewEnrichLogStage(workers int) TransformRecordStage {
	return func(ctx context.Context, in <-chan *LogRecord) <-chan *LogRecord {
		return EnrichLogStage(ctx, in, workers)
	}
}

// EnrichLogStage adds metadata to log records using a worker pool for concurrent processing.
func EnrichLogStage(ctx context.Context, in <-chan *LogRecord, workers int) <-chan *LogRecord {
	out := make(chan *LogRecord)

	go func() {
		defer close(out)

		// Create a channel for worker results
		results := make(chan *LogRecord, workers)

		// Use sync to track worker completion
		var workersDone = make(chan struct{})
		activeWorkers := workers

		// Fan-out: start a worker pool
		for i := 0; i < workers; i++ {
			go func(workerID int) {
				defer func() {
					// Signal this worker is done
					workersDone <- struct{}{}
				}()

				for record := range in {
					// Simulate HTTP request delay
					time.Sleep(100 * time.Millisecond)

					// We need to make a copy of the record to avoid mutating the original
					recordVal := *record
					recordVal.IP = "127.0.0.1"

					select {
					case <-ctx.Done():
						return
					case results <- &recordVal:
					}
				}
			}(i)
		}

		// Close the results channel when all workers are done
		go func() {
			for i := 0; i < activeWorkers; i++ {
				<-workersDone
			}
			close(results)
		}()

		// Fan-in: send results to an output channel
		for enriched := range results {
			select {
			case <-ctx.Done():
				return
			case out <- enriched:
			}
		}
	}()

	return out
}

// NewSerializeLogStage creates a transform stage that serializes log records to JSON
func NewSerializeLogStage() TransformRecordToBytesStage {
	return SerializeLogStage
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

// ============================================================================
// Pipeline Stages - Sink
// ============================================================================

// NewPersistToFileStage creates a sink stage that writes log records to a JSON file
func NewPersistToFileStage(filename string) SinkBytesStage {
	return func(ctx context.Context, in <-chan []byte) <-chan struct{} {
		return PersistToFileStage(ctx, in, filename)
	}
}

// PersistToFileStage writes JSON records to a file
func PersistToFileStage(ctx context.Context, in <-chan []byte, filename string) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)

		// Open a file for writing
		file, err := os.Create(filename)
		if err != nil {
			fmt.Printf("error creating file %s: %v\n", filename, err)
			return
		}
		defer file.Close()

		// Write opening bracket for JSON array
		if _, err := file.WriteString("[\n"); err != nil {
			fmt.Printf("error writing to file: %v\n", err)
			return
		}

		first := true
		for jsonData := range in {
			// Add comma separator for all but the first record
			if !first {
				if _, err := file.WriteString(",\n"); err != nil {
					fmt.Printf("error writing to file: %v\n", err)
					continue
				}
			}
			first = false

			// Write the JSON record
			if _, err := file.Write(jsonData); err != nil {
				fmt.Printf("error writing to file: %v\n", err)
				continue
			}

			fmt.Printf("persisted log to file: %s\n", filename)

			select {
			case <-ctx.Done():
				return
			case out <- struct{}{}:
			}
		}

		// Write a closing bracket for JSON array
		if _, err := file.WriteString("\n]\n"); err != nil {
			fmt.Printf("error writing to file: %v\n", err)
		}
	}()
	return out
}
