package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kestfor/raft/pkg/kvclient"
)

type storeClient interface {
	Put(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) ([]byte, error)
}

type latency struct {
	Min  float64 `json:"min_ms"`
	Mean float64 `json:"mean_ms"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
}

type operationResult struct {
	Success int      `json:"success"`
	Errors  int      `json:"errors"`
	Latency *latency `json:"latency"`
}

type result struct {
	Workload            string                     `json:"workload"`
	Repeat              int                        `json:"repeat"`
	Requested           int                        `json:"requested"`
	Completed           int                        `json:"completed"`
	Success             int                        `json:"success"`
	Errors              int                        `json:"errors"`
	Seconds             float64                    `json:"seconds"`
	Throughput          float64                    `json:"successful_requests_per_second"`
	CompletedRate       float64                    `json:"completed_requests_per_second"`
	PayloadMiBPerSecond float64                    `json:"payload_mib_per_second"`
	Latency             *latency                   `json:"latency"`
	Operations          map[string]operationResult `json:"operations"`
	ErrorCounts         map[string]int             `json:"error_counts"`
	Before              kvclient.Status            `json:"node_before"`
	After               kvclient.Status            `json:"node_after"`
	StatusError         string                     `json:"status_error,omitempty"`
}

type progress struct {
	completed atomic.Int64
	failures  atomic.Int64
}

type workerResult struct {
	get, put             []time.Duration
	getErrors, putErrors int
	errors               map[string]int
}

// benchmark is closed-loop: each worker starts its next operation only after
// the preceding one finishes. There is no rate limiter, retry or pipelining.
func benchmark(ctx context.Context, client storeClient, cfg settings, workload, prefix string, p *progress) result {
	value := bytes.Repeat([]byte("x"), cfg.Size)
	keys := make([]string, cfg.Keyspace)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}

	workers := min(cfg.Clients, cfg.Requests)
	parts := make([]workerResult, workers)
	var next atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	for worker := range workers {
		wg.Go(func() {
			part := &parts[worker]
			part.errors = make(map[string]int)
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= cfg.Requests {
					return
				}

				// Multiplication permutes a 100-operation cycle, avoiding a burst
				// of all reads followed by all writes. The ratio is deterministic.
				read := workload == "get" || (workload == "mixed" && (i*37)%100 < cfg.ReadPercent)
				key := keys[i%len(keys)]
				begin := time.Now()
				var err error
				if read {
					var got []byte
					got, err = client.Get(ctx, key)
					if err == nil && !bytes.Equal(got, value) {
						err = errValueMismatch
					}
				} else {
					_, err = client.Put(ctx, key, value)
				}

				elapsed := time.Since(begin)
				if err != nil {
					part.errors[errorClass(err)]++
					if read {
						part.getErrors++
					} else {
						part.putErrors++
					}

					p.failures.Add(1)
				} else if read {
					part.get = append(part.get, elapsed)
				} else {
					part.put = append(part.put, elapsed)
				}

				p.completed.Add(1)
			}
		})
	}

	wg.Wait()
	seconds := time.Since(start).Seconds()

	var reads, writes []time.Duration
	var readErrors, writeErrors int
	r := result{Workload: workload, Requested: cfg.Requests, Seconds: seconds, ErrorCounts: make(map[string]int)}
	for _, part := range parts {
		reads = append(reads, part.get...)
		writes = append(writes, part.put...)
		readErrors += part.getErrors
		writeErrors += part.putErrors
		for name, count := range part.errors {
			r.ErrorCounts[name] += count
		}
	}

	r.Success = len(reads) + len(writes)
	r.Errors = readErrors + writeErrors
	r.Completed = r.Success + r.Errors

	r.Throughput = float64(r.Success) / seconds
	r.CompletedRate = float64(r.Completed) / seconds
	r.PayloadMiBPerSecond = float64(r.Success) * float64(cfg.Size) / (1 << 20) / seconds

	r.Operations = map[string]operationResult{
		"get": {Success: len(reads), Errors: readErrors, Latency: summarize(reads)},
		"put": {Success: len(writes), Errors: writeErrors, Latency: summarize(writes)},
	}

	r.Latency = summarize(append(reads, writes...))

	return r
}

var errValueMismatch = errors.New("value mismatch")

func errorClass(err error) string {
	var response *kvclient.Error
	switch {
	case errors.Is(err, errValueMismatch):
		return "value_mismatch"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &response):
		return fmt.Sprintf("http_%d", response.StatusCode)
	default:
		return "transport"
	}
}

// Percentiles are exact nearest-rank over successful operations, in milliseconds.
// Failed operation latency is deliberately not mixed into the success distribution.
func summarize(samples []time.Duration) *latency {
	if len(samples) == 0 {
		return nil
	}

	slices.Sort(samples)
	var sum float64
	for _, sample := range samples {
		sum += float64(sample) / float64(time.Millisecond)
	}

	percentile := func(p float64) float64 {
		return float64(samples[int(math.Ceil(p*float64(len(samples))))-1]) / float64(time.Millisecond)
	}

	return &latency{
		Min:  float64(samples[0]) / float64(time.Millisecond),
		Mean: sum / float64(len(samples)),
		P50:  percentile(.5),
		P95:  percentile(.95),
		P99:  percentile(.99),
		Max:  percentile(1),
	}
}
