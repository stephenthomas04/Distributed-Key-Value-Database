// benchmark measures a fixed number of closed-loop operations on one leader.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/client"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/store"
)

type sample struct {
	LatencyMS float64 `json:"latency_ms"`
	Error     string  `json:"error,omitempty"`
}

func run() error {
	addr := flag.String("addr", "localhost:7001", "reachable node")
	mode := flag.String("mode", "read", "read or write")
	count := flag.Int("n", 1000, "measured operations")
	concurrency := flag.Int("concurrency", 1, "workers")
	size := flag.Int("value-size", 256, "value bytes")
	keys := flag.Int("keys", 100, "preloaded keys")
	warmup := flag.Int("warmup", 100, "unmeasured operations")
	out := flag.String("out", "result.json", "JSON output")
	flag.Parse()
	if (*mode != "read" && *mode != "write") || *count < 1 || *concurrency < 1 || *keys < 1 || *warmup < 0 || *size < 0 || *size > store.MaxValueBytes {
		return fmt.Errorf("invalid benchmark parameters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	c, err := client.Leader(ctx, *addr)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	kv := api.NewKeyValueClient(c)
	value := []byte(strings.Repeat("x", *size))
	operation := func(i int, write bool) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := fmt.Sprintf("bench-%d", i%*keys)
		if write {
			_, e := kv.Put(ctx, &api.PutRequest{Key: key, Value: value})
			return e
		}
		r, e := kv.Get(ctx, &api.KeyRequest{Key: key})
		if e == nil && string(r.Value) != string(value) {
			return fmt.Errorf("unexpected value")
		}
		return e
	}
	for i := 0; i < *keys; i++ {
		if err := operation(i, true); err != nil {
			return fmt.Errorf("preload: %w", err)
		}
	}
	for i := 0; i < *warmup; i++ {
		if err := operation(i, *mode == "write"); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}
	samples := make([]sample, *count)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := w; idx < *count; idx += *concurrency {
				s := time.Now()
				err := operation(idx, *mode == "write")
				samples[idx].LatencyMS = float64(time.Since(s)) / float64(time.Millisecond)
				if err != nil {
					samples[idx].Error = err.Error()
				}
			}
		}()
	}
	wg.Wait()
	duration := time.Since(start).Seconds()
	var latencies []float64
	for _, s := range samples {
		if s.Error == "" {
			latencies = append(latencies, s.LatencyMS)
		}
	}
	errors := len(samples) - len(latencies)
	sort.Float64s(latencies)
	percentile := func(p float64) interface{} {
		if len(latencies) == 0 {
			return nil
		}
		return latencies[int(math.Ceil(float64(len(latencies))*p))-1]
	}
	result := map[string]interface{}{"mode": *mode, "operations": *count, "concurrency": *concurrency, "value_bytes": *size, "keys": *keys, "warmup": *warmup, "duration_seconds": duration, "success_ops_per_second": float64(len(latencies)) / duration, "errors": errors, "p50_ms": percentile(.50), "p95_ms": percentile(.95), "p99_ms": percentile(.99), "samples": samples, "started_utc": start.UTC().Format(time.RFC3339Nano), "go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "latency_population": "successful operations only"}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(*out, append(data, '\n'), 0644); err != nil {
		return err
	}
	fmt.Printf("%s c=%d: %.2f successful ops/s, %d errors; %s\n", *mode, *concurrency, float64(len(latencies))/duration, errors, *out)
	if errors > 0 {
		return fmt.Errorf("benchmark had %d errors (results saved)", errors)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
