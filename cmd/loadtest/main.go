package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	base := flag.String("url", "http://localhost:8080", "API base URL")
	total := flag.Int("n", 100, "requests")
	concurrency := flag.Int("c", 10, "concurrency")
	flag.Parse()

	jobs := make(chan int)
	latencies := make(chan time.Duration, *total)
	var failures atomic.Int64
	client := &http.Client{Timeout: 5 * time.Second}
	var wg sync.WaitGroup
	started := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				body := []byte(fmt.Sprintf(`{"name":"performance-%06d","nodes":1}`, i))
				req, _ := http.NewRequest(http.MethodPost, *base+"/v1/clusters", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", fmt.Sprintf("performance-%d-%d", time.Now().UnixNano(), i))
				t0 := time.Now()
				resp, err := client.Do(req)
				latencies <- time.Since(t0)
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 300 {
					failures.Add(1)
				}
			}
		}()
	}
	for i := 0; i < *total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(latencies)

	var vals []time.Duration
	for d := range latencies {
		vals = append(vals, d)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	elapsed := time.Since(started)
	fmt.Printf("requests=%d concurrency=%d failures=%d elapsed=%s rps=%.1f p50=%s p95=%s p99=%s\n",
		*total, *concurrency, failures.Load(), elapsed.Round(time.Millisecond), float64(*total)/elapsed.Seconds(), pct(vals, 50), pct(vals, 95), pct(vals, 99))
}

func pct(v []time.Duration, p int) time.Duration {
	if len(v) == 0 {
		return 0
	}
	i := (len(v) - 1) * p / 100
	return v[i].Round(time.Microsecond)
}
