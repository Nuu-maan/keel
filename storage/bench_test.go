package storage

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Not b.TempDir: /tmp is often tmpfs, where fsync is free and the numbers mean nothing.
// KEEL_BENCH_DIR selects the disk to measure; the default is the package directory.
func benchDir(b *testing.B) string {
	dir, err := os.MkdirTemp(cmp.Or(os.Getenv("KEEL_BENCH_DIR"), "."), "keel-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func BenchmarkPut(b *testing.B) {
	for _, perProc := range []int{0, 1, 16, 128} {
		goroutines := max(perProc*runtime.GOMAXPROCS(0), 1)
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			s, err := Open(benchDir(b), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			value := make([]byte, 100)
			var seq atomic.Int64
			put := func() error { return s.Put(fmt.Appendf(nil, "key-%d", seq.Add(1)), value) }
			b.ResetTimer()
			if perProc == 0 {
				for b.Loop() {
					if err := put(); err != nil {
						b.Fatal(err)
					}
				}
				return
			}
			b.SetParallelism(perProc)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := put(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkPutLatency(b *testing.B) {
	s, err := Open(benchDir(b), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	value := make([]byte, 100)
	var seq atomic.Int64
	var mu sync.Mutex
	var latencies []time.Duration
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local []time.Duration
		for pb.Next() {
			start := time.Now()
			if err := s.Put(fmt.Appendf(nil, "key-%d", seq.Add(1)), value); err != nil {
				b.Error(err)
				return
			}
			local = append(local, time.Since(start))
		}
		mu.Lock()
		latencies = append(latencies, local...)
		mu.Unlock()
	})
	b.StopTimer()
	slices.Sort(latencies)
	for _, q := range []struct {
		unit     string
		quantile float64
	}{{"p50-µs", 0.5}, {"p99-µs", 0.99}, {"p99.9-µs", 0.999}, {"max-µs", 1}} {
		b.ReportMetric(float64(latencies[int(q.quantile*float64(len(latencies)-1))].Microseconds()), q.unit)
	}
}

func BenchmarkWriteAmplification(b *testing.B) {
	value := make([]byte, 100)
	for _, mib := range []int{16, 64, 128} {
		b.Run(fmt.Sprintf("data=%dMiB", mib), func(b *testing.B) {
			puts := mib << 20 / (16 + len(value))
			for b.Loop() {
				s, err := Open(b.TempDir(), Options{MemtableSize: 256 << 10, LevelSize: 1 << 20, TableSize: 256 << 10})
				if err != nil {
					b.Fatal(err)
				}
				rng := rand.New(rand.NewPCG(1, 1))
				for range puts {
					if err := s.Put(fmt.Appendf(nil, "key-%011d", rng.IntN(2*puts)), value); err != nil {
						b.Fatal(err)
					}
				}
				s.Close()
				b.ReportMetric(float64(s.tableBytes.Load())/float64(s.userBytes), "write-amp")
			}
		})
	}
}
