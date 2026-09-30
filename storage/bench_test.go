package storage

import (
	"cmp"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
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
