package storage

import (
	"cmp"
	"fmt"
	"os"
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
	for _, writers := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			s, err := Open(benchDir(b), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			value := make([]byte, 100)
			var seq atomic.Int64
			b.SetParallelism(writers)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := s.Put(fmt.Appendf(nil, "key-%d", seq.Add(1)), value); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
