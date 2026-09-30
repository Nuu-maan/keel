package storage

import (
	"fmt"
	"testing"
)

func TestBloomHasNoFalseNegativesAndFewFalsePositives(t *testing.T) {
	const n = 10000
	hashes := make([]uint64, n)
	for i := range hashes {
		hashes[i] = bloomHash(fmt.Appendf(nil, "present-%d", i))
	}
	filter := newBloom(hashes)

	for i := range n {
		if !bloomMayContain(filter, fmt.Appendf(nil, "present-%d", i)) {
			t.Fatalf("false negative for present-%d", i)
		}
	}
	falsePositives := 0
	for i := range n {
		if bloomMayContain(filter, fmt.Appendf(nil, "absent-%d", i)) {
			falsePositives++
		}
	}
	if rate := float64(falsePositives) / n; rate > 0.02 {
		t.Fatalf("false positive rate %.3f, want <= 0.02", rate)
	}
}

func TestBloomTreatsMissingFilterAsMaybe(t *testing.T) {
	if !bloomMayContain(nil, []byte("k")) {
		t.Fatal("a missing filter must not rule keys out")
	}
}
