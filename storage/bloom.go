package storage

import "hash/fnv"

const (
	bloomBitsPerKey = 10
	bloomProbes     = 7
)

func bloomHash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

func newBloom(hashes []uint64) []byte {
	nbits := max(len(hashes)*bloomBitsPerKey, 64)
	filter := make([]byte, (nbits+7)/8+1)
	filter[len(filter)-1] = bloomProbes
	for _, h := range hashes {
		forEachProbe(filter, h, func(byteIdx int, mask byte) bool {
			filter[byteIdx] |= mask
			return true
		})
	}
	return filter
}

func bloomMayContain(filter, key []byte) bool {
	if len(filter) < 2 {
		return true
	}
	return forEachProbe(filter, bloomHash(key), func(byteIdx int, mask byte) bool {
		return filter[byteIdx]&mask != 0
	})
}

func forEachProbe(filter []byte, h uint64, visit func(byteIdx int, mask byte) bool) bool {
	nbits := uint64(len(filter)-1) * 8
	probes := int(filter[len(filter)-1])
	h1, h2 := h&0xffffffff, h>>32
	for i := range uint64(probes) {
		bit := (h1 + i*h2) % nbits
		if !visit(int(bit/8), 1<<(bit%8)) {
			return false
		}
	}
	return true
}
