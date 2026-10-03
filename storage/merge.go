package storage

import (
	"bytes"
	"container/heap"
	"errors"
)

type mergeItem struct {
	rec Record
	src int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].rec.Key, h[j].rec.Key); c != 0 {
		return c < 0
	}
	return h[i].src < h[j].src
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

// mergeIter yields each key once, in order, from sources listed newest first: when
// several sources hold a key, the lowest-numbered source's version wins.
type mergeIter struct {
	srcs []*tableIter
	h    mergeHeap
}

func newMergeIter(srcs []*tableIter) *mergeIter {
	m := &mergeIter{srcs: srcs}
	for i := range srcs {
		m.advance(i)
	}
	return m
}

func (m *mergeIter) advance(src int) {
	if rec, ok := m.srcs[src].next(); ok {
		heap.Push(&m.h, mergeItem{rec: rec, src: src})
	}
}

func (m *mergeIter) next() (Record, bool) {
	if m.h.Len() == 0 {
		return Record{}, false
	}
	top := heap.Pop(&m.h).(mergeItem)
	m.advance(top.src)
	for m.h.Len() > 0 && bytes.Equal(m.h[0].rec.Key, top.rec.Key) {
		older := heap.Pop(&m.h).(mergeItem)
		m.advance(older.src)
	}
	return top.rec, true
}

func (m *mergeIter) err() error {
	var errs []error
	for _, src := range m.srcs {
		errs = append(errs, src.err)
	}
	return errors.Join(errs...)
}
