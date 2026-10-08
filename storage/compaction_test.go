package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func checkLevels(t *testing.T, s *Store) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.levels[0]) >= s.opts.CompactionTrigger {
		t.Fatalf("L0 holds %d tables, trigger is %d", len(s.levels[0]), s.opts.CompactionTrigger)
	}
	for level, tables := range s.levels[1:] {
		for i := 1; i < len(tables); i++ {
			smallest, err := tables[i].smallestKey()
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Compare(tables[i-1].largest(), smallest) >= 0 {
				t.Fatalf("L%d tables %d and %d overlap or are out of order", level+1, tables[i-1].num, tables[i].num)
			}
		}
	}
	ssts, _ := filepath.Glob(filepath.Join(s.dir, "*.sst"))
	if live := len(slices.Concat(s.levels...)); len(ssts) != live {
		t.Fatalf("%d SSTable files on disk for %d live tables", len(ssts), live)
	}
}

func levelRecords(t *testing.T, tables []*sstable) []Record {
	t.Helper()
	var recs []Record
	for _, table := range tables {
		all, err := table.all()
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, all...)
	}
	return recs
}

func TestCompactionKeepsTombstoneWhileDeeperLevelHoldsKey(t *testing.T) {
	dir := t.TempDir()
	opts := Options{CompactionTrigger: 1000}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	step := func(f func() error) {
		t.Helper()
		if err := f(); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	step(s.flush)
	step(func() error { return s.compactLevel(0) })
	moved := s.levels[1][0].num
	step(func() error { return s.compactLevel(1) })
	if len(s.levels[2]) != 1 || s.levels[2][0].num != moved {
		t.Fatalf("a table with nothing below it should move down without a rewrite, L2 = %v", s.levels[2])
	}

	if err := s.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put([]byte("other"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	step(s.flush)
	if err := s.Put([]byte("k0"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	step(s.flush)
	step(func() error { return s.compactLevel(0) })

	recs := levelRecords(t, s.levels[1])
	if !slices.ContainsFunc(recs, func(r Record) bool { return string(r.Key) == "k" && r.Op == OpDelete }) {
		t.Fatalf("tombstone for k dropped while L2 still holds k: L1 = %v", recs)
	}
	checkLevels(t, s)

	s.Close()
	if s, err = Open(dir, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurrected from L2: %v", err)
	}
}
