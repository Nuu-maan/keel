package storage

import "testing"

func TestSnapshotMergesTablesTombstonesAndMemtable(t *testing.T) {
	s, err := Open(t.TempDir(), Options{MemtableSize: 1, CompactionTrigger: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, op := range []struct {
		key, value string
		delete     bool
	}{
		{"keep", "old", false}, {"gone", "old", false}, {"keep", "new", false}, {"gone", "", true}, {"fresh", "mem", false},
	} {
		if op.delete {
			err = s.Delete([]byte(op.key))
		} else {
			err = s.Put([]byte(op.key), []byte(op.value))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	image, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(image) != 2 || string(image["keep"]) != "new" || string(image["fresh"]) != "mem" {
		t.Fatalf("wrong snapshot: %v", image)
	}
	image["keep"][0] = 'x'
	value, err := s.Get([]byte("keep"))
	if err != nil || string(value) != "new" {
		t.Fatal("snapshot aliased live data", err)
	}
}
