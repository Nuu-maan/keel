package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func tableOf(t *testing.T, recs ...Record) *sstable {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := writeSSTable(path, recs); err != nil {
		t.Fatal(err)
	}
	table, err := openSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { table.close() })
	return table
}

func put(key, value string) Record {
	return Record{Op: OpPut, Key: []byte(key), Value: []byte(value)}
}

func del(key string) Record {
	return Record{Op: OpDelete, Key: []byte(key)}
}

func TestTableIterYieldsEveryRecordAcrossBlocks(t *testing.T) {
	recs := sampleRecords(3000)
	table := tableOf(t, recs...)
	it := table.iter()
	for i := range recs {
		rec, ok := it.next()
		if !ok || string(rec.Key) != string(recs[i].Key) || rec.Op != recs[i].Op {
			t.Fatalf("record %d = %+v, %v; want %+v", i, rec, ok, recs[i])
		}
	}
	if _, ok := it.next(); ok || it.err != nil {
		t.Fatalf("iterator not exhausted cleanly: err %v", it.err)
	}
	if smallest, err := table.smallestKey(); err != nil || string(smallest) != string(recs[0].Key) {
		t.Fatalf("smallest = %q, %v", smallest, err)
	}
	if string(table.largest()) != string(recs[len(recs)-1].Key) {
		t.Fatalf("largest = %q", table.largest())
	}
}

func TestMergeIterNewestSourceWins(t *testing.T) {
	newest := tableOf(t, put("b", "new"), del("d"))
	middle := tableOf(t, put("a", "mid"), put("b", "mid"), put("e", "mid"))
	oldest := tableOf(t, put("a", "old"), put("c", "old"), put("d", "old"), put("e", "old"))

	m := newMergeIter([]*tableIter{newest.iter(), middle.iter(), oldest.iter()})
	var got []string
	for rec, ok := m.next(); ok; rec, ok = m.next() {
		if rec.Op == OpDelete {
			got = append(got, string(rec.Key)+"=<deleted>")
		} else {
			got = append(got, fmt.Sprintf("%s=%s", rec.Key, rec.Value))
		}
	}
	if err := m.err(); err != nil {
		t.Fatal(err)
	}
	want := "[a=mid b=new c=old d=<deleted> e=mid]"
	if fmt.Sprint(got) != want {
		t.Fatalf("merged %v, want %s", got, want)
	}
}
