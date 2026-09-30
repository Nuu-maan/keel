package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func sampleRecords(n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		key := fmt.Sprintf("key-%06d", i*2)
		recs[i] = Record{Op: OpPut, Key: []byte(key), Value: []byte("value-" + key)}
		if i%10 == 0 {
			recs[i] = Record{Op: OpDelete, Key: []byte(key)}
		}
	}
	return recs
}

func buildSSTable(t *testing.T, recs []Record) (string, *sstable) {
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
	return path, table
}

func TestSSTableLookup(t *testing.T) {
	recs := sampleRecords(5000)
	_, table := buildSSTable(t, recs)
	if len(table.index) < 10 {
		t.Fatalf("expected many blocks, got %d", len(table.index))
	}

	for _, want := range recs {
		got, ok, err := table.get(want.Key)
		if err != nil || !ok || got.Op != want.Op || string(got.Value) != string(want.Value) {
			t.Fatalf("get(%s) = %+v, %v, %v", want.Key, got, ok, err)
		}
	}
	for _, missing := range []string{"", "a", "key-000001", "key-004999", "zzz"} {
		if _, ok, err := table.get([]byte(missing)); ok || err != nil {
			t.Fatalf("get(%q) found=%v err=%v", missing, ok, err)
		}
	}
}

func TestSSTableAllReturnsEveryRecordInOrder(t *testing.T) {
	recs := sampleRecords(3000)
	_, table := buildSSTable(t, recs)
	got, err := table.all()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("got %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i].Op != recs[i].Op || string(got[i].Key) != string(recs[i].Key) || string(got[i].Value) != string(recs[i].Value) {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], recs[i])
		}
	}
}

func TestSSTableEmpty(t *testing.T) {
	_, table := buildSSTable(t, nil)
	if _, ok, err := table.get([]byte("k")); ok || err != nil {
		t.Fatalf("found=%v err=%v", ok, err)
	}
}

func TestSSTableDetectsCorruptBlock(t *testing.T) {
	path, _ := buildSSTable(t, sampleRecords(100))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[5] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	table, err := openSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.close()
	if _, _, err := table.get([]byte("key-000000")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestSSTableRejectsDamagedFooterOrIndex(t *testing.T) {
	for name, damage := range map[string]func([]byte) []byte{
		"truncated": func(b []byte) []byte { return b[:len(b)-1] },
		"bad magic": func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
		"bad index": func(b []byte) []byte { b[len(b)-footerSize-1] ^= 0xff; return b },
		"bad filter": func(b []byte) []byte {
			filterOffset := binary.LittleEndian.Uint64(b[len(b)-footerSize:])
			b[filterOffset] ^= 0xff
			return b
		},
		"tiny file": func(b []byte) []byte { return b[:3] },
		"huge offset": func(b []byte) []byte {
			copy(b[len(b)-footerSize:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			path, _ := buildSSTable(t, sampleRecords(100))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, damage(data), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := openSSTable(path); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("want ErrCorrupt, got %v", err)
			}
		})
	}
}

func TestSSTableFilterSkipsBlockReads(t *testing.T) {
	recs := sampleRecords(2000)
	path, _ := buildSSTable(t, recs)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dataEnd := binary.LittleEndian.Uint64(data[len(data)-footerSize:])
	for i := range dataEnd {
		data[i] = 0xee
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	table, err := openSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.close()

	if _, _, err := table.get(recs[0].Key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("present key should reach the garbage block, got %v", err)
	}
	blockReads := 0
	for i := range 2000 {
		if _, _, err := table.get(fmt.Appendf(nil, "key-%06d", i*2+1)); err != nil {
			blockReads++
		}
	}
	if blockReads > 40 {
		t.Fatalf("%d of 2000 absent-key lookups read a data block, want <= 40", blockReads)
	}
}
