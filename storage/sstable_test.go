package storage

import (
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
