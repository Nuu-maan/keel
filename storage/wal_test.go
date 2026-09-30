package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeRecords(t *testing.T, path string, recs ...Record) {
	t.Helper()
	w, err := OpenWAL(path, func(Record) {})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
}

func replayAll(path string) ([]Record, error) {
	var got []Record
	w, err := OpenWAL(path, func(r Record) { got = append(got, r) })
	if err != nil {
		return nil, err
	}
	return got, w.Close()
}

func TestWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writeRecords(t, path,
		Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		Record{Op: OpPut, Key: []byte(""), Value: nil},
		Record{Op: OpDelete, Key: []byte("a")},
	)
	got, err := replayAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || string(got[0].Value) != "1" || got[2].Op != OpDelete || string(got[2].Key) != "a" {
		t.Fatalf("unexpected replay: %+v", got)
	}
}

func TestWALTruncatesTornTail(t *testing.T) {
	for name, tail := range map[string][]byte{
		"partial header":  {0x01, 0x02, 0x03},
		"partial payload": encode(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})[:headerSize+2],
		"bad checksum":    withFlippedChecksum(encode(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.log")
			writeRecords(t, path, Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
			appendBytes(t, path, tail)

			writeRecords(t, path, Record{Op: OpPut, Key: []byte("c"), Value: []byte("3")})
			got, err := replayAll(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 || string(got[0].Key) != "a" || string(got[1].Key) != "c" {
				t.Fatalf("unexpected replay: %+v", got)
			}
		})
	}
}

func TestWALRejectsMidFileCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	writeRecords(t, path,
		Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[headerSize+2] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := replayAll(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func withFlippedChecksum(rec []byte) []byte {
	rec[0] ^= 0xff
	return rec
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}
