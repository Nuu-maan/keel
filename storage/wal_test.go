package storage

import (
	"bytes"
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

func TestWALAppendsBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWAL(path, func(Record) {})
	if err != nil {
		t.Fatal(err)
	}
	err = w.Append(
		Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		Record{Op: OpDelete, Key: []byte("b")},
		Record{Op: OpPut, Key: []byte("c"), Value: []byte("3")},
	)
	w.Close()
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayAll(path)
	if err != nil || len(got) != 3 || string(got[0].Key) != "a" || got[1].Op != OpDelete || string(got[2].Value) != "3" {
		t.Fatalf("replay = %+v, %v", got, err)
	}
}

func TestWALTruncatesTornTail(t *testing.T) {
	for name, tail := range map[string][]byte{
		"partial header":              {0x01, 0x02, 0x03},
		"partial payload":             encode(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})[:headerSize+2],
		"corrupt last payload":        withFlippedLastByte(encode(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")})),
		"zeros after partial header":  append(encode(Record{Op: OpPut, Key: []byte("b")})[:5], make([]byte, 64)...),
		"zeros after partial payload": append(encode(Record{Op: OpPut, Key: []byte("b"), Value: bytes.Repeat([]byte("x"), 100)})[:headerSize+10], make([]byte, 200)...),
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
	for name, offset := range map[string]int{
		"payload":      headerSize + 2,
		"length field": 5,
		"checksum":     0,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.log")
			writeRecords(t, path,
				Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
				Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
			)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[offset] ^= 0xff
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := replayAll(path); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("want ErrCorrupt, got %v", err)
			}
		})
	}
}

func withFlippedLastByte(rec []byte) []byte {
	rec[len(rec)-1] ^= 0xff
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
