package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	for _, want := range []manifest{
		{logNum: 7},
		{logNum: 42, levels: [][]uint64{{3, 9, 41}}},
		{logNum: 50, levels: [][]uint64{{48}, nil, {12, 7, 30}}},
	} {
		if err := writeManifest(dir, want); err != nil {
			t.Fatal(err)
		}
		got, found, err := readManifest(dir)
		if err != nil || !found || got.logNum != want.logNum || fmt.Sprint(got.levels) != fmt.Sprint(want.levels) {
			t.Fatalf("read %+v, %v, %v; want %+v", got, found, err, want)
		}
	}
}

func TestManifestMissing(t *testing.T) {
	if _, found, err := readManifest(t.TempDir()); found || err != nil {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestManifestDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	if err := writeManifest(dir, manifest{logNum: 5, levels: [][]uint64{{2, 4}}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len("log ")] = '6'
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readManifest(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestManifestReadsLegacyTablesLineAsLevelZero(t *testing.T) {
	body := "log 9\ntables 3 8\n"
	data := fmt.Appendf(nil, "%scrc %d\n", body, crc32.Checksum([]byte(body), crcTable))
	m, err := parseManifest(data)
	if err != nil || m.logNum != 9 || fmt.Sprint(m.levels) != "[[3 8]]" {
		t.Fatalf("parsed %+v, %v", m, err)
	}
}
