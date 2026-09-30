package storage

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	for _, want := range []manifest{
		{logNum: 7},
		{logNum: 42, tables: []uint64{3, 9, 41}},
	} {
		if err := writeManifest(dir, want); err != nil {
			t.Fatal(err)
		}
		got, found, err := readManifest(dir)
		if err != nil || !found || got.logNum != want.logNum || !slices.Equal(got.tables, want.tables) {
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
	if err := writeManifest(dir, manifest{logNum: 5, tables: []uint64{2, 4}}); err != nil {
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
