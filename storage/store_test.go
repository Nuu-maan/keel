package storage

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.Put([]byte("k1"), []byte("v1"))
	s.Put([]byte("k2"), []byte("v2"))
	s.Put([]byte("k1"), []byte("v1b"))
	s.Delete([]byte("k2"))
	s.Close()

	s, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, err := s.Get([]byte("k1")); err != nil || string(v) != "v1b" {
		t.Fatalf("k1 = %q, %v", v, err)
	}
	if _, err := s.Get([]byte("k2")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k2: want ErrNotFound, got %v", err)
	}
}

func TestStoreCopiesValues(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v := []byte("abc")
	s.Put([]byte("k"), v)
	v[0] = 'x'
	got, _ := s.Get([]byte("k"))
	got[1] = 'x'
	if again, _ := s.Get([]byte("k")); string(again) != "abc" {
		t.Fatalf("stored value was aliased: %q", again)
	}
}

const crashDirEnv = "KEEL_CRASH_DIR"

var crashOpts = Options{MemtableSize: 4 << 10}

func TestCrashRecovery(t *testing.T) {
	if dir := os.Getenv(crashDirEnv); dir != "" {
		runCrashWriter(dir)
		return
	}
	dir := t.TempDir()
	for round := range 20 {
		acked := killWriterAfter(t, dir, 1+rand.IntN(300))

		s, err := Open(dir, crashOpts)
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}
		for _, key := range acked {
			if v, err := s.Get([]byte(key)); err != nil || string(v) != "v-"+key {
				t.Fatalf("round %d: acknowledged write %q lost (got %q, %v)", round, key, v, err)
			}
		}
		s.Close()
	}
	if ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(ssts) == 0 {
		t.Fatal("writer never flushed, so crashes during flush were not exercised")
	}
}

func killWriterAfter(t *testing.T, dir string, acks int) []string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
	cmd.Env = append(os.Environ(), crashDirEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var acked []string
	sc := bufio.NewScanner(out)
	for len(acked) < acks && sc.Scan() {
		acked = append(acked, sc.Text())
	}
	cmd.Process.Kill()
	cmd.Wait()
	if len(acked) < acks {
		t.Fatalf("writer exited after %d acks", len(acked))
	}
	return acked
}

func runCrashWriter(dir string) {
	s, err := Open(dir, crashOpts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	prefix := time.Now().UnixNano()
	for i := 0; ; i++ {
		key := fmt.Sprintf("%d-%d", prefix, i)
		if err := s.Put([]byte(key), []byte("v-"+key)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(key)
	}
}

func TestStoreMatchesModelAcrossFlushesAndReopens(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableSize: 2 << 10}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()

	model := map[string]string{}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 5000 {
		key := fmt.Sprintf("k%04d", rng.IntN(500))
		if rng.IntN(4) == 0 {
			if err := s.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(model, key)
		} else {
			val := fmt.Sprintf("v%d", i)
			if err := s.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			model[key] = val
		}
		if i%1000 == 999 {
			s.Close()
			if s, err = Open(dir, opts); err != nil {
				t.Fatal(err)
			}
		}
	}

	if len(s.tables) < 5 {
		t.Fatalf("expected several flushes, got %d SSTables", len(s.tables))
	}
	for i := range 500 {
		key := fmt.Sprintf("k%04d", i)
		got, err := s.Get([]byte(key))
		want, ok := model[key]
		if !ok {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s: want ErrNotFound, got %q, %v", key, got, err)
			}
			continue
		}
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q, %v; want %q", key, got, err, want)
		}
	}
	if wals, _ := filepath.Glob(filepath.Join(dir, "*.wal")); len(wals) != 1 {
		t.Fatalf("want exactly one live WAL, got %v", wals)
	}
}

func TestOpenDiscardsFlushedWALs(t *testing.T) {
	dir := t.TempDir()
	stale, err := OpenWAL(filepath.Join(dir, "000001.wal"), func(Record) {})
	if err != nil {
		t.Fatal(err)
	}
	stale.Append(Record{Op: OpPut, Key: []byte("k"), Value: []byte("stale")})
	stale.Close()
	if err := writeSSTable(filepath.Join(dir, "000002.sst"), []Record{{Op: OpPut, Key: []byte("k"), Value: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000003.sst.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, err := s.Get([]byte("k")); err != nil || string(v) != "new" {
		t.Fatalf("k = %q, %v; stale WAL was replayed over the SSTable", v, err)
	}
	for _, gone := range []string{"000001.wal", "000003.sst.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, gone)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s should have been removed, stat err = %v", gone, err)
		}
	}
}
