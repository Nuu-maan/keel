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

var crashOpts = Options{MemtableSize: 4 << 10, CompactionTrigger: 3}

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
	s, err := Open(dir, crashOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.nextNum < 20 {
		t.Fatalf("only %d files ever created, so crashes during flush and compaction were barely exercised", s.nextNum)
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

	if len(s.tables) >= defaultCompactionTrigger {
		t.Fatalf("%d SSTables, compaction should keep it below %d", len(s.tables), defaultCompactionTrigger)
	}
	if ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(ssts) != len(s.tables) {
		t.Fatalf("%d SSTable files on disk for %d live tables", len(ssts), len(s.tables))
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

func TestOpenDeletesFilesNotInManifest(t *testing.T) {
	dir := t.TempDir()
	put := func(key, value string) Record { return Record{Op: OpPut, Key: []byte(key), Value: []byte(value)} }
	writeWAL := func(name string, rec Record) {
		w, err := OpenWAL(filepath.Join(dir, name), func(Record) {})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
		w.Close()
	}
	writeWAL("000001.wal", put("k", "stale"))
	if err := writeSSTable(filepath.Join(dir, "000002.sst"), []Record{put("k", "flushed")}); err != nil {
		t.Fatal(err)
	}
	writeWAL("000003.wal", put("live", "yes"))
	if err := writeSSTable(filepath.Join(dir, "000004.sst"), []Record{put("k", "orphan")}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000005.sst.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(dir, manifest{logNum: 3, levels: [][]uint64{{2}}}); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for key, want := range map[string]string{"k": "flushed", "live": "yes"} {
		if v, err := s.Get([]byte(key)); err != nil || string(v) != want {
			t.Fatalf("%s = %q, %v; want %q", key, v, err, want)
		}
	}
	for _, gone := range []string{"000001.wal", "000004.sst", "000005.sst.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, gone)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s should have been removed, stat err = %v", gone, err)
		}
	}
}

func TestOpenRefusesSSTablesWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	if err := writeSSTable(filepath.Join(dir, "000001.sst"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("opened a store whose manifest is missing")
	}
}

func TestCompactionDropsOverwritesAndTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableSize: 1 << 10, CompactionTrigger: 1000}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	for round := range 5 {
		for i := range 100 {
			key := fmt.Appendf(nil, "k%03d", i)
			if round == 4 && i%2 == 0 {
				if err := s.Delete(key); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err := s.Put(key, fmt.Appendf(nil, "v%d", round)); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.mu.Lock()
	if err := s.flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()

	if len(s.tables) != 1 {
		t.Fatalf("%d tables after compaction, want 1", len(s.tables))
	}
	recs, err := s.tables[0].all()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 50 {
		t.Fatalf("compacted table holds %d records, want the 50 live keys", len(recs))
	}
	for _, rec := range recs {
		if rec.Op != OpPut || string(rec.Value) != "v4" {
			t.Fatalf("unexpected record in compacted table: %+v", rec)
		}
	}

	s.Close()
	if s, err = Open(dir, opts); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		v, err := s.Get(fmt.Appendf(nil, "k%03d", i))
		if i%2 == 0 && !errors.Is(err, ErrNotFound) || i%2 == 1 && (err != nil || string(v) != "v4") {
			t.Fatalf("k%03d = %q, %v after reopen", i, v, err)
		}
	}
	if ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(ssts) != 1 {
		t.Fatalf("compaction inputs left on disk: %v", ssts)
	}
}

func TestConcurrentWritesShareBatches(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const writers = 64
	s.mu.Lock()
	errs := make(chan error, writers)
	for i := range writers {
		go func() { errs <- s.Put(fmt.Appendf(nil, "k%d", i), []byte("v")) }()
	}
	time.Sleep(100 * time.Millisecond)
	s.mu.Unlock()
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	if s.batches > 4 {
		t.Fatalf("%d writers took %d WAL commits, want them batched into a few", writers, s.batches)
	}
	for i := range writers {
		if _, err := s.Get(fmt.Appendf(nil, "k%d", i)); err != nil {
			t.Fatalf("k%d: %v", i, err)
		}
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	s, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := s.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}
