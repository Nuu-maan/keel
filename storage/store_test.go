package storage

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
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

func TestCrashRecovery(t *testing.T) {
	if dir := os.Getenv(crashDirEnv); dir != "" {
		runCrashWriter(dir)
		return
	}
	dir := t.TempDir()
	for round := range 20 {
		acked := killWriterAfter(t, dir, 1+rand.IntN(300))

		s, err := Open(dir, Options{})
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
	s, err := Open(dir, Options{})
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
