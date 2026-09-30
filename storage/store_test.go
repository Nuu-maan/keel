package storage

import (
	"testing"
)

func TestStoreSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put([]byte("k1"), []byte("v1"))
	s.Put([]byte("k2"), []byte("v2"))
	s.Put([]byte("k1"), []byte("v1b"))
	s.Delete([]byte("k2"))
	s.Close()

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, ok := s.Get([]byte("k1")); !ok || string(v) != "v1b" {
		t.Fatalf("k1 = %q, %v", v, ok)
	}
	if _, ok := s.Get([]byte("k2")); ok {
		t.Fatal("k2 should be deleted")
	}
}

func TestStoreCopiesValues(t *testing.T) {
	s, err := Open(t.TempDir())
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
