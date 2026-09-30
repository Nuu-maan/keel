package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var ErrNotFound = errors.New("storage: key not found")

type Store struct {
	mu  sync.RWMutex
	wal *WAL
	mem map[string]Record
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{mem: map[string]Record{}}
	wal, err := OpenWAL(filepath.Join(dir, "wal.log"), s.apply)
	if err != nil {
		return nil, err
	}
	s.wal = wal
	return s, nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.mem[string(key)]
	if !ok || rec.Op == OpDelete {
		return nil, ErrNotFound
	}
	return bytes.Clone(rec.Value), nil
}

func (s *Store) Put(key, value []byte) error {
	return s.write(Record{Op: OpPut, Key: bytes.Clone(key), Value: bytes.Clone(value)})
}

func (s *Store) Delete(key []byte) error {
	return s.write(Record{Op: OpDelete, Key: bytes.Clone(key)})
}

func (s *Store) Close() error {
	return s.wal.Close()
}

func (s *Store) write(rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.Append(rec); err != nil {
		return err
	}
	s.apply(rec)
	return nil
}

func (s *Store) apply(rec Record) {
	s.mem[string(rec.Key)] = rec
}
