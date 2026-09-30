package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu  sync.RWMutex
	wal *WAL
	mem map[string][]byte
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{mem: map[string][]byte{}}
	wal, err := OpenWAL(filepath.Join(dir, "wal.log"), s.apply)
	if err != nil {
		return nil, err
	}
	s.wal = wal
	return s, nil
}

func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.mem[string(key)]
	return bytes.Clone(v), ok
}

func (s *Store) Put(key, value []byte) error {
	return s.write(Record{Op: OpPut, Key: key, Value: bytes.Clone(value)})
}

func (s *Store) Delete(key []byte) error {
	return s.write(Record{Op: OpDelete, Key: key})
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
	switch rec.Op {
	case OpPut:
		s.mem[string(rec.Key)] = rec.Value
	case OpDelete:
		delete(s.mem, string(rec.Key))
	}
}
