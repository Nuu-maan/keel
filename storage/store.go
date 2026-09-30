package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("storage: key not found")

const defaultMemtableSize = 4 << 20

type Options struct {
	MemtableSize int
}

type Store struct {
	dir  string
	opts Options

	mu      sync.RWMutex
	wal     *WAL
	walNum  uint64
	mem     map[string]Record
	memSize int
	tables  []*sstable
	nextNum uint64
	err     error
}

func Open(dir string, opts Options) (*Store, error) {
	if opts.MemtableSize <= 0 {
		opts.MemtableSize = defaultMemtableSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ssts, wals, err := listFiles(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, opts: opts, mem: map[string]Record{}, nextNum: 1}
	if err := s.recover(ssts, wals); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// The manifest is the only record of which files are live. Flush writes its new files
// first and then atomically replaces the manifest, so anything the manifest doesn't list
// was left behind by an interrupted flush and is deleted. WALs below the manifest's log
// number are already in SSTables; replaying them would resurrect overwritten values.
func (s *Store) recover(ssts, wals []uint64) error {
	m, found, err := readManifest(s.dir)
	if err != nil {
		return err
	}
	if !found && len(ssts) > 0 {
		return fmt.Errorf("storage: SSTables exist but %s is missing", manifestName)
	}
	s.nextNum = slices.Max(slices.Concat(ssts, wals, m.tables, []uint64{m.logNum})) + 1

	for _, num := range ssts {
		if !slices.Contains(m.tables, num) {
			if err := os.Remove(s.path(num, "sst")); err != nil {
				return err
			}
		}
	}
	for _, num := range m.tables {
		table, err := s.openTable(num)
		if err != nil {
			return err
		}
		s.tables = append([]*sstable{table}, s.tables...)
	}
	for _, num := range wals {
		if num < m.logNum {
			if err := os.Remove(s.path(num, "wal")); err != nil {
				return err
			}
			continue
		}
		if s.wal != nil {
			s.wal.Close()
		}
		wal, err := OpenWAL(s.path(num, "wal"), s.apply)
		if err != nil {
			s.wal = nil
			return err
		}
		s.wal, s.walNum = wal, num
	}
	if s.wal != nil {
		return nil
	}
	return s.openNewWAL()
}

func (s *Store) openNewWAL() error {
	wal, err := OpenWAL(s.path(s.nextNum, "wal"), func(Record) {})
	if err != nil {
		return err
	}
	s.wal, s.walNum = wal, s.nextNum
	s.nextNum++
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.mem[string(key)]
	for i := 0; !ok && i < len(s.tables); i++ {
		var err error
		if rec, ok, err = s.tables[i].get(key); err != nil {
			return nil, err
		}
	}
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
	var errs []error
	if s.wal != nil {
		errs = append(errs, s.wal.Close())
	}
	for _, t := range s.tables {
		errs = append(errs, t.close())
	}
	return errors.Join(errs...)
}

func (s *Store) write(rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.memSize >= s.opts.MemtableSize {
		if err := s.flush(); err != nil {
			s.err = fmt.Errorf("storage: flush failed, store is read-only: %w", err)
			return s.err
		}
	}
	if err := s.wal.Append(rec); err != nil {
		return err
	}
	s.apply(rec)
	return nil
}

func (s *Store) apply(rec Record) {
	s.mem[string(rec.Key)] = rec
	s.memSize += len(rec.Key) + len(rec.Value)
}

// Any failure here is fatal to the store: the manifest may already name the new WAL,
// in which case recovery would discard the old one and lose writes appended to it.
func (s *Store) flush() error {
	recs := make([]Record, 0, len(s.mem))
	for _, rec := range s.mem {
		recs = append(recs, rec)
	}
	slices.SortFunc(recs, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })

	table, err := s.writeTable(recs)
	if err != nil {
		return err
	}
	oldWAL, oldNum := s.wal, s.walNum
	if err := s.openNewWAL(); err != nil {
		return err
	}
	if err := s.commit(append([]*sstable{table}, s.tables...)); err != nil {
		return err
	}
	s.mem, s.memSize = map[string]Record{}, 0
	oldWAL.Close()
	os.Remove(s.path(oldNum, "wal"))
	return nil
}

func (s *Store) commit(tables []*sstable) error {
	m := manifest{logNum: s.walNum}
	for _, t := range slices.Backward(tables) {
		m.tables = append(m.tables, t.num)
	}
	if err := writeManifest(s.dir, m); err != nil {
		return err
	}
	s.tables = tables
	return nil
}

func (s *Store) writeTable(recs []Record) (*sstable, error) {
	num := s.nextNum
	s.nextNum++
	if err := writeSSTable(s.path(num, "sst"), recs); err != nil {
		return nil, err
	}
	return s.openTable(num)
}

func (s *Store) openTable(num uint64) (*sstable, error) {
	table, err := openSSTable(s.path(num, "sst"))
	if err != nil {
		return nil, err
	}
	table.num = num
	return table, nil
}

func (s *Store) path(num uint64, ext string) string {
	return filepath.Join(s.dir, fmt.Sprintf("%06d.%s", num, ext))
}

func listFiles(dir string) (ssts, wals []uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, nil, err
			}
			continue
		}
		var num uint64
		var ext string
		if n, _ := fmt.Sscanf(name, "%d.%s", &num, &ext); n != 2 {
			continue
		}
		switch ext {
		case "sst":
			ssts = append(ssts, num)
		case "wal":
			wals = append(wals, num)
		}
	}
	slices.Sort(ssts)
	slices.Sort(wals)
	return ssts, wals, nil
}
