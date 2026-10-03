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

var (
	ErrNotFound = errors.New("storage: key not found")
	ErrClosed   = errors.New("storage: store is closed")
)

const (
	defaultMemtableSize      = 4 << 20
	defaultCompactionTrigger = 4
)

type Options struct {
	MemtableSize      int
	CompactionTrigger int
}

// Only the writer goroutine touches wal, walNum, memSize, nextNum, err, batches and the
// byte counters.
// mu guards mem and tables, which readers also use; the writer holds it only to apply
// a batch or swap in flushed tables, never across a WAL fsync.
type Store struct {
	dir  string
	opts Options

	writes    chan writeReq
	quit      chan struct{}
	closeOnce sync.Once
	done      sync.WaitGroup

	mu     sync.RWMutex
	mem    map[string]Record
	tables []*sstable

	wal     *WAL
	walNum  uint64
	memSize int
	nextNum uint64
	err     error
	batches int

	userBytes  int64
	tableBytes int64
}

type writeReq struct {
	rec  Record
	done chan error
}

func Open(dir string, opts Options) (*Store, error) {
	if opts.MemtableSize <= 0 {
		opts.MemtableSize = defaultMemtableSize
	}
	if opts.CompactionTrigger <= 1 {
		opts.CompactionTrigger = defaultCompactionTrigger
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ssts, wals, err := listFiles(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:     dir,
		opts:    opts,
		writes:  make(chan writeReq),
		quit:    make(chan struct{}),
		mem:     map[string]Record{},
		nextNum: 1,
	}
	if err := s.recover(ssts, wals); err != nil {
		s.Close()
		return nil, err
	}
	s.done.Add(1)
	go s.writeLoop()
	return s, nil
}

// The manifest is the only record of which files are live. Flush and compaction write
// their new files first and then atomically replace the manifest, so anything the
// manifest doesn't list was left behind by an interrupted operation and is deleted.
// WALs below the manifest's log number are already in SSTables; replaying them would
// resurrect overwritten values.
func (s *Store) recover(ssts, wals []uint64) error {
	m, found, err := readManifest(s.dir)
	if err != nil {
		return err
	}
	if !found && len(ssts) > 0 {
		return fmt.Errorf("storage: SSTables exist but %s is missing", manifestName)
	}
	s.nextNum = slices.Max(slices.Concat(ssts, wals, m.tables(), []uint64{m.logNum})) + 1

	for _, num := range ssts {
		if !slices.Contains(m.tables(), num) {
			if err := os.Remove(s.path(num, "sst")); err != nil {
				return err
			}
		}
	}
	for _, num := range m.tables() {
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

func (s *Store) Snapshot() (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := map[string][]byte{}
	for _, t := range slices.Backward(s.tables) {
		recs, err := t.all()
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			if rec.Op == OpDelete {
				delete(result, string(rec.Key))
			} else {
				result[string(rec.Key)] = bytes.Clone(rec.Value)
			}
		}
	}
	for key, rec := range s.mem {
		if rec.Op == OpDelete {
			delete(result, key)
		} else {
			result[key] = bytes.Clone(rec.Value)
		}
	}
	return result, nil
}

func (s *Store) Put(key, value []byte) error {
	return s.write(Record{Op: OpPut, Key: bytes.Clone(key), Value: bytes.Clone(value)})
}

func (s *Store) Delete(key []byte) error {
	return s.write(Record{Op: OpDelete, Key: bytes.Clone(key)})
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() { close(s.quit) })
	s.done.Wait()
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
	req := writeReq{rec: rec, done: make(chan error, 1)}
	select {
	case s.writes <- req:
		return <-req.done
	case <-s.quit:
		return ErrClosed
	}
}

func (s *Store) writeLoop() {
	defer s.done.Done()
	for {
		select {
		case req := <-s.writes:
			batch := []writeReq{req}
			for more := true; more; {
				select {
				case req := <-s.writes:
					batch = append(batch, req)
				default:
					more = false
				}
			}
			err := s.commitBatch(batch)
			for _, req := range batch {
				req.done <- err
			}
		case <-s.quit:
			return
		}
	}
}

func (s *Store) commitBatch(batch []writeReq) error {
	if s.err != nil {
		return s.err
	}
	if s.memSize >= s.opts.MemtableSize {
		s.mu.Lock()
		err := s.flushAndCompact()
		s.mu.Unlock()
		if err != nil {
			s.err = fmt.Errorf("storage: flush or compaction failed, store is read-only: %w", err)
			return s.err
		}
	}
	recs := make([]Record, len(batch))
	for i, req := range batch {
		recs[i] = req.rec
	}
	if err := s.wal.Append(recs...); err != nil {
		return err
	}
	s.batches++
	for _, rec := range recs {
		s.userBytes += int64(len(rec.Key) + len(rec.Value))
	}
	s.mu.Lock()
	for _, rec := range recs {
		s.apply(rec)
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) apply(rec Record) {
	s.mem[string(rec.Key)] = rec
	s.memSize += len(rec.Key) + len(rec.Value)
}

func (s *Store) flushAndCompact() error {
	if err := s.flush(); err != nil {
		return err
	}
	if len(s.tables) < s.opts.CompactionTrigger {
		return nil
	}
	return s.compact()
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

// Merging every table means no older table can still hold a key, so tombstones and
// overwritten values can be dropped. A partial merge must keep its tombstones.
func (s *Store) compact() error {
	merged := map[string]Record{}
	for _, t := range slices.Backward(s.tables) {
		recs, err := t.all()
		if err != nil {
			return err
		}
		for _, rec := range recs {
			merged[string(rec.Key)] = rec
		}
	}
	live := make([]Record, 0, len(merged))
	for _, rec := range merged {
		if rec.Op == OpPut {
			live = append(live, rec)
		}
	}
	slices.SortFunc(live, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })

	table, err := s.writeTable(live)
	if err != nil {
		return err
	}
	inputs := s.tables
	if err := s.commit([]*sstable{table}); err != nil {
		table.close()
		return err
	}
	for _, t := range inputs {
		t.close()
		os.Remove(s.path(t.num, "sst"))
	}
	return nil
}

func (s *Store) commit(tables []*sstable) error {
	m := manifest{logNum: s.walNum, levels: [][]uint64{nil}}
	for _, t := range slices.Backward(tables) {
		m.levels[0] = append(m.levels[0], t.num)
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
	table, err := s.openTable(num)
	if err != nil {
		return nil, err
	}
	s.tableBytes += table.size
	return table, nil
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
