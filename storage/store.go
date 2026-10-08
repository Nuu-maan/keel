package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrNotFound = errors.New("storage: key not found")
	ErrClosed   = errors.New("storage: store is closed")
)

const (
	defaultMemtableSize      = 4 << 20
	defaultCompactionTrigger = 4
	defaultLevelSize         = 16 << 20
	defaultTableSize         = 2 << 20
	levelMultiplier          = 10
)

// CompactionTrigger is the number of L0 tables that starts an L0 compaction.
// LevelSize is the size limit of L1; each deeper level may hold ten times more.
// TableSize is the target size of a compaction output table.
type Options struct {
	MemtableSize      int
	CompactionTrigger int
	LevelSize         int64
	TableSize         int64
}

// Only the writer goroutine touches wal, walNum, logNum, memSize, err, batches,
// compactFrom and the byte counters. mu guards mem and levels, which readers also use.
// The writer is the only goroutine that changes them, so it reads them without the
// lock and holds it only to apply a batch or swap in new tables, never across I/O.
//
// levels[0] holds flushed tables newest first, and their key ranges may overlap.
// Every deeper level is a sorted run of non-overlapping tables. For any key, a version
// in a shallower level is newer than one in a deeper level.
type Store struct {
	dir  string
	opts Options

	writes    chan writeReq
	quit      chan struct{}
	closeOnce sync.Once
	done      sync.WaitGroup

	mu     sync.RWMutex
	mem    map[string]Record
	levels [][]*sstable

	wal     *WAL
	walNum  uint64
	logNum  uint64
	memSize int
	nextNum atomic.Uint64
	err     error
	batches int

	userBytes  int64
	tableBytes int64

	compactFrom [maxLevels][]byte
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
	if opts.LevelSize <= 0 {
		opts.LevelSize = defaultLevelSize
	}
	if opts.TableSize <= 0 {
		opts.TableSize = defaultTableSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ssts, wals, err := listFiles(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:    dir,
		opts:   opts,
		writes: make(chan writeReq),
		quit:   make(chan struct{}),
		mem:    map[string]Record{},
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
	if !found {
		if err := writeManifest(s.dir, m); err != nil {
			return err
		}
	}
	s.nextNum.Store(slices.Max(slices.Concat(ssts, wals, m.tables(), []uint64{m.logNum})) + 1)
	s.logNum = m.logNum

	for _, num := range ssts {
		if !slices.Contains(m.tables(), num) {
			if err := os.Remove(s.path(num, "sst")); err != nil {
				return err
			}
		}
	}
	s.levels = make([][]*sstable, maxLevels)
	for level, nums := range m.levels {
		for _, num := range nums {
			table, err := s.openTable(num)
			if err != nil {
				return err
			}
			s.levels[level] = append(s.levels[level], table)
		}
	}
	slices.Reverse(s.levels[0])
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
	num := s.nextNum.Add(1) - 1
	wal, err := OpenWAL(s.path(num, "wal"), func(Record) {})
	if err != nil {
		return err
	}
	s.wal, s.walNum = wal, num
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.mem[string(key)]
	for _, t := range s.candidates(key) {
		if ok {
			break
		}
		var err error
		if rec, ok, err = t.get(key); err != nil {
			return nil, err
		}
	}
	if !ok || rec.Op == OpDelete {
		return nil, ErrNotFound
	}
	return bytes.Clone(rec.Value), nil
}

func (s *Store) candidates(key []byte) []*sstable {
	tables := slices.Clone(s.levels[0])
	for _, level := range s.levels[1:] {
		i := sort.Search(len(level), func(i int) bool { return bytes.Compare(level[i].largest(), key) >= 0 })
		if i < len(level) {
			tables = append(tables, level[i])
		}
	}
	return tables
}

func (s *Store) oldestFirst() []*sstable {
	var tables []*sstable
	for _, level := range slices.Backward(s.levels[1:]) {
		tables = append(tables, level...)
	}
	for _, t := range slices.Backward(s.levels[0]) {
		tables = append(tables, t)
	}
	return tables
}

func (s *Store) Snapshot() (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := map[string][]byte{}
	for _, t := range s.oldestFirst() {
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
	for _, t := range slices.Concat(s.levels...) {
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
		if err := s.flushAndCompact(); err != nil {
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
	for {
		level, ok := s.pickCompaction()
		if !ok {
			return nil
		}
		if err := s.compactLevel(level); err != nil {
			return err
		}
	}
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
	levels := slices.Clone(s.levels)
	levels[0] = append([]*sstable{table}, levels[0]...)
	s.logNum = oldNum + 1
	if err := s.commit(levels); err != nil {
		return err
	}
	s.mu.Lock()
	s.mem = map[string]Record{}
	s.mu.Unlock()
	s.memSize = 0
	oldWAL.Close()
	os.Remove(s.path(oldNum, "wal"))
	return nil
}

func (s *Store) pickCompaction() (int, bool) {
	if len(s.levels[0]) >= s.opts.CompactionTrigger {
		return 0, true
	}
	limit := s.opts.LevelSize
	for level := 1; level < maxLevels-1; level++ {
		if levelBytes(s.levels[level]) > limit {
			return level, true
		}
		limit *= levelMultiplier
	}
	return 0, false
}

func levelBytes(tables []*sstable) int64 {
	var n int64
	for _, t := range tables {
		n += t.size
	}
	return n
}

// L0 must be compacted as a whole: its tables overlap, so moving only the newer ones
// down would leave an older version above them that shadows the newer data.
func (s *Store) compactLevel(level int) error {
	upper := s.levels[0]
	if level > 0 {
		upper = []*sstable{s.nextToCompact(level)}
	}
	lo, hi, err := keyRange(upper)
	if err != nil {
		return err
	}
	var lower, keep []*sstable
	for _, t := range s.levels[level+1] {
		overlaps, err := overlapsRange(t, lo, hi)
		if err != nil {
			return err
		}
		if overlaps {
			lower = append(lower, t)
		} else {
			keep = append(keep, t)
		}
	}
	if level > 0 {
		s.compactFrom[level] = upper[0].largest()
	}

	outputs := upper
	if len(upper) > 1 || len(lower) > 0 {
		inputs := slices.Concat(upper, lower)
		if lo, hi, err = keyRange(inputs); err != nil {
			return err
		}
		bottom, err := s.isBottom(level+1, lo, hi)
		if err != nil {
			return err
		}
		if outputs, err = s.mergeTables(inputs, bottom); err != nil {
			return err
		}
	}

	levels := slices.Clone(s.levels)
	levels[level] = slices.DeleteFunc(slices.Clone(levels[level]), func(t *sstable) bool { return slices.Contains(upper, t) })
	levels[level+1] = slices.Concat(keep, outputs)
	slices.SortFunc(levels[level+1], func(a, b *sstable) int { return bytes.Compare(a.largest(), b.largest()) })
	if err := s.commit(levels); err != nil {
		return err
	}
	for _, t := range slices.Concat(upper, lower) {
		if !slices.Contains(outputs, t) {
			t.close()
			os.Remove(s.path(t.num, "sst"))
		}
	}
	return nil
}

func (s *Store) nextToCompact(level int) *sstable {
	tables := s.levels[level]
	for _, t := range tables {
		if bytes.Compare(t.largest(), s.compactFrom[level]) > 0 {
			return t
		}
	}
	return tables[0]
}

// A tombstone may be dropped only when no deeper level can still hold an older
// version of its key; otherwise dropping it would resurrect that version.
func (s *Store) isBottom(level int, lo, hi []byte) (bool, error) {
	for _, tables := range s.levels[level+1:] {
		for _, t := range tables {
			overlaps, err := overlapsRange(t, lo, hi)
			if err != nil || overlaps {
				return false, err
			}
		}
	}
	return true, nil
}

func (s *Store) mergeTables(inputs []*sstable, dropTombstones bool) ([]*sstable, error) {
	iters := make([]*tableIter, len(inputs))
	for i, t := range inputs {
		iters[i] = t.iter()
	}
	merged := newMergeIter(iters)
	var outputs []*sstable
	var chunk []Record
	var chunkBytes int64
	emit := func() error {
		table, err := s.writeTable(chunk)
		if err != nil {
			return err
		}
		table.smallest = bytes.Clone(chunk[0].Key)
		outputs = append(outputs, table)
		chunk, chunkBytes = nil, 0
		return nil
	}
	for rec, ok := merged.next(); ok; rec, ok = merged.next() {
		if dropTombstones && rec.Op == OpDelete {
			continue
		}
		chunk = append(chunk, rec)
		chunkBytes += int64(len(rec.Key) + len(rec.Value) + 4)
		if chunkBytes >= s.opts.TableSize {
			if err := emit(); err != nil {
				return nil, err
			}
		}
	}
	if err := merged.err(); err != nil {
		return nil, err
	}
	if len(chunk) > 0 {
		if err := emit(); err != nil {
			return nil, err
		}
	}
	return outputs, nil
}

func keyRange(tables []*sstable) (lo, hi []byte, err error) {
	for _, t := range tables {
		smallest, err := t.smallestKey()
		if err != nil {
			return nil, nil, err
		}
		if lo == nil || bytes.Compare(smallest, lo) < 0 {
			lo = smallest
		}
		if hi == nil || bytes.Compare(t.largest(), hi) > 0 {
			hi = t.largest()
		}
	}
	return lo, hi, nil
}

func overlapsRange(t *sstable, lo, hi []byte) (bool, error) {
	smallest, err := t.smallestKey()
	if err != nil {
		return false, err
	}
	return bytes.Compare(smallest, hi) <= 0 && bytes.Compare(t.largest(), lo) >= 0, nil
}

func (s *Store) commit(levels [][]*sstable) error {
	m := manifest{logNum: s.logNum}
	for i, tables := range levels {
		nums := make([]uint64, len(tables))
		for j, t := range tables {
			nums[j] = t.num
		}
		if i == 0 {
			slices.Reverse(nums)
		}
		m.levels = append(m.levels, nums)
	}
	for len(m.levels) > 1 && len(m.levels[len(m.levels)-1]) == 0 {
		m.levels = m.levels[:len(m.levels)-1]
	}
	if err := writeManifest(s.dir, m); err != nil {
		return err
	}
	s.mu.Lock()
	s.levels = levels
	s.mu.Unlock()
	return nil
}

func (s *Store) writeTable(recs []Record) (*sstable, error) {
	num := s.nextNum.Add(1) - 1
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
