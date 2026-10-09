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
	l0StopFactor             = 3
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

// The writer goroutine owns wal, walNum, memSize, batches, userBytes and flushed. When
// the memtable fills, the writer moves it to imm and starts a background job that
// flushes it, and it starts the next one only after the previous has finished. Each
// flush wakes the compactor goroutine, which owns compactFrom. Writes stop while L0
// holds l0StopFactor times the compaction trigger, so L0 can't outgrow compaction.
//
// mu guards mem, imm, levels and err, which readers also use. Only the writer changes
// mem, so it reads mem without the lock. Levels change only through commit, which
// holds commitMu to serialize manifest updates and guard logNum. Nobody holds mu
// across I/O.
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

	compactWake    chan struct{}
	compacted      chan struct{}
	stopCompaction chan struct{}
	compactor      sync.WaitGroup

	commitMu sync.Mutex

	mu     sync.RWMutex
	mem    map[string]Record
	imm    map[string]Record
	levels [][]*sstable
	err    error

	wal     *WAL
	walNum  uint64
	memSize int
	batches int
	flushed chan struct{}

	nextNum atomic.Uint64
	logNum  uint64

	userBytes  int64
	tableBytes atomic.Int64

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

		compactWake:    make(chan struct{}, 1),
		compacted:      make(chan struct{}, 1),
		stopCompaction: make(chan struct{}),
	}
	if err := s.recover(ssts, wals); err != nil {
		s.Close()
		return nil, err
	}
	s.done.Add(1)
	go s.writeLoop()
	s.compactor.Add(1)
	go s.compactLoop()
	notify(s.compactWake)
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
	if !ok {
		rec, ok = s.imm[string(key)]
	}
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
	for _, recs := range []map[string]Record{s.imm, s.mem} {
		for key, rec := range recs {
			if rec.Op == OpDelete {
				delete(result, key)
			} else {
				result[key] = bytes.Clone(rec.Value)
			}
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
	s.closeOnce.Do(func() {
		close(s.quit)
		s.done.Wait()
		close(s.stopCompaction)
	})
	s.compactor.Wait()
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
	if err := s.failure(); err != nil {
		return err
	}
	if s.memSize >= s.opts.MemtableSize {
		if err := s.rotate(); err != nil {
			return err
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

func (s *Store) failure() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

// A failed flush or compaction may have replaced the manifest without making it
// durable, so a later commit built on the in-memory levels could drop live files.
func (s *Store) fail(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = fmt.Errorf("storage: flush or compaction failed, store is read-only: %w", err)
	}
	return s.err
}

func (s *Store) rotate() error {
	if s.flushed != nil {
		<-s.flushed
	}
	for {
		if err := s.failure(); err != nil {
			return err
		}
		if len(s.version()[0]) < l0StopFactor*s.opts.CompactionTrigger {
			break
		}
		<-s.compacted
	}
	oldWAL, oldNum := s.wal, s.walNum
	if err := s.openNewWAL(); err != nil {
		return s.fail(err)
	}
	s.mu.Lock()
	imm := s.mem
	s.imm, s.mem = imm, map[string]Record{}
	s.mu.Unlock()
	s.memSize = 0
	done := make(chan struct{})
	s.flushed = done
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		defer close(done)
		err := s.flush(imm, oldNum)
		oldWAL.Close()
		if err != nil {
			s.fail(err)
			return
		}
		os.Remove(s.path(oldNum, "wal"))
		notify(s.compactWake)
	}()
	return nil
}

func notify(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (s *Store) compactLoop() {
	defer s.compactor.Done()
	for {
		select {
		case <-s.compactWake:
			s.compactAll()
		case <-s.stopCompaction:
			s.compactAll()
			return
		}
	}
}

func (s *Store) compactAll() {
	for s.failure() == nil {
		level, ok := s.pickCompaction()
		if !ok {
			return
		}
		if err := s.compactLevel(level); err != nil {
			s.fail(err)
		}
		notify(s.compacted)
	}
}

func (s *Store) flush(imm map[string]Record, walNum uint64) error {
	recs := make([]Record, 0, len(imm))
	for _, rec := range imm {
		recs = append(recs, rec)
	}
	slices.SortFunc(recs, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })

	table, err := s.writeTable(recs)
	if err != nil {
		return err
	}
	err = s.commit(func(levels [][]*sstable) {
		levels[0] = append([]*sstable{table}, levels[0]...)
		s.logNum = walNum + 1
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.imm = nil
	s.mu.Unlock()
	return nil
}

func (s *Store) version() [][]*sstable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.levels
}

// Picks the level furthest past its limit. Always preferring L0 would starve deeper
// levels while writes keep flushing, and each L0 compaction would then rewrite an
// ever larger L1.
func (s *Store) pickCompaction() (int, bool) {
	levels := s.version()
	best, bestScore := -1, 1.0
	if n := len(levels[0]); n >= s.opts.CompactionTrigger {
		best, bestScore = 0, float64(n)/float64(s.opts.CompactionTrigger)
	}
	limit := s.opts.LevelSize
	for level := 1; level < maxLevels-1; level++ {
		if score := float64(levelBytes(levels[level])) / float64(limit); score > bestScore {
			best, bestScore = level, score
		}
		limit *= levelMultiplier
	}
	return best, best >= 0
}

func levelBytes(tables []*sstable) int64 {
	var n int64
	for _, t := range tables {
		n += t.size
	}
	return n
}

// L0 must be compacted as a whole: its tables overlap, so moving only the newer ones
// down would leave an older version above them that shadows the newer data. Tables
// flushed to L0 during the merge are newer than every input, so they stay above it.
func (s *Store) compactLevel(level int) error {
	levels := s.version()
	upper := levels[0]
	if level > 0 {
		upper = []*sstable{s.nextToCompact(level, levels[level])}
	}
	lo, hi, err := keyRange(upper)
	if err != nil {
		return err
	}
	var lower []*sstable
	for _, t := range levels[level+1] {
		overlaps, err := overlapsRange(t, lo, hi)
		if err != nil {
			return err
		}
		if overlaps {
			lower = append(lower, t)
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
		bottom, err := isBottom(levels[level+2:], lo, hi)
		if err != nil {
			return err
		}
		if outputs, err = s.mergeTables(inputs, bottom); err != nil {
			return err
		}
	}

	err = s.commit(func(levels [][]*sstable) {
		levels[level] = slices.DeleteFunc(slices.Clone(levels[level]), func(t *sstable) bool { return slices.Contains(upper, t) })
		kept := slices.DeleteFunc(slices.Clone(levels[level+1]), func(t *sstable) bool { return slices.Contains(lower, t) })
		levels[level+1] = slices.Concat(kept, outputs)
		slices.SortFunc(levels[level+1], func(a, b *sstable) int { return bytes.Compare(a.largest(), b.largest()) })
	})
	if err != nil {
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

func (s *Store) nextToCompact(level int, tables []*sstable) *sstable {
	for _, t := range tables {
		if bytes.Compare(t.largest(), s.compactFrom[level]) > 0 {
			return t
		}
	}
	return tables[0]
}

// A tombstone may be dropped only when no deeper level can still hold an older
// version of its key; otherwise dropping it would resurrect that version.
func isBottom(deeper [][]*sstable, lo, hi []byte) (bool, error) {
	for _, tables := range deeper {
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

// Flush and compaction may run at once, so each applies its change to the levels as
// they are at commit time, never to a copy taken before its I/O.
func (s *Store) commit(edit func(levels [][]*sstable)) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	levels := slices.Clone(s.levels)
	edit(levels)
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
	s.tableBytes.Add(table.size)
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
