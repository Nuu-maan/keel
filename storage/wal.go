package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
)

type Op byte

const (
	OpPut Op = iota + 1
	OpDelete
)

type Record struct {
	Op    Op
	Key   []byte
	Value []byte
}

var ErrCorrupt = errors.New("wal: corrupt record")

const headerSize = 12

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func encode(rec Record) []byte {
	buf := make([]byte, headerSize, headerSize+1+binary.MaxVarintLen64+len(rec.Key)+len(rec.Value))
	buf = append(buf, byte(rec.Op))
	buf = binary.AppendUvarint(buf, uint64(len(rec.Key)))
	buf = append(buf, rec.Key...)
	buf = append(buf, rec.Value...)
	payload := buf[headerSize:]
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[8:12], crc32.Checksum(buf[0:8], crcTable))
	return buf
}

func decode(payload []byte) (Record, error) {
	if len(payload) == 0 {
		return Record{}, ErrCorrupt
	}
	op := Op(payload[0])
	if op != OpPut && op != OpDelete {
		return Record{}, ErrCorrupt
	}
	keyLen, n := binary.Uvarint(payload[1:])
	if n <= 0 || keyLen > uint64(len(payload)-1-n) {
		return Record{}, ErrCorrupt
	}
	keyEnd := 1 + n + int(keyLen)
	return Record{Op: op, Key: payload[1+n : keyEnd], Value: payload[keyEnd:]}, nil
}

type WAL struct {
	f   *os.File
	err error
}

func OpenWAL(path string, apply func(Record)) (*WAL, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	end, err := replay(f, apply)
	if err == nil {
		err = f.Truncate(end)
	}
	if err == nil {
		_, err = f.Seek(end, io.SeekStart)
	}
	if err == nil && created {
		err = syncDir(filepath.Dir(path))
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{f: f}, nil
}

// A crash can leave a prefix of the last record, possibly followed by zeros where the
// filesystem extended the file. A bad record is therefore a torn tail only if every
// byte after it is zero; anything else means corruption, and truncating would silently
// drop acknowledged writes that follow it.
func replay(f *os.File, apply func(Record)) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	r := bufio.NewReader(f)
	header := make([]byte, headerSize)
	var off int64
	for off+headerSize <= size {
		if _, err := io.ReadFull(r, header); err != nil {
			return 0, err
		}
		if crc32.Checksum(header[0:8], crcTable) != binary.LittleEndian.Uint32(header[8:12]) {
			return tornOrCorrupt(f, off, off+headerSize, size)
		}
		n := binary.LittleEndian.Uint32(header[4:8])
		end := off + headerSize + int64(n)
		if end > size {
			return off, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, err
		}
		if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(header[0:4]) {
			return tornOrCorrupt(f, off, end, size)
		}
		rec, err := decode(payload)
		if err != nil {
			return 0, fmt.Errorf("%w at offset %d", err, off)
		}
		apply(rec)
		off = end
	}
	return off, nil
}

func tornOrCorrupt(f *os.File, off, from, size int64) (int64, error) {
	buf := make([]byte, 64<<10)
	for pos := from; pos < size; {
		n, err := f.ReadAt(buf[:min(int64(len(buf)), size-pos)], pos)
		if err != nil {
			return 0, err
		}
		if slices.ContainsFunc(buf[:n], func(b byte) bool { return b != 0 }) {
			return 0, fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		pos += int64(n)
	}
	return off, nil
}

// After a failed write or fsync the on-disk state is unknown (a retried fsync can
// report success while data is lost), so the WAL refuses all further appends.
func (w *WAL) Append(rec Record) error {
	if w.err != nil {
		return w.err
	}
	if _, err := w.f.Write(encode(rec)); err != nil {
		w.err = err
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *WAL) Close() error {
	return w.f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
