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

const headerSize = 8

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

func replay(f *os.File, apply func(Record)) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	r := bufio.NewReader(f)
	header := make([]byte, headerSize)
	var off int64
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, nil
			}
			return 0, err
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
			if end == size {
				return off, nil
			}
			return 0, fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		rec, err := decode(payload)
		if err != nil {
			return 0, fmt.Errorf("%w at offset %d", err, off)
		}
		apply(rec)
		off = end
	}
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
