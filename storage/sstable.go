package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

const (
	targetBlockSize = 4 << 10
	footerSize      = 24
	sstMagic        = 0x4b45454c53535431
)

func writeSSTable(path string, recs []Record) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(encodeSSTable(recs))
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

func encodeSSTable(recs []Record) []byte {
	var out, block, index []byte
	for i, rec := range recs {
		block = binary.AppendUvarint(block, uint64(len(rec.Key)))
		block = append(block, rec.Key...)
		block = append(block, byte(rec.Op))
		block = binary.AppendUvarint(block, uint64(len(rec.Value)))
		block = append(block, rec.Value...)
		if len(block) < targetBlockSize && i < len(recs)-1 {
			continue
		}
		block = binary.LittleEndian.AppendUint32(block, crc32.Checksum(block, crcTable))
		index = binary.AppendUvarint(index, uint64(len(rec.Key)))
		index = append(index, rec.Key...)
		index = binary.AppendUvarint(index, uint64(len(out)))
		index = binary.AppendUvarint(index, uint64(len(block)))
		out = append(out, block...)
		block = block[:0]
	}
	indexOffset := len(out)
	out = append(out, index...)
	out = binary.LittleEndian.AppendUint64(out, uint64(indexOffset))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(index)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(index, crcTable))
	return binary.LittleEndian.AppendUint64(out, sstMagic)
}

type blockHandle struct {
	lastKey []byte
	offset  uint64
	length  uint64
}

type sstable struct {
	f     *os.File
	index []blockHandle
}

func openSSTable(path string) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	index, err := readIndex(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &sstable{f: f, index: index}, nil
}

func readIndex(f *os.File) ([]blockHandle, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(info.Size())
	if size < footerSize {
		return nil, ErrCorrupt
	}
	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, int64(size-footerSize)); err != nil {
		return nil, err
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := uint64(binary.LittleEndian.Uint32(footer[8:12]))
	if binary.LittleEndian.Uint64(footer[16:24]) != sstMagic || indexOffset > size || indexOffset+indexLen != size-footerSize {
		return nil, ErrCorrupt
	}
	buf := make([]byte, indexLen)
	if _, err := f.ReadAt(buf, int64(indexOffset)); err != nil {
		return nil, err
	}
	if crc32.Checksum(buf, crcTable) != binary.LittleEndian.Uint32(footer[12:16]) {
		return nil, ErrCorrupt
	}

	var index []blockHandle
	c := cursor{buf: buf}
	for len(c.buf) > 0 {
		h := blockHandle{lastKey: c.bytes(c.uvarint()), offset: c.uvarint(), length: c.uvarint()}
		if c.bad || h.length < 4 || h.length > indexOffset || h.offset > indexOffset-h.length {
			return nil, ErrCorrupt
		}
		index = append(index, h)
	}
	return index, nil
}

func (t *sstable) get(key []byte) (Record, bool, error) {
	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].lastKey, key) >= 0
	})
	if i == len(t.index) {
		return Record{}, false, nil
	}
	h := t.index[i]
	block := make([]byte, h.length)
	if _, err := t.f.ReadAt(block, int64(h.offset)); err != nil {
		return Record{}, false, err
	}
	data, sum := block[:len(block)-4], block[len(block)-4:]
	if crc32.Checksum(data, crcTable) != binary.LittleEndian.Uint32(sum) {
		return Record{}, false, fmt.Errorf("%s: %w in block at offset %d", t.f.Name(), ErrCorrupt, h.offset)
	}

	c := cursor{buf: data}
	for len(c.buf) > 0 {
		k := c.bytes(c.uvarint())
		kind := c.bytes(1)
		v := c.bytes(c.uvarint())
		if c.bad || (Op(kind[0]) != OpPut && Op(kind[0]) != OpDelete) {
			return Record{}, false, fmt.Errorf("%s: %w in block at offset %d", t.f.Name(), ErrCorrupt, h.offset)
		}
		switch bytes.Compare(k, key) {
		case 0:
			return Record{Op: Op(kind[0]), Key: k, Value: v}, true, nil
		case 1:
			return Record{}, false, nil
		}
	}
	return Record{}, false, nil
}

func (t *sstable) close() error {
	return t.f.Close()
}

type cursor struct {
	buf []byte
	bad bool
}

func (c *cursor) uvarint() uint64 {
	v, n := binary.Uvarint(c.buf)
	if n <= 0 {
		c.fail()
		return 0
	}
	c.buf = c.buf[n:]
	return v
}

func (c *cursor) bytes(n uint64) []byte {
	if n > uint64(len(c.buf)) {
		c.fail()
		return nil
	}
	b := c.buf[:n:n]
	c.buf = c.buf[n:]
	return b
}

func (c *cursor) fail() {
	c.bad = true
	c.buf = nil
}
