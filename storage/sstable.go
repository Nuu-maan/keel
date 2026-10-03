package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

const (
	targetBlockSize = 4 << 10
	footerSize      = 40
	sstMagic        = 0x4b45454c53535432
)

func writeSSTable(path string, recs []Record) error {
	return writeFileAtomic(path, encodeSSTable(recs))
}

func encodeSSTable(recs []Record) []byte {
	var out, block, index []byte
	hashes := make([]uint64, len(recs))
	for i, rec := range recs {
		hashes[i] = bloomHash(rec.Key)
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
	filter := newBloom(hashes)
	filterOffset := len(out)
	out = append(out, filter...)
	indexOffset := len(out)
	out = append(out, index...)
	out = appendSection(out, filterOffset, filter)
	out = appendSection(out, indexOffset, index)
	return binary.LittleEndian.AppendUint64(out, sstMagic)
}

func appendSection(out []byte, offset int, data []byte) []byte {
	out = binary.LittleEndian.AppendUint64(out, uint64(offset))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(data)))
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(data, crcTable))
}

type blockHandle struct {
	lastKey []byte
	offset  uint64
	length  uint64
}

type sstable struct {
	num    uint64
	size   int64
	f      *os.File
	filter []byte
	index  []blockHandle
}

func openSSTable(path string) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t, err := readMeta(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

type section struct {
	offset, length uint64
	crc            uint32
}

func parseSection(b []byte) section {
	return section{
		offset: binary.LittleEndian.Uint64(b[0:8]),
		length: uint64(binary.LittleEndian.Uint32(b[8:12])),
		crc:    binary.LittleEndian.Uint32(b[12:16]),
	}
}

func readSection(f *os.File, s section) ([]byte, error) {
	buf := make([]byte, s.length)
	if _, err := f.ReadAt(buf, int64(s.offset)); err != nil {
		return nil, err
	}
	if crc32.Checksum(buf, crcTable) != s.crc {
		return nil, ErrCorrupt
	}
	return buf, nil
}

func readMeta(f *os.File) (*sstable, error) {
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
	filterSec, indexSec := parseSection(footer[0:16]), parseSection(footer[16:32])
	if binary.LittleEndian.Uint64(footer[32:40]) != sstMagic ||
		filterSec.offset > size || filterSec.offset+filterSec.length != indexSec.offset ||
		indexSec.offset > size || indexSec.offset+indexSec.length != size-footerSize {
		return nil, ErrCorrupt
	}
	filter, err := readSection(f, filterSec)
	if err != nil {
		return nil, err
	}
	buf, err := readSection(f, indexSec)
	if err != nil {
		return nil, err
	}

	dataEnd := filterSec.offset
	var index []blockHandle
	c := cursor{buf: buf}
	for len(c.buf) > 0 {
		h := blockHandle{lastKey: c.bytes(c.uvarint()), offset: c.uvarint(), length: c.uvarint()}
		if c.bad || h.length < 4 || h.length > dataEnd || h.offset > dataEnd-h.length {
			return nil, ErrCorrupt
		}
		index = append(index, h)
	}
	return &sstable{f: f, size: int64(size), filter: filter, index: index}, nil
}

func (t *sstable) get(key []byte) (Record, bool, error) {
	if !bloomMayContain(t.filter, key) {
		return Record{}, false, nil
	}
	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].lastKey, key) >= 0
	})
	if i == len(t.index) {
		return Record{}, false, nil
	}
	var found Record
	var ok bool
	err := t.scanBlock(t.index[i], func(rec Record) bool {
		switch bytes.Compare(rec.Key, key) {
		case 0:
			found, ok = rec, true
			return false
		case 1:
			return false
		}
		return true
	})
	return found, ok, err
}

func (t *sstable) all() ([]Record, error) {
	var recs []Record
	for _, h := range t.index {
		err := t.scanBlock(h, func(rec Record) bool {
			recs = append(recs, rec)
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	return recs, nil
}

func (t *sstable) scanBlock(h blockHandle, visit func(Record) bool) error {
	block := make([]byte, h.length)
	if _, err := t.f.ReadAt(block, int64(h.offset)); err != nil {
		return err
	}
	data, sum := block[:len(block)-4], block[len(block)-4:]
	if crc32.Checksum(data, crcTable) != binary.LittleEndian.Uint32(sum) {
		return t.corruptBlock(h)
	}
	c := cursor{buf: data}
	for len(c.buf) > 0 {
		k := c.bytes(c.uvarint())
		kind := c.bytes(1)
		v := c.bytes(c.uvarint())
		if c.bad || (Op(kind[0]) != OpPut && Op(kind[0]) != OpDelete) {
			return t.corruptBlock(h)
		}
		if !visit(Record{Op: Op(kind[0]), Key: k, Value: v}) {
			return nil
		}
	}
	return nil
}

func (t *sstable) corruptBlock(h blockHandle) error {
	return fmt.Errorf("%s: %w in block at offset %d", t.f.Name(), ErrCorrupt, h.offset)
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
