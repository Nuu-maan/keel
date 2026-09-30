package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
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
