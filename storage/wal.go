package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
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

