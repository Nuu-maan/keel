package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const MaxFrameSize = 16 << 20

var (
	ErrFrameTooLarge = errors.New("wire: frame exceeds maximum size")
	ErrMalformed     = errors.New("wire: malformed message")
)

type Op byte

const (
	OpGet Op = iota + 1
	OpPut
	OpDelete
)

type Status byte

const (
	StatusOK Status = iota
	StatusNotFound
	StatusError
)

type Request struct {
	ID    uint64
	Op    Op
	Key   []byte
	Value []byte
}

type Response struct {
	ID     uint64
	Status Status
	Value  []byte
}

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(payload)), uint32(len(payload)))
	_, err := w.Write(append(frame, payload...))
	return err
}

func ReadFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}

func (r Request) Encode() []byte {
	b := make([]byte, 0, 9+binary.MaxVarintLen64+len(r.Key)+len(r.Value))
	b = append(b, byte(r.Op))
	b = binary.BigEndian.AppendUint64(b, r.ID)
	b = binary.AppendUvarint(b, uint64(len(r.Key)))
	b = append(b, r.Key...)
	return append(b, r.Value...)
}

func DecodeRequest(p []byte) (Request, error) {
	if len(p) < 9 {
		return Request{}, ErrMalformed
	}
	req := Request{Op: Op(p[0]), ID: binary.BigEndian.Uint64(p[1:9])}
	if req.Op < OpGet || req.Op > OpDelete {
		return Request{}, fmt.Errorf("%w: unknown op %d", ErrMalformed, req.Op)
	}
	keyLen, n := binary.Uvarint(p[9:])
	if n <= 0 || keyLen > uint64(len(p)-9-n) {
		return Request{}, ErrMalformed
	}
	keyEnd := 9 + n + int(keyLen)
	req.Key, req.Value = p[9+n:keyEnd], p[keyEnd:]
	return req, nil
}

func (r Response) Encode() []byte {
	b := make([]byte, 0, 9+len(r.Value))
	b = append(b, byte(r.Status))
	b = binary.BigEndian.AppendUint64(b, r.ID)
	return append(b, r.Value...)
}

func DecodeResponse(p []byte) (Response, error) {
	if len(p) < 9 || Status(p[0]) > StatusError {
		return Response{}, ErrMalformed
	}
	return Response{Status: Status(p[0]), ID: binary.BigEndian.Uint64(p[1:9]), Value: p[9:]}, nil
}
