package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	for _, p := range [][]byte{{}, []byte("hello"), bytes.Repeat([]byte("x"), 70000)} {
		if err := WriteFrame(&buf, p); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []int{0, 5, 70000} {
		got, err := ReadFrame(&buf)
		if err != nil || len(got) != want {
			t.Fatalf("got %d bytes, %v; want %d", len(got), err, want)
		}
	}
	if _, err := ReadFrame(&buf); err != io.EOF {
		t.Fatalf("want io.EOF at a clean boundary, got %v", err)
	}
}

func TestReadFrameRejectsOversizedLengthBeforeAllocating(t *testing.T) {
	header := binary.BigEndian.AppendUint32(nil, MaxFrameSize+1)
	if _, err := ReadFrame(bytes.NewReader(header)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

func TestReadFrameReportsTruncation(t *testing.T) {
	frame := append(binary.BigEndian.AppendUint32(nil, 10), "short"...)
	if _, err := ReadFrame(bytes.NewReader(frame)); err != io.ErrUnexpectedEOF {
		t.Fatalf("want io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestRequestRoundTrip(t *testing.T) {
	for _, want := range []Request{
		{ID: 1, Op: OpGet, Key: []byte("k")},
		{ID: 1<<64 - 1, Op: OpPut, Key: []byte("key"), Value: []byte("value")},
		{ID: 7, Op: OpDelete, Key: []byte{}},
	} {
		got, err := DecodeRequest(want.Encode())
		if err != nil || got.ID != want.ID || got.Op != want.Op || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("got %+v, %v; want %+v", got, err, want)
		}
	}
}

func TestResponseRoundTrip(t *testing.T) {
	want := Response{ID: 42, Status: StatusNotFound, Value: []byte("v")}
	got, err := DecodeResponse(want.Encode())
	if err != nil || got.ID != want.ID || got.Status != want.Status || string(got.Value) != "v" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestDecodeRejectsMalformedMessages(t *testing.T) {
	valid := Request{ID: 1, Op: OpPut, Key: []byte("key"), Value: []byte("v")}.Encode()
	for name, p := range map[string][]byte{
		"empty":            {},
		"short header":     valid[:5],
		"unknown op":       append([]byte{9}, valid[1:]...),
		"key past the end": append(append([]byte{}, valid[:9]...), 50, 'k'),
	} {
		if _, err := DecodeRequest(p); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: want ErrMalformed, got %v", name, err)
		}
	}
	if _, err := DecodeResponse([]byte{9, 0, 0, 0, 0, 0, 0, 0, 1}); !errors.Is(err, ErrMalformed) {
		t.Errorf("unknown status: want ErrMalformed, got %v", err)
	}
}
