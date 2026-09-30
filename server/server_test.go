package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Nuu-maan/keel/client"
	"github.com/Nuu-maan/keel/storage"
	"github.com/Nuu-maan/keel/wire"
)

func startServer(t *testing.T, opts Options) (string, *Server) {
	t.Helper()
	store, err := storage.Open(t.TempDir(), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(store, opts)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() {
		srv.Close()
		store.Close()
	})
	return ln.Addr().String(), srv
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPutGetDelete(t *testing.T) {
	addr, _ := startServer(t, Options{})
	c := dial(t, addr)
	ctx := context.Background()

	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(ctx, []byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if err := c.Delete(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, []byte("k")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestPipelinedCallsOnOneConnection(t *testing.T) {
	addr, _ := startServer(t, Options{})
	c := dial(t, addr)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 500)
	for i := range 500 {
		wg.Go(func() {
			key := fmt.Appendf(nil, "k%d", i)
			if err := c.Put(ctx, key, key); err != nil {
				errs <- err
				return
			}
			v, err := c.Get(ctx, key)
			if err != nil || string(v) != string(key) {
				errs <- fmt.Errorf("%s = %q, %v", key, v, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestServerDropsConnectionOnBadInput(t *testing.T) {
	addr, _ := startServer(t, Options{})
	for name, frame := range map[string][]byte{
		"unknown op":     append(binary.BigEndian.AppendUint32(nil, 9), 99, 0, 0, 0, 0, 0, 0, 0, 1),
		"oversize frame": binary.BigEndian.AppendUint32(nil, wire.MaxFrameSize+1),
	} {
		t.Run(name, func(t *testing.T) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.Write(frame); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("want the server to close the connection, got %v", err)
			}
		})
	}
}

func TestCloseFailsOutstandingClients(t *testing.T) {
	addr, srv := startServer(t, Options{})
	c := dial(t, addr)
	ctx := context.Background()
	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.Get(ctx, []byte("k")); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a connection error after server Close, got %v", err)
	}
}

func TestIdleConnectionIsClosed(t *testing.T) {
	addr, _ := startServer(t, Options{IdleTimeout: 100 * time.Millisecond})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("want the server to close an idle connection, got %v", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("idle connection closed after %v", waited)
	}
}
