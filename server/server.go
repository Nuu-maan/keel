package server

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/Nuu-maan/keel/client"
	"github.com/Nuu-maan/keel/storage"
	"github.com/Nuu-maan/keel/wire"
)

type Options struct {
	MaxInFlight  int
	IdleTimeout  time.Duration
	WriteTimeout time.Duration
}

type Store interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Delete([]byte) error
}

type Server struct {
	store Store
	opts  Options

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closed   bool
	handlers sync.WaitGroup
}

func New(store Store, opts Options) *Server {
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = 256
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 5 * time.Minute
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	return &Server{store: store, opts: opts, conns: map[net.Conn]struct{}{}}
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return net.ErrClosed
	}
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return nil
			}
			return err
		}
		if !s.track(conn) {
			conn.Close()
			return nil
		}
		go s.handle(conn)
	}
}

// Close stops accepting, closes every connection, and waits for requests already
// running to finish, so the caller can close the store as soon as Close returns.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
	s.handlers.Wait()
	return err
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	s.handlers.Add(1)
	return true
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	conn.Close()
	s.handlers.Done()
}

// Requests on one connection run concurrently so that they can share group commits,
// and responses go back in completion order, matched to requests by ID. Once
// MaxInFlight requests are running, the loop stops reading and TCP flow control
// pushes back on the client.
func (s *Server) handle(conn net.Conn) {
	defer s.untrack(conn)
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	var writeMu sync.Mutex
	slots := make(chan struct{}, s.opts.MaxInFlight)
	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	for {
		conn.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
		frame, err := wire.ReadFrame(r)
		if err != nil {
			return
		}
		req, err := wire.DecodeRequest(frame)
		if err != nil {
			return
		}
		slots <- struct{}{}
		inFlight.Add(1)
		go func() {
			defer inFlight.Done()
			defer func() { <-slots }()
			resp := s.execute(req).Encode()
			writeMu.Lock()
			defer writeMu.Unlock()
			conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
			if err := wire.WriteFrame(w, resp); err != nil || w.Flush() != nil {
				conn.Close()
			}
		}()
	}
}

func (s *Server) execute(req wire.Request) wire.Response {
	var value []byte
	var err error
	switch req.Op {
	case wire.OpGet:
		value, err = s.store.Get(req.Key)
	case wire.OpPut:
		err = s.store.Put(req.Key, req.Value)
	case wire.OpDelete:
		err = s.store.Delete(req.Key)
	}
	switch {
	case err == nil:
		return wire.Response{ID: req.ID, Status: wire.StatusOK, Value: value}
	case errors.Is(err, storage.ErrNotFound) || errors.Is(err, client.ErrNotFound):
		return wire.Response{ID: req.ID, Status: wire.StatusNotFound}
	default:
		return wire.Response{ID: req.ID, Status: wire.StatusError, Value: []byte(err.Error())}
	}
}
