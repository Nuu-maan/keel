package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"

	"github.com/Nuu-maan/keel/wire"
)

var ErrNotFound = errors.New("keel: key not found")

type ServerError struct {
	Message string
}

func (e *ServerError) Error() string { return "keel: server error: " + e.Message }

// Client is safe for concurrent use. Calls from many goroutines are pipelined over
// one connection and matched to responses by request ID.
type Client struct {
	conn net.Conn

	writeMu sync.Mutex
	w       *bufio.Writer

	mu      sync.Mutex
	pending map[uint64]chan wire.Response
	nextID  uint64
	err     error
	broken  chan struct{}
}

func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:    conn,
		w:       bufio.NewWriter(conn),
		pending: map[uint64]chan wire.Response{},
		broken:  make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	resp, err := c.call(ctx, wire.Request{Op: wire.OpGet, Key: key})
	return resp.Value, err
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.call(ctx, wire.Request{Op: wire.OpPut, Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.call(ctx, wire.Request{Op: wire.OpDelete, Key: key})
	return err
}

func (c *Client) Close() error {
	c.fail(net.ErrClosed)
	return nil
}

func (c *Client) call(ctx context.Context, req wire.Request) (wire.Response, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return wire.Response{}, c.err
	}
	c.nextID++
	req.ID = c.nextID
	done := make(chan wire.Response, 1)
	c.pending[req.ID] = done
	c.mu.Unlock()
	defer c.forget(req.ID)

	if err := c.send(ctx, req); err != nil {
		c.fail(err)
		return wire.Response{}, err
	}
	select {
	case resp := <-done:
		switch resp.Status {
		case wire.StatusOK:
			return resp, nil
		case wire.StatusNotFound:
			return resp, ErrNotFound
		default:
			return resp, &ServerError{Message: string(resp.Value)}
		}
	case <-ctx.Done():
		return wire.Response{}, ctx.Err()
	case <-c.broken:
		return wire.Response{}, c.err
	}
}

func (c *Client) send(ctx context.Context, req wire.Request) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	deadline, _ := ctx.Deadline()
	c.conn.SetWriteDeadline(deadline)
	if err := wire.WriteFrame(c.w, req.Encode()); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Client) forget(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) readLoop() {
	r := bufio.NewReader(c.conn)
	for {
		frame, err := wire.ReadFrame(r)
		if err != nil {
			c.fail(err)
			return
		}
		resp, err := wire.DecodeResponse(frame)
		if err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		done := c.pending[resp.ID]
		c.mu.Unlock()
		if done != nil {
			select {
			case done <- resp:
			default:
			}
		}
	}
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.broken)
	c.conn.Close()
}
