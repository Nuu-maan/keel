package raft

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Nuu-maan/keel/client"
	"github.com/Nuu-maan/keel/storage"
)

var ErrNotLeader = errors.New("raft: no available leader")
var ErrNoQuorum = errors.New("raft: write not committed; quorum unavailable")

type Peer struct {
	RaftAddr, ClientAddr string
}

type ClusterOptions struct {
	Tick          time.Duration
	ElectionTicks int
	PeerTimeout   time.Duration
}

type Cluster struct {
	mu          sync.Mutex
	peerMu      sync.Mutex
	node        *Node
	app         *storage.Store
	peers       map[uint64]Peer
	httpClient  *http.Client
	serverTLS   *tls.Config
	secure      bool
	server      *http.Server
	errors      chan error
	stop        chan struct{}
	done        chan struct{}
	leaderTerm  uint64
	next, match map[uint64]uint64
	closed      bool
	tick        time.Duration
}

func OpenCluster(dir string, id uint64, peers map[uint64]Peer, app *storage.Store, opts ClusterOptions) (*Cluster, error) {
	if opts.Tick == 0 {
		opts.Tick = 100 * time.Millisecond
	}
	if opts.ElectionTicks == 0 {
		opts.ElectionTicks = 10
	}
	if opts.PeerTimeout == 0 {
		opts.PeerTimeout = 500 * time.Millisecond
	}
	if opts.Tick < time.Millisecond || opts.PeerTimeout < time.Millisecond {
		return nil, errors.New("raft: invalid timing options")
	}
	if app == nil || len(peers) == 0 {
		return nil, errors.New("raft: missing application store or peers")
	}
	existing, err := app.Snapshot()
	if err != nil {
		return nil, err
	}
	members := make([]uint64, 0, len(peers))
	ownPeers := make(map[uint64]Peer, len(peers))
	raftAddrs, clientAddrs := map[string]bool{}, map[string]bool{}
	for id, p := range peers {
		if p.RaftAddr == "" || p.ClientAddr == "" {
			return nil, errors.New("raft: missing peer address")
		}
		if raftAddrs[p.RaftAddr] || clientAddrs[p.ClientAddr] {
			return nil, errors.New("raft: duplicate peer address")
		}
		if _, _, err := net.SplitHostPort(p.RaftAddr); err != nil {
			return nil, err
		}
		if _, _, err := net.SplitHostPort(p.ClientAddr); err != nil {
			return nil, err
		}
		raftAddrs[p.RaftAddr], clientAddrs[p.ClientAddr] = true, true
		members = append(members, id)
		ownPeers[id] = p
	}
	slices.Sort(members)
	apply := func(cmd Command) error {
		switch cmd.Op {
		case Put:
			return app.Put(cmd.Key, cmd.Value)
		case Delete:
			return app.Delete(cmd.Key)
		}
		return nil
	}
	restore := func(image map[string][]byte) error {
		current, err := app.Snapshot()
		if err != nil {
			return err
		}
		for key := range current {
			if _, ok := image[key]; !ok {
				if err := app.Delete([]byte(key)); err != nil {
					return err
				}
			}
		}
		for key, value := range image {
			if err := app.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}
	node, err := Open(dir, Config{ID: id, Members: members, ElectionTicks: opts.ElectionTicks, Apply: apply, Restore: restore, RequireExisting: len(existing) > 0})
	if err != nil {
		return nil, err
	}
	return &Cluster{node: node, app: app, peers: ownPeers, httpClient: &http.Client{Timeout: opts.PeerTimeout}, stop: make(chan struct{}), done: make(chan struct{}), errors: make(chan error, 1), next: map[uint64]uint64{}, match: map[uint64]uint64{}, tick: opts.Tick}, nil
}

func (c *Cluster) EnableTLS(certFile, keyFile, caFile string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server != nil || c.closed {
		return errors.New("raft: TLS must be configured before start")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("raft: invalid peer CA")
	}
	c.serverTLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	c.httpClient.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}}}
	c.secure = true
	return nil
}

func (c *Cluster) Start(ln net.Listener) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.server != nil {
		ln.Close()
		return errors.New("raft: cluster already started or closed")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /vote", c.handleVote)
	mux.HandleFunc("POST /append", c.handleAppend)
	mux.HandleFunc("POST /snapshot", c.handleSnapshot)
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	if !c.secure {
		for _, p := range c.peers {
			host, _, err := net.SplitHostPort(p.RaftAddr)
			if err != nil || (host != "localhost" && !net.ParseIP(strings.Trim(host, "[]")).IsLoopback()) {
				ln.Close()
				c.server = nil
				return errors.New("raft: non-loopback peers require TLS")
			}
		}
	} else {
		ln = tls.NewListener(ln, c.serverTLS)
	}
	go func() {
		if err := c.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case c.errors <- err:
			default:
			}
		}
	}()
	go c.run()
	return nil
}

func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.stop)
	srv := c.server
	c.mu.Unlock()
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := srv.Shutdown(ctx); err != nil {
			srv.Close()
		}
		cancel()
		<-c.done
	}
	return c.node.Close()
}

func (c *Cluster) Status() Status       { return c.node.Status() }
func (c *Cluster) Errors() <-chan error { return c.errors }

func (c *Cluster) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.tick)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.mu.Lock()
			messages, err := c.node.Tick()
			if err != nil {
				select {
				case c.errors <- err:
				default:
				}
				c.mu.Unlock()
				return
			}
			{
				for _, m := range messages {
					if m.Type != RequestVote {
						continue
					}
					var vote Message
					if c.post(m.To, "vote", m, &vote) == nil {
						c.node.Step(vote)
					}
				}
				if c.node.Status().Role == Leader {
					c.replicateAll()
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Cluster) post(id uint64, route string, request, reply any) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.httpClient.Timeout)
	defer cancel()
	scheme := "http://"
	if c.secure {
		scheme = "https://"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, scheme+c.peers[id].RaftAddr+"/"+route, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("raft: peer %d replied %s", id, res.Status)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 16<<20)).Decode(reply)
}

func decodeRPC(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		http.Error(w, "extra request data", http.StatusBadRequest)
		return false
	}
	return true
}

func (c *Cluster) handleVote(w http.ResponseWriter, r *http.Request) {
	var req Message
	if !decodeRPC(w, r, &req) {
		return
	}
	if req.Type != RequestVote {
		http.Error(w, "invalid vote request", http.StatusBadRequest)
		return
	}
	out, err := c.node.Step(req)
	if err != nil || len(out) != 1 {
		http.Error(w, "vote rejected", http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(out[0])
}

func (c *Cluster) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req AppendRequest
	if !decodeRPC(w, r, &req) {
		return
	}
	res, err := c.node.Append(req)
	if err != nil {
		http.Error(w, "append rejected", http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(res)
}

func (c *Cluster) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req InstallRequest
	if !decodeRPC(w, r, &req) {
		return
	}
	res, err := c.node.Install(req)
	if err != nil {
		http.Error(w, "snapshot rejected", http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(res)
}

func (c *Cluster) replicatePeer(id uint64, term uint64) bool {
	for attempts := 0; attempts < 256; attempts++ {
		current, commit, last := c.node.LogInfo()
		if current != term || c.node.Status().Role != Leader {
			return false
		}
		c.peerMu.Lock()
		index := c.next[id]
		c.peerMu.Unlock()
		if index == 0 {
			index = last + 1
		}
		if index > last+1 {
			index = last + 1
		}
		c.node.mu.Lock()
		base := c.node.snapshot.Index
		c.node.mu.Unlock()
		if index <= base {
			snap, err := c.node.SnapshotForPeer()
			if err != nil {
				return false
			}
			var res AppendResponse
			if err := c.post(id, "snapshot", InstallRequest{From: c.node.Status().ID, To: id, Term: term, Snapshot: snap}, &res); err != nil {
				return false
			}
			if res.Term > term {
				c.node.Step(Message{Type: HeartbeatResponse, From: id, To: c.node.Status().ID, Term: res.Term})
				return false
			}
			if !res.Success || res.Term != term || res.From != id {
				return false
			}
			c.peerMu.Lock()
			c.match[id] = max(c.match[id], res.MatchIndex)
			c.next[id] = res.MatchIndex + 1
			c.peerMu.Unlock()
			continue
		}
		prev, entries, err := c.node.EntriesFrom(index, 8)
		if err != nil {
			return false
		}
		req := AppendRequest{From: c.node.Status().ID, To: id, Term: term, PrevIndex: index - 1, PrevTerm: prev, LeaderCommit: commit, Entries: entries}
		var res AppendResponse
		if err := c.post(id, "append", req, &res); err != nil {
			return false
		}
		if res.Term > term {
			c.node.Step(Message{Type: HeartbeatResponse, From: id, To: req.From, Term: res.Term})
			return false
		}
		if res.Term != term || res.From != id {
			return false
		}
		if !res.Success {
			if index <= 1 {
				return false
			}
			c.peerMu.Lock()
			c.next[id] = index - 1
			c.peerMu.Unlock()
			continue
		}
		c.peerMu.Lock()
		c.match[id] = max(c.match[id], res.MatchIndex)
		c.next[id] = res.MatchIndex + 1
		c.peerMu.Unlock()
		if res.MatchIndex == last {
			return true
		}
	}
	return false
}

func (c *Cluster) replicateAll() (bool, error) {
	status := c.node.Status()
	if status.Role != Leader {
		return false, ErrNotLeader
	}
	term, _, last := c.node.LogInfo()
	if c.leaderTerm != term {
		c.leaderTerm = term
		clear(c.next)
		clear(c.match)
		for id := range c.peers {
			c.next[id] = last + 1
		}
	}
	replies := 1
	results := make(chan bool, len(c.peers)-1)
	for id := range c.peers {
		if id != status.ID {
			go func(id uint64) { results <- c.replicatePeer(id, term) }(id)
		}
	}
	for range len(c.peers) - 1 {
		if <-results {
			replies++
		}
	}
	if c.node.Status().Role != Leader || c.node.Status().Term != term {
		return false, ErrNotLeader
	}
	matches := []uint64{last}
	for id := range c.peers {
		if id != status.ID {
			matches = append(matches, c.match[id])
		}
	}
	slices.Sort(matches)
	majority := len(matches)/2 + 1
	candidate := matches[len(matches)-majority]
	_, commit, _ := c.node.LogInfo()
	if candidate > commit {
		c.node.mu.Lock()
		currentTerm := c.node.termAt(candidate) == term
		c.node.mu.Unlock()
		if currentTerm {
			if err := c.node.Commit(candidate); err != nil {
				return false, err
			}
			c.node.mu.Lock()
			due := c.node.state.Commit-c.node.snapshot.Index >= 128
			c.node.mu.Unlock()
			if due {
				if err := c.node.CreateSnapshot(); err != nil {
					return false, err
				}
			}
		}
	}
	return replies >= majority, nil
}

func (c *Cluster) submit(cmd Command) error {
	if len(cmd.Key)+len(cmd.Value) > 1<<20 {
		return errors.New("raft: command exceeds 1 MiB")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return storage.ErrClosed
	}
	if c.node.Status().Role != Leader {
		c.mu.Unlock()
		return c.forward(cmd, nil)
	}
	index, err := c.node.Propose(cmd)
	if err == nil {
		_, err = c.replicateAll()
		if err == nil {
			_, commit, _ := c.node.LogInfo()
			if commit < index {
				err = ErrNoQuorum
			}
		}
	}
	c.mu.Unlock()
	return err
}

func (c *Cluster) Put(key, value []byte) error {
	return c.submit(Command{Op: Put, Key: key, Value: value})
}
func (c *Cluster) Delete(key []byte) error { return c.submit(Command{Op: Delete, Key: key}) }

func (c *Cluster) Get(key []byte) ([]byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, storage.ErrClosed
	}
	if c.node.Status().Role != Leader {
		c.mu.Unlock()
		var value []byte
		err := c.forward(Command{Key: key}, &value)
		return value, err
	}
	ok, err := c.replicateAll()
	if err == nil && !ok {
		err = ErrNoQuorum
	}
	if err == nil {
		term, commit, _ := c.node.LogInfo()
		c.node.mu.Lock()
		ready := commit > 0 && c.node.termAt(commit) == term
		c.node.mu.Unlock()
		if !ready {
			err = ErrNoQuorum
		}
	}
	var value []byte
	if err == nil {
		value, err = c.app.Get(key)
	}
	c.mu.Unlock()
	return value, err
}

func (c *Cluster) forward(cmd Command, value *[]byte) error {
	status := c.node.Status()
	if status.Leader == 0 || status.Leader == status.ID {
		return ErrNotLeader
	}
	peer, ok := c.peers[status.Leader]
	if !ok {
		return ErrNotLeader
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := client.Dial(ctx, peer.ClientAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if value != nil {
		*value, err = conn.Get(ctx, cmd.Key)
		if errors.Is(err, client.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}
	if cmd.Op == Put {
		return conn.Put(ctx, cmd.Key, cmd.Value)
	}
	return conn.Delete(ctx, cmd.Key)
}
