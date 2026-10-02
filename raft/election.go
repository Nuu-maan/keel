package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/Nuu-maan/keel/storage"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type MessageType uint8

const (
	RequestVote MessageType = iota + 1
	Vote
	Heartbeat
	HeartbeatResponse
)

type Message struct {
	Type                      MessageType
	From, To, Term            uint64
	LastLogIndex, LastLogTerm uint64
	Granted                   bool
}

type Config struct {
	ID                            uint64
	Members                       []uint64
	ElectionTicks, HeartbeatTicks int
	Apply                         func(Command) error
	Restore                       func(map[string][]byte) error
	RequireExisting               bool
}

type Status struct {
	ID, Term, VotedFor, Leader uint64
	Role                       Role
}

type hardState struct {
	ID                     uint64
	Members                []uint64
	Term, VotedFor, Commit uint64
}

type Node struct {
	mu               sync.Mutex
	config           Config
	state            hardState
	store            *storage.Store
	log              []Entry
	snapshot         Snapshot
	image            map[string][]byte
	applied          uint64
	role             Role
	leader           uint64
	votes            map[uint64]bool
	elapsed, timeout int
	err              error
	closed           bool
}

// Open requires an exclusively owned directory, separate from application data.
func Open(dir string, cfg Config) (*Node, error) {
	cfg.Members = slices.Clone(cfg.Members)
	slices.Sort(cfg.Members)
	if cfg.ID == 0 || len(cfg.Members) == 0 || cfg.Members[0] == 0 || !slices.Contains(cfg.Members, cfg.ID) {
		return nil, errors.New("raft: membership must contain the nonzero local ID")
	}
	for i := 1; i < len(cfg.Members); i++ {
		if cfg.Members[i] == cfg.Members[i-1] {
			return nil, errors.New("raft: duplicate member")
		}
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.HeartbeatTicks < 1 || cfg.ElectionTicks <= cfg.HeartbeatTicks || cfg.ElectionTicks > math.MaxInt/2 {
		return nil, errors.New("raft: invalid tick intervals")
	}
	s, err := storage.Open(dir, storage.Options{})
	if err != nil {
		return nil, err
	}
	n := &Node{config: cfg, store: s, state: hardState{ID: cfg.ID, Members: cfg.Members}}
	data, err := s.Get([]byte("state"))
	switch {
	case errors.Is(err, storage.ErrNotFound):
		legacy, legacyErr := s.Get([]byte("hard-state"))
		if legacyErr == nil {
			err = json.Unmarshal(legacy, &n.state)
			if err == nil {
				logData, logErr := s.Get([]byte("log"))
				if logErr == nil {
					err = json.Unmarshal(logData, &n.log)
				} else if !errors.Is(logErr, storage.ErrNotFound) {
					err = logErr
				}
			}
		} else if errors.Is(legacyErr, storage.ErrNotFound) {
			_, logErr := s.Get([]byte("log"))
			if logErr == nil {
				err = errors.New("raft: log exists without state")
			} else if errors.Is(logErr, storage.ErrNotFound) && cfg.RequireExisting {
				err = errors.New("raft: state missing for existing application data")
			} else if errors.Is(logErr, storage.ErrNotFound) {
				err = nil
			} else {
				err = logErr
			}
		} else {
			err = legacyErr
		}
		if err == nil {
			err = n.persistDisk(n.state, n.log, Snapshot{})
		}
	case err == nil:
		var disk diskState
		err = json.Unmarshal(data, &disk)
		if err == nil {
			n.state, n.log, n.snapshot = disk.State, disk.Log, disk.Snapshot
		}
	}
	if err == nil && (n.state.ID != cfg.ID || !slices.Equal(n.state.Members, cfg.Members) ||
		(n.state.VotedFor != 0 && !slices.Contains(cfg.Members, n.state.VotedFor)) ||
		(n.state.Term == 0 && n.state.VotedFor != 0)) {
		err = errors.New("raft: invalid state or changed membership")
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	if err := n.loadLog(); err != nil {
		s.Close()
		return nil, err
	}
	n.image = cloneImage(n.snapshot.Data)
	n.applied = n.snapshot.Index
	if n.snapshot.Index > 0 && n.config.Restore != nil {
		err = n.config.Restore(n.image)
	}
	if err == nil {
		err = n.applyCommitted()
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	n.resetTimeout()
	return n, nil
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.config.ID, Term: n.state.Term, VotedFor: n.state.VotedFor, Leader: n.leader, Role: n.role}
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	return n.store.Close()
}

// Tick advances logical time; callers choose the tick duration and deliver returned messages.
func (n *Node) Tick() ([]Message, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return nil, err
	}
	n.elapsed++
	if n.role == Leader {
		if n.elapsed < n.config.HeartbeatTicks {
			return nil, nil
		}
		n.elapsed = 0
		return n.broadcast(Heartbeat), nil
	}
	if n.elapsed < n.timeout {
		return nil, nil
	}
	if n.state.Term == math.MaxUint64 {
		n.err = errors.New("raft: term exhausted")
		return nil, n.err
	}
	state := n.state
	state.Term++
	state.VotedFor = n.config.ID
	if err := n.persist(state); err != nil {
		return nil, err
	}
	n.role, n.leader = Candidate, 0
	n.votes = map[uint64]bool{n.config.ID: true}
	n.resetTimeout()
	if n.hasMajority() {
		return n.becomeLeader()
	}
	return n.broadcast(RequestVote), nil
}

func (n *Node) Step(m Message) ([]Message, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return nil, err
	}
	if m.To != n.config.ID || m.From == n.config.ID || !slices.Contains(n.config.Members, m.From) ||
		m.Type < RequestVote || m.Type > HeartbeatResponse || m.Term == 0 ||
		(m.Type == RequestVote && ((m.LastLogIndex == 0) != (m.LastLogTerm == 0) || m.LastLogTerm > m.Term)) {
		return nil, errors.New("raft: invalid message")
	}
	if m.Term > n.state.Term {
		state := n.state
		state.Term, state.VotedFor = m.Term, 0
		if err := n.persist(state); err != nil {
			return nil, err
		}
		n.role, n.leader, n.votes = Follower, 0, nil
	}
	switch m.Type {
	case RequestVote:
		fresh := m.LastLogTerm > n.termAt(n.lastIndex()) || (m.LastLogTerm == n.termAt(n.lastIndex()) && m.LastLogIndex >= n.lastIndex())
		grant := m.Term == n.state.Term && fresh && (n.state.VotedFor == 0 || n.state.VotedFor == m.From)
		if grant {
			state := n.state
			state.VotedFor = m.From
			if err := n.persist(state); err != nil {
				return nil, err
			}
			n.resetTimeout()
		}
		return []Message{{Type: Vote, From: n.config.ID, To: m.From, Term: n.state.Term, Granted: grant}}, nil
	case Vote:
		if m.Term == n.state.Term && n.role == Candidate && m.Granted {
			n.votes[m.From] = true
			if n.hasMajority() {
				return n.becomeLeader()
			}
		}
	case Heartbeat:
		if m.Term == n.state.Term {
			n.role, n.leader, n.votes = Follower, m.From, nil
			n.resetTimeout()
		}
		return []Message{{Type: HeartbeatResponse, From: n.config.ID, To: m.From, Term: n.state.Term}}, nil
	}
	return nil, nil
}

func (n *Node) failure() error {
	if n.closed {
		return storage.ErrClosed
	}
	return n.err
}

func (n *Node) persist(state hardState) error { return n.persistDisk(state, n.log, n.snapshot) }

func (n *Node) persistDisk(state hardState, log []Entry, snapshot Snapshot) error {
	data, err := json.Marshal(diskState{State: state, Log: log, Snapshot: snapshot})
	if err == nil {
		err = n.store.Put([]byte("state"), data)
	}
	if err != nil {
		n.err = fmt.Errorf("raft: persist state: %w", err)
		n.role, n.leader = Follower, 0
		return n.err
	}
	n.state, n.log, n.snapshot = state, log, snapshot
	return nil
}

func (n *Node) resetTimeout() {
	n.elapsed = 0
	n.timeout = n.config.ElectionTicks + rand.IntN(n.config.ElectionTicks)
}

func (n *Node) hasMajority() bool { return len(n.votes) > len(n.config.Members)/2 }

func (n *Node) becomeLeader() ([]Message, error) {
	if _, err := n.appendEntry(Command{}); err != nil {
		return nil, err
	}
	n.role, n.leader, n.elapsed = Leader, n.config.ID, 0
	n.votes = nil
	return n.broadcast(Heartbeat), nil
}

func (n *Node) broadcast(kind MessageType) []Message {
	var messages []Message
	for _, id := range n.config.Members {
		if id != n.config.ID {
			messages = append(messages, Message{Type: kind, From: n.config.ID, To: id, Term: n.state.Term,
				LastLogIndex: n.lastIndex(), LastLogTerm: n.termAt(n.lastIndex())})
		}
	}
	return messages
}
