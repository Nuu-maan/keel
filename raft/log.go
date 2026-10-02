package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/Nuu-maan/keel/storage"
)

const (
	Noop uint8 = iota
	Put
	Delete
)

type Command struct {
	Op         uint8
	Key, Value []byte
}

type Entry struct {
	Term    uint64
	Command Command
}

type AppendRequest struct {
	From, Term, PrevIndex, PrevTerm, LeaderCommit uint64
	Entries                                       []Entry
}

type AppendResponse struct {
	Term, MatchIndex uint64
	Success          bool
}

func (n *Node) loadLog() error {
	data, err := n.store.Get([]byte("log"))
	if errors.Is(err, storage.ErrNotFound) {
		err = nil
	} else if err == nil {
		err = json.Unmarshal(data, &n.log)
	}
	if err != nil {
		return err
	}
	if n.state.Commit > uint64(len(n.log)) {
		return errors.New("raft: commit exceeds log")
	}
	for i, e := range n.log {
		if e.Term == 0 || e.Term > n.state.Term || e.Command.Op > Delete {
			return fmt.Errorf("raft: invalid log entry %d", i+1)
		}
	}
	if len(n.log) > 0 {
		last := n.log[len(n.log)-1]
		if n.config.LastLogIndex != 0 && (n.config.LastLogIndex != uint64(len(n.log)) || n.config.LastLogTerm != last.Term) {
			return errors.New("raft: recovered log position differs")
		}
		n.config.LastLogIndex, n.config.LastLogTerm = uint64(len(n.log)), last.Term
	}
	return nil
}

func (n *Node) storeEntries(entries []Entry, from uint64) error {
	next := append(append([]Entry(nil), n.log[:from-1]...), entries...)
	// ponytail: whole-log rewrite is O(log size) per append; use a transactional segmented log when write volume warrants it.
	data, err := json.Marshal(next)
	if err == nil {
		err = n.store.Put([]byte("log"), data)
	}
	if err != nil {
		return n.failWrite(err)
	}
	n.log = next
	n.config.LastLogIndex = uint64(len(next))
	n.config.LastLogTerm = next[len(next)-1].Term
	return nil
}

func (n *Node) failWrite(err error) error {
	n.err = fmt.Errorf("raft: durable log write: %w", err)
	n.role, n.leader = Follower, 0
	return n.err
}

func (n *Node) appendEntry(cmd Command) (uint64, error) {
	if cmd.Op > Delete || (cmd.Op != Noop && len(cmd.Key) == 0) {
		return 0, errors.New("raft: invalid command")
	}
	cmd.Key, cmd.Value = bytes.Clone(cmd.Key), bytes.Clone(cmd.Value)
	index := uint64(len(n.log)) + 1
	if err := n.storeEntries([]Entry{{Term: n.state.Term, Command: cmd}}, index); err != nil {
		return 0, err
	}
	return index, nil
}

func (n *Node) Propose(cmd Command) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return 0, err
	}
	if n.role != Leader {
		return 0, errors.New("raft: not leader")
	}
	return n.appendEntry(cmd)
}

func (n *Node) Append(req AppendRequest) (AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return AppendResponse{}, err
	}
	if req.From == n.config.ID || !slices.Contains(n.config.Members, req.From) || req.Term == 0 ||
		req.PrevIndex > math.MaxInt || req.LeaderCommit > math.MaxInt || req.PrevTerm > req.Term || len(req.Entries) > 256 {
		return AppendResponse{}, errors.New("raft: invalid append request")
	}
	for _, e := range req.Entries {
		if e.Term == 0 || e.Term > req.Term || e.Command.Op > Delete {
			return AppendResponse{}, errors.New("raft: invalid entry")
		}
	}
	if req.Term > n.state.Term {
		state := n.state
		state.Term, state.VotedFor = req.Term, 0
		if err := n.persist(state); err != nil {
			return AppendResponse{}, err
		}
		n.role, n.leader, n.votes = Follower, 0, nil
	}
	resp := AppendResponse{Term: n.state.Term}
	if req.Term < n.state.Term {
		return resp, nil
	}
	n.role, n.leader, n.votes = Follower, req.From, nil
	n.resetTimeout()
	if req.PrevIndex > uint64(len(n.log)) || (req.PrevIndex > 0 && n.log[req.PrevIndex-1].Term != req.PrevTerm) {
		return resp, nil
	}
	for i, e := range req.Entries {
		at := req.PrevIndex + uint64(i) + 1
		if at <= uint64(len(n.log)) {
			old := n.log[at-1]
			if old.Term == e.Term && old.Command.Op == e.Command.Op && bytes.Equal(old.Command.Key, e.Command.Key) && bytes.Equal(old.Command.Value, e.Command.Value) {
				continue
			}
			if old.Term == e.Term {
				return AppendResponse{}, errors.New("raft: same term and index have different commands")
			}
			if at <= n.state.Commit {
				return AppendResponse{}, errors.New("raft: attempted to overwrite committed entry")
			}
		}
		if err := n.storeEntries(req.Entries[i:], at); err != nil {
			return AppendResponse{}, err
		}
		break
	}
	resp.Success = true
	resp.MatchIndex = req.PrevIndex + uint64(len(req.Entries))
	if req.LeaderCommit > n.state.Commit {
		commit := min(req.LeaderCommit, uint64(len(n.log)))
		if err := n.commitTo(commit); err != nil {
			return AppendResponse{}, err
		}
	}
	return resp, nil
}

func (n *Node) Commit(index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return err
	}
	if n.role != Leader || index == 0 || index > uint64(len(n.log)) || n.log[index-1].Term != n.state.Term {
		return errors.New("raft: invalid leader commit")
	}
	return n.commitTo(index)
}

func (n *Node) commitTo(index uint64) error {
	if index <= n.state.Commit {
		return nil
	}
	state := n.state
	state.Commit = index
	if err := n.persist(state); err != nil {
		return err
	}
	return n.applyCommitted()
}

func (n *Node) applyCommitted() error {
	for n.applied < n.state.Commit {
		cmd := n.log[n.applied].Command
		if cmd.Op != Noop && n.config.Apply != nil {
			if err := n.config.Apply(cmd); err != nil {
				return n.failWrite(err)
			}
		}
		n.applied++
	}
	return nil
}

func (n *Node) LogInfo() (term, commit, last uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.Term, n.state.Commit, uint64(len(n.log))
}

func (n *Node) EntriesFrom(index uint64, limit int) (uint64, []Entry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return 0, nil, err
	}
	if n.role != Leader || index == 0 || index > uint64(len(n.log))+1 {
		return 0, nil, errors.New("raft: invalid replication index")
	}
	var prev uint64
	if index > 1 {
		prev = n.log[index-2].Term
	}
	end := min(uint64(len(n.log)), index+uint64(limit)-1)
	if index > end {
		return prev, nil, nil
	}
	entries := make([]Entry, end-index+1)
	copy(entries, n.log[index-1:end])
	return prev, entries, nil
}
