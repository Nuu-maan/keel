package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
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
	From, To, Term, PrevIndex, PrevTerm, LeaderCommit uint64
	Entries                                           []Entry
}

type Snapshot struct {
	Index, Term uint64
	Data        map[string][]byte
}

type diskState struct {
	State    hardState
	Log      []Entry
	Snapshot Snapshot
}

type InstallRequest struct {
	From, To, Term uint64
	Snapshot       Snapshot
}

type AppendResponse struct {
	From, Term, MatchIndex uint64
	Success                bool
}

func (n *Node) lastIndex() uint64 { return n.snapshot.Index + uint64(len(n.log)) }

func (n *Node) termAt(index uint64) uint64 {
	if index == n.snapshot.Index {
		return n.snapshot.Term
	}
	if index < n.snapshot.Index || index > n.lastIndex() {
		return 0
	}
	return n.log[index-n.snapshot.Index-1].Term
}

func cloneImage(image map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(image))
	for k, v := range image {
		out[k] = bytes.Clone(v)
	}
	return out
}

func (n *Node) loadLog() error {
	if n.snapshot.Index > n.state.Commit || (n.snapshot.Index > 0 && (n.snapshot.Term == 0 || n.snapshot.Term > n.state.Term || n.snapshot.Data == nil)) || (n.snapshot.Index == 0 && n.snapshot.Term != 0) || n.state.Commit > n.lastIndex() {
		return errors.New("raft: invalid snapshot or commit index")
	}
	prev := n.snapshot.Term
	for i, e := range n.log {
		if e.Term < prev || e.Term > n.state.Term || e.Command.Op > Delete || (e.Command.Op != Noop && len(e.Command.Key) == 0) || (e.Command.Op == Noop && (len(e.Command.Key) != 0 || len(e.Command.Value) != 0)) || (e.Command.Op == Delete && len(e.Command.Value) != 0) || len(e.Command.Key)+len(e.Command.Value) > 1<<20 {
			return fmt.Errorf("raft: invalid log entry %d", n.snapshot.Index+uint64(i)+1)
		}
		prev = e.Term
	}
	return nil
}

func (n *Node) storeEntries(entries []Entry, from uint64) error {
	next := append(append([]Entry(nil), n.log[:from-n.snapshot.Index-1]...), entries...)
	for i := range next {
		next[i].Command.Key = bytes.Clone(next[i].Command.Key)
		next[i].Command.Value = bytes.Clone(next[i].Command.Value)
	}
	// ponytail: whole-log rewrite is O(log size) per append; use a transactional segmented log when write volume warrants it.
	if err := n.persistDisk(n.state, next, n.snapshot); err != nil {
		return err
	}
	return nil
}

func (n *Node) failWrite(err error) error {
	n.err = fmt.Errorf("raft: durable log write: %w", err)
	n.role, n.leader = Follower, 0
	return n.err
}

func (n *Node) appendEntry(cmd Command) (uint64, error) {
	if cmd.Op > Delete || (cmd.Op != Noop && len(cmd.Key) == 0) || len(cmd.Key)+len(cmd.Value) > 1<<20 {
		return 0, errors.New("raft: invalid command")
	}
	cmd.Key, cmd.Value = bytes.Clone(cmd.Key), bytes.Clone(cmd.Value)
	index := n.lastIndex() + 1
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
	if req.From == n.config.ID || req.To != n.config.ID || !slices.Contains(n.config.Members, req.From) || req.Term == 0 ||
		req.PrevIndex > math.MaxInt || req.LeaderCommit > math.MaxInt || req.PrevTerm > req.Term || len(req.Entries) > 256 {
		return AppendResponse{}, errors.New("raft: invalid append request")
	}
	previousTerm := req.PrevTerm
	for _, e := range req.Entries {
		if e.Term < previousTerm || e.Term > req.Term || e.Command.Op > Delete || (e.Command.Op != Noop && len(e.Command.Key) == 0) || (e.Command.Op == Noop && (len(e.Command.Key) != 0 || len(e.Command.Value) != 0)) || (e.Command.Op == Delete && len(e.Command.Value) != 0) || len(e.Command.Key)+len(e.Command.Value) > 1<<20 {
			return AppendResponse{}, errors.New("raft: invalid entry")
		}
		previousTerm = e.Term
	}
	if req.Term > n.state.Term {
		state := n.state
		state.Term, state.VotedFor = req.Term, 0
		if err := n.persist(state); err != nil {
			return AppendResponse{}, err
		}
		n.role, n.leader, n.votes = Follower, 0, nil
	}
	resp := AppendResponse{From: n.config.ID, Term: n.state.Term}
	if req.Term < n.state.Term {
		return resp, nil
	}
	n.role, n.leader, n.votes = Follower, req.From, nil
	n.resetTimeout()
	if req.PrevIndex < n.snapshot.Index || req.PrevIndex > n.lastIndex() || n.termAt(req.PrevIndex) != req.PrevTerm {
		return resp, nil
	}
	for i, e := range req.Entries {
		at := req.PrevIndex + uint64(i) + 1
		if at <= n.lastIndex() {
			old := n.log[at-n.snapshot.Index-1]
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
		commit := min(req.LeaderCommit, n.lastIndex())
		if err := n.commitTo(commit); err != nil {
			return AppendResponse{}, err
		}
		if n.state.Commit-n.snapshot.Index >= 128 {
			if err := n.createSnapshot(); err != nil {
				return AppendResponse{}, err
			}
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
	if n.role != Leader || index == 0 || index > n.lastIndex() || n.termAt(index) != n.state.Term {
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
		cmd := n.log[n.applied-n.snapshot.Index].Command
		if cmd.Op != Noop && n.config.Apply != nil {
			if err := n.config.Apply(cmd); err != nil {
				return n.failWrite(err)
			}
		}
		if cmd.Op == Put {
			n.image[string(cmd.Key)] = bytes.Clone(cmd.Value)
		}
		if cmd.Op == Delete {
			delete(n.image, string(cmd.Key))
		}
		n.applied++
	}
	return nil
}

func (n *Node) LogInfo() (term, commit, last uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.Term, n.state.Commit, n.lastIndex()
}

func (n *Node) EntriesFrom(index uint64, limit int) (uint64, []Entry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return 0, nil, err
	}
	if n.role != Leader || index <= n.snapshot.Index || index > n.lastIndex()+1 || limit <= 0 {
		return 0, nil, errors.New("raft: invalid replication index")
	}
	var prev uint64
	if index > 1 {
		prev = n.termAt(index - 1)
	}
	end := min(n.lastIndex(), index+uint64(limit)-1)
	if index > end {
		return prev, nil, nil
	}
	entries := make([]Entry, end-index+1)
	copy(entries, n.log[index-n.snapshot.Index-1:end-n.snapshot.Index])
	return prev, entries, nil
}

func (n *Node) CreateSnapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.createSnapshot()
}

func (n *Node) createSnapshot() error {
	if err := n.failure(); err != nil {
		return err
	}
	if n.state.Commit <= n.snapshot.Index {
		return nil
	}
	index := n.state.Commit
	snap := Snapshot{Index: index, Term: n.termAt(index), Data: cloneImage(n.image)}
	encoded, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if len(encoded) > 8<<20 {
		return nil
	}
	suffix := append([]Entry(nil), n.log[index-n.snapshot.Index:]...)
	if err := n.persistDisk(n.state, suffix, snap); err != nil {
		return err
	}
	return nil
}

func (n *Node) SnapshotForPeer() (Snapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return Snapshot{}, err
	}
	if n.role != Leader || n.snapshot.Index == 0 {
		return Snapshot{}, errors.New("raft: no leader snapshot")
	}
	snap := n.snapshot
	snap.Data = cloneImage(snap.Data)
	return snap, nil
}

func (n *Node) Install(req InstallRequest) (AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.failure(); err != nil {
		return AppendResponse{}, err
	}
	if req.From == n.config.ID || req.To != n.config.ID || !slices.Contains(n.config.Members, req.From) || req.Term == 0 ||
		req.Snapshot.Index == 0 || req.Snapshot.Index > math.MaxInt || req.Snapshot.Term == 0 || req.Snapshot.Term > req.Term || req.Snapshot.Data == nil {
		return AppendResponse{}, errors.New("raft: invalid snapshot request")
	}
	if req.Term > n.state.Term {
		state := n.state
		state.Term, state.VotedFor = req.Term, 0
		if err := n.persist(state); err != nil {
			return AppendResponse{}, err
		}
		n.role, n.leader, n.votes = Follower, 0, nil
	}
	res := AppendResponse{From: n.config.ID, Term: n.state.Term}
	if req.Term < n.state.Term {
		return res, nil
	}
	n.role, n.leader, n.votes = Follower, req.From, nil
	n.resetTimeout()
	if req.Snapshot.Index <= n.state.Commit {
		res.Success = true
		res.MatchIndex = n.state.Commit
		return res, nil
	}
	state := n.state
	state.Commit = req.Snapshot.Index
	snap := req.Snapshot
	snap.Data = cloneImage(snap.Data)
	if err := n.persistDisk(state, nil, snap); err != nil {
		return AppendResponse{}, err
	}
	n.image = cloneImage(snap.Data)
	n.applied = snap.Index
	if n.config.Restore != nil {
		if err := n.config.Restore(n.image); err != nil {
			return AppendResponse{}, n.failWrite(err)
		}
	}
	res.Success = true
	res.MatchIndex = snap.Index
	return res, nil
}
