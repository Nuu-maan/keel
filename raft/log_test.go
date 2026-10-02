package raft

import (
	"errors"
	"testing"
)

func TestReplicationCommitAndRestart(t *testing.T) {
	members := []uint64{1, 2, 3}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	applied := []map[string]string{{}, {}, {}}
	nodes := make([]*Node, 3)
	open := func(i int) *Node {
		n, err := Open(dirs[i], Config{ID: uint64(i + 1), Members: members, Apply: func(c Command) error {
			if c.Op == Put {
				applied[i][string(c.Key)] = string(c.Value)
			}
			if c.Op == Delete {
				delete(applied[i], string(c.Key))
			}
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for i := range nodes {
		nodes[i] = open(i)
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	campaign(t, nodes[0])
	step(t, nodes[0], Message{Type: Vote, From: 2, To: 1, Term: 1, Granted: true})
	leader := nodes[0]
	if _, commit, last := leader.LogInfo(); commit != 0 || last != 1 {
		t.Fatal("leader no-op should start uncommitted")
	}
	req := AppendRequest{From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1}}}
	if r, err := nodes[1].Append(req); err != nil || !r.Success || r.MatchIndex != 1 {
		t.Fatalf("no-op append: %+v %v", r, err)
	}
	if err := leader.Commit(1); err != nil {
		t.Fatal(err)
	}
	index, err := leader.Propose(Command{Op: Put, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 {
		t.Fatal("unexpected index")
	}
	if applied[0]["k"] != "" {
		t.Fatal("applied before quorum")
	}
	req = AppendRequest{From: 1, To: 2, Term: 1, PrevIndex: 1, PrevTerm: 1, LeaderCommit: 1, Entries: []Entry{{Term: 1, Command: Command{Op: Put, Key: []byte("k"), Value: []byte("v")}}}}
	if r, err := nodes[1].Append(req); err != nil || !r.Success {
		t.Fatalf("append: %+v %v", r, err)
	}
	if err := leader.Commit(2); err != nil {
		t.Fatal(err)
	}
	if applied[0]["k"] != "v" || applied[1]["k"] != "" {
		t.Fatal("wrong application point")
	}
	req.Entries = nil
	req.PrevIndex = 2
	req.LeaderCommit = 2
	if r, err := nodes[1].Append(req); err != nil || !r.Success {
		t.Fatalf("commit on follower: %+v %v", r, err)
	}
	if applied[1]["k"] != "v" {
		t.Fatal("follower failed to apply committed write")
	}
	if err := nodes[1].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[1] = open(1)
	if applied[1]["k"] != "v" {
		t.Fatal("committed write missing after restart")
	}
	if _, commit, last := nodes[1].LogInfo(); commit != 2 || last != 2 {
		t.Fatalf("lost log metadata %d %d", commit, last)
	}
}

func TestConflictAndCommittedPrefix(t *testing.T) {
	n := openNode(t, 2, []uint64{1, 2, 3})
	appendReq := func(req AppendRequest) AppendResponse {
		t.Helper()
		r, err := n.Append(req)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := appendReq(AppendRequest{From: 1, To: 2, Term: 1, PrevIndex: 1}); r.Success {
		t.Fatal("accepted missing predecessor")
	}
	appendReq(AppendRequest{From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1}, {Term: 1, Command: Command{Op: Put, Key: []byte("k"), Value: []byte("old")}}}, LeaderCommit: 1})
	if r := appendReq(AppendRequest{From: 3, To: 2, Term: 2, PrevIndex: 1, PrevTerm: 2}); r.Success {
		t.Fatal("accepted mismatched predecessor")
	}
	if _, err := n.Append(AppendRequest{From: 3, To: 2, Term: 2, PrevIndex: 0, Entries: []Entry{{Term: 2}}}); err == nil {
		t.Fatal("overwrote committed prefix")
	}
	req := AppendRequest{From: 3, To: 2, Term: 2, PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Term: 2, Command: Command{Op: Put, Key: []byte("k"), Value: []byte("new")}}}}
	if r := appendReq(req); !r.Success || r.MatchIndex != 2 {
		t.Fatalf("conflict replacement: %+v", r)
	}
	if _, _, last := n.LogInfo(); last != 2 {
		t.Fatal("bad truncation")
	}
	if _, err := n.Append(AppendRequest{From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if s := n.Status(); s.Term != 2 || s.Leader != 3 {
		t.Fatalf("stale leader altered state: %+v", s)
	}
}

func TestApplyFailureStopsNode(t *testing.T) {
	dir := t.TempDir()
	n, err := Open(dir, Config{ID: 1, Members: []uint64{1}, Apply: func(Command) error { return errors.New("disk down") }})
	if err != nil {
		t.Fatal(err)
	}
	campaign(t, n)
	if _, err := n.Propose(Command{Op: Put, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := n.Commit(2); err == nil {
		t.Fatal("acknowledged failed application")
	}
	if n.Status().Role != Follower {
		t.Fatal("kept leadership after failed application")
	}
	if _, err := n.Tick(); err == nil {
		t.Fatal("continued after application failure")
	}
	n.Close()
	if reopened, err := Open(dir, Config{ID: 1, Members: []uint64{1}, Apply: func(Command) error { return errors.New("disk down") }}); err == nil {
		reopened.Close()
		t.Fatal("reopened despite failed replay")
	}
}
