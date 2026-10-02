package raft

import (
	"errors"
	"testing"

	"github.com/Nuu-maan/keel/storage"
)

func openNode(t *testing.T, id uint64, members []uint64) *Node {
	t.Helper()
	n, err := Open(t.TempDir(), Config{ID: id, Members: members})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
	})
	return n
}

func campaign(t *testing.T, n *Node) []Message {
	t.Helper()
	for range 20 {
		out, err := n.Tick()
		if err != nil {
			t.Fatal(err)
		}
		if n.Status().Role != Follower {
			return out
		}
	}
	t.Fatal("election never started")
	return nil
}

func step(t *testing.T, n *Node, m Message) []Message {
	t.Helper()
	out, err := n.Step(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestElectionAndFailover(t *testing.T) {
	members := []uint64{1, 2, 3}
	nodes := map[uint64]*Node{}
	for _, id := range members {
		nodes[id] = openNode(t, id, members)
	}
	deliver := func(queue []Message, isolated uint64) {
		t.Helper()
		for len(queue) > 0 {
			m := queue[0]
			queue = queue[1:]
			if m.From == isolated || m.To == isolated {
				continue
			}
			queue = append(queue, step(t, nodes[m.To], m)...)
		}
	}
	deliver(campaign(t, nodes[1]), 0)
	if nodes[1].Status().Role != Leader {
		t.Fatal("no leader with majority")
	}
	for range 100 {
		for _, id := range members {
			out, err := nodes[id].Tick()
			if err != nil {
				t.Fatal(err)
			}
			deliver(out, 0)
		}
	}
	for _, n := range nodes {
		if s := n.Status(); s.Term != 1 || s.Leader != 1 {
			t.Fatalf("unstable leadership: %+v", s)
		}
	}
	deliver(campaign(t, nodes[2]), 1)
	if s := nodes[2].Status(); s.Role != Leader || s.Term != 2 {
		t.Fatalf("no failover: %+v", s)
	}
	out, err := nodes[2].Tick()
	if err != nil {
		t.Fatal(err)
	}
	deliver(out, 0)
	if s := nodes[1].Status(); s.Role != Follower || s.Term != 2 || s.Leader != 2 {
		t.Fatalf("old leader did not step down: %+v", s)
	}
}

func TestDuplicateStaleAndMinorityVotes(t *testing.T) {
	n := openNode(t, 1, []uint64{1, 2, 3, 4, 5})
	campaign(t, n)
	vote := Message{Type: Vote, From: 2, To: 1, Term: 1, Granted: true}
	step(t, n, vote)
	step(t, n, vote)
	if n.Status().Role != Candidate {
		t.Fatal("duplicate vote formed majority")
	}
	for range 20 {
		if _, err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	term := n.Status().Term
	step(t, n, vote)
	if n.Status().Role != Candidate {
		t.Fatal("stale vote elected leader")
	}
	for _, id := range []uint64{2, 3} {
		step(t, n, Message{Type: Vote, From: id, To: 1, Term: term, Granted: true})
	}
	if n.Status().Role != Leader {
		t.Fatal("distinct majority did not elect leader")
	}
	step(t, n, Message{Type: HeartbeatResponse, From: 2, To: 1, Term: term + 1})
	if n.Status().Role != Follower {
		t.Fatal("higher-term response did not demote leader")
	}
}

func TestVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ID: 1, Members: []uint64{1, 2, 3}}
	n, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := Message{Type: RequestVote, From: 2, To: 1, Term: 4}
	if !step(t, n, req)[0].Granted {
		t.Fatal("first vote rejected")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n, err = Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s := n.Status(); s.Term != 4 || s.VotedFor != 2 || s.Role != Follower {
		t.Fatalf("lost state: %+v", s)
	}
	req.From = 3
	if step(t, n, req)[0].Granted {
		t.Fatal("voted twice in one term")
	}
	req.From = 2
	if !step(t, n, req)[0].Granted {
		t.Fatal("repeat vote rejected")
	}
	req.Term = 3
	if r := step(t, n, req)[0]; r.Granted || r.Term != 4 {
		t.Fatalf("stale vote: %+v", r)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Members = []uint64{1, 2, 4}
	if changed, err := Open(dir, cfg); err == nil {
		changed.Close()
		t.Fatal("accepted changed membership")
	}
}

func TestLogFreshness(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ID: 1, Members: []uint64{1, 2, 3}}
	n, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]Entry, 10)
	for i := range entries {
		entries[i].Term = 3
	}
	if _, err := n.Append(AppendRequest{From: 2, To: 1, Term: 3, Entries: entries}); err != nil {
		t.Fatal(err)
	}
	step(t, n, Message{Type: Heartbeat, From: 2, To: 1, Term: 5})
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n, err = Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for _, tc := range []struct {
		index, term uint64
		grant       bool
	}{{100, 2, false}, {9, 3, false}, {10, 3, true}, {1, 4, true}} {
		req := Message{Type: RequestVote, From: 2, To: 1, Term: 5, LastLogIndex: tc.index, LastLogTerm: tc.term}
		if got := step(t, n, req)[0].Granted; got != tc.grant {
			t.Fatalf("log %d/%d: granted %v", tc.index, tc.term, got)
		}
	}
}

func TestPersistenceFailureStopsParticipation(t *testing.T) {
	n := openNode(t, 1, []uint64{1, 2, 3})
	if err := n.store.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() { n.closed = true }()
	req := Message{Type: RequestVote, From: 2, To: 1, Term: 1}
	out, err := n.Step(req)
	if !errors.Is(err, storage.ErrClosed) || len(out) != 0 {
		t.Fatalf("responded despite failed persistence: %v %v", out, err)
	}
	if _, err := n.Tick(); err == nil {
		t.Fatal("continued after persistence failure")
	}
	if _, err := n.Step(req); err == nil {
		t.Fatal("processed message after persistence failure")
	}
}

func TestSingleNodeAndInvalidMessages(t *testing.T) {
	n := openNode(t, 1, []uint64{1})
	campaign(t, n)
	if n.Status().Role != Leader {
		t.Fatal("single node did not elect itself")
	}
	before := n.Status()
	if _, err := n.Step(Message{Type: Heartbeat, From: 2, To: 1, Term: 99}); err == nil {
		t.Fatal("accepted outsider")
	}
	if n.Status() != before {
		t.Fatal("outsider changed state")
	}
}

func TestSplitVoteRetriesAndHeartbeatDemotesCandidate(t *testing.T) {
	n := openNode(t, 1, []uint64{1, 2, 3})
	campaign(t, n)
	term := n.Status().Term
	for range 20 {
		if _, err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n.Status().Term <= term || n.Status().Role != Candidate {
		t.Fatal("split vote did not retry")
	}
	term = n.Status().Term
	step(t, n, Message{Type: Heartbeat, From: 2, To: 1, Term: term})
	if s := n.Status(); s.Role != Follower || s.Leader != 2 {
		t.Fatalf("candidate ignored leader: %+v", s)
	}
	n.elapsed = n.timeout - 1
	step(t, n, Message{Type: Heartbeat, From: 2, To: 1, Term: term - 1})
	if _, err := n.Tick(); err != nil {
		t.Fatal(err)
	}
	if n.Status().Role != Candidate {
		t.Fatal("stale heartbeat delayed election")
	}
}

func TestInvalidConfigAndCorruptState(t *testing.T) {
	for _, cfg := range []Config{
		{}, {ID: 1, Members: []uint64{1, 1}}, {ID: 1, Members: []uint64{0, 1}},
		{ID: 1, Members: []uint64{2}}, {ID: 1, Members: []uint64{1}, ElectionTicks: -1},
		{ID: 1, Members: []uint64{1}, ElectionTicks: 2, HeartbeatTicks: 2},
	} {
		if n, err := Open(t.TempDir(), cfg); err == nil {
			n.Close()
			t.Fatalf("accepted config: %+v", cfg)
		}
	}
	dir := t.TempDir()
	cfg := Config{ID: 1, Members: []uint64{1, 2, 3}}
	n, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.store.Put([]byte("state"), []byte(`{"State":{"ID":1,"Members":[1,2,3],"Term":2,"VotedFor":99}}`)); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := Open(dir, cfg); err == nil {
		n.Close()
		t.Fatal("accepted invalid durable vote")
	}
}

func TestInvalidMessageDoesNotChangeTerm(t *testing.T) {
	n := openNode(t, 1, []uint64{1, 2, 3})
	for _, m := range []Message{
		{Type: Heartbeat, From: 2, To: 3, Term: 10},
		{Type: Heartbeat, From: 1, To: 1, Term: 10},
		{Type: 99, From: 2, To: 1, Term: 10},
		{Type: RequestVote, From: 2, To: 1, Term: 10, LastLogIndex: 1, LastLogTerm: 11},
		{Type: RequestVote, From: 2, To: 1, Term: 10, LastLogIndex: 1},
	} {
		if _, err := n.Step(m); err == nil {
			t.Fatalf("accepted message: %+v", m)
		}
		if n.Status().Term != 0 {
			t.Fatal("invalid message changed term")
		}
	}
}
