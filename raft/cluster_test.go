package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nuu-maan/keel/client"
	"github.com/Nuu-maan/keel/server"
	"github.com/Nuu-maan/keel/storage"
)

type runningNode struct {
	cluster *Cluster
	server  *server.Server
	app     *storage.Store
}

func (n *runningNode) close() {
	n.server.Close()
	n.cluster.Close()
	n.app.Close()
}

func TestClusterFailoverAndCatchUp(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	peers := map[uint64]Peer{}
	peerListeners := make([]net.Listener, 3)
	clientListeners := make([]net.Listener, 3)
	for i := range peerListeners {
		var err error
		peerListeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		clientListeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peers[uint64(i+1)] = Peer{RaftAddr: peerListeners[i].Addr().String(), ClientAddr: clientListeners[i].Addr().String()}
	}
	start := func(i int, pl, cl net.Listener) *runningNode {
		app, err := storage.Open(filepath.Join(dirs[i], "app"), storage.Options{})
		if err != nil {
			t.Fatal(err)
		}
		cluster, err := OpenCluster(filepath.Join(dirs[i], "raft"), uint64(i+1), peers, app, ClusterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.Start(pl); err != nil {
			t.Fatal(err)
		}
		srv := server.New(cluster, server.Options{})
		go srv.Serve(cl)
		return &runningNode{cluster: cluster, server: srv, app: app}
	}
	nodes := make([]*runningNode, 3)
	for i := range nodes {
		nodes[i] = start(i, peerListeners[i], clientListeners[i])
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.close()
			}
		}
	}()
	waitLeader := func(exclude int) int {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			leader := -1
			for i, n := range nodes {
				if i != exclude && n != nil && n.cluster.Status().Role == Leader {
					if leader >= 0 {
						leader = -1
						break
					}
					leader = i
				}
			}
			if leader >= 0 {
				ready := true
				for i, n := range nodes {
					if i != exclude && i != leader && n != nil && n.cluster.Status().Leader != uint64(leader+1) {
						ready = false
					}
				}
				if ready {
					return leader
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("no stable leader")
		return -1
	}
	old := waitLeader(-1)
	follower := (old + 1) % 3
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dial := func(i int) *client.Client {
		t.Helper()
		c, err := client.Dial(ctx, peers[uint64(i+1)].ClientAddr)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := dial(follower)
	if err := c.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Get(ctx, []byte("k")); err != nil || string(value) != "v1" {
		t.Fatalf("forwarded read: %q %v", value, err)
	}
	if err := c.Put(ctx, []byte("gone"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	nodes[old].close()
	nodes[old] = nil
	next := waitLeader(old)
	if next == old {
		t.Fatal("failed to replace leader")
	}
	other := 3 - old - next
	c = dial(other)
	if err := c.Put(ctx, []byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Get(ctx, []byte("k")); err != nil || string(value) != "v2" {
		t.Fatalf("failover read: %q %v", value, err)
	}
	if err := c.Delete(ctx, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := nodes[next].cluster.node.CreateSnapshot(); err != nil {
		t.Fatal(err)
	}
	c = dial(next)
	if err := c.Put(ctx, []byte("tail"), []byte("after snapshot")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	pl, err := net.Listen("tcp", peers[uint64(old+1)].RaftAddr)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := net.Listen("tcp", peers[uint64(old+1)].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	nodes[old] = start(old, pl, cl)
	_, target, _ := nodes[next].cluster.node.LogInfo()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, commit, last := nodes[old].cluster.node.LogInfo()
		if commit >= target && last >= target && nodes[old].cluster.Status().Leader == uint64(next+1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(fmt.Sprintf("old node did not catch up: commit=%d last=%d", commit, last))
		}
		time.Sleep(20 * time.Millisecond)
	}
	c = dial(old)
	if value, err := c.Get(ctx, []byte("k")); err != nil || string(value) != "v2" {
		t.Fatalf("recovered follower read: %q %v", value, err)
	}
	if value, err := c.Get(ctx, []byte("tail")); err != nil || string(value) != "after snapshot" {
		t.Fatalf("missing suffix: %q %v", value, err)
	}
	if _, err := c.Get(ctx, []byte("gone")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("snapshot kept deleted key: %v", err)
	}
	c.Close()
	nodes[old].close()
	nodes[old] = nil
	pl, err = net.Listen("tcp", peers[uint64(old+1)].RaftAddr)
	if err != nil {
		t.Fatal(err)
	}
	cl, err = net.Listen("tcp", peers[uint64(old+1)].ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	nodes[old] = start(old, pl, cl)
	waitLeader(-1)
	c = dial(old)
	if value, err := c.Get(ctx, []byte("tail")); err != nil || string(value) != "after snapshot" {
		t.Fatalf("restart lost suffix: %q %v", value, err)
	}
	c.Close()
	for i := range nodes {
		if i != next {
			nodes[i].close()
			nodes[i] = nil
		}
	}
	if err := nodes[next].cluster.Put([]byte("isolated"), []byte("no")); err == nil {
		t.Fatal("isolated old leader acknowledged write")
	}
	if _, err := nodes[next].cluster.Get([]byte("k")); err == nil {
		t.Fatal("isolated old leader served read")
	}
}

func TestMinorityCannotAcknowledgeOrRead(t *testing.T) {
	dir := t.TempDir()
	pl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	app, err := storage.Open(filepath.Join(dir, "app"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	peers := map[uint64]Peer{1: {RaftAddr: pl.Addr().String(), ClientAddr: "127.0.0.1:1"}, 2: {RaftAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:2"}, 3: {RaftAddr: "127.0.0.1:2", ClientAddr: "127.0.0.1:3"}}
	cluster, err := OpenCluster(filepath.Join(dir, "raft"), 1, peers, app, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Start(pl); err != nil {
		t.Fatal(err)
	}
	defer func() { cluster.Close(); app.Close() }()
	time.Sleep(2 * time.Second)
	if err := cluster.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("minority acknowledged write")
	}
	if _, err := cluster.Get([]byte("k")); err == nil {
		t.Fatal("minority served read")
	}
}
