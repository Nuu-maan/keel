package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Nuu-maan/keel/raft"
	"github.com/Nuu-maan/keel/server"
	"github.com/Nuu-maan/keel/storage"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "client listen address")
	dir := flag.String("dir", "data", "data directory")
	id := flag.Uint64("id", 0, "Raft node ID; zero keeps standalone mode")
	peerAddr := flag.String("peer-addr", "", "Raft peer listen address")
	peerCert := flag.String("peer-cert", "", "peer TLS certificate")
	peerKey := flag.String("peer-key", "", "peer TLS private key")
	peerCA := flag.String("peer-ca", "", "peer TLS CA certificate")
	tick := flag.Duration("raft-tick", 100*time.Millisecond, "Raft timer tick")
	electionTicks := flag.Int("election-ticks", 10, "minimum election timeout in Raft ticks")
	peerTimeout := flag.Duration("peer-timeout", 500*time.Millisecond, "peer RPC timeout")
	peers := flag.String("peers", "", "comma-separated id=raft-addr@client-addr entries")
	flag.Parse()
	if err := run(*addr, *dir, *id, *peerAddr, *peers, *peerCert, *peerKey, *peerCA, raft.ClusterOptions{Tick: *tick, ElectionTicks: *electionTicks, PeerTimeout: *peerTimeout}); err != nil {
		slog.Error("keeld stopped", "err", err)
		os.Exit(1)
	}
}

func parsePeers(spec string) (map[uint64]raft.Peer, error) {
	peers := map[uint64]raft.Peer{}
	if spec == "" {
		return nil, errors.New("cluster peers required")
	}
	for _, part := range strings.Split(spec, ",") {
		idText, addrs, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid peer %q", part)
		}
		id, err := strconv.ParseUint(idText, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid peer ID %q", idText)
		}
		raftAddr, clientAddr, ok := strings.Cut(addrs, "@")
		if !ok || raftAddr == "" || clientAddr == "" {
			return nil, fmt.Errorf("invalid peer addresses %q", part)
		}
		if _, ok := peers[id]; ok {
			return nil, fmt.Errorf("duplicate peer %d", id)
		}
		peers[id] = raft.Peer{RaftAddr: raftAddr, ClientAddr: clientAddr}
	}
	return peers, nil
}

func run(addr, dir string, id uint64, peerAddr, peerSpec, peerCert, peerKey, peerCA string, opts raft.ClusterOptions) error {
	if id == 0 && (peerAddr != "" || peerSpec != "" || peerCert != "" || peerKey != "" || peerCA != "") {
		return errors.New("-id is required for cluster mode")
	}
	var store *storage.Store
	var service server.Store
	var cluster *raft.Cluster
	var err error
	if id == 0 {
		store, err = storage.Open(dir, storage.Options{})
		if err != nil {
			return err
		}
		service = store
	} else {
		entries, readErr := os.ReadDir(dir)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		for _, entry := range entries {
			name := entry.Name()
			if name == "MANIFEST" || strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, ".sst") {
				return errors.New("standalone data directory cannot be opened in cluster mode")
			}
		}
		peers, err := parsePeers(peerSpec)
		if err != nil {
			return err
		}
		own, ok := peers[id]
		if !ok || own.RaftAddr != peerAddr || own.ClientAddr != addr {
			return errors.New("local ID and addresses must match -peers")
		}
		store, err = storage.Open(filepath.Join(dir, "app"), storage.Options{})
		if err != nil {
			return err
		}
		cluster, err = raft.OpenCluster(filepath.Join(dir, "raft"), id, peers, store, opts)
		if err != nil {
			store.Close()
			return err
		}
		if peerCert != "" || peerKey != "" || peerCA != "" {
			if peerCert == "" || peerKey == "" || peerCA == "" {
				cluster.Close()
				store.Close()
				return errors.New("all peer TLS files are required")
			}
			if err := cluster.EnableTLS(peerCert, peerKey, peerCA); err != nil {
				cluster.Close()
				store.Close()
				return err
			}
		}
		service = cluster
	}
	defer store.Close()
	if cluster != nil {
		defer cluster.Close()
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if cluster != nil {
		peerLn, err := net.Listen("tcp", peerAddr)
		if err != nil {
			ln.Close()
			return err
		}
		if err = cluster.Start(peerLn); err != nil {
			ln.Close()
			return err
		}
	}
	srv := server.New(service, server.Options{})
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	slog.Info("keeld listening", "addr", addr, "dir", dir, "id", id)
	select {
	case sig := <-stop:
		slog.Info("shutting down", "signal", sig.String())
		return srv.Close()
	case err := <-serveErr:
		srv.Close()
		return err
	case err := <-clusterErrors(cluster):
		srv.Close()
		return err
	}
}

func clusterErrors(c *raft.Cluster) <-chan error {
	if c == nil {
		return nil
	}
	return c.Errors()
}
