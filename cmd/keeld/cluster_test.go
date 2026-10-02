package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Nuu-maan/keel/client"
	"github.com/Nuu-maan/keel/raft"
)

func TestClusterSurvivesProcessKill(t *testing.T) {
	if os.Getenv("KEEL_CLUSTER_HELPER") == "1" {
		id, _ := strconv.ParseUint(os.Getenv("KEEL_ID"), 10, 64)
		if err := run(os.Getenv("KEEL_CLIENT_ADDR"), os.Getenv("KEEL_DIR"), id, os.Getenv("KEEL_RAFT_ADDR"), os.Getenv("KEEL_PEERS"), "", "", "", raft.ClusterOptions{}); err != nil {
			t.Fatal(err)
		}
		return
	}
	ports := make([]string, 6)
	listeners := make([]net.Listener, 6)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		ports[i] = ln.Addr().String()
	}
	for _, ln := range listeners {
		ln.Close()
	}
	peers := make([]string, 3)
	for i := range peers {
		peers[i] = fmt.Sprintf("%d=%s@%s", i+1, ports[i+3], ports[i])
	}
	spec := strings.Join(peers, ",")
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	commands := make([]*exec.Cmd, 3)
	start := func(i int) {
		cmd := exec.Command(exe, "-test.run=^TestClusterSurvivesProcessKill$")
		cmd.Env = append(os.Environ(), "KEEL_CLUSTER_HELPER=1", fmt.Sprintf("KEEL_ID=%d", i+1), "KEEL_CLIENT_ADDR="+ports[i], "KEEL_RAFT_ADDR="+ports[i+3], "KEEL_PEERS="+spec, "KEEL_DIR="+filepath.Join(dir, fmt.Sprintf("node-%d", i+1)))
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands[i] = cmd
	}
	defer func() {
		for _, cmd := range commands {
			if cmd != nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		}
	}()
	for i := range commands {
		start(i)
	}
	call := func(addr string, op func(*client.Client, context.Context) error) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, err := client.Dial(ctx, addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		return op(conn, ctx)
	}
	eventually := func(fn func() error) {
		t.Helper()
		deadline := time.Now().Add(12 * time.Second)
		var err error
		for time.Now().Before(deadline) {
			err = fn()
			if err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal(err)
	}
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		eventually(func() error {
			return call(ports[0], func(c *client.Client, ctx context.Context) error { return c.Put(ctx, []byte(key), []byte(key)) })
		})
	}
	if err := commands[0].Process.Kill(); err != nil {
		t.Fatal(err)
	}
	commands[0].Wait()
	commands[0] = nil
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		eventually(func() error {
			return call(ports[1], func(c *client.Client, ctx context.Context) error {
				got, err := c.Get(ctx, []byte(key))
				if err != nil {
					return err
				}
				if string(got) != key {
					return fmt.Errorf("%s = %q", key, got)
				}
				return nil
			})
		})
	}
	start(0)
	eventually(func() error {
		return call(ports[0], func(c *client.Client, ctx context.Context) error {
			got, err := c.Get(ctx, []byte("key-9"))
			if err != nil {
				return err
			}
			if string(got) != "key-9" {
				return fmt.Errorf("restarted node returned %q", got)
			}
			return nil
		})
	})
}
