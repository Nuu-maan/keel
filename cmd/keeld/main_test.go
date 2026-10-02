package main

import (
	"github.com/Nuu-maan/keel/raft"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRejectStandaloneDirectoryInClusterMode(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001.wal"), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run("127.0.0.1:0", dir, 1, "127.0.0.1:0", "1=127.0.0.1:0@127.0.0.1:0", "", "", "", raft.ClusterOptions{})
	if err == nil || !strings.Contains(err.Error(), "standalone data") {
		t.Fatalf("want data-layout error, got %v", err)
	}
}
