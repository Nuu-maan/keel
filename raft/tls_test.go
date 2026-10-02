package raft

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nuu-maan/keel/storage"
)

func TestPeerTLSRequiresClientCertificate(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "keel test peer"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "peer.pem"), filepath.Join(dir, "peer.key"), filepath.Join(dir, "ca.pem")
	for path, data := range map[string][]byte{certFile: cert, keyFile: keyPEM, caFile: cert} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	app, err := storage.Open(filepath.Join(dir, "app"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	peers := map[uint64]Peer{1: {RaftAddr: ln.Addr().String(), ClientAddr: "127.0.0.1:1"}, 2: {RaftAddr: "127.0.0.1:2", ClientAddr: "127.0.0.1:2"}}
	cluster, err := OpenCluster(filepath.Join(dir, "raft"), 1, peers, app, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.EnableTLS(certFile, keyFile, caFile); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Start(ln); err != nil {
		t.Fatal(err)
	}
	defer func() { cluster.Close(); app.Close() }()
	req := Message{Type: RequestVote, From: 2, To: 1, Term: 1}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(cert)
	unauthenticated := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}}}
	if res, err := unauthenticated.Post("https://"+ln.Addr().String()+"/vote", "application/json", bytes.NewReader(data)); err == nil {
		res.Body.Close()
		t.Fatal("peer accepted unauthenticated request")
	}
	var vote Message
	if err := cluster.post(1, "vote", req, &vote); err != nil || !vote.Granted {
		t.Fatalf("authenticated vote: %+v %v", vote, err)
	}
}

func TestRemotePeerNeedsTLS(t *testing.T) {
	dir := t.TempDir()
	app, err := storage.Open(filepath.Join(dir, "app"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	peers := map[uint64]Peer{1: {RaftAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1"}, 2: {RaftAddr: "192.0.2.1:2", ClientAddr: "192.0.2.1:3"}}
	cluster, err := OpenCluster(filepath.Join(dir, "raft"), 1, peers, app, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Start(ln); err == nil || !strings.Contains(err.Error(), "require TLS") {
		t.Fatalf("remote peer accepted without TLS: %v", err)
	}
}
