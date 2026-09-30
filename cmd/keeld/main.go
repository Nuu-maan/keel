package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nuu-maan/keel/server"
	"github.com/Nuu-maan/keel/storage"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "address to listen on")
	dir := flag.String("dir", "data", "data directory")
	flag.Parse()

	if err := run(*addr, *dir); err != nil {
		slog.Error("keeld stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, dir string) error {
	store, err := storage.Open(dir, storage.Options{})
	if err != nil {
		return err
	}
	defer store.Close()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := server.New(store, server.Options{})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	slog.Info("keeld listening", "addr", ln.Addr().String(), "dir", dir)

	select {
	case sig := <-stop:
		slog.Info("shutting down", "signal", sig.String())
		return srv.Close()
	case err := <-serveErr:
		srv.Close()
		return err
	}
}
