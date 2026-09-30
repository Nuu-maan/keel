package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Nuu-maan/keel/client"
)

const usage = `usage: keelctl [-addr host:port] <command>

commands:
  get <key>
  put <key> <value>
  del <key>
`

func main() {
	addr := flag.String("addr", "127.0.0.1:7070", "server address")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, *addr, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "keelctl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr string, args []string) error {
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	c, err := client.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer c.Close()

	switch {
	case args[0] == "get" && len(args) == 2:
		v, err := c.Get(ctx, []byte(args[1]))
		if errors.Is(err, client.ErrNotFound) {
			return fmt.Errorf("%s: not found", args[1])
		}
		if err != nil {
			return err
		}
		fmt.Println(string(v))
		return nil
	case args[0] == "put" && len(args) == 3:
		return c.Put(ctx, []byte(args[1]), []byte(args[2]))
	case args[0] == "del" && len(args) == 2:
		return c.Delete(ctx, []byte(args[1]))
	}
	flag.Usage()
	os.Exit(2)
	return nil
}
