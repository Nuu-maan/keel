# Keel

A replicated key-value store built from scratch in Go: LSM storage engine, custom TCP protocol, Raft consensus, and chaos-tested linearizability.

## Roadmap

- [ ] Storage engine: write-ahead log, memtable, SSTables, compaction
- [ ] Wire protocol over TCP
- [ ] Raft replication
- [ ] Chaos testing with linearizability checks
- [ ] Metrics and dashboards
- [ ] Benchmarks

## Development

```
go test -race ./...
```
