# Graphene

A small graph database written from scratch in Go (standard library only). It stores a property graph, answers a Cypher-like query language over HTTP, survives crashes through a write-ahead log, and serves concurrent readers from consistent snapshots.

I built it to understand how a graph database works underneath the query language: storage, recovery, indexing, planning and concurrency control.

## Architecture

```
            Client (HTTP)
                 |
           Query Gateway          cmd/graphene      POST /query, /checkpoint, GET /stats
                 |
        Parser + Query Planner    query/parser.go   lexer, recursive-descent parser
                 |                query/engine.go   access-path choice, result cache
        -------------------
        |                 |
   Graph Index       Query Engine                   index seek / label scan, expand, BFS
        |                 |
        -------------------
                 |
           Storage Layer          graph/store.go    adjacency lists, MVCC versions
                 |
        WAL + Object Store        graph/wal.go      wal.log, snapshot.json
```

## What is implemented

| Feature | How |
|---|---|
| Nodes, edges, properties | Labelled nodes and typed, directed edges, both with properties |
| Adjacency list | `out` and `in` edge lists per node, so both directions are O(degree) |
| Query language | `CREATE`, `MATCH` with multi-hop patterns, `WHERE`, `RETURN`, `count(*)`, `LIMIT`, `SET`, `DELETE`, `EXPLAIN` |
| Traversal | `BFS FROM ... DEPTH n` and `SHORTEST PATH FROM ... TO ...` (unweighted, BFS with parent map) |
| Write-ahead log | Length + CRC32 framed records, one write per commit batch, optional fsync; torn tails are detected and truncated on recovery |
| Persistence | `Checkpoint` writes a compacted snapshot through an `ObjectStore` interface (atomic temp file + rename), then truncates the WAL. Recovery = snapshot + replay of records with a newer LSN |
| Secondary indexes | `CREATE INDEX ON :Label(prop)`; hash index used automatically by the planner |
| Snapshot reads | MVCC: every node/edge carries created/deleted versions and node properties are copy-on-write version chains. A reader pins one version and never sees a partial commit |
| Concurrency | Many readers, one writer at a time; each commit is one atomic version bump |
| Caching | LRU result cache tagged with the store version, so any write invalidates it without bookkeeping |
| Benchmark | `graphene bench` |

## Run it

```bash
go test -race ./...
go run ./cmd/graphene serve -data ./data -addr :7474
```

```bash
q() { curl -s localhost:7474/query -d "$1"; echo; }

q "CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob', age: 25})"
q "MATCH (b:Person {name: 'Bob'}) CREATE (b)-[:KNOWS]->(c:Person {name: 'Carol', age: 41})"
q "CREATE INDEX ON :Person(name)"
q "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name, c.age"
q "MATCH (p:Person) WHERE p.age >= 25 RETURN p.name LIMIT 10"
q "EXPLAIN MATCH (p:Person) WHERE p.name = 'Carol' RETURN p"
q "SHORTEST PATH FROM (a:Person {name: 'Alice'}) TO (c:Person {name: 'Carol'}) VIA :KNOWS"
q "BFS FROM (a:Person {name: 'Alice'}) DEPTH 2"
q "MATCH (p:Person {name: 'Bob'}) SET p.age = 26"
q "MATCH (p:Person {name: 'Bob'}) DELETE p"
curl -s -X POST localhost:7474/checkpoint
```

Kill the server at any point and restart it: the data comes back from `snapshot.json` plus `wal.log`.

## Benchmark

`go run ./cmd/graphene bench -n 50000` on a 2-core Linux container (50,000 nodes, 150,000 edges, 2 concurrent workers, 1 second per workload):

| Workload | Queries/s | Avg latency |
|---|---|---|
| Point lookup, index seek | 153,180 | 13 µs |
| Point lookup, label scan (no index) | 116 | 17.2 ms |
| 2-hop traversal | 49,224 | 41 µs |
| Shortest path, random pairs | 47 | 42.6 ms |
| Repeated query (cache hit) | 312,419 | 6 µs |
| Writes (CREATE through WAL, no fsync) | 156,870 | 13 µs |

The index makes point lookups roughly 1,300 times faster than scanning the label. These numbers include parsing and planning, and go through the engine directly rather than HTTP.

## Design notes

**Recovery is "apply ops in order".** The WAL and the snapshot both store the same `Op` records, and `apply` is idempotent, so there is one code path for normal writes, WAL replay and snapshot load. Each record has an LSN; the snapshot stores the last LSN it covers, which makes a crash between "snapshot written" and "WAL truncated" harmless.

**Indexes are candidate sets.** An update adds the node to the new value's bucket and leaves the old entry in place. Readers re-check the property against their snapshot, so a stale entry can never produce a wrong result and old snapshots can still find the old value.

**The cache needs no invalidation logic.** Each entry remembers the version it was computed at and is only served while the store is still at that version.

## Limitations

- Single node. "Distributed" is future work: the `ObjectStore` interface is the seam for S3/MinIO, but only the local filesystem implementation exists.
- The whole graph lives in memory; snapshots are full, not incremental.
- Old MVCC versions and deleted records are only reclaimed on restart (no background GC).
- Match-then-write queries read on a snapshot and then commit, so they are not serializable under contention.
- The language is a subset of Cypher: no `OR`, no variable-length patterns, no `ORDER BY`, no edge variables, and shortest path follows outgoing edges only.

## Next steps

MinIO-backed object store, incremental snapshots, version garbage collection, weighted shortest path (Dijkstra), and replication of the WAL to followers.
