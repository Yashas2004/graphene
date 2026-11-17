package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yashas2004/graphene/graph"
)

func open(t testing.TB, dir string) (*graph.Store, *Engine) {
	s, err := graph.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() }) // Windows cannot delete an open file
	return s, NewEngine(s)
}

func must(t testing.TB, e *Engine, q string) *Result {
	r, err := e.Run(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func seed(t testing.TB, e *Engine) {
	must(t, e, "CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob', age: 25})")
	must(t, e, "MATCH (b:Person {name: 'Bob'}) CREATE (b)-[:KNOWS]->(c:Person {name: 'Carol', age: 41})")
	must(t, e, "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(d:Person {name: 'Dave', age: 19})")
	must(t, e, "MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'Dave'}) CREATE (a)-[:BLOCKS]->(d)")
}

func TestQueries(t *testing.T) {
	_, e := open(t, t.TempDir())
	seed(t, e)

	r := must(t, e, "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name")
	if len(r.Rows) != 1 || r.Rows[0][0] != "Carol" {
		t.Fatalf("2-hop: %v", r.Rows)
	}
	r = must(t, e, "MATCH (p:Person) WHERE p.age >= 25 AND p.name <> 'Bob' RETURN count(*)")
	if r.Rows[0][0] != 2 {
		t.Fatalf("where/count: %v", r.Rows)
	}
	r = must(t, e, "MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(a) RETURN a.name")
	if len(r.Rows) != 1 || r.Rows[0][0] != "Alice" {
		t.Fatalf("incoming: %v", r.Rows)
	}
	r = must(t, e, "SHORTEST PATH FROM (a:Person {name: 'Alice'}) TO (d:Person {name: 'Dave'})")
	if r.Rows[0][1] != 1 {
		t.Fatalf("shortest path should use BLOCKS edge: %v", r.Rows)
	}
	r = must(t, e, "SHORTEST PATH FROM (a:Person {name: 'Alice'}) TO (d:Person {name: 'Dave'}) VIA :KNOWS")
	if r.Rows[0][1] != 3 {
		t.Fatalf("shortest path via KNOWS: %v", r.Rows)
	}
	r = must(t, e, "BFS FROM (a:Person {name: 'Alice'}) VIA :KNOWS DEPTH 2")
	if len(r.Rows) != 3 {
		t.Fatalf("bfs depth 2: %d rows", len(r.Rows))
	}

	must(t, e, "MATCH (b:Person {name: 'Bob'}) SET b.age = 26")
	r = must(t, e, "MATCH (b:Person {name: 'Bob'}) RETURN b.age")
	if r.Rows[0][0] != 26.0 {
		t.Fatalf("set: %v", r.Rows)
	}
	must(t, e, "MATCH (b:Person {name: 'Bob'}) DELETE b")
	r = must(t, e, "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b")
	if len(r.Rows) != 0 {
		t.Fatalf("delete should detach edges: %v", r.Rows)
	}
}

func TestPlannerUsesIndexAndCache(t *testing.T) {
	_, e := open(t, t.TempDir())
	seed(t, e)
	q := "MATCH (p:Person) WHERE p.name = 'Carol' RETURN p.age"
	if r := must(t, e, "EXPLAIN "+q); !strings.HasPrefix(r.Plan[0], "LabelScan") {
		t.Fatalf("plan before index: %v", r.Plan)
	}
	must(t, e, "CREATE INDEX ON :Person(name)")
	r := must(t, e, "EXPLAIN "+q)
	if !strings.HasPrefix(r.Plan[0], "IndexSeek") || r.Rows[0][0] != 41.0 {
		t.Fatalf("plan after index: %v %v", r.Plan, r.Rows)
	}
	must(t, e, q)
	if !must(t, e, q).Cached {
		t.Fatal("second run should hit the cache")
	}
	must(t, e, "MATCH (p:Person {name: 'Carol'}) SET p.age = 42")
	r = must(t, e, q)
	if r.Cached || r.Rows[0][0] != 42.0 {
		t.Fatalf("write must invalidate cache: %+v", r)
	}
	// stale index entry must not resurrect the old value
	must(t, e, "MATCH (p:Person {name: 'Carol'}) SET p.name = 'Caroline'")
	if r = must(t, e, "MATCH (p:Person {name: 'Carol'}) RETURN p"); len(r.Rows) != 0 {
		t.Fatalf("stale index hit: %v", r.Rows)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s, e := open(t, t.TempDir())
	seed(t, e)
	snap := s.Snapshot()
	must(t, e, "MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
	must(t, e, "MATCH (p:Person {name: 'Dave'}) DELETE p")
	must(t, e, "CREATE (x:Person {name: 'Eve'})")
	live := 0
	for _, id := range snap.IDs("Person") {
		nv, ok := snap.Node(id)
		if !ok {
			continue
		}
		live++
		if nv.Props["name"] == "Alice" && nv.Props["age"] != 30.0 {
			t.Fatalf("snapshot saw later update: %v", nv.Props)
		}
	}
	if live != 4 {
		t.Fatalf("snapshot should still see exactly the 4 original nodes, saw %d", live)
	}
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	s, e := open(t, dir)
	seed(t, e)
	must(t, e, "CREATE INDEX ON :Person(name)")
	s.Close()

	// 1. recovery from WAL only
	s, e = open(t, dir)
	if r := must(t, e, "MATCH (p:Person) RETURN count(*)"); r.Rows[0][0] != 4 {
		t.Fatalf("WAL replay: %v", r.Rows)
	}
	// 2. checkpoint, then more writes, then a torn record at the WAL tail
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	must(t, e, "MATCH (p:Person {name: 'Dave'}) SET p.age = 20")
	must(t, e, "CREATE (x:Person {name: 'Eve', age: 50})")
	s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{9, 0, 0, 0, 1, 2, 3}) // simulated crash mid-write
	f.Close()

	s, e = open(t, dir)
	if r := must(t, e, "MATCH (p:Person) RETURN count(*)"); r.Rows[0][0] != 5 {
		t.Fatalf("snapshot + WAL: %v", r.Rows)
	}
	if r := must(t, e, "MATCH (p:Person {name: 'Dave'}) RETURN p.age"); r.Rows[0][0] != 20.0 {
		t.Fatalf("post-checkpoint update lost: %v", r.Rows)
	}
	if r := must(t, e, "EXPLAIN MATCH (p:Person {name: 'Eve'}) RETURN p"); !strings.HasPrefix(r.Plan[0], "IndexSeek") || len(r.Rows) != 1 {
		t.Fatalf("index not recovered: %v", r.Plan)
	}
	if r := must(t, e, "SHORTEST PATH FROM (a:Person {name: 'Alice'}) TO (c:Person {name: 'Carol'})"); r.Rows[0][1] != 2 {
		t.Fatalf("edges not recovered: %v", r.Rows)
	}
	// writes after a torn tail must survive another restart
	must(t, e, "CREATE (x:Person {name: 'Frank'})")
	s.Close()
	_, e = open(t, dir)
	if r := must(t, e, "MATCH (p:Person) RETURN count(*)"); r.Rows[0][0] != 6 {
		t.Fatalf("after torn tail: %v", r.Rows)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	_, e := open(t, t.TempDir())
	seed(t, e)
	must(t, e, "CREATE INDEX ON :N(i)")
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				must(t, e, fmt.Sprintf("CREATE (n:N {i: %d})", w*1000+i))
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				must(t, e, "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name")
				must(t, e, "MATCH (n:N) RETURN count(*)")
			}
		}()
	}
	wg.Wait()
	if r := must(t, e, "MATCH (n:N) RETURN count(*)"); r.Rows[0][0] != 800 {
		t.Fatalf("lost writes: %v", r.Rows)
	}
}

func TestCacheEviction(t *testing.T) {
	c := newResultCache(3)
	for i := 0; i < 3; i++ {
		c.put(fmt.Sprint("q", i), 1, &Result{})
	}
	c.get("q0", 1) // mark q0 as recently used
	c.put("q3", 1, &Result{})
	if _, ok := c.get("q0", 1); !ok {
		t.Fatal("recently used entry was evicted")
	}
	if _, ok := c.get("q1", 1); ok {
		t.Fatal("q1 should have been evicted")
	}
	if _, ok := c.get("q3", 1); !ok {
		t.Fatal("new entry missing")
	}
	if _, ok := c.get("q3", 2); ok {
		t.Fatal("entry from an older store version must not be served")
	}
	for i := 4; i < 50; i++ { // ring never grows past its capacity
		c.put(fmt.Sprint("q", i), 1, &Result{})
	}
	if len(c.keys) != 3 {
		t.Fatalf("ring size %d", len(c.keys))
	}
}

func BenchmarkIndexSeek(b *testing.B) {
	_, e := open(b, b.TempDir())
	must(b, e, "CREATE INDEX ON :N(i)")
	for i := 0; i < 10000; i++ {
		must(b, e, fmt.Sprintf("CREATE (n:N {i: %d})", i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			must(b, e, fmt.Sprintf("MATCH (n:N {i: %d}) RETURN n", i%10000))
		}
	})
}
