package query

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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
	defer snap.Close()
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

// TestShortestPathMatchesPlainBFS checks the bidirectional search against a
// simple one-directional BFS on random directed graphs: the length must be
// identical, and the returned path must follow real edges from src to dst.
func TestShortestPathMatchesPlainBFS(t *testing.T) {
	s, e := open(t, t.TempDir())
	must(t, e, "CREATE INDEX ON :V(i)")
	rng := rand.New(rand.NewSource(42))
	const n = 300
	ids := make([]uint64, n)
	var ops []graph.Op
	for i := 0; i < n; i++ {
		ids[i] = s.AllocID()
		ops = append(ops, graph.Op{Kind: "node", ID: ids[i], Label: "V", Props: map[string]any{"i": float64(i)}})
	}
	for i := 0; i < n; i++ {
		for j := 0; j < 2; j++ { // sparse, so some pairs are far apart or unreachable
			typ := "A"
			if rng.Intn(3) == 0 {
				typ = "B"
			}
			ops = append(ops, graph.Op{Kind: "edge", ID: s.AllocID(), Label: typ, From: ids[i], To: ids[rng.Intn(n)]})
		}
	}
	if err := s.Commit(ops); err != nil {
		t.Fatal(err)
	}
	sn := s.Snapshot()
	plainBFS := func(src, dst uint64, typ string) int {
		dist := map[uint64]int{src: 0}
		queue := []uint64{src}
		for head := 0; head < len(queue); head++ {
			cur := queue[head]
			if cur == dst {
				return dist[cur]
			}
			for _, nb := range sn.Neighbors(cur, true, typ, nil) {
				if _, seen := dist[nb]; !seen {
					dist[nb] = dist[cur] + 1
					queue = append(queue, nb)
				}
			}
		}
		return -1
	}
	unreachable := 0
	for trial := 0; trial < 600; trial++ {
		a, b := rng.Intn(n), rng.Intn(n)
		typ, via := "", ""
		if trial%2 == 1 {
			typ, via = "A", " VIA :A"
		}
		want := plainBFS(ids[a], ids[b], typ)
		r := must(t, e, fmt.Sprintf("SHORTEST PATH FROM (a:V {i: %d}) TO (b:V {i: %d})%s", a, b, via))
		if want == -1 {
			unreachable++
			if len(r.Rows) != 0 {
				t.Fatalf("%d->%d%s: found a path where none exists", a, b, via)
			}
			continue
		}
		if len(r.Rows) != 1 || r.Rows[0][1] != want {
			t.Fatalf("%d->%d%s: want length %d, got %v", a, b, via, want, r.Rows)
		}
		path := r.Rows[0][0].([]any)
		if len(path) != want+1 || path[0].(graph.NodeView).ID != ids[a] || path[want].(graph.NodeView).ID != ids[b] {
			t.Fatalf("%d->%d%s: bad path endpoints or size", a, b, via)
		}
		for k := 0; k < want; k++ {
			from, to := path[k].(graph.NodeView).ID, path[k+1].(graph.NodeView).ID
			linked := false
			for _, nb := range sn.Neighbors(from, true, typ, nil) {
				if nb == to {
					linked = true
				}
			}
			if !linked {
				t.Fatalf("%d->%d%s: path step %d is not an edge", a, b, via, k)
			}
		}
	}
	if unreachable == 0 || unreachable == 600 {
		t.Fatalf("test graph should mix reachable and unreachable pairs, got %d unreachable", unreachable)
	}
}

func TestWeightedShortestPath(t *testing.T) {
	_, e := open(t, t.TempDir())
	// A -> D directly costs 10; A -> B -> C -> D costs 1 + 2 + 3 = 6.
	must(t, e, "CREATE (a:City {name: 'A'})-[:ROAD {km: 1}]->(b:City {name: 'B'})-[:ROAD {km: 2}]->(c:City {name: 'C'})-[:ROAD {km: 3}]->(d:City {name: 'D'})")
	must(t, e, "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) CREATE (a)-[:ROAD {km: 10}]->(d)")
	base := "SHORTEST PATH FROM (a:City {name: 'A'}) TO (d:City {name: 'D'})"

	if r := must(t, e, base); r.Rows[0][1] != 1 {
		t.Fatalf("unweighted should take the direct edge: %v", r.Rows[0][1])
	}
	r := must(t, e, base+" WEIGHT km")
	if r.Rows[0][1] != 3 || r.Rows[0][2] != 6.0 || r.Columns[2] != "cost" {
		t.Fatalf("weighted should take the 3-hop route costing 6: hops=%v cost=%v", r.Rows[0][1], r.Rows[0][2])
	}
	// an edge without the weight property cannot be used
	must(t, e, "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) CREATE (a)-[:ROAD]->(d)")
	if r = must(t, e, base+" WEIGHT km"); r.Rows[0][2] != 6.0 {
		t.Fatalf("edge without km must be ignored: %v", r.Rows[0][2])
	}
	// no route in the reverse direction
	if r = must(t, e, "SHORTEST PATH FROM (d:City {name: 'D'}) TO (a:City {name: 'A'}) WEIGHT km"); len(r.Rows) != 0 {
		t.Fatalf("expected no path: %v", r.Rows)
	}
	// negative weights are rejected instead of giving a wrong answer
	must(t, e, "MATCH (a:City {name: 'A'}), (b:City {name: 'B'}) CREATE (a)-[:ROAD {km: -5}]->(b)")
	if _, err := e.Run(base + " WEIGHT km"); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("expected a negative-weight error, got %v", err)
	}
}

// TestDijkstraMatchesBellmanFord checks costs against an independent
// algorithm on random weighted graphs, and that each returned path is real
// and adds up to the reported cost.
func TestDijkstraMatchesBellmanFord(t *testing.T) {
	s, e := open(t, t.TempDir())
	must(t, e, "CREATE INDEX ON :V(i)")
	rng := rand.New(rand.NewSource(7))
	const n = 200
	ids := make([]uint64, n)
	index := map[uint64]int{}
	var ops []graph.Op
	for i := 0; i < n; i++ {
		ids[i] = s.AllocID()
		index[ids[i]] = i
		ops = append(ops, graph.Op{Kind: "node", ID: ids[i], Label: "V", Props: map[string]any{"i": float64(i)}})
	}
	type edge struct {
		from, to int
		w        float64
	}
	var edges []edge
	for i := 0; i < n; i++ {
		for j := 0; j < 3; j++ {
			ed := edge{i, rng.Intn(n), float64(rng.Intn(20))} // whole numbers, so sums are exact
			edges = append(edges, ed)
			ops = append(ops, graph.Op{Kind: "edge", ID: s.AllocID(), Label: "E", From: ids[ed.from], To: ids[ed.to], Props: map[string]any{"w": ed.w}})
		}
	}
	if err := s.Commit(ops); err != nil {
		t.Fatal(err)
	}
	cheapest := map[[2]int]float64{} // cheapest direct edge between two nodes
	for _, ed := range edges {
		k := [2]int{ed.from, ed.to}
		if old, ok := cheapest[k]; !ok || ed.w < old {
			cheapest[k] = ed.w
		}
	}
	bellmanFord := func(src int) []float64 {
		dist := make([]float64, n)
		for i := range dist {
			dist[i] = -1 // unreachable
		}
		dist[src] = 0
		for changed := true; changed; {
			changed = false
			for _, ed := range edges {
				if dist[ed.from] < 0 {
					continue
				}
				if d := dist[ed.from] + ed.w; dist[ed.to] < 0 || d < dist[ed.to] {
					dist[ed.to] = d
					changed = true
				}
			}
		}
		return dist
	}
	unreachable := 0
	for trial := 0; trial < 40; trial++ {
		a := rng.Intn(n)
		want := bellmanFord(a)
		for k := 0; k < 15; k++ {
			b := rng.Intn(n)
			r := must(t, e, fmt.Sprintf("SHORTEST PATH FROM (a:V {i: %d}) TO (b:V {i: %d}) WEIGHT w", a, b))
			if want[b] < 0 {
				unreachable++
				if len(r.Rows) != 0 {
					t.Fatalf("%d->%d: found a path where none exists", a, b)
				}
				continue
			}
			if len(r.Rows) != 1 || r.Rows[0][2] != want[b] {
				t.Fatalf("%d->%d: want cost %v, got %v", a, b, want[b], r.Rows)
			}
			path := r.Rows[0][0].([]any)
			sum := 0.0
			for step := 0; step+1 < len(path); step++ {
				from := index[path[step].(graph.NodeView).ID]
				to := index[path[step+1].(graph.NodeView).ID]
				w, ok := cheapest[[2]int{from, to}]
				if !ok {
					t.Fatalf("%d->%d: path step %d is not an edge", a, b, step)
				}
				sum += w
			}
			if index[path[0].(graph.NodeView).ID] != a || index[path[len(path)-1].(graph.NodeView).ID] != b || sum != want[b] {
				t.Fatalf("%d->%d: path does not add up to cost %v (got %v)", a, b, want[b], sum)
			}
		}
	}
	t.Logf("%d of 600 pairs unreachable", unreachable)
}

// exported is a canonical form of everything live in a store.
func exported(s *graph.Store) string {
	data := s.Export()
	lines := make([]string, 0, len(data.Ops))
	for _, op := range data.Ops {
		b, _ := json.Marshal(op)
		lines = append(lines, string(b))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func TestVacuumReclaimsDeadState(t *testing.T) {
	s, e := open(t, t.TempDir())
	must(t, e, "CREATE INDEX ON :N(v)")
	for i := 0; i < 50; i++ {
		must(t, e, fmt.Sprintf("CREATE (a:N {k: %d, v: 0})-[:E]->(b:M {k: %d})", i, i))
	}
	for round := 1; round <= 5; round++ {
		for i := 0; i < 50; i++ {
			must(t, e, fmt.Sprintf("MATCH (a:N {k: %d}) SET a.v = %d", i, round))
		}
	}
	for i := 0; i < 20; i++ {
		must(t, e, fmt.Sprintf("MATCH (a:N {k: %d}) DELETE a", i))
	}
	before := s.Stats()
	if before["nodes"] != 100 || before["node_versions"] != 350 || before["edges"] != 50 {
		t.Fatalf("unexpected state before vacuum: %v", before)
	}
	want := exported(s)

	if removed := s.Vacuum(); removed != 20+20+30*5 { // 20 nodes, their 20 edges, 5 old versions on each of 30 nodes
		t.Fatalf("vacuum removed %d", removed)
	}
	after := s.Stats()
	if after["nodes"] != 80 || after["node_versions"] != 80 || after["edges"] != 30 {
		t.Fatalf("unexpected state after vacuum: %v", after)
	}
	if exported(s) != want {
		t.Fatal("vacuum changed the live graph")
	}
	// the rebuilt index finds current values and nothing stale
	if r := must(t, e, "EXPLAIN MATCH (a:N {v: 5}) RETURN count(*)"); !strings.HasPrefix(r.Plan[0], "IndexSeek") || r.Rows[0][0] != 30 {
		t.Fatalf("index after vacuum: %v %v", r.Plan, r.Rows)
	}
	if r := must(t, e, "MATCH (a:N {v: 0}) RETURN count(*)"); r.Rows[0][0] != 0 {
		t.Fatalf("stale value still matches: %v", r.Rows)
	}
	if r := must(t, e, "MATCH (b:M)<-[:E]-(a) RETURN count(*)"); r.Rows[0][0] != 30 {
		t.Fatalf("edges after vacuum: %v", r.Rows)
	}
	// writes and deletes still work on the compacted structures
	must(t, e, "MATCH (a:N {k: 30}) DELETE a")
	must(t, e, "MATCH (a:N {k: 31}) SET a.v = 9")
	if r := must(t, e, "MATCH (a:N {v: 5}) RETURN count(*)"); r.Rows[0][0] != 28 {
		t.Fatalf("after further writes: %v", r.Rows)
	}
}

func TestVacuumKeepsWhatAnOpenSnapshotReads(t *testing.T) {
	s, e := open(t, t.TempDir())
	must(t, e, "CREATE INDEX ON :N(v)")
	must(t, e, "CREATE (a:N {k: 1, v: 10})-[:E]->(b:N {k: 2, v: 20})")
	snap := s.Snapshot()
	must(t, e, "MATCH (a:N {k: 1}) SET a.v = 11")
	must(t, e, "MATCH (b:N {k: 2}) DELETE b")

	if removed := s.Vacuum(); removed != 0 {
		t.Fatalf("vacuum removed %d records an open snapshot can still see", removed)
	}
	ids, _ := snap.Lookup("N", "v", 10.0)
	if len(ids) != 1 {
		t.Fatalf("old index entry gone: %v", ids)
	}
	if nv, ok := snap.Node(ids[0]); !ok || nv.Props["v"] != 10.0 {
		t.Fatalf("snapshot lost its version: %v %v", nv, ok)
	}
	if nb := snap.Neighbors(ids[0], true, "E", nil); len(nb) != 1 {
		t.Fatalf("snapshot lost the deleted edge: %v", nb)
	}

	snap.Close()
	if removed := s.Vacuum(); removed != 3 { // node 2, the edge, node 1's old version
		t.Fatalf("after close, vacuum removed %d", removed)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("using a closed snapshot should panic")
		}
	}()
	snap.Node(ids[0])
}

// TestVacuumUnderLoad runs the same random workload on two stores, one of
// them vacuumed constantly while readers and writers are active, and checks
// they end in the same state and that it survives checkpoint and recovery.
func TestVacuumUnderLoad(t *testing.T) {
	plain, pe := open(t, t.TempDir())
	dir := t.TempDir()
	vacuumed, ve := open(t, dir)
	for _, e := range []*Engine{pe, ve} {
		must(t, e, "CREATE INDEX ON :P(k)")
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				vacuumed.Vacuum()
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					must(t, ve, "MATCH (a:P)-[:R]->(b) WHERE a.v >= 0 RETURN count(*)")
					must(t, ve, "MATCH (a:P {k: 3}) RETURN a")
				}
			}
		}()
	}
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 400; i++ {
		var q string
		switch rng.Intn(5) {
		case 0, 1:
			q = fmt.Sprintf("CREATE (a:P {k: %d, v: 0})-[:R]->(b:P {k: %d, v: 0})", i, i+100000)
		case 2:
			q = fmt.Sprintf("MATCH (a:P {k: %d}) SET a.v = %d", rng.Intn(i+1), i)
		case 3:
			q = fmt.Sprintf("MATCH (a:P {k: %d}) DELETE a", rng.Intn(i+1))
		case 4:
			q = fmt.Sprintf("MATCH (a:P {k: %d}), (b:P {k: %d}) CREATE (a)-[:R]->(b)", rng.Intn(i+1), rng.Intn(i+1))
		}
		must(t, pe, q)
		must(t, ve, q)
	}
	close(stop)
	wg.Wait()
	want := exported(plain)
	if exported(vacuumed) != want {
		t.Fatal("vacuumed store diverged from the plain one")
	}
	if err := vacuumed.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	stats := vacuumed.Stats()
	if stats["nodes"] != stats["node_versions"] {
		t.Fatalf("checkpoint should leave one version per node: %v", stats)
	}
	vacuumed.Close()
	recovered, _ := open(t, dir)
	if exported(recovered) != want {
		t.Fatal("state after vacuum, checkpoint and recovery differs")
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
