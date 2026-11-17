package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Yashas2004/graphene/graph"
	"github.com/Yashas2004/graphene/query"
)

func openStore(t *testing.T, dir string) *graph.Store {
	s, err := graph.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func startLeader(t *testing.T) (*graph.Store, *query.Engine, string) {
	s := openStore(t, t.TempDir())
	mux := http.NewServeMux()
	Register(mux, s)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return s, query.NewEngine(s), srv.URL
}

// startFollower runs a follower until the test ends (or stop is called).
func startFollower(t *testing.T, s *graph.Store, leader string) (f *Follower, stop func()) {
	f = &Follower{Store: s, Leader: leader, Wait: 100 * time.Millisecond, Logf: t.Logf}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	stop = func() { cancel(); <-done }
	t.Cleanup(stop)
	return f, stop
}

func waitForLSN(t *testing.T, s *graph.Store, lsn uint64) {
	deadline := time.Now().Add(10 * time.Second)
	for s.LSN() != lsn {
		if time.Now().After(deadline) {
			t.Fatalf("follower stuck at LSN %d, want %d", s.LSN(), lsn)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForSnapshots waits for the follower's snapshot counter. The counter is
// bumped just after the store is restored, so it can trail the LSN briefly.
func waitForSnapshots(t *testing.T, f *Follower, want int64) {
	deadline := time.Now().Add(5 * time.Second)
	for f.Snapshots.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d snapshot downloads, got %d", want, f.Snapshots.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// state is a canonical form of everything live in a store, so two stores can
// be compared exactly.
func state(s *graph.Store) string {
	data := s.Export()
	lines := make([]string, 0, len(data.Ops))
	for _, op := range data.Ops {
		b, _ := json.Marshal(op) // map keys are sorted by encoding/json
		lines = append(lines, string(b))
	}
	sort.Strings(lines)
	return fmt.Sprintf("lsn=%d\n%s", data.LSN, strings.Join(lines, "\n"))
}

func run(t *testing.T, e *query.Engine, q string) *query.Result {
	r, err := e.Run(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

// randomWrites applies a mix of creates, edges, updates and deletes.
func randomWrites(t *testing.T, e *query.Engine, rng *rand.Rand, n, base int) {
	for i := 0; i < n; i++ {
		k := base + i
		switch rng.Intn(5) {
		case 0, 1:
			run(t, e, fmt.Sprintf("CREATE (a:P {k: %d, v: 0})-[:R]->(b:P {k: %d, v: 0})", k, k+100000))
		case 2:
			run(t, e, fmt.Sprintf("MATCH (a:P {k: %d}) SET a.v = %d", base+rng.Intn(i+1), i))
		case 3:
			run(t, e, fmt.Sprintf("MATCH (a:P {k: %d}) DELETE a", base+rng.Intn(i+1)))
		case 4:
			run(t, e, fmt.Sprintf("MATCH (a:P {k: %d}), (b:P {k: %d}) CREATE (a)-[:R]->(b)", base+rng.Intn(i+1), base+rng.Intn(i+1)))
		}
	}
}

func TestFollowerTailsLeader(t *testing.T) {
	leader, le, url := startLeader(t)
	follower := openStore(t, t.TempDir())
	startFollower(t, follower, url)
	fe := query.NewEngine(follower)
	fe.Follower, fe.Leader = true, url

	run(t, le, "CREATE INDEX ON :P(k)")
	randomWrites(t, le, rand.New(rand.NewSource(1)), 300, 0)
	waitForLSN(t, follower, leader.LSN())
	if state(leader) != state(follower) {
		t.Fatal("follower state differs from leader")
	}

	// same query, same answer, and the follower uses the replicated index
	q := "MATCH (a:P) WHERE a.v > 0 RETURN count(*)"
	if l, f := run(t, le, q), run(t, fe, q); fmt.Sprint(l.Rows) != fmt.Sprint(f.Rows) {
		t.Fatalf("leader %v, follower %v", l.Rows, f.Rows)
	}
	if r := run(t, fe, "EXPLAIN MATCH (a:P {k: 1}) RETURN a"); !strings.HasPrefix(r.Plan[0], "IndexSeek") {
		t.Fatalf("index not replicated: %v", r.Plan)
	}
	// followers refuse writes
	if _, err := fe.Run("CREATE (x:P {k: -1})"); err == nil || !strings.Contains(err.Error(), "read-only follower") {
		t.Fatalf("follower accepted a write: %v", err)
	}
}

func TestNewFollowerBootstrapsFromSnapshot(t *testing.T) {
	leader, le, url := startLeader(t)
	leader.SetReplicationBuffer(10) // keep very little history
	run(t, le, "CREATE INDEX ON :P(k)")
	randomWrites(t, le, rand.New(rand.NewSource(2)), 300, 0)

	follower := openStore(t, t.TempDir())
	f, _ := startFollower(t, follower, url)
	waitForLSN(t, follower, leader.LSN())
	waitForSnapshots(t, f, 1)
	if state(leader) != state(follower) {
		t.Fatal("follower state differs after snapshot bootstrap")
	}
	// and it keeps tailing normally afterwards
	leader.SetReplicationBuffer(65536)
	randomWrites(t, le, rand.New(rand.NewSource(3)), 5, 500000)
	waitForLSN(t, follower, leader.LSN())
	if f.Snapshots.Load() != 1 || state(leader) != state(follower) {
		t.Fatalf("follower did not tail after bootstrap (snapshots=%d)", f.Snapshots.Load())
	}
}

func TestFollowerRestartResumesFromItsOwnLog(t *testing.T) {
	leader, le, url := startLeader(t)
	dir := t.TempDir()
	follower := openStore(t, dir)
	_, stop := startFollower(t, follower, url)
	rng := rand.New(rand.NewSource(4))
	randomWrites(t, le, rng, 100, 0)
	waitForLSN(t, follower, leader.LSN())
	stop()
	follower.Close()

	randomWrites(t, le, rng, 100, 1000) // leader moves on while the follower is down

	follower = openStore(t, dir)
	if follower.LSN() == 0 {
		t.Fatal("follower lost its state on restart")
	}
	f, _ := startFollower(t, follower, url)
	waitForLSN(t, follower, leader.LSN())
	if f.Snapshots.Load() != 0 {
		t.Fatalf("follower should catch up from ops, but downloaded %d snapshots", f.Snapshots.Load())
	}
	if state(leader) != state(follower) {
		t.Fatal("follower state differs after restart")
	}
}

func TestFollowerTooFarBehindResyncs(t *testing.T) {
	leader, le, url := startLeader(t)
	dir := t.TempDir()
	follower := openStore(t, dir)
	_, stop := startFollower(t, follower, url)
	rng := rand.New(rand.NewSource(5))
	randomWrites(t, le, rng, 50, 0)
	waitForLSN(t, follower, leader.LSN())
	stop()

	leader.SetReplicationBuffer(10)     // from now on the leader keeps very little history
	randomWrites(t, le, rng, 200, 1000) // far more than its buffer holds

	f, _ := startFollower(t, follower, url)
	waitForLSN(t, follower, leader.LSN())
	waitForSnapshots(t, f, 1)
	if state(leader) != state(follower) {
		t.Fatal("follower state differs after resync")
	}
	// the resynced state must also survive a restart
	want := state(follower)
	follower.Close()
	if got := state(openStore(t, dir)); got != want {
		t.Fatal("resynced follower state did not survive a restart")
	}
}
