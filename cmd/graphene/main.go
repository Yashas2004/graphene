// Command graphene runs the query gateway (serve) or the benchmark (bench).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yashas2004/graphene/graph"
	"github.com/Yashas2004/graphene/query"
	"github.com/Yashas2004/graphene/replication"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: graphene serve [-data dir] [-addr :7474] [-s3-endpoint url -s3-bucket name] [-follow leader-url] | graphene bench [-n 50000]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	switch os.Args[1] {
	case "serve":
		data := fs.String("data", "./data", "data directory")
		addr := fs.String("addr", ":7474", "listen address")
		nosync := fs.Bool("nosync", false, "skip fsync on commit (faster, less durable)")
		s3Endpoint := fs.String("s3-endpoint", "", "S3/MinIO endpoint for snapshots, e.g. http://localhost:9000 (empty = local disk)")
		s3Bucket := fs.String("s3-bucket", "graphene", "S3 bucket for snapshots (must exist)")
		s3Region := fs.String("s3-region", "us-east-1", "S3 region")
		follow := fs.String("follow", "", "run as a read-only follower of this leader, e.g. http://localhost:7474")
		fs.Parse(os.Args[2:])
		var obj graph.ObjectStore = graph.FSStore{Dir: *data}
		if *s3Endpoint != "" {
			// Credentials come from the environment so they never show up
			// in the process list or shell history.
			access, secret := os.Getenv("GRAPHENE_S3_ACCESS_KEY"), os.Getenv("GRAPHENE_S3_SECRET_KEY")
			if access == "" || secret == "" {
				log.Fatal("set GRAPHENE_S3_ACCESS_KEY and GRAPHENE_S3_SECRET_KEY to use -s3-endpoint")
			}
			obj = graph.S3Store{Endpoint: *s3Endpoint, Bucket: *s3Bucket, Region: *s3Region, AccessKey: access, SecretKey: secret}
			log.Printf("snapshots go to %s (bucket %s)", *s3Endpoint, *s3Bucket)
		}
		serve(*data, *addr, !*nosync, obj, strings.TrimRight(*follow, "/"))
	case "bench":
		n := fs.Int("n", 50000, "number of nodes")
		fs.Parse(os.Args[2:])
		bench(*n)
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}

func serve(data, addr string, sync bool, obj graph.ObjectStore, leader string) {
	if err := os.MkdirAll(data, 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := graph.OpenWith(data, sync, obj)
	if err != nil {
		log.Fatal(err)
	}
	eng := query.NewEngine(store)
	role := "leader"
	if leader != "" {
		role = "follower of " + leader
		eng.Follower, eng.Leader = true, leader
		go (&replication.Follower{Store: store, Leader: leader}).Run(context.Background())
	}
	// Every node serves the replication endpoints, so a follower can itself
	// be followed.
	replication.Register(http.DefaultServeMux, store)
	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(v)
	}
	// POST /query — body is the raw query text, or JSON {"query": "..."}.
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		text := string(body)
		if strings.HasPrefix(strings.TrimSpace(text), "{") {
			var req struct {
				Query string `json:"query"`
			}
			if json.Unmarshal(body, &req) == nil {
				text = req.Query
			}
		}
		res, err := eng.Run(text)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	http.HandleFunc("/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Checkpoint(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := store.Stats()
		stats["role"] = role
		writeJSON(w, http.StatusOK, stats)
	})
	log.Printf("graphene %s listening on %s (data=%s, recovered %v)", role, addr, data, store.Stats())
	log.Fatal(http.ListenAndServe(addr, nil))
}

func bench(n int) {
	dir, _ := os.MkdirTemp("", "graphene-bench")
	defer os.RemoveAll(dir)
	store, err := graph.Open(dir, false)
	if err != nil {
		log.Fatal(err)
	}
	eng := query.NewEngine(store)
	eng.Run("CREATE INDEX ON :Person(uid)")

	start := time.Now()
	rng := rand.New(rand.NewSource(1))
	ids := make([]uint64, n)
	var ops []graph.Op
	flush := func() {
		if err := store.Commit(ops); err != nil {
			log.Fatal(err)
		}
		ops = nil
	}
	for i := 0; i < n; i++ {
		ids[i] = store.AllocID()
		ops = append(ops, graph.Op{Kind: "node", ID: ids[i], Label: "Person",
			Props: map[string]any{"uid": float64(i), "k": float64(i), "age": float64(i % 80)}})
		if len(ops) >= 1000 {
			flush()
		}
	}
	flush()
	for i := 0; i < n; i++ {
		for j := 0; j < 3; j++ {
			ops = append(ops, graph.Op{Kind: "edge", ID: store.AllocID(), Label: "KNOWS", From: ids[i], To: ids[rng.Intn(n)]})
		}
		if len(ops) >= 1000 {
			flush()
		}
	}
	flush()
	workers := runtime.NumCPU()
	fmt.Printf("loaded %d nodes, %d edges in %v (%d workers)\n\n", n, 3*n, time.Since(start).Round(time.Millisecond), workers)
	fmt.Printf("%-34s %12s %12s\n", "workload", "queries/s", "avg latency")

	run := func(name string, gen func(r *rand.Rand) string) {
		var count atomic.Int64
		var wg sync.WaitGroup
		deadline := time.Now().Add(time.Second)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				for time.Now().Before(deadline) {
					if _, err := eng.Run(gen(r)); err != nil {
						log.Fatal(err)
					}
					count.Add(1)
				}
			}(int64(w))
		}
		wg.Wait()
		c := count.Load()
		avg := time.Second * time.Duration(workers) / time.Duration(c)
		if avg >= 10*time.Microsecond {
			avg = avg.Round(time.Microsecond)
		}
		fmt.Printf("%-34s %12d %12v\n", name, c, avg)
	}
	run("point lookup (index seek)", func(r *rand.Rand) string {
		return fmt.Sprintf("MATCH (a:Person {uid: %d}) RETURN a.age", r.Intn(n))
	})
	run("point lookup (label scan)", func(r *rand.Rand) string {
		return fmt.Sprintf("MATCH (a:Person {k: %d}) RETURN a.age", r.Intn(n))
	})
	run("2-hop traversal", func(r *rand.Rand) string {
		return fmt.Sprintf("MATCH (a:Person {uid: %d})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.uid", r.Intn(n))
	})
	run("shortest path", func(r *rand.Rand) string {
		return fmt.Sprintf("SHORTEST PATH FROM (a:Person {uid: %d}) TO (b:Person {uid: %d})", r.Intn(n), r.Intn(n))
	})
	run("repeated query (cache hit)", func(r *rand.Rand) string {
		return "MATCH (a:Person {uid: 7})-[:KNOWS]->(b) RETURN b.uid"
	})
	run("writes (CREATE, WAL, no fsync)", func(r *rand.Rand) string {
		return fmt.Sprintf("CREATE (t:Tmp {x: %d})", r.Intn(1000000))
	})
}
