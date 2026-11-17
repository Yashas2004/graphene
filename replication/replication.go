// Package replication copies a leader's writes to read-only followers.
//
// It is asynchronous, single-leader log shipping. The leader keeps its most
// recent ops in memory; a follower long-polls for ops after its own LSN and
// applies them in order. A follower that is new, or has fallen behind what
// the leader still holds, downloads a full snapshot and continues from there.
// There is no leader election and no automatic failover.
package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Yashas2004/graphene/graph"
)

const (
	maxBatch = 5000
	maxWait  = 30 * time.Second
)

type opsResponse struct {
	Ops       []graph.Op `json:"ops"`
	LeaderLSN uint64     `json:"leader_lsn"`
}

// Register adds the two endpoints followers use to mux:
//
//	GET /replicate/ops?after=<lsn>&wait_ms=<n>   ops after lsn; 410 Gone if too old
//	GET /replicate/snapshot                      full compacted snapshot
func Register(mux *http.ServeMux, s *graph.Store) {
	mux.HandleFunc("/replicate/ops", func(w http.ResponseWriter, r *http.Request) {
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil {
			http.Error(w, "after must be a number", http.StatusBadRequest)
			return
		}
		waitMS, _ := strconv.Atoi(r.URL.Query().Get("wait_ms"))
		wait := time.Duration(waitMS) * time.Millisecond
		if wait > maxWait {
			wait = maxWait
		}
		ops, ok := s.OpsAfter(after, maxBatch)
		if ok && len(ops) == 0 && wait > 0 { // caught up: hold the request until something changes
			s.WaitForChange(after, wait, r.Context().Done())
			ops, ok = s.OpsAfter(after, maxBatch)
		}
		if !ok {
			http.Error(w, "ops no longer available, fetch a snapshot", http.StatusGone)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(opsResponse{Ops: ops, LeaderLSN: s.LSN()})
	})
	mux.HandleFunc("/replicate/snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Export())
	})
}

// Follower keeps a local store in sync with a leader.
type Follower struct {
	Store  *graph.Store
	Leader string        // base URL, e.g. http://localhost:7474
	Wait   time.Duration // long-poll duration; zero means 10s
	Client *http.Client  // nil means a client whose timeout outlasts Wait
	Logf   func(format string, args ...any)

	Snapshots atomic.Int64 // how many full snapshots were downloaded
}

// Run syncs until ctx is cancelled. Errors are logged and retried.
func (f *Follower) Run(ctx context.Context) {
	if f.Wait == 0 {
		f.Wait = 10 * time.Second
	}
	if f.Client == nil {
		f.Client = &http.Client{Timeout: f.Wait + 20*time.Second}
	}
	if f.Logf == nil {
		f.Logf = log.Printf
	}
	for ctx.Err() == nil {
		if err := f.step(ctx); err != nil && ctx.Err() == nil {
			f.Logf("replication: %v (retrying)", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
		}
	}
}

// step does one round: fetch the next ops and apply them, or resync from a
// snapshot when the leader says the ops are gone or there is a gap.
func (f *Follower) step(ctx context.Context) error {
	url := fmt.Sprintf("%s/replicate/ops?after=%d&wait_ms=%d", f.Leader, f.Store.LSN(), f.Wait.Milliseconds())
	var resp opsResponse
	status, err := f.get(ctx, url, &resp)
	if err != nil {
		return err
	}
	if status == http.StatusGone {
		return f.resync(ctx)
	}
	if len(resp.Ops) == 0 {
		return nil
	}
	err = f.Store.ApplyReplicated(resp.Ops)
	if errors.Is(err, graph.ErrGap) {
		return f.resync(ctx)
	}
	return err
}

func (f *Follower) resync(ctx context.Context) error {
	var data graph.SnapshotData
	status, err := f.get(ctx, f.Leader+"/replicate/snapshot", &data)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("snapshot: unexpected status %d", status)
	}
	if err := f.Store.Restore(data); err != nil {
		return err
	}
	f.Snapshots.Add(1)
	f.Logf("replication: restored snapshot at LSN %d", data.LSN)
	return nil
}

func (f *Follower) get(ctx context.Context, url string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := f.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusGone {
		return resp.StatusCode, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, fmt.Errorf("%s: %s: %s", url, resp.Status, body)
	}
	return resp.StatusCode, json.NewDecoder(resp.Body).Decode(out)
}
