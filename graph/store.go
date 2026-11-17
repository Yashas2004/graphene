// Package graph is the storage engine: an in-memory adjacency-list property
// graph with MVCC snapshot reads, secondary indexes, a write-ahead log and
// checkpointing to an object store.
package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type propVer struct {
	ver   uint64
	props map[string]any // immutable once published (copy-on-write)
}

type node struct {
	id               uint64
	label            string
	vers             []propVer
	created, deleted uint64
}

type edge struct {
	id, from, to     uint64
	typ              string
	props            map[string]any
	created, deleted uint64
}

type NodeView struct {
	ID    uint64         `json:"id"`
	Label string         `json:"label"`
	Props map[string]any `json:"props"`
}

type EdgeView struct {
	ID, From, To uint64
	Type         string
	Props        map[string]any
}

type Store struct {
	mu        sync.RWMutex
	nodes     map[uint64]*node
	edges     map[uint64]*edge
	out, in   map[uint64][]uint64            // adjacency lists of edge ids
	byLabel   map[string][]uint64            // label -> node ids
	indexes   map[string]map[string][]uint64 // "Label.prop" -> value -> node ids
	indexDefs map[string][2]string
	version   atomic.Uint64
	nextID    atomic.Uint64
	lsn       uint64
	wal       *WAL
	obj       ObjectStore

	// Replication: the most recent ops are kept in memory so followers can
	// tail them, and notify is closed (then replaced) on every change so a
	// waiting follower request wakes up.
	recent    []Op
	recentCap int
	notify    chan struct{}

	// Versions that long-lived snapshots are reading, so Vacuum knows what
	// it must keep. Guarded by snapMu, never by mu.
	snapMu sync.Mutex
	open   map[uint64]int
}

const snapshotKey = "snapshot.json"

// SnapshotData is a compacted copy of the live graph: the ops that rebuild
// it, and the LSN it is consistent with. It is the checkpoint file format
// and what a new follower downloads from the leader.
type SnapshotData struct {
	LSN    uint64 `json:"lsn"`
	NextID uint64 `json:"next_id"`
	Ops    []Op   `json:"ops"`
}

// Open recovers the store from dir, keeping snapshots in dir as well.
func Open(dir string, syncWrites bool) (*Store, error) {
	return OpenWith(dir, syncWrites, FSStore{Dir: dir})
}

// OpenWith recovers the store: load the last snapshot from obj, then replay
// any WAL records in dir that are newer than it. The WAL always lives on
// local disk; obj holds checkpoints and may be remote (S3Store).
func OpenWith(dir string, syncWrites bool, obj ObjectStore) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		nodes: map[uint64]*node{}, edges: map[uint64]*edge{},
		out: map[uint64][]uint64{}, in: map[uint64][]uint64{},
		byLabel: map[string][]uint64{}, indexes: map[string]map[string][]uint64{},
		indexDefs: map[string][2]string{}, obj: obj,
		recentCap: 65536, notify: make(chan struct{}), open: map[uint64]int{},
	}
	b, err := s.obj.Get(snapshotKey)
	if err == nil {
		var sf SnapshotData
		if err := json.Unmarshal(b, &sf); err != nil {
			return nil, fmt.Errorf("corrupt snapshot: %w", err)
		}
		for _, op := range sf.Ops {
			s.apply(op, 1)
		}
		s.lsn = sf.LSN
		s.bump(sf.NextID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	walPath := filepath.Join(dir, "wal.log")
	valid, err := replayWAL(walPath, func(op Op) {
		if op.LSN > s.lsn { // skip records already covered by the snapshot
			s.apply(op, 1)
			s.lsn = op.LSN
		}
	})
	if err != nil {
		return nil, err
	}
	if valid > 0 || fileExists(walPath) {
		if err := os.Truncate(walPath, valid); err != nil { // drop torn tail
			return nil, err
		}
	}
	if s.wal, err = openWAL(walPath, syncWrites); err != nil {
		return nil, err
	}
	s.version.Store(1)
	return s, nil
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

func (s *Store) Close() error    { return s.wal.Close() }
func (s *Store) Version() uint64 { return s.version.Load() }
func (s *Store) AllocID() uint64 { return s.nextID.Add(1) }

func (s *Store) bump(id uint64) {
	for {
		cur := s.nextID.Load()
		if id <= cur || s.nextID.CompareAndSwap(cur, id) {
			return
		}
	}
}

func idxKey(label, key string) string { return label + "." + key }
func valKey(v any) string             { return fmt.Sprintf("%T:%v", v, v) }

// Commit makes a batch of ops durable (WAL first) and then visible, as one
// atomic version bump.
func (s *Store) Commit(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range ops {
		s.lsn++
		ops[i].LSN = s.lsn
	}
	if err := s.wal.Append(ops); err != nil {
		return err
	}
	ver := s.version.Load() + 1
	for _, op := range ops {
		s.apply(op, ver)
	}
	s.version.Store(ver)
	s.published(ops)
	return nil
}

// published records newly applied ops for followers and wakes any follower
// request that is waiting. Caller holds the write lock.
func (s *Store) published(ops []Op) {
	s.recent = append(s.recent, ops...)
	if len(s.recent) > 2*s.recentCap { // trim in bulk so the cost is amortized
		s.recent = append([]Op(nil), s.recent[len(s.recent)-s.recentCap:]...)
	}
	close(s.notify)
	s.notify = make(chan struct{})
}

// LSN is the sequence number of the last op this store has applied.
func (s *Store) LSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lsn
}

// SetReplicationBuffer sets how many recent ops are kept for followers.
func (s *Store) SetReplicationBuffer(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recentCap = n
}

// OpsAfter returns up to max ops with an LSN greater than lsn. ok is false
// when they cannot be served from the in-memory buffer: the follower is too
// far behind (or ahead of this store) and must download a full snapshot.
func (s *Store) OpsAfter(lsn uint64, max int) (ops []Op, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lsn == s.lsn {
		return nil, true
	}
	if lsn > s.lsn || len(s.recent) == 0 || lsn+1 < s.recent[0].LSN {
		return nil, false
	}
	start := int(lsn + 1 - s.recent[0].LSN)
	end := len(s.recent)
	if end-start > max {
		end = start + max
	}
	return append([]Op(nil), s.recent[start:end]...), true
}

// WaitForChange blocks until the store moves past lsn, the timeout passes or
// done is closed. It returns immediately if the store is not at lsn.
func (s *Store) WaitForChange(lsn uint64, timeout time.Duration, done <-chan struct{}) {
	s.mu.RLock()
	ch, cur := s.notify, s.lsn
	s.mu.RUnlock()
	if cur != lsn {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	case <-done:
	}
}

// ErrGap means replicated ops did not continue directly from this store's
// LSN, so applying them would skip writes.
var ErrGap = errors.New("replication gap: ops do not follow the local LSN")

// ApplyReplicated is the follower's write path: it logs and applies ops that
// came from a leader, keeping the leader's LSNs. Ops at or below the local
// LSN are skipped, so re-delivery is harmless.
func (s *Store) ApplyReplicated(ops []Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := ops[:0:0]
	next := s.lsn + 1
	for _, op := range ops {
		if op.LSN < next {
			continue
		}
		if op.LSN != next {
			return ErrGap
		}
		fresh = append(fresh, op)
		next++
	}
	if len(fresh) == 0 {
		return nil
	}
	if err := s.wal.Append(fresh); err != nil {
		return err
	}
	ver := s.version.Load() + 1
	for _, op := range fresh {
		s.apply(op, ver)
	}
	s.lsn = next - 1
	s.version.Store(ver)
	s.published(fresh)
	return nil
}

// Export returns a compacted snapshot of the live graph.
func (s *Store) Export() SnapshotData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exportLocked()
}

func (s *Store) exportLocked() SnapshotData {
	sf := SnapshotData{LSN: s.lsn, NextID: s.nextID.Load()}
	for _, n := range s.nodes {
		if n.deleted == 0 {
			sf.Ops = append(sf.Ops, Op{Kind: "node", ID: n.id, Label: n.label, Props: n.vers[len(n.vers)-1].props})
		}
	}
	for _, e := range s.edges {
		if e.deleted == 0 {
			sf.Ops = append(sf.Ops, Op{Kind: "edge", ID: e.id, Label: e.typ, From: e.from, To: e.to, Props: e.props})
		}
	}
	for _, d := range s.indexDefs {
		sf.Ops = append(sf.Ops, Op{Kind: "index", Label: d[0], Key: d[1]})
	}
	return sf
}

// Restore replaces the whole store with a snapshot from a leader. A follower
// uses it when it is new or too far behind to catch up from recent ops.
//
// The WAL is emptied before the new snapshot is written. A crash between the
// two leaves the previous checkpoint with no WAL: an older but consistent
// state, from which the follower simply syncs again.
func (s *Store) Restore(data SnapshotData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.Truncate(); err != nil {
		return err
	}
	if err := s.obj.Put(snapshotKey, b); err != nil {
		return err
	}
	s.nodes, s.edges = map[uint64]*node{}, map[uint64]*edge{}
	s.out, s.in = map[uint64][]uint64{}, map[uint64][]uint64{}
	s.byLabel, s.indexes = map[string][]uint64{}, map[string]map[string][]uint64{}
	s.indexDefs = map[string][2]string{}
	ver := s.version.Load() + 1
	for _, op := range data.Ops {
		s.apply(op, ver)
	}
	s.bump(data.NextID)
	s.lsn = data.LSN
	s.version.Store(ver)
	s.recent = nil
	s.published(nil)
	return nil
}

// apply mutates in-memory state. Caller holds the write lock (or is in
// single-threaded recovery). It is idempotent so replay is safe.
func (s *Store) apply(op Op, ver uint64) {
	switch op.Kind {
	case "node":
		if _, ok := s.nodes[op.ID]; ok {
			return
		}
		props := op.Props
		if props == nil {
			props = map[string]any{}
		}
		s.nodes[op.ID] = &node{id: op.ID, label: op.Label, created: ver, vers: []propVer{{ver, props}}}
		s.byLabel[op.Label] = append(s.byLabel[op.Label], op.ID)
		for k, v := range props {
			s.indexAdd(op.Label, k, v, op.ID)
		}
		s.bump(op.ID)
	case "edge":
		if _, ok := s.edges[op.ID]; ok {
			return
		}
		f, t := s.nodes[op.From], s.nodes[op.To]
		if f == nil || t == nil || f.deleted != 0 || t.deleted != 0 {
			return
		}
		s.edges[op.ID] = &edge{id: op.ID, from: op.From, to: op.To, typ: op.Label, props: op.Props, created: ver}
		s.out[op.From] = append(s.out[op.From], op.ID)
		s.in[op.To] = append(s.in[op.To], op.ID)
		s.bump(op.ID)
	case "set":
		n := s.nodes[op.ID]
		if n == nil || n.deleted != 0 {
			return
		}
		old := n.vers[len(n.vers)-1].props
		np := make(map[string]any, len(old)+1)
		for k, v := range old {
			np[k] = v
		}
		np[op.Key] = op.Val
		n.vers = append(n.vers, propVer{ver, np})
		s.indexAdd(n.label, op.Key, op.Val, n.id)
	case "delnode":
		n := s.nodes[op.ID]
		if n == nil || n.deleted != 0 {
			return
		}
		n.deleted = ver
		for _, list := range [][]uint64{s.out[op.ID], s.in[op.ID]} {
			for _, eid := range list {
				if e := s.edges[eid]; e.deleted == 0 {
					e.deleted = ver
				}
			}
		}
	case "deledge":
		if e := s.edges[op.ID]; e != nil && e.deleted == 0 {
			e.deleted = ver
		}
	case "index":
		k := idxKey(op.Label, op.Key)
		if _, ok := s.indexes[k]; ok {
			return
		}
		s.indexes[k] = map[string][]uint64{}
		s.indexDefs[k] = [2]string{op.Label, op.Key}
		for _, id := range s.byLabel[op.Label] {
			n := s.nodes[id]
			if n.deleted != 0 {
				continue
			}
			if v, ok := n.vers[len(n.vers)-1].props[op.Key]; ok {
				s.indexAdd(op.Label, op.Key, v, id)
			}
		}
	}
}

// Index buckets are candidate sets: entries are never removed on update, the
// reader re-checks the property against its snapshot.
func (s *Store) indexAdd(label, key string, v any, id uint64) {
	if ix, ok := s.indexes[idxKey(label, key)]; ok {
		vk := valKey(v)
		ix[vk] = append(ix[vk], id)
	}
}

// Vacuum frees everything no reader can see any more: deleted nodes and
// edges, superseded property versions and stale index entries. It returns
// how many records and versions it removed.
func (s *Store) Vacuum() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vacuumLocked()
}

// vacuumLocked needs the write lock. Holding it means no query is in its
// read phase (those hold the read lock), so the only readers to respect are
// long-lived snapshots, which are registered in s.open. The horizon is the
// oldest version any of them reads; state that is invisible at the horizon
// and every later version can go. The store version does not change,
// because nothing visible changes.
func (s *Store) vacuumLocked() int {
	horizon := s.version.Load()
	s.snapMu.Lock()
	for v := range s.open {
		if v < horizon {
			horizon = v
		}
	}
	s.snapMu.Unlock()

	removed := 0
	for id, e := range s.edges {
		if e.deleted != 0 && e.deleted <= horizon {
			delete(s.edges, id)
			removed++
		}
	}
	for id, n := range s.nodes {
		if n.deleted != 0 && n.deleted <= horizon {
			// Its edges were deleted at the same version or earlier, so
			// they are already gone from s.edges.
			delete(s.nodes, id)
			removed++
			continue
		}
		keep := 0 // newest version a reader at the horizon would pick
		for i, v := range n.vers {
			if v.ver <= horizon {
				keep = i
			}
		}
		if keep > 0 {
			n.vers = append([]propVer(nil), n.vers[keep:]...)
			removed += keep
		}
	}
	for _, adjacency := range []map[uint64][]uint64{s.out, s.in} {
		for id, list := range adjacency {
			kept := list[:0]
			for _, eid := range list {
				if _, ok := s.edges[eid]; ok {
					kept = append(kept, eid)
				}
			}
			if len(kept) == 0 {
				delete(adjacency, id)
			} else {
				adjacency[id] = kept
			}
		}
	}
	for label, list := range s.byLabel {
		kept := list[:0]
		for _, id := range list {
			if _, ok := s.nodes[id]; ok {
				kept = append(kept, id)
			}
		}
		if len(kept) == 0 {
			delete(s.byLabel, label)
		} else {
			s.byLabel[label] = kept
		}
	}
	// Rebuild each index from the versions that survived. A node appears
	// under every value one of its remaining versions holds.
	for k, def := range s.indexDefs {
		ix := map[string][]uint64{}
		for _, id := range s.byLabel[def[0]] {
			for _, v := range s.nodes[id].vers {
				val, ok := v.props[def[1]]
				if !ok {
					continue
				}
				vk := valKey(val)
				if bucket := ix[vk]; len(bucket) == 0 || bucket[len(bucket)-1] != id {
					ix[vk] = append(bucket, id)
				}
			}
		}
		s.indexes[k] = ix
	}
	return removed
}

// Checkpoint vacuums, writes a compacted snapshot to the object store and
// truncates the WAL. A crash between the last two steps is safe: replay
// skips by LSN.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vacuumLocked()
	b, err := json.Marshal(s.exportLocked())
	if err != nil {
		return err
	}
	if err := s.obj.Put(snapshotKey, b); err != nil {
		return err
	}
	return s.wal.Truncate()
}

func (s *Store) Stats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := 0
	for _, n := range s.nodes {
		versions += len(n.vers)
	}
	s.snapMu.Lock()
	openSnapshots := 0
	for _, count := range s.open {
		openSnapshots += count
	}
	s.snapMu.Unlock()
	// nodes and edges count records held in memory, including deleted ones
	// that Vacuum has not reclaimed yet.
	return map[string]any{"nodes": len(s.nodes), "edges": len(s.edges), "node_versions": versions, "indexes": len(s.indexes),
		"open_snapshots": openSnapshots, "version": s.version.Load(), "lsn": s.lsn}
}

// Snapshot is a consistent read view pinned to one committed version.
// Readers never observe a half-applied commit. There are two kinds:
//
//   - Store.PinnedSnapshot holds the store's read lock until released. It is
//     what queries use: one lock acquisition per query, and Vacuum (which
//     needs the write lock) cannot run underneath it.
//   - Store.Snapshot takes the lock per access and can be held across
//     writes. It is registered so Vacuum keeps the versions it reads; call
//     Close when done, or Vacuum will hold that history back.
//
// A Snapshot belongs to one goroutine and must not be used after release.
type Snapshot struct {
	s          *Store
	Ver        uint64
	pinned     bool
	registered bool
	closed     bool
}

// Snapshot returns a long-lived snapshot. Call Close when finished.
func (s *Store) Snapshot() *Snapshot {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	ver := s.version.Load()
	s.open[ver]++
	return &Snapshot{s: s, Ver: ver, registered: true}
}

// PinnedSnapshot returns a snapshot that holds the read lock, and an
// idempotent function that releases it. The caller must release before
// committing a write. Per-access locking made traversals spend their time
// contending on the lock's reader count when many cores ran queries at once.
func (s *Store) PinnedSnapshot() (*Snapshot, func()) {
	s.mu.RLock()
	sn := &Snapshot{s: s, Ver: s.version.Load(), pinned: true}
	return sn, func() {
		if sn.pinned {
			sn.pinned = false
			sn.closed = true
			s.mu.RUnlock()
		}
	}
}

// Close releases a snapshot from Store.Snapshot. It is safe to call twice.
func (sn *Snapshot) Close() {
	if !sn.registered || sn.closed {
		return
	}
	sn.closed = true
	sn.s.snapMu.Lock()
	defer sn.s.snapMu.Unlock()
	if sn.s.open[sn.Ver]--; sn.s.open[sn.Ver] <= 0 {
		delete(sn.s.open, sn.Ver)
	}
}

// check stops a released snapshot from silently reading state that Vacuum
// may already have reclaimed.
func (sn *Snapshot) check() {
	if sn.closed {
		panic("graph: snapshot used after it was released")
	}
}

func (n *node) at(ver uint64) (map[string]any, bool) {
	if n.created > ver || (n.deleted != 0 && n.deleted <= ver) {
		return nil, false
	}
	for i := len(n.vers) - 1; i >= 0; i-- {
		if n.vers[i].ver <= ver {
			return n.vers[i].props, true
		}
	}
	return nil, false
}

func (sn *Snapshot) Node(id uint64) (NodeView, bool) {
	sn.check()
	if !sn.pinned {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	n := sn.s.nodes[id]
	if n == nil {
		return NodeView{}, false
	}
	props, ok := n.at(sn.Ver)
	if !ok {
		return NodeView{}, false
	}
	return NodeView{ID: id, Label: n.label, Props: props}, true
}

// IDs returns candidate node ids for a label ("" means every node).
func (sn *Snapshot) IDs(label string) []uint64 {
	sn.check()
	if !sn.pinned {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	if label != "" {
		return append([]uint64(nil), sn.s.byLabel[label]...)
	}
	ids := make([]uint64, 0, len(sn.s.nodes))
	for id := range sn.s.nodes {
		ids = append(ids, id)
	}
	return ids
}

// Lookup returns index candidates; ok is false when no such index exists.
func (sn *Snapshot) Lookup(label, key string, val any) ([]uint64, bool) {
	sn.check()
	if !sn.pinned {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	ix, ok := sn.s.indexes[idxKey(label, key)]
	if !ok {
		return nil, false
	}
	return append([]uint64(nil), ix[valKey(val)]...), true
}

// Neighbors appends to buf the nodes reachable from id over visible outgoing
// (or incoming) edges, optionally restricted to one edge type. Passing the
// same buffer back in lets a traversal run without allocating per node.
func (sn *Snapshot) Neighbors(id uint64, outgoing bool, typ string, buf []uint64) []uint64 {
	sn.check()
	if !sn.pinned {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	list := sn.s.in[id]
	if outgoing {
		list = sn.s.out[id]
	}
	for _, eid := range list {
		e := sn.s.edges[eid]
		if e.created > sn.Ver || (e.deleted != 0 && e.deleted <= sn.Ver) {
			continue
		}
		if typ != "" && e.typ != typ {
			continue
		}
		if outgoing {
			buf = append(buf, e.to)
		} else {
			buf = append(buf, e.from)
		}
	}
	return buf
}

// Edges returns the visible outgoing (or incoming) edges of a node.
func (sn *Snapshot) Edges(id uint64, outgoing bool) []EdgeView {
	sn.check()
	if !sn.pinned {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	list := sn.s.in[id]
	if outgoing {
		list = sn.s.out[id]
	}
	res := make([]EdgeView, 0, len(list))
	for _, eid := range list {
		e := sn.s.edges[eid]
		if e.created > sn.Ver || (e.deleted != 0 && e.deleted <= sn.Ver) {
			continue
		}
		res = append(res, EdgeView{ID: e.id, From: e.from, To: e.to, Type: e.typ, Props: e.props})
	}
	return res
}
