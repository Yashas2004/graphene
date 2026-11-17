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
}

const snapshotKey = "snapshot.json"

type snapFile struct {
	LSN    uint64 `json:"lsn"`
	NextID uint64 `json:"next_id"`
	Ops    []Op   `json:"ops"`
}

// Open recovers the store from dir: load the last snapshot, then replay any
// WAL records newer than it.
func Open(dir string, syncWrites bool) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		nodes: map[uint64]*node{}, edges: map[uint64]*edge{},
		out: map[uint64][]uint64{}, in: map[uint64][]uint64{},
		byLabel: map[string][]uint64{}, indexes: map[string]map[string][]uint64{},
		indexDefs: map[string][2]string{}, obj: FSStore{Dir: dir},
	}
	b, err := s.obj.Get(snapshotKey)
	if err == nil {
		var sf snapFile
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

// Checkpoint writes a compacted snapshot to the object store and truncates
// the WAL. A crash between the two steps is safe: replay skips by LSN.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sf := snapFile{LSN: s.lsn, NextID: s.nextID.Load()}
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
	b, err := json.Marshal(sf)
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
	return map[string]any{"nodes": len(s.nodes), "edges": len(s.edges), "indexes": len(s.indexes), "version": s.version.Load(), "lsn": s.lsn}
}

// Snapshot is a consistent read view pinned to one committed version.
// Readers never block each other and never observe a half-applied commit.
type Snapshot struct {
	s      *Store
	Ver    uint64
	pinned bool
}

// Pin takes the store's read lock once for a whole query instead of once per
// node or edge access, and returns an idempotent release function. Per-access
// locking made traversals spend their time contending on the lock's reader
// count when many cores ran queries at once. A Snapshot belongs to a single
// goroutine. The caller must release before committing a write.
func (sn *Snapshot) Pin() func() {
	sn.s.mu.RLock()
	sn.pinned = true
	return func() {
		if sn.pinned {
			sn.pinned = false
			sn.s.mu.RUnlock()
		}
	}
}

func (s *Store) Snapshot() *Snapshot { return &Snapshot{s: s, Ver: s.version.Load()} }

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

// Edges returns the visible outgoing (or incoming) edges of a node.
func (sn *Snapshot) Edges(id uint64, outgoing bool) []EdgeView {
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
