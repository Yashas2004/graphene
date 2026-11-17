package query

import (
	"container/heap"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yashas2004/graphene/graph"
)

type Result struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
	Plan    []string `json:"plan,omitempty"`
	Cached  bool     `json:"cached"`
	Micros  int64    `json:"micros"`
}

// Engine plans and executes queries. It is safe for concurrent use: every
// read runs on its own snapshot.
type Engine struct {
	S     *graph.Store
	cache *resultCache

	// Follower makes the engine read-only: its data comes from a leader, so
	// a local write would make the two diverge. Leader is shown in the error.
	Follower bool
	Leader   string
}

func NewEngine(s *graph.Store) *Engine { return &Engine{S: s, cache: newResultCache(1024)} }

func (e *Engine) Run(text string) (*Result, error) {
	start := time.Now()
	text = strings.TrimSpace(text)
	// Look in the cache before parsing: only read-only, non-EXPLAIN queries
	// are ever stored, so a hit is always safe to serve.
	ver := e.S.Version()
	if r, ok := e.cache.get(text, ver); ok {
		cp := *r
		cp.Cached = true
		cp.Micros = time.Since(start).Microseconds()
		return &cp, nil
	}
	q, err := Parse(text)
	if err != nil {
		return nil, err
	}
	if e.Follower && !q.ReadOnly() && !q.Explain {
		return nil, fmt.Errorf("this node is a read-only follower; send writes to the leader at %s", e.Leader)
	}
	cacheable := q.ReadOnly() && !q.Explain
	res, err := e.exec(q)
	if err != nil {
		return nil, err
	}
	if cacheable {
		e.cache.put(text, ver, res)
	}
	cp := *res
	cp.Micros = time.Since(start).Microseconds()
	return &cp, nil
}

type row map[string]uint64

func (e *Engine) exec(q *Query) (*Result, error) {
	res := &Result{Rows: [][]any{}}
	switch q.Kind {
	case "index":
		res.Plan = []string{fmt.Sprintf("CreateIndex(:%s.%s)", q.IdxLabel, q.IdxProp)}
		if q.Explain {
			return res, nil
		}
		err := e.S.Commit([]graph.Op{{Kind: "index", Label: q.IdxLabel, Key: q.IdxProp}})
		res.Columns, res.Rows = []string{"ops_applied"}, [][]any{{1}}
		return res, err
	case "create":
		return e.create(q, []row{{}}, res)
	}

	// One read lock for the whole read phase. It is released before any
	// commit below, because Commit needs the write lock.
	sn, unpin := e.S.PinnedSnapshot()
	defer unpin()
	if q.Kind == "path" || q.Kind == "bfs" {
		return e.traverse(sn, q, res)
	}

	rows := []row{{}}
	for _, c := range q.Match {
		rows = e.expand(sn, rows, c, q.Where, &res.Plan)
	}
	if len(q.Where) > 0 {
		res.Plan = append(res.Plan, fmt.Sprintf("Filter(%d predicates)", len(q.Where)))
		kept := rows[:0]
		for _, r := range rows {
			if e.where(sn, r, q.Where) {
				kept = append(kept, r)
			}
		}
		rows = kept
	}

	switch {
	case len(q.Sets) > 0:
		var ops []graph.Op
		for _, r := range rows {
			for _, s := range q.Sets {
				id, ok := r[s.Var]
				if !ok {
					return nil, fmt.Errorf("unknown variable %q", s.Var)
				}
				ops = append(ops, graph.Op{Kind: "set", ID: id, Key: s.Prop, Val: s.Val})
			}
		}
		unpin()
		return e.write(q, ops, "Set", res)
	case len(q.Deletes) > 0:
		seen := map[uint64]bool{}
		var ops []graph.Op
		for _, r := range rows {
			for _, v := range q.Deletes {
				id, ok := r[v]
				if !ok {
					return nil, fmt.Errorf("unknown variable %q", v)
				}
				if !seen[id] {
					seen[id] = true
					ops = append(ops, graph.Op{Kind: "delnode", ID: id})
				}
			}
		}
		unpin()
		return e.write(q, ops, "DetachDelete", res)
	case len(q.Create) > 0:
		unpin()
		return e.create(q, rows, res)
	}

	if q.Count {
		res.Plan = append(res.Plan, "Aggregate(count)")
		res.Columns, res.Rows = []string{"count(*)"}, [][]any{{len(rows)}}
		return res, nil
	}
	for _, r := range q.Return {
		name := r.Var
		if r.Prop != "" {
			name += "." + r.Prop
		}
		res.Columns = append(res.Columns, name)
	}
	res.Plan = append(res.Plan, "Project("+strings.Join(res.Columns, ", ")+")")
	for _, r := range rows {
		if q.Limit > 0 && len(res.Rows) >= q.Limit {
			break
		}
		out := make([]any, 0, len(q.Return))
		for _, ret := range q.Return {
			id, ok := r[ret.Var]
			if !ok {
				return nil, fmt.Errorf("unknown variable %q", ret.Var)
			}
			nv, _ := sn.Node(id)
			if ret.Prop == "" {
				out = append(out, nv)
			} else {
				out = append(out, nv.Props[ret.Prop])
			}
		}
		res.Rows = append(res.Rows, out)
	}
	return res, nil
}

func (e *Engine) write(q *Query, ops []graph.Op, name string, res *Result) (*Result, error) {
	res.Plan = append(res.Plan, fmt.Sprintf("%s(%d ops)", name, len(ops)))
	res.Columns = []string{"ops_applied"}
	if q.Explain {
		return res, nil
	}
	res.Rows = [][]any{{len(ops)}}
	return res, e.S.Commit(ops)
}

func (e *Engine) create(q *Query, rows []row, res *Result) (*Result, error) {
	var ops []graph.Op
	for _, base := range rows {
		r := row{}
		for k, v := range base {
			r[k] = v
		}
		for _, c := range q.Create {
			ids := make([]uint64, len(c.Nodes))
			for i, n := range c.Nodes {
				if id, ok := r[n.Var]; ok && n.Var != "" {
					ids[i] = id
					continue
				}
				ids[i] = e.S.AllocID()
				ops = append(ops, graph.Op{Kind: "node", ID: ids[i], Label: n.Label, Props: n.Props})
				if n.Var != "" {
					r[n.Var] = ids[i]
				}
			}
			for i, ed := range c.Edges {
				from, to := ids[i], ids[i+1]
				if ed.Dir == -1 {
					from, to = to, from
				}
				ops = append(ops, graph.Op{Kind: "edge", ID: e.S.AllocID(), Label: ed.Type, From: from, To: to, Props: ed.Props})
			}
		}
	}
	return e.write(q, ops, "Create", res)
}

// candidates is the planner's access-path choice for the first node of a
// pattern: index seek > label scan > full scan.
func (e *Engine) candidates(sn *graph.Snapshot, pat NodePat, where []Cond, plan *[]string) []uint64 {
	note := func(s string) {
		if plan != nil {
			*plan = append(*plan, s)
		}
	}
	if pat.Label == "" {
		note("AllNodesScan")
		return sn.IDs("")
	}
	eq := map[string]any{}
	for k, v := range pat.Props {
		eq[k] = v
	}
	for _, c := range where {
		if pat.Var != "" && c.Var == pat.Var && c.Op == "=" {
			eq[c.Prop] = c.Val
		}
	}
	for k, v := range eq {
		if ids, ok := sn.Lookup(pat.Label, k, v); ok {
			note(fmt.Sprintf("IndexSeek(:%s.%s = %v)", pat.Label, k, v))
			return ids
		}
	}
	note(fmt.Sprintf("LabelScan(:%s)", pat.Label))
	return sn.IDs(pat.Label)
}

func matches(sn *graph.Snapshot, id uint64, pat NodePat) bool {
	nv, ok := sn.Node(id)
	if !ok || (pat.Label != "" && nv.Label != pat.Label) {
		return false
	}
	for k, v := range pat.Props {
		if nv.Props[k] != v {
			return false
		}
	}
	return true
}

func (e *Engine) expand(sn *graph.Snapshot, rows []row, c Chain, where []Cond, plan *[]string) []row {
	var out []row
	for ri, r := range rows {
		p := plan
		if ri > 0 {
			p = nil // record the plan once, not per row
		}
		first := c.Nodes[0]
		var cands []uint64
		if id, ok := r[first.Var]; ok && first.Var != "" {
			cands = []uint64{id}
		} else {
			cands = e.candidates(sn, first, where, p)
		}
		if p != nil {
			for _, ed := range c.Edges {
				*p = append(*p, fmt.Sprintf("Expand(:%s dir=%d)", ed.Type, ed.Dir))
			}
		}
		for _, id := range cands {
			if !matches(sn, id, first) {
				continue
			}
			nr := bind(r, first.Var, id)
			e.hop(sn, nr, c, 0, id, &out)
		}
	}
	return out
}

func bind(r row, name string, id uint64) row {
	nr := make(row, len(r)+1)
	for k, v := range r {
		nr[k] = v
	}
	if name != "" {
		nr[name] = id
	}
	return nr
}

func (e *Engine) hop(sn *graph.Snapshot, r row, c Chain, i int, cur uint64, out *[]row) {
	if i == len(c.Edges) {
		*out = append(*out, r)
		return
	}
	ed, target := c.Edges[i], c.Nodes[i+1]
	step := func(outgoing bool) {
		for _, ev := range sn.Edges(cur, outgoing) {
			if ed.Type != "" && ev.Type != ed.Type {
				continue
			}
			ok := true
			for k, v := range ed.Props {
				if ev.Props[k] != v {
					ok = false
				}
			}
			next := ev.To
			if !outgoing {
				next = ev.From
			}
			if !ok || !matches(sn, next, target) {
				continue
			}
			if bound, has := r[target.Var]; has && target.Var != "" && bound != next {
				continue
			}
			e.hop(sn, bind(r, target.Var, next), c, i+1, next, out)
		}
	}
	if ed.Dir >= 0 {
		step(true)
	}
	if ed.Dir <= 0 {
		step(false)
	}
}

func (e *Engine) where(sn *graph.Snapshot, r row, conds []Cond) bool {
	for _, c := range conds {
		id, ok := r[c.Var]
		if !ok {
			return false
		}
		nv, _ := sn.Node(id)
		if !compare(nv.Props[c.Prop], c.Op, c.Val) {
			return false
		}
	}
	return true
}

func compare(a any, op string, b any) bool {
	cmp := 0
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		if !ok {
			return false
		}
		if x < y {
			cmp = -1
		} else if x > y {
			cmp = 1
		}
	case string:
		y, ok := b.(string)
		if !ok {
			return false
		}
		cmp = strings.Compare(x, y)
	default:
		if op == "=" {
			return a != nil && a == b
		}
		return op == "<>" && a != b
	}
	switch op {
	case "=":
		return cmp == 0
	case "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	}
	return cmp >= 0
}

// traverse runs BFS FROM and SHORTEST PATH queries on the snapshot.
func (e *Engine) traverse(sn *graph.Snapshot, q *Query, res *Result) (*Result, error) {
	find := func(p NodePat) (uint64, bool) {
		for _, id := range e.candidates(sn, p, nil, &res.Plan) {
			if matches(sn, id, p) {
				return id, true
			}
		}
		return 0, false
	}
	src, ok := find(q.From)
	if q.Kind == "path" {
		res.Columns = []string{"path", "length"}
		dst, ok2 := find(q.To)
		if q.Weight != "" {
			res.Columns = append(res.Columns, "cost")
			res.Plan = append(res.Plan, "ShortestPath(Dijkstra, weight="+q.Weight+")")
		} else {
			res.Plan = append(res.Plan, "ShortestPath(BidirectionalBFS)")
		}
		if !ok || !ok2 {
			return res, nil
		}
		var ids []uint64
		var cost float64
		if q.Weight != "" {
			var err error
			if ids, cost, err = weightedShortestPath(sn, src, dst, q.EdgeType, q.Weight); err != nil {
				return nil, err
			}
		} else {
			ids = shortestPath(sn, src, dst, q.EdgeType)
		}
		if ids == nil {
			return res, nil
		}
		path := make([]any, 0, len(ids))
		for _, id := range ids {
			nv, _ := sn.Node(id)
			path = append(path, nv)
		}
		row := []any{path, len(ids) - 1}
		if q.Weight != "" {
			row = append(row, cost)
		}
		res.Rows = [][]any{row}
		return res, nil
	}

	res.Columns = []string{"node", "depth"}
	res.Plan = append(res.Plan, "BFS")
	if !ok {
		return res, nil
	}
	depth := map[uint64]int{src: 0}
	queue := []uint64{src}
	var buf []uint64
	for head := 0; head < len(queue); head++ {
		cur := queue[head]
		nv, _ := sn.Node(cur)
		res.Rows = append(res.Rows, []any{nv, depth[cur]})
		if q.Depth >= 0 && depth[cur] >= q.Depth {
			continue
		}
		buf = sn.Neighbors(cur, true, q.EdgeType, buf[:0])
		for _, nb := range buf {
			if _, seen := depth[nb]; !seen {
				depth[nb] = depth[cur] + 1
				queue = append(queue, nb)
			}
		}
	}
	return res, nil
}

// shortestPath is a bidirectional BFS over directed edges: one search walks
// outgoing edges from src, the other walks incoming edges from dst, and each
// round expands whichever frontier is smaller by one full level. A plain BFS
// visits most of the graph before reaching a far target; two searches that
// meet in the middle each visit far fewer nodes.
//
// The first node discovered by both sides lies on a shortest path. Every node
// is checked against the other side at the moment it is discovered, so if a
// shorter connection existed it would have been detected in an earlier level.
func shortestPath(sn *graph.Snapshot, src, dst uint64, typ string) []uint64 {
	if src == dst {
		return []uint64{src}
	}
	fwd := map[uint64]uint64{src: src} // node -> previous node on the way from src
	bwd := map[uint64]uint64{dst: dst} // node -> next node on the way to dst
	fwdFrontier, bwdFrontier := []uint64{src}, []uint64{dst}
	var buf []uint64
	for len(fwdFrontier) > 0 && len(bwdFrontier) > 0 {
		forward := len(fwdFrontier) <= len(bwdFrontier)
		frontier, mine, other := fwdFrontier, fwd, bwd
		if !forward {
			frontier, mine, other = bwdFrontier, bwd, fwd
		}
		var next []uint64
		for _, cur := range frontier {
			buf = sn.Neighbors(cur, forward, typ, buf[:0])
			for _, nb := range buf {
				if _, seen := mine[nb]; seen {
					continue
				}
				mine[nb] = cur
				if _, met := other[nb]; met {
					var path []uint64
					for n := nb; ; n = fwd[n] {
						path = append(path, n)
						if n == src {
							break
						}
					}
					for l, r := 0, len(path)-1; l < r; l, r = l+1, r-1 {
						path[l], path[r] = path[r], path[l]
					}
					for n := nb; n != dst; {
						n = bwd[n]
						path = append(path, n)
					}
					return path
				}
				next = append(next, nb)
			}
		}
		if forward {
			fwdFrontier = next
		} else {
			bwdFrontier = next
		}
	}
	return nil
}

// weightedShortestPath is Dijkstra's algorithm over outgoing edges, using a
// numeric edge property as the cost. Edges that lack the property (or hold a
// non-number) are not usable. A negative weight is an error, because
// Dijkstra's result is only correct when no edge has a negative cost.
func weightedShortestPath(sn *graph.Snapshot, src, dst uint64, typ, weight string) ([]uint64, float64, error) {
	dist := map[uint64]float64{src: 0}
	parent := map[uint64]uint64{}
	done := map[uint64]bool{}
	pq := &costQueue{{node: src, cost: 0}}
	for pq.Len() > 0 {
		cur := heap.Pop(pq).(costItem)
		if done[cur.node] {
			continue // an older, more expensive entry for a node already settled
		}
		done[cur.node] = true
		if cur.node == dst {
			path := []uint64{dst}
			for n := dst; n != src; {
				n = parent[n]
				path = append(path, n)
			}
			for l, r := 0, len(path)-1; l < r; l, r = l+1, r-1 {
				path[l], path[r] = path[r], path[l]
			}
			return path, cur.cost, nil
		}
		for _, ev := range sn.Edges(cur.node, true) {
			if typ != "" && ev.Type != typ {
				continue
			}
			w, ok := ev.Props[weight].(float64)
			if !ok {
				continue
			}
			if w < 0 {
				return nil, 0, fmt.Errorf("edge %d has negative %s (%v); weighted shortest path needs non-negative weights", ev.ID, weight, w)
			}
			next := cur.cost + w
			if old, seen := dist[ev.To]; !seen || next < old {
				dist[ev.To] = next
				parent[ev.To] = cur.node
				heap.Push(pq, costItem{node: ev.To, cost: next})
			}
		}
	}
	return nil, 0, nil
}

type costItem struct {
	node uint64
	cost float64
}

// costQueue is a min-heap of nodes ordered by the cheapest known cost.
type costQueue []costItem

func (q costQueue) Len() int           { return len(q) }
func (q costQueue) Less(i, j int) bool { return q[i].cost < q[j].cost }
func (q costQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *costQueue) Push(x any)        { *q = append(*q, x.(costItem)) }
func (q *costQueue) Pop() any {
	old := *q
	item := old[len(old)-1]
	*q = old[:len(old)-1]
	return item
}

// resultCache is a CLOCK (second-chance) cache with lock-free reads. Entries
// are tagged with the store version they were computed at, so any committed
// write invalidates everything implicitly.
//
// The first version was a mutex-guarded LRU. Moving an entry to the front on
// every hit needs an exclusive lock, so with 16 workers a cache hit was
// slower than an index seek. Here a hit is a sync.Map load plus at most one
// atomic flag write; only inserts take the mutex.
type resultCache struct {
	items sync.Map // query text -> *cacheEntry
	mu    sync.Mutex
	keys  []string // clock ring
	hand  int
	cap   int
}

type cacheEntry struct {
	ver  uint64
	res  *Result
	used atomic.Bool
}

func newResultCache(n int) *resultCache { return &resultCache{cap: n} }

func (c *resultCache) get(key string, ver uint64) (*Result, bool) {
	v, ok := c.items.Load(key)
	if !ok {
		return nil, false
	}
	en := v.(*cacheEntry)
	if en.ver != ver {
		return nil, false
	}
	if !en.used.Load() { // skip the write when already set: no cache-line bouncing
		en.used.Store(true)
	}
	return en.res, true
}

func (c *resultCache) put(key string, ver uint64, res *Result) {
	en := &cacheEntry{ver: ver, res: res}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items.Load(key); ok {
		c.items.Store(key, en) // key already has a slot in the ring
		return
	}
	if len(c.keys) < c.cap {
		c.keys = append(c.keys, key)
		c.items.Store(key, en)
		return
	}
	for { // evict the first entry not used since the hand last passed it
		victim := c.keys[c.hand]
		v, _ := c.items.Load(victim)
		if old := v.(*cacheEntry); old.used.Load() {
			old.used.Store(false)
			c.hand = (c.hand + 1) % c.cap
			continue
		}
		c.items.Delete(victim)
		c.keys[c.hand] = key
		c.hand = (c.hand + 1) % c.cap
		c.items.Store(key, en)
		return
	}
}
