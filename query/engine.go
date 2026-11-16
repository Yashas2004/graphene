package query

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
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
	cache *lru
}

func NewEngine(s *graph.Store) *Engine { return &Engine{S: s, cache: newLRU(1024)} }

func (e *Engine) Run(text string) (*Result, error) {
	start := time.Now()
	text = strings.TrimSpace(text)
	q, err := Parse(text)
	if err != nil {
		return nil, err
	}
	cacheable := q.ReadOnly() && !q.Explain
	ver := e.S.Version()
	if cacheable {
		if r, ok := e.cache.get(text, ver); ok {
			cp := *r
			cp.Cached = true
			cp.Micros = time.Since(start).Microseconds()
			return &cp, nil
		}
	}
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
	sn := e.S.Snapshot()
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
	case "path", "bfs":
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
		return e.write(q, ops, "DetachDelete", res)
	case len(q.Create) > 0:
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

// traverse runs BFS (and unweighted shortest path, which is BFS with a
// parent map) over outgoing edges of the snapshot.
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
	var dst uint64
	if q.Kind == "path" {
		res.Columns = []string{"path", "length"}
		var ok2 bool
		dst, ok2 = find(q.To)
		ok = ok && ok2
		res.Plan = append(res.Plan, "ShortestPath(BFS)")
	} else {
		res.Columns = []string{"node", "depth"}
		res.Plan = append(res.Plan, "BFS")
	}
	if !ok {
		return res, nil
	}
	parent := map[uint64]uint64{src: src}
	depth := map[uint64]int{src: 0}
	queue := []uint64{src}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if q.Kind == "bfs" {
			nv, _ := sn.Node(cur)
			res.Rows = append(res.Rows, []any{nv, depth[cur]})
			if q.Depth >= 0 && depth[cur] >= q.Depth {
				continue
			}
		} else if cur == dst {
			var path []any
			for n := dst; ; n = parent[n] {
				nv, _ := sn.Node(n)
				path = append([]any{nv}, path...)
				if n == src {
					break
				}
			}
			res.Rows = [][]any{{path, len(path) - 1}}
			return res, nil
		}
		for _, ev := range sn.Edges(cur, true) {
			if q.EdgeType != "" && ev.Type != q.EdgeType {
				continue
			}
			if _, seen := parent[ev.To]; !seen {
				parent[ev.To] = cur
				depth[ev.To] = depth[cur] + 1
				queue = append(queue, ev.To)
			}
		}
	}
	return res, nil
}

// lru is a result cache. Entries are tagged with the store version they were
// computed at, so any committed write invalidates everything implicitly.
type lru struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	items map[string]*list.Element
}

type lruEntry struct {
	key string
	ver uint64
	res *Result
}

func newLRU(n int) *lru { return &lru{cap: n, order: list.New(), items: map[string]*list.Element{}} }

func (c *lru) get(key string, ver uint64) (*Result, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok || el.Value.(*lruEntry).ver != ver {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*lruEntry).res, true
}

func (c *lru) put(key string, ver uint64, res *Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value = &lruEntry{key, ver, res}
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&lruEntry{key, ver, res})
	if c.order.Len() > c.cap {
		last := c.order.Back()
		c.order.Remove(last)
		delete(c.items, last.Value.(*lruEntry).key)
	}
}
