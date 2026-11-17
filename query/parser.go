// Package query implements the Cypher-like language: lexer, parser, planner
// and executor.
package query

import (
	"fmt"
	"strconv"
	"strings"
)

type NodePat struct {
	Var, Label string
	Props      map[string]any
}

type EdgePat struct {
	Type  string
	Props map[string]any
	Dir   int // 1 = ->, -1 = <-, 0 = either
}

// Chain is (n0)-[e0]-(n1)-[e1]-(n2)...
type Chain struct {
	Nodes []NodePat
	Edges []EdgePat
}

type Cond struct {
	Var, Prop, Op string
	Val           any
}

type Ret struct{ Var, Prop string }

type SetItem struct {
	Var, Prop string
	Val       any
}

type Query struct {
	Kind     string // match | create | index | path | bfs
	Explain  bool
	Match    []Chain
	Where    []Cond
	Return   []Ret
	Count    bool
	Limit    int
	Sets     []SetItem
	Deletes  []string
	Create   []Chain
	From, To NodePat
	EdgeType string
	Weight   string // edge property used as the cost in SHORTEST PATH ... WEIGHT prop
	Depth    int
	IdxLabel string
	IdxProp  string
}

func (q *Query) ReadOnly() bool {
	if q.Kind == "path" || q.Kind == "bfs" {
		return true
	}
	return q.Kind == "match" && len(q.Sets) == 0 && len(q.Deletes) == 0 && len(q.Create) == 0
}

type tok struct {
	k byte // 'i' ident, 'n' number, 's' string, 'p' punctuation, 0 end
	s string
	n float64
}

func isAlpha(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func lex(in string) ([]tok, error) {
	var ts []tok
	i := 0
	for i < len(in) {
		c := in[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';':
			i++
		case isAlpha(c):
			j := i
			for j < len(in) && (isAlpha(in[j]) || isDigit(in[j])) {
				j++
			}
			ts = append(ts, tok{k: 'i', s: in[i:j]})
			i = j
		case isDigit(c):
			j := i
			for j < len(in) && isDigit(in[j]) {
				j++
			}
			if j+1 < len(in) && in[j] == '.' && isDigit(in[j+1]) {
				j++
				for j < len(in) && isDigit(in[j]) {
					j++
				}
			}
			n, _ := strconv.ParseFloat(in[i:j], 64)
			ts = append(ts, tok{k: 'n', n: n, s: in[i:j]})
			i = j
		case c == '\'' || c == '"':
			j := strings.IndexByte(in[i+1:], c)
			if j < 0 {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			ts = append(ts, tok{k: 's', s: in[i+1 : i+1+j]})
			i += j + 2
		default:
			if i+1 < len(in) {
				two := in[i : i+2]
				if two == "<=" || two == ">=" || two == "<>" {
					ts = append(ts, tok{k: 'p', s: two})
					i += 2
					continue
				}
			}
			if strings.IndexByte("()[]{}:,.-<>=*", c) < 0 {
				return nil, fmt.Errorf("unexpected character %q at %d", c, i)
			}
			ts = append(ts, tok{k: 'p', s: string(c)})
			i++
		}
	}
	return ts, nil
}

type parser struct {
	ts []tok
	i  int
}

func (p *parser) at(off int) tok {
	if p.i+off < len(p.ts) {
		return p.ts[p.i+off]
	}
	return tok{}
}
func (p *parser) isP(s string) bool { t := p.at(0); return t.k == 'p' && t.s == s }
func (p *parser) acceptP(s string) bool {
	if p.isP(s) {
		p.i++
		return true
	}
	return false
}
func (p *parser) expectP(s string) error {
	if !p.acceptP(s) {
		return fmt.Errorf("expected %q near token %d (%q)", s, p.i, p.at(0).s)
	}
	return nil
}
func (p *parser) kw(s string) bool {
	t := p.at(0)
	if t.k == 'i' && strings.EqualFold(t.s, s) {
		p.i++
		return true
	}
	return false
}
func (p *parser) ident() (string, error) {
	t := p.at(0)
	if t.k != 'i' {
		return "", fmt.Errorf("expected identifier near token %d (%q)", p.i, t.s)
	}
	p.i++
	return t.s, nil
}

func (p *parser) value() (any, error) {
	t := p.at(0)
	switch {
	case t.k == 's':
		p.i++
		return t.s, nil
	case t.k == 'n':
		p.i++
		return t.n, nil
	case t.k == 'p' && t.s == "-" && p.at(1).k == 'n':
		p.i += 2
		return -p.at(-1).n, nil
	case p.kw("true"):
		return true, nil
	case p.kw("false"):
		return false, nil
	}
	return nil, fmt.Errorf("expected a value near token %d (%q)", p.i, t.s)
}

// props parses key: value pairs; the opening brace is already consumed.
func (p *parser) props() (map[string]any, error) {
	m := map[string]any{}
	for !p.acceptP("}") {
		if len(m) > 0 {
			if err := p.expectP(","); err != nil {
				return nil, err
			}
		}
		k, err := p.ident()
		if err != nil {
			return nil, err
		}
		if err := p.expectP(":"); err != nil {
			return nil, err
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

func (p *parser) node() (NodePat, error) {
	var n NodePat
	var err error
	if err = p.expectP("("); err != nil {
		return n, err
	}
	if p.at(0).k == 'i' {
		n.Var, _ = p.ident()
	}
	if p.acceptP(":") {
		if n.Label, err = p.ident(); err != nil {
			return n, err
		}
	}
	if p.acceptP("{") {
		if n.Props, err = p.props(); err != nil {
			return n, err
		}
	}
	return n, p.expectP(")")
}

func (p *parser) chain() (Chain, error) {
	var c Chain
	n, err := p.node()
	if err != nil {
		return c, err
	}
	c.Nodes = append(c.Nodes, n)
	for {
		dir := 0
		if p.isP("-") {
			p.i++
			dir = 1
		} else if p.isP("<") && p.at(1).k == 'p' && p.at(1).s == "-" {
			p.i += 2
			dir = -1
		} else {
			return c, nil
		}
		var e EdgePat
		if p.acceptP("[") {
			if p.at(0).k == 'i' {
				p.i++ // edge variables are accepted but not bound
			}
			if p.acceptP(":") {
				if e.Type, err = p.ident(); err != nil {
					return c, err
				}
			}
			if p.acceptP("{") {
				if e.Props, err = p.props(); err != nil {
					return c, err
				}
			}
			if err = p.expectP("]"); err != nil {
				return c, err
			}
		}
		if err = p.expectP("-"); err != nil {
			return c, err
		}
		if dir == 1 && !p.acceptP(">") {
			dir = 0
		}
		e.Dir = dir
		if n, err = p.node(); err != nil {
			return c, err
		}
		c.Edges = append(c.Edges, e)
		c.Nodes = append(c.Nodes, n)
	}
}

func (p *parser) chains() ([]Chain, error) {
	var cs []Chain
	for {
		c, err := p.chain()
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
		if !p.acceptP(",") {
			return cs, nil
		}
	}
}

func Parse(text string) (*Query, error) {
	ts, err := lex(text)
	if err != nil {
		return nil, err
	}
	p := &parser{ts: ts}
	q := &Query{Depth: -1}
	q.Explain = p.kw("EXPLAIN")
	switch {
	case p.kw("CREATE"):
		if p.kw("INDEX") {
			q.Kind = "index"
			if !p.kw("ON") {
				return nil, fmt.Errorf("expected ON")
			}
			if err = p.expectP(":"); err != nil {
				return nil, err
			}
			if q.IdxLabel, err = p.ident(); err != nil {
				return nil, err
			}
			if err = p.expectP("("); err != nil {
				return nil, err
			}
			if q.IdxProp, err = p.ident(); err != nil {
				return nil, err
			}
			err = p.expectP(")")
		} else {
			q.Kind = "create"
			q.Create, err = p.chains()
		}
	case p.kw("MATCH"):
		q.Kind = "match"
		err = p.matchBody(q)
	case p.kw("SHORTEST"):
		q.Kind = "path"
		if !p.kw("PATH") || !p.kw("FROM") {
			return nil, fmt.Errorf("expected SHORTEST PATH FROM (..) TO (..) [VIA :TYPE] [WEIGHT prop]")
		}
		if q.From, err = p.node(); err != nil {
			return nil, err
		}
		if !p.kw("TO") {
			return nil, fmt.Errorf("expected TO")
		}
		if q.To, err = p.node(); err != nil {
			return nil, err
		}
		if err = p.via(q); err == nil && p.kw("WEIGHT") {
			q.Weight, err = p.ident()
		}
	case p.kw("BFS"):
		q.Kind = "bfs"
		if !p.kw("FROM") {
			return nil, fmt.Errorf("expected BFS FROM (..)")
		}
		if q.From, err = p.node(); err != nil {
			return nil, err
		}
		if err = p.via(q); err == nil && p.kw("DEPTH") {
			var v any
			if v, err = p.value(); err == nil {
				f, _ := v.(float64)
				q.Depth = int(f)
			}
		}
	default:
		return nil, fmt.Errorf("query must start with MATCH, CREATE, SHORTEST PATH or BFS")
	}
	if err != nil {
		return nil, err
	}
	if p.i != len(p.ts) {
		return nil, fmt.Errorf("unexpected %q at token %d", p.at(0).s, p.i)
	}
	return q, nil
}

func (p *parser) via(q *Query) error {
	if !p.kw("VIA") {
		return nil
	}
	if err := p.expectP(":"); err != nil {
		return err
	}
	var err error
	q.EdgeType, err = p.ident()
	return err
}

func (p *parser) matchBody(q *Query) error {
	var err error
	if q.Match, err = p.chains(); err != nil {
		return err
	}
	if p.kw("WHERE") {
		for {
			var c Cond
			if c.Var, err = p.ident(); err != nil {
				return err
			}
			if err = p.expectP("."); err != nil {
				return err
			}
			if c.Prop, err = p.ident(); err != nil {
				return err
			}
			t := p.at(0)
			if t.k != 'p' || !strings.Contains("= <> < > <= >=", t.s) {
				return fmt.Errorf("expected comparison operator, got %q", t.s)
			}
			p.i++
			c.Op = t.s
			if c.Val, err = p.value(); err != nil {
				return err
			}
			q.Where = append(q.Where, c)
			if !p.kw("AND") {
				break
			}
		}
	}
	switch {
	case p.kw("RETURN"):
		for {
			var r Ret
			if r.Var, err = p.ident(); err != nil {
				return err
			}
			if strings.EqualFold(r.Var, "count") && p.acceptP("(") {
				if err = p.expectP("*"); err != nil {
					return err
				}
				if err = p.expectP(")"); err != nil {
					return err
				}
				q.Count = true
			} else {
				if p.acceptP(".") {
					if r.Prop, err = p.ident(); err != nil {
						return err
					}
				}
				q.Return = append(q.Return, r)
			}
			if !p.acceptP(",") {
				break
			}
		}
		if p.kw("LIMIT") {
			v, err := p.value()
			if err != nil {
				return err
			}
			f, _ := v.(float64)
			q.Limit = int(f)
		}
	case p.kw("SET"):
		for {
			var s SetItem
			if s.Var, err = p.ident(); err != nil {
				return err
			}
			if err = p.expectP("."); err != nil {
				return err
			}
			if s.Prop, err = p.ident(); err != nil {
				return err
			}
			if err = p.expectP("="); err != nil {
				return err
			}
			if s.Val, err = p.value(); err != nil {
				return err
			}
			q.Sets = append(q.Sets, s)
			if !p.acceptP(",") {
				break
			}
		}
	case p.kw("DELETE"):
		for {
			v, err := p.ident()
			if err != nil {
				return err
			}
			q.Deletes = append(q.Deletes, v)
			if !p.acceptP(",") {
				break
			}
		}
	case p.kw("CREATE"):
		q.Create, err = p.chains()
		return err
	default:
		return fmt.Errorf("MATCH must be followed by RETURN, SET, DELETE or CREATE")
	}
	return nil
}
