package trie

import (
	"sort"
	"strings"
	"sync"
)

type Trie interface {
	String() string
	AbsPath() string
	Match(string) Trie
	AddSub(string) Trie
	AttachID(string)
	DropID(string)
	DropAllSubID(string)
	ExistID(string) bool
	IDs() []string
}

func New() Trie {
	return &trie{
		ids: make([]string, 0, 10),
	}
}

type trie struct {
	// the word between //
	sync.RWMutex
	ids      []string
	fragment string
	depth    uint
	parent   *trie
	// TODO:: pprof 显示map读取占据最多时间，每一级都有一次读操作，考虑是否使用slice降低每次读取时间，还是使用缓存，降低读取次数
	subTrie map[string]*trie
}

func (t *trie) String() string {
	res := t.string()
	if len(res) == 0 {
		return ""
	}
	sort.Strings(res)
	return strings.Join(res, "\n")
}

func (t *trie) string() []string {
	res := make([]string, 0, 10)
	if t.ids != nil && len(t.ids) != 0 {
		res = append(res, t.AbsPath())
	}
	for _, subT := range t.subTrie {
		if subT != nil {
			for _, s := range subT.string() {
				res = append(res, s)
			}
		}
	}
	return res
}

func (t *trie) AbsPath() string {
	if t.parent != nil {
		return t.parent.AbsPath() + "/" + t.fragment
	}
	return t.fragment
}

func (t *trie) ExistID(id string) (ok bool) {
	if t.ids == nil {
		return
	}
	t.RLock()
	for _, i := range t.ids {
		if i == id {
			ok = true
			break
		}
	}
	t.RUnlock()
	return
}

func (t *trie) AttachID(id string) {
	if t.ExistID(id) {
		return
	}
	t.Lock()
	if t.ids == nil {
		t.ids = make([]string, 0, 10)
	}
	t.ids = append(t.ids, id)
	t.Unlock()
}

func (t *trie) DropID(id string) {
	if t.ExistID(id) {
		t.Lock()
		var index int
		var v string
		for index, v = range t.ids {
			if v == id {
				break
			}
		}
		t.ids = append(t.ids[:index], t.ids[index+1:]...)
		t.Unlock()
	}
}

func (t *trie) DropAllSubID(id string) {
	t.DropID(id)
	for _, sub := range t.subTrie {
		sub.DropAllSubID(id)
	}
}

func (t *trie) IDs() []string {
	return t.ids
}

func (t *trie) AddSub(absPath string) Trie {
	if t.parent != nil {
		return t.parent.AddSub(absPath)
	}
	fragments := strings.Split(strings.TrimPrefix(absPath, "/"), "/")
	return t.add(fragments)
}

func (t *trie) add(fragments []string) Trie {
	if len(fragments) == 0 {
		return t
	}
	f := fragments[0]
	if t.subTrie == nil {
		t.subTrie = make(map[string]*trie)
	}
	if n := t.subTrie[f]; n != nil {
		return n.add(fragments[1:])
	}
	next := &trie{
		ids:      make([]string, 0, 10),
		fragment: f,
		depth:    t.depth + 1,
		parent:   t,
	}
	t.subTrie[f] = next
	return next.add(fragments[1:])
}

func (t *trie) Match(absPath string) Trie {
	if absPath == "" {
		return t
	}
	if absPath[0] == '/' {
		absPath = absPath[1:]
	}
	var res Trie
	for i, v := range absPath {
		if v == '/' {
			res = t.subMatch(absPath[:i])
			if res == nil {
				return nil
			}
			return res.Match(absPath[i:])
		}
	}
	return t.subMatch(absPath)
}

func (t *trie) subMatch(f string) Trie {
	if t.subTrie != nil && t.subTrie[f] != nil {
		return t.subTrie[f]
	}
	return nil
}
