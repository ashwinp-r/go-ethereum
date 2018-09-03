package trie

import (
)

type List struct {
	// sentinel.prev is the back of th list
	// sentinel.next is the front of the list
	sentinel shortNode
	len int
}

func NewList() *List {
	var l List
	l.sentinel.setnext(&l.sentinel)
	l.sentinel.setprev(&l.sentinel)
	l.len = 0
	return &l
}

func (l *List) Len() int {
	return l.len
}

func (l *List) PushToBack(n nodep) {
	if n.next() != nil || n.prev() != nil {
		panic("Invariant failed")
	}
	n.setprev(l.sentinel.prev())
	n.setnext(&l.sentinel)
	l.sentinel.prev().setnext(n)
	l.sentinel.setprev(n)
	l.len++
}

func (l *List) ShrinkTo(targetSize int) {
	for l.len > targetSize {
		next := l.sentinel.next()
		next2 := next.next()
		l.sentinel.setnext(next2)
		next2.setprev(&l.sentinel)
		// unlink the removed node
		next.setprev(nil)
		next.setnext(nil)
		l.len--
	}
}

func (l *List) Remove(n nodep) {
	next := n.next()
	prev := n.prev()
	next.setprev(prev)
	prev.setnext(next)
	n.setprev(nil)
	n.setnext(nil)
	l.len--
}

func (l *List) ForEach(f func(nodep)) {
	for p := l.sentinel.next(); p != &l.sentinel; p = p.next() {
		f(p)
	}
}
