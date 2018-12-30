package avl

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"encoding/binary"
	"runtime"
	"os"
	"sort"
	"github.com/petar/GoLLRB/llrb"
	"runtime/debug"
)

// AVL+ organised into pages, with history

type Avl3 struct {
	currentVersion Version
	trace bool
	maxPageId PageID
	pageMap map[PageID][]byte // Pages by pageId
	maxValueId uint64
	valueMap map[uint64][]byte // Large values by valueId
	valueHashes map[uint64]Hash // Value hashes
	valueLens map[uint64]uint32 // Value lengths
	freelist []PageID // Free list of pages
	prevRoot Ref3 // Root of the previous version that is getting "peeled off" from the current version
	root Ref3 // Root of the current update buffer
	versions map[Version]Version3 // root pageId for a version
	modifiedPages map[PageID][]Ref3
	commitedCounter uint64
	pageSpace uint64
	pageFile, valueFile, verFile *os.File
	hashLength uint32
	compare func([]byte, []byte) int
}

type Version3 struct {
	pageId PageID
	treeIndex uint32
}

func NewAvl3() *Avl3 {
	t := &Avl3{
		pageMap: make(map[PageID][]byte),
		valueMap: make(map[uint64][]byte),
		valueHashes: make(map[uint64]Hash),
		valueLens: make(map[uint64]uint32),
		versions: make(map[Version]Version3),
		modifiedPages: make(map[PageID][]Ref3),
	}
	t.hashLength = 32
	t.compare = bytes.Compare
	return t
}

func (t *Avl3) SetCompare(c func([]byte, []byte) int) {
	t.compare = c
}

func (t *Avl3) walkToArrowPoint(r Ref3, key []byte, height uint32) (point Ref3, found bool) {
	if t.trace {
		fmt.Printf("walkToArrowPoint arrow: %s %d, key: %s %d\n", r.getmax(), r.getheight(), key, height)
	}
	current := r
	for {
		switch n := current.(type) {
		case nil:
			return nil, false
		case *Leaf3:
			if height != 1 || !bytes.Equal(key, n.key) {
				return nil, false
			}
			return n, true
		case *Fork3:
			if t.trace {
				fmt.Printf("walkToArrowPoint(Fork3) %s %d, key %s %d\n", n.max, n.height, key, height)
			}
			if n.height < height {
				return nil, false
			} else if n.height > height {
				switch t.compare(key, n.left.getmax()) {
				case -1, 0:
					current = n.left
				case 1:
					current = n.right
				}
			} else {
				if !bytes.Equal(key, n.max) {
					return nil, false
				}
				return n, true
			}
		case *Arrow3:
			panic(fmt.Errorf("Arrow3 P.%d with height %d max %s, expected height %d key %s", n.pageId, n.height, n.max, height, key))
		}
	}
}

func (t *Avl3) UseFiles(pageFileName, valueFileName, verFileName string, read bool) {
	var err error
	if read {
		t.pageFile, err = os.Open(pageFileName)
	} else {
		t.pageFile, err = os.OpenFile(pageFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		panic(err)
	}
	if read {
		t.valueFile, err = os.Open(valueFileName)
	} else {
		t.valueFile, err = os.OpenFile(valueFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		panic(err)
	}
	if read {
		t.verFile, err = os.Open(verFileName)
	} else {
		t.verFile, err = os.OpenFile(verFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		fmt.Printf("Could not open version file: %v\n", err)
		return
	}
	// Read versions
	var verdata [12]byte
	var lastVersion Version3
	for n, _ := t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)); n >0; n, _ = t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)) {
		lastVersion.pageId = PageID(binary.BigEndian.Uint64(verdata[:]))
		lastVersion.treeIndex = binary.BigEndian.Uint32(verdata[8:])
		t.versions[t.currentVersion] = lastVersion
		t.currentVersion++
	}
	if t.currentVersion > 0 {
		t.maxPageId = lastVersion.pageId
		fmt.Printf("Deserialising page %d\n", lastVersion.pageId)
		root := t.deserialisePage(lastVersion.pageId, nil, 0, lastVersion.treeIndex, false)
		prevRootArrow := &Arrow3{height: root.getheight(), max: root.getmax()}
		t.root = &Arrow3{pageId: lastVersion.pageId, height: root.getheight(), max: root.getmax(), arrow: prevRootArrow}
		t.prevRoot = prevRootArrow
	}
}

func (t *Avl3) Close() {
	if t.pageFile != nil {
		t.pageFile.Close()
	}
	if t.valueFile != nil {
		t.valueFile.Close()
	}
	if t.verFile != nil {
		t.verFile.Close()
	}
}

func (t *Avl3) CurrentVersion() Version {
	return t.currentVersion
}

func (t *Avl3) SetHashLength(hashLength uint32) {
	t.hashLength = hashLength
}

// Common fields for a tree node
type Node3 struct {
	arrow *Arrow3 // Pointer back to the arrow that points to this fork
	pinnedPageId PageID // 0 if node is not pinned to a page, otherwise the page Id where this node should stay
}

type Leaf3 struct {
	Node3
	key, value []byte
	valueId uint64
	valueLen uint32
}

type Fork3 struct {
	Node3
	height uint32 // Height of the entire subtree rooted at this node, including node itself
	left, right Ref3
	max []byte // Largest key in the subtree
}

type Arrow3 struct {
	arrow *Arrow3
	pageId PageID // Connection of this page to the page on the disk. Normally pageId corresponds to offset pageId*PageSize in the database file
	height uint32 // Height of the entire subtree rooted at this page
	max []byte
	parent *Fork3 // Fork that have this arrow as either left of right branch
	parentC int   // -1 if the parent has this arrow as left branch, 1 if the parent has this arrow as right branch
}

// Reference can be either a WbtNode3, or WbtArrow3. The latter is used when the leaves of one page reference another page
type Ref3 interface {
	getheight() uint32
	nkey() []byte
	getmax() []byte
	dot(*Avl3, *graphContext, string)
	heightsCorrect(path string) (uint32, bool)
	balanceCorrect() bool
	prepare(t *Avl3, buffer *CommitBuffer) Tree3
}

func (t *Avl3) nextPageId() PageID {
	if len(t.freelist) > 0 {
		nextId := t.freelist[len(t.freelist)-1]
		t.freelist = t.freelist[:len(t.freelist)-1]
		return nextId
	}
	t.maxPageId++
	return t.maxPageId
}

func (t *Avl3) freePageId(pageId PageID) {
	if data, ok := t.pageMap[pageId]; ok {
		t.pageSpace -= uint64(len(data))
		delete(t.pageMap, pageId)
		t.freelist = append(t.freelist, pageId)
	}
}

func (t *Avl3) nextValueId() uint64 {
	return t.maxValueId + 1
}

func (t *Avl3) freeValueId(valueId uint64) {
	if valueId == 0 {
		return
	}
	delete(t.valueMap, valueId)
	delete(t.valueHashes, valueId)
	delete(t.valueLens, valueId)
}

func (t *Avl3) addValue(valueId uint64, value []byte) {
	if t.valueFile == nil {
		t.valueMap[valueId] = value
		t.valueLens[valueId] = uint32(len(value))
		t.maxValueId++
	} else {
		if _, err := t.valueFile.WriteAt(value, int64(valueId)-1); err != nil {
			panic(err)
		}
		t.maxValueId += uint64(len(value))
	}
}

func (t *Avl3) SetTracing(tracing bool) {
	t.trace = tracing
}

func (l *Leaf3) getheight() uint32 {
	return 1
}

func (f *Fork3) getheight() uint32 {
	return f.height
}

func (a *Arrow3) getheight() uint32 {
	return a.height
}

func (l *Leaf3) nkey() []byte {
	return l.key
}

func (f *Fork3) nkey() []byte {
	return f.max
}

func (a *Arrow3) nkey() []byte {
	return a.max
}

func (l *Leaf3) getmax() []byte {
	return l.key
}

func (f *Fork3) getmax() []byte {
	return f.max
}

func (a *Arrow3) getmax() []byte {
	return a.max
}

func (l *Leaf3) nvalue(t *Avl3) []byte {
	if l.valueId == 0 {
		return l.value
	} else if t.valueFile == nil {
		return t.valueMap[l.valueId]
	} else {
		val := make([]byte, l.valueLen)
		if _, err := t.valueFile.ReadAt(val, int64(l.valueId)-1); err != nil {
			panic(err)
		}
		return val
	}
}

func (t *Avl3) Get(key []byte) ([]byte, bool) {
	trace := t.trace
	var current Ref3 = t.root
	for {
		switch n := current.(type) {
		case nil:
			return nil, false
		case *Leaf3:
			if trace {
				fmt.Printf("Get %s on leaf %s\n", key, n.key)
			}
			if bytes.Equal(key, n.key) {
				return n.nvalue(t), true
			}
			return nil, false
		case *Fork3:
			if trace {
				fmt.Printf("Get %s on fork %s %d\n", key, n.max, n.height)
			}
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				if trace {
					fmt.Printf("Go left\n")
				}	
				current = n.left
			case 1:
				if trace {
					fmt.Printf("Go right\n")
				}
				current = n.right
			}
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic("")
			}
		}
	}
}

func (t *Avl3) IsLeaf(r Ref3) bool {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow3:
			current = t.deserialisePage(r.pageId, r.max, r.height, 0, false)
			if current == nil {
				panic("")
			}
		case *Fork3:
			return false
		case *Leaf3:
			return true
		}
	}
	panic("")
}

func (t *Avl3) Peel(r Ref3, key []byte, ins int) Ref3 {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow3:
			point := t.deserialisePage(r.pageId, r.max, r.height, 0, true)
			if point == nil {
				panic("")
			}
			if r.arrow != nil {
				if t.trace {
					fmt.Printf("Moving arrow P.%d[%s %d] over the arrow P.%d[%s %d]\n", r.arrow.pageId, r.arrow.max, r.arrow.height, r.pageId, r.max, r.height)
				}
				r.arrow.pageId = r.pageId
				switch n := point.(type) {
				case *Leaf3:
					n.arrow = r.arrow
				case *Fork3:
					n.arrow = r.arrow
				case *Arrow3:
					n.arrow = r.arrow
				}
				r.arrow = nil
			}
			current = point
		case *Fork3:
			return r
		case *Leaf3:
			return r
		}
	}
	panic("")
}

func (t *Avl3) moveArrowOverFork(a *Arrow3, f *Fork3) {
	if t.trace {
		fmt.Printf("Moving arrow P.%d[%s %d] over the fork %s\n", a.pageId, a.max, a.height, f.nkey())
	}
	var lArrow *Arrow3 = &Arrow3{pageId: a.pageId, parentC: -1, height: f.left.getheight(), max: f.left.getmax()}
	var rArrow *Arrow3 = &Arrow3{pageId: a.pageId, parentC: 1, height: f.right.getheight(), max: f.right.getmax()}
	if t.trace {
		fmt.Printf("Left arrow P.%d[%s %d], right arrow P.%d[%s %d]\n", lArrow.pageId, lArrow.max, lArrow.height, rArrow.pageId, rArrow.max, rArrow.height)
	}
	fork := &Fork3{max: f.max, height: f.height, left: lArrow, right: rArrow}
	lArrow.parent = fork
	rArrow.parent = fork
	f.arrow = nil
	switch n := f.left.(type) {
	case *Fork3:
		n.arrow = lArrow
	case *Leaf3:
		n.arrow = lArrow
	case *Arrow3:
		n.arrow = lArrow
		lArrow.pageId = n.pageId
	}
	switch n := f.right.(type) {
	case *Fork3:
		n.arrow = rArrow
	case *Leaf3:
		n.arrow = rArrow
	case *Arrow3:
		n.arrow = rArrow
		rArrow.pageId = n.pageId
	}
	if a.parent == nil {
		t.prevRoot = fork
	} else {
		switch a.parentC {
		case -1:
			a.parent.left = fork
		case 1:
			a.parent.right = fork
		}
	}
	// When the pinned node gets modified, the arrow pointing to it moves over the pinned node.
	// That causes the pinned node to get unpinned in the current state, but pinned in the previous
	// state.
	if f.pinnedPageId != PageID(0) {
		fork.pinnedPageId = f.pinnedPageId
		if f.pinnedPageId == 1645 {
			fmt.Printf("fork repinned %d\n", f.pinnedPageId)
		}
		f.pinnedPageId = PageID(0)
	}
}

func (t *Avl3) moveArrowOverLeaf(a *Arrow3, l *Leaf3) {
	if t.trace {
		fmt.Printf("Moving arrow P.%d[%s %d] over the leaf %s\n", a.pageId, a.max, a.height, l.nkey())
	}
	leaf := &Leaf3{key: l.key, value: l.nvalue(t)}
	leaf.valueLen = uint32(len(leaf.value))
	if a.parent == nil {
		t.prevRoot = leaf
	} else {
		switch a.parentC {
		case -1:
			a.parent.left = leaf
		case 1:
			a.parent.right = leaf
		}		
	}
	l.arrow = nil
}

func (t *Avl3) Insert(key, value []byte) bool {
	inserted := true
	var current Ref3 = t.root
	loop: for {
		switch n := current.(type) {
		case nil:
			break loop
		case *Leaf3:
			if bytes.Equal(key, n.key) {
				if bytes.Equal(value, n.nvalue(t)) {
					return false
				} else {
					inserted = false
				}
			}
			break loop
		case *Fork3:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic("")
			}
		}
	}
	t.root = t.insert(t.root, key, value)
	return inserted
}

func (t *Avl3) insert(current Ref3, key, value []byte) Ref3 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		if trace {
			fmt.Printf("Inserting %s, on nil\n", key)
		}
		return &Leaf3{key: key, value: value, valueLen: uint32(len(value))}
	case *Arrow3:
		return t.insert(t.Peel(n, key, 1), key, value)
	case *Leaf3:
		if trace {
			fmt.Printf("Inserting %s, on Leaf %s\n", key, n.key)
		}
		var newnode *Fork3
		switch t.compare(key, n.key) {
		case 0:
			if n.arrow != nil {
				t.moveArrowOverLeaf(n.arrow, n)
			}
			n.value = value
			t.freeValueId(n.valueId)
			n.valueId = 0
			n.valueLen = uint32(len(value))
			return n
		case -1:
			newnode = &Fork3{max: n.key, height: 2, left: &Leaf3{key: key, value: value, valueLen: uint32(len(value))}, right: n}
		case 1:
			newnode = &Fork3{max: key, height: 2, left: n, right: &Leaf3{key: key, value: value, valueLen: uint32(len(value))}}
		}
		return newnode
	case *Fork3:
		c := t.compare(key, n.left.getmax())
		if trace {
			fmt.Printf("Inserting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		if n.arrow != nil {
			t.moveArrowOverFork(n.arrow, n)
		}
		// Prepare it for the next iteraion
		switch c {
		case 0, -1:
			n.left = t.insert(n.left, key, value)
		case 1:
			n.right = t.insert(n.right, key, value)
			n.max = n.right.getmax()
		}
		lHeight := n.left.getheight()
		rHeight := n.right.getheight()
		n.height = 1 + maxu32(lHeight, rHeight)
		if rHeight > lHeight {
			if rHeight - lHeight > 1 {
				// nr is a Fork, because its height is at least 3
				nr := t.Peel(n.right, key, 2).(*Fork3)
				if nr.arrow != nil {
					t.moveArrowOverFork(nr.arrow, nr)
				}
				if nr.right.getheight() >= nr.left.getheight() {
					if trace {
						fmt.Printf("Single rotation from right to left, n %s %d, nr %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight())
					}
					n.right = nr.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nr.left.getmax()
					nr.left = n
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					if trace {
						fmt.Printf("n %s %d, nr %s %d, nrl %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight(), nr.left.nkey(), nr.left.getheight())
					}
					return nr
				} else {
					if trace {
						fmt.Printf("Double rotation from right to left, n %s %d, nr %s %d, nrl %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight(), nr.left.nkey(), nr.left.getheight())
					}
					// height of nrl is more than height of nrr. nr has height of at least 3, therefore nrl has a height of at least 2
					// nrl is a Fork
					nrl := t.Peel(nr.left, key, 3).(*Fork3)
					if nrl.arrow != nil {
						t.moveArrowOverFork(nrl.arrow, nrl)
					}
					n.right = nrl.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nrl.left.getmax()
					nrl.left = n
					nr.left = nrl.right
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					nrl.right = nr
					nrl.height = 1 + maxu32(nrl.left.getheight(), nrl.right.getheight())
					nrl.max = nr.max
					return nrl
				}
			} else {
				return n
			}
		} else if lHeight - rHeight > 1 {
			nl := t.Peel(n.left, key, 4).(*Fork3)
			if nl.arrow != nil {
				t.moveArrowOverFork(nl.arrow, nl)
			}
			if nl.left.getheight() >= nl.right.getheight() {
				if trace {
					fmt.Printf("Single rotation from left to right, n %s %d, nl %s %d\n",
						n.nkey(), n.getheight(), nl.nkey(), nl.getheight())
				}
				n.left = nl.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nl.right = n
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = n.max
				if t.compare(key, nl.max) == 1 {
					nl.max = key
				}
				return nl
			} else {
				if trace {
					fmt.Printf("Double rotation from left to right, n %s %d, nl %s %d, nlr %s %d\n",
						n.nkey(), n.getheight(), nl.nkey(), nl.getheight(), nl.right.nkey(), nl.right.getheight())
				}
				nlr := t.Peel(nl.right, key, 5).(*Fork3)
				if nlr.arrow != nil {
					t.moveArrowOverFork(nlr.arrow, nlr)
				}
				n.left = nlr.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nlr.right = n
				nlr.max = n.max
				nl.right = nlr.left
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = nlr.left.getmax()
				nlr.left = nl
				nlr.height = 1 + maxu32(nlr.left.getheight(), nlr.right.getheight())
				return nlr
			}
		} else {
			return n
		}
	}
	panic("")
}

func (t *Avl3) Delete(key []byte) bool {
	var current Ref3 = t.root
	loop: for {
		switch n := current.(type) {
		case nil:
			return false
		case *Leaf3:
			if bytes.Equal(key, n.key) {
				break loop
			} else {
				return false
			}
		case *Fork3:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic("")
			}
			if current.getheight() != n.height {
				panic(fmt.Sprintf("deseailised size %d, arrow height %d", current.getheight(), n.height))
			}
		}
	}
	t.root = t.delete(t.root, key)
	return true
}

func (t *Avl3) delete(current Ref3, key []byte) Ref3 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		panic("nil")
	case *Arrow3:
		return t.delete(t.Peel(n, key, 6), key)
	case *Leaf3:
		// Assuming that key is equal to n.key
		if trace {
			fmt.Printf("Deleting on leaf %s\n", n.nkey())
		}
		if n.arrow == nil {
			// Don't release the value because it is still used by the previous version
			t.freeValueId(n.valueId)
		}
		return nil
	case *Fork3:
		c := t.compare(key, n.left.getmax())
		if n.arrow != nil {
			t.moveArrowOverFork(n.arrow, n)
		}
		// Special cases when both right and left are leaves
		if t.IsLeaf(n.left) && t.IsLeaf(n.right) {
			nl := t.Peel(n.left, key, 7).(*Leaf3)
			nr := t.Peel(n.right, key, 8).(*Leaf3)
			if trace {
				fmt.Printf("Special case: Leaf, Leaf\n")
			}
			switch c {
			case 0, -1:
				if nl.arrow == nil {
					t.freeValueId(nl.valueId)
				}
				return nr
			case 1:
				if nr.arrow == nil {
					t.freeValueId(nr.valueId)
				}
				return nl
			}
			panic("")
		}
		if trace {
			fmt.Printf("Deleting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		switch c {
		case 0, -1:
			n.left = t.delete(n.left, key)
			if n.left == nil {
				return n.right
			}
		case 1:
			n.right = t.delete(n.right, key)
			if n.right == nil {
				return n.left
			}
			n.max = n.right.getmax()
		}
		lHeight := n.left.getheight()
		rHeight := n.right.getheight()
		n.height = 1 + maxu32(lHeight, rHeight)
		if rHeight > lHeight {
			if rHeight - lHeight > 1 {
				// nr is a Fork, because its height is at least 3
				nr := t.Peel(n.right, key, 9).(*Fork3)
				if nr.arrow != nil {
					t.moveArrowOverFork(nr.arrow, nr)
				}
				if nr.right.getheight() >= nr.left.getheight() {
					if trace {
						fmt.Printf("Single rotation from right to left, n %s, nr %s\n",
							n.nkey(), nr.nkey())
					}
					n.right = nr.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nr.left.getmax()
					nr.left = n
					nr.height = 1 + maxu32(nr.left.getheight(), n.right.getheight())
					return nr
				} else {
					if trace {
						fmt.Printf("Double rotation from right to left, n %s, nr %s\n",
							n.nkey(), nr.nkey())
					}
					// height of nrl is more than height of nrr. nr has height of at least 3, therefore nrl has a height of at least 2
					// nrl is a Fork
					nrl := t.Peel(nr.left, key, 10).(*Fork3)
					if nrl.arrow != nil {
						t.moveArrowOverFork(nrl.arrow, nrl)
					}
					n.right = nrl.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nrl.left.getmax()
					nrl.left = n
					nr.left = nrl.right
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					nrl.right = nr
					nrl.height = 1 + maxu32(nrl.left.getheight(), nrl.right.getheight())
					nrl.max = nr.max
					return nrl
				}
			} else {
				return n
			}
		} else if lHeight - rHeight > 1 {
			nl := t.Peel(n.left, key, 11).(*Fork3)
			if nl.arrow != nil {
				t.moveArrowOverFork(nl.arrow, nl)
			}
			if nl.left.getheight() >= nl.right.getheight() {
				if trace {
					fmt.Printf("Single rotation from left to right, n %s, nl %s\n",
						n.nkey(), nl.nkey())
				}
				n.left = nl.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nl.right = n
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = n.max
				return nl
			} else {
				if trace {
					fmt.Printf("Double rotation from left to right, n %s, nl %s, nlr %s\n",
						n.nkey(), nl.nkey(), nl.right.nkey())
				}
				nlr := t.Peel(nl.right, key, 12).(*Fork3)
				if nlr.arrow != nil {
					t.moveArrowOverFork(nlr.arrow, nlr)
				}
				n.left = nlr.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nlr.right = n
				nlr.max = n.max
				nl.right = nlr.left
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = nlr.left.getmax()
				nlr.left = nl
				nlr.height = 1 + maxu32(nlr.left.getheight(), nlr.right.getheight())
				return nlr
			}
		} else {
			return n
		}
	}
	panic("")
}

func (t *Avl3) Dot() *graphContext {
	fmt.Printf("Dotting the root\n")
	ctx := &graphContext{}
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: "root0",
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel("root", 16, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)
	t.root.dot(t, ctx, "root")
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: "root0",
		To: "root",
	})
	if t.prevRoot != nil {
		fmt.Printf("Dotting the prevRoot\n")
		gn := &graphNode{
			Attrs: map[string]string{},
			Path: "prevRoot0",
		}
		for k, v := range defaultGraphNodeAttrs {
			gn.Attrs[k] = v
		}
		gn.Label = mkLabel("prev", 16, "sans-serif")
		ctx.Nodes = append(ctx.Nodes, gn)
		t.prevRoot.dot(t, ctx, "prevRoot")
		ctx.Edges = append(ctx.Edges, &graphEdge{
			From: "prevRoot0",
			To: "prevRoot",
		})
	}
	return ctx
}

func (a *Arrow3) dot(t *Avl3, ctx *graphContext, path string) {
	fmt.Printf("Dotting the arrow P.%d max %s, height %d\n", a.pageId, a.max, a.height)
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(fmt.Sprintf("P.%d", a.pageId), 16, "sans-serif")
	gn.Label += mkLabel(string(a.max), 16, "sans-serif")
	gn.Label += mkLabel(fmt.Sprintf("%d", a.height), 10, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)

	if a.pageId != PageID(0) {
		point := t.deserialisePage(a.pageId, a.max, a.height, 0, false)
		if point == nil {
			panic("")
		} else {
			pointPath := fmt.Sprintf("%p", point)
			if point != nil {
				point.dot(t, ctx, pointPath)
			}
			ctx.Edges = append(ctx.Edges, &graphEdge{
				From: path,
				To: pointPath,
			})
		}
	}
}

func (l *Leaf3) dot(t *Avl3, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(l.nkey()), 16, "sans-serif")
	gn.Label += mkLabel(string(l.nvalue(t)), 10, "sans-serif")
	if l.pinnedPageId != PageID(0) {
		gn.Label += mkLabel(fmt.Sprintf("* %d", l.pinnedPageId), 10, "sans-serif")
	} else if l.arrow != nil {
		gn.Label += mkLabel("*", 10, "sans-serif")
	}
	ctx.Nodes = append(ctx.Nodes, gn)
}

func (f *Fork3) dot(t *Avl3, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(f.max), 16, "sans-serif")
	gn.Label += mkLabel(fmt.Sprintf("%d", f.height), 10, "sans-serif")
	if f.pinnedPageId != PageID(0) {
		gn.Label += mkLabel(fmt.Sprintf("* %d", f.pinnedPageId), 10, "sans-serif")
	} else if f.arrow != nil {
		gn.Label += mkLabel("*", 10, "sans-serif")
	}
	ctx.Nodes = append(ctx.Nodes, gn)
	leftPath := fmt.Sprintf("%p", f.left)
	f.left.dot(t, ctx, leftPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: leftPath,
	})
	rightPath := fmt.Sprintf("%p", f.right)
	f.right.dot(t, ctx, rightPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: rightPath,
	})
}

func (l *Leaf3) heightsCorrect(path string) (uint32, bool) {
	return 1, true
}

func (f *Fork3) heightsCorrect(path string) (uint32, bool) {
	leftHeight, leftCorrect := f.left.heightsCorrect(path + "l")
	rightHeight, rightCorrect := f.right.heightsCorrect(path + "r")
	height, correct := 1+maxu32(leftHeight, rightHeight), leftCorrect&&rightCorrect&&(1+maxu32(leftHeight, rightHeight) == f.height)
	if !correct {
		fmt.Printf("At path %s, key %s, expected %d, got %d\n", path, f.max, height, f.height)
	}
	return height, correct
}

func (l *Leaf3) balanceCorrect() bool {
	return true
}

func (f *Fork3) balanceCorrect() bool {
	lHeight := f.left.getheight()
	rHeight := f.right.getheight()
	var balanced bool
	if rHeight >= lHeight {
		balanced = (rHeight - lHeight) < 2
	} else {
		balanced = (lHeight - rHeight) < 2
	}
	return balanced && f.left.balanceCorrect() && f.right.balanceCorrect()
}

func (a *Arrow3) heightsCorrect(path string) (uint32, bool) {
	return a.height, true
}

func (a *Arrow3) balanceCorrect() bool {
	return true
}

func (t *Avl3) pageSize3(leafCount, arrowCount, keyBodySize, valBodySize, structBits uint32, prefix []byte) uint32 {
	nodeCount := leafCount + arrowCount
	prefixLen := uint32(len(prefix))

	return 4 /* nodeCount */ +
		4*((nodeCount+31)/32) /* pageBits */ +
		4 /* prefixOffset */ +
		4*nodeCount + 4 /* key header */ +
		(12+t.hashLength)*arrowCount /* arrow header */ +
		4*leafCount /* value header */ +
		4*((structBits+31)/32) /* structBits */ +
		prefixLen +
		keyBodySize - nodeCount*prefixLen /* Discount bodySize using prefixLen */ +
		valBodySize
}

type Forest3 struct {
	refs []Ref3
	arrows []*Arrow3
	prefix []byte
	leafCount uint32
	arrowCount uint32
	keyBodySize, valBodySize, structBits uint32
}

type Tree3 struct {
	ref Ref3
	arrow *Arrow3
	prefix []byte
	leafCount uint32
	arrowCount uint32
	keyBodySize, valBodySize, structBits uint32
	pageId PageID
	pinned bool
}

func (tree Tree3) ToForest() Forest3 {
	var arrows []*Arrow3
	if tree.arrow != nil {
		arrows = append(arrows, tree.arrow)
	}
	return Forest3 {
		refs: []Ref3{tree.ref},
		arrows: arrows,
		prefix: tree.prefix,
		leafCount: tree.leafCount,
		arrowCount: tree.arrowCount,
		keyBodySize: tree.keyBodySize,
		valBodySize: tree.valBodySize,
		structBits: tree.structBits,
	}
}

// Structure holding pages to be committed. These can be rearranged further
type CommitBuffer struct {
	pinned map[PageID]Forest3 // Forests pinned to specific page number
	distinct []Tree3 // Forests that need to be kept on different pages from each other
	nextDistinctId PageID
	free []Tree3 // Unrestricted trees
}

type PageContainer struct {
	f Forest3
	tieBreaker int
	pageId    PageID
	space uint32 // Space left in the container
}

func (f Forest3) Len() int {
	return len(f.refs)
}

func (f Forest3) Less(i, j int) bool {
	return bytes.Compare(f.refs[i].getmax(), f.refs[i].getmax()) < 0
}

func (f Forest3) Swap(i, j int) {
	f.refs[i], f.refs[j] = f.refs[j], f.refs[i]
}

func (a *PageContainer) Less(b llrb.Item) bool {
	bi := b.(*PageContainer)
	if a.space == bi.space {
		return a.tieBreaker < bi.tieBreaker
	}
	return a.space < bi.space
}

type ForestItem struct {
	f Forest3
	Size uint32 // Size of the item
}

func (a *ForestItem) Less(b llrb.Item) bool {
	bi := b.(*ForestItem)
	return a.Size < bi.Size
}

func (buffer *CommitBuffer) AddPinnedForest(pageId PageID, newForest Forest3) {
	oldForest, ok := buffer.pinned[pageId]
	if ok {
		buffer.pinned[pageId] = Forest3 {
			refs: append(oldForest.refs, newForest.refs...),
			arrows: append(oldForest.arrows, newForest.arrows...),
			prefix: commonPrefix(oldForest.prefix, newForest.prefix),
			leafCount: oldForest.leafCount + newForest.leafCount,
			arrowCount: oldForest.arrowCount + newForest.arrowCount,
			keyBodySize: oldForest.keyBodySize + newForest.keyBodySize,
			valBodySize: oldForest.valBodySize + newForest.valBodySize,
			structBits: oldForest.structBits + newForest.structBits + 1,
		}
	} else {
		buffer.pinned[pageId] = newForest
	}
}

func (buffer *CommitBuffer) Pack(t *Avl3) []*PageContainer {
	pageContainers := llrb.New()
	nextTieBreaker := 0
	// Create containers for pinned forests
	for id, forest := range buffer.pinned {
		container := &PageContainer{
			f: forest,
			tieBreaker: nextTieBreaker,
			pageId: id,
			space: PageSize - t.pageSize3(forest.leafCount, forest.arrowCount, forest.keyBodySize, forest.valBodySize, forest.structBits, forest.prefix),
		}
		if id == PageID(1645) {
			fmt.Printf("Container %d, refs %d, Stack %s\n", id, len(container.f.refs), debug.Stack())
		}
		nextTieBreaker++
		pageContainers.InsertNoReplace(container)
	}
	// Create containers for distinct forests
	for _, tree := range buffer.distinct {
		container := &PageContainer{
			f: tree.ToForest(),
			tieBreaker: nextTieBreaker,
			space: PageSize - t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix),
		}
		nextTieBreaker++
		pageContainers.InsertNoReplace(container)
	}
	items := llrb.New()
	for _, tree := range buffer.free {
		item := &ForestItem{
			f: tree.ToForest(),
			Size: t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix),
		}
		items.InsertNoReplace(item)
	}
	// Best Fit Desceasing bin packing algorithm
	items.DescendLessOrEqual(items.Max(), func(x llrb.Item) bool {
		i := x.(*ForestItem)
		// Choose the smallest container that would fit this item
		var oldContainer *PageContainer
		var newContainer *PageContainer
		pageContainers.AscendGreaterOrEqual(&PageContainer{space: i.Size - 12}, func(y llrb.Item) bool {
			c := y.(*PageContainer)
			cLeafCount := i.f.leafCount + c.f.leafCount
			cArrowCount := i.f.arrowCount + c.f.arrowCount
			cKeyBodySize := i.f.keyBodySize + c.f.keyBodySize
			cValBodySize := i.f.valBodySize + c.f.valBodySize
			cStructBits := i.f.structBits + c.f.structBits + 1 /* extra structBit for separator */
			cPrefix := commonPrefix(i.f.prefix, c.f.prefix)
			cSize := t.pageSize3(cLeafCount, cArrowCount, cKeyBodySize, cValBodySize, cStructBits, cPrefix)
			if cSize <= PageSize {
				oldContainer = c
				newContainer = &PageContainer{
					f: Forest3 {
						refs: append(i.f.refs, c.f.refs...),
						arrows: append(i.f.arrows, c.f.arrows...),
						prefix: cPrefix,
						leafCount: cLeafCount,
						arrowCount: cArrowCount,
						keyBodySize: cKeyBodySize,
						valBodySize: cValBodySize,
						structBits: cStructBits,
					},
					tieBreaker: nextTieBreaker,
					space: PageSize - cSize,
				}
				nextTieBreaker++
				return false
			}
			return true
		})
		if oldContainer != nil {
			pageContainers.Delete(oldContainer)
		}
		if newContainer == nil {
			// No appropriate container found, create a new one
			newContainer = &PageContainer{
				f: Forest3{
					refs: i.f.refs,
					arrows: i.f.arrows,
					prefix: i.f.prefix,
					leafCount: i.f.leafCount,
					arrowCount: i.f.arrowCount,
					keyBodySize: i.f.keyBodySize,
					valBodySize: i.f.valBodySize,
					structBits: i.f.structBits,
				},
				tieBreaker: nextTieBreaker,
				space: PageSize - t.pageSize3(i.f.leafCount, i.f.arrowCount, i.f.keyBodySize, i.f.valBodySize, i.f.structBits, i.f.prefix),
			}
			nextTieBreaker++
		}
		pageContainers.InsertNoReplace(newContainer)
		return true
	})
	// Sort forests in the containers
	containers := make([]*PageContainer, pageContainers.Len())
	i := 0
	pageContainers.DescendLessOrEqual(pageContainers.Max(), func(y llrb.Item) bool {
		c := y.(*PageContainer)
		sort.Sort(c.f)
		containers[i] = c
		i++
		return true
	})
	return containers
}

type PageIDs []PageID

func (p PageIDs) Len() int {
	return len(p)
}
func (p PageIDs) Less(i, j int) bool {
	return p[i] < p[j]
}
func (p PageIDs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Split current buffer into pages and commit them
func (t *Avl3) Commit() uint64 {
	if t.root == nil {
		return 0
	}
	trace := t.trace
	startCounter := t.commitedCounter
	var buffer CommitBuffer
	buffer.pinned = make(map[PageID]Forest3)
	if trace {
		fmt.Printf("len(t.modifiedPages) = %d\n", len(t.modifiedPages))
	}
	pageIds := make(PageIDs, len(t.modifiedPages))
	idx := 0
	for pageId := range t.modifiedPages {
		pageIds[idx] = pageId
		idx++
	}
	sort.Sort(pageIds)
	for _, pageId := range pageIds {
		modRefs := t.modifiedPages[pageId]
		if pageId == 1645 {
			fmt.Printf("pageId = %d, len(modRefs) = %d\n", pageId, len(modRefs))
		}
		var cPrefix []byte
		var cLeafCount, cArrowCount, cKeyBodySize, cValBodySize, cStructBits uint32
		for i, r := range modRefs {
			tree := r.prepare(t, &buffer)
			cPrefix = commonPrefix(tree.prefix, cPrefix)
			cLeafCount += tree.leafCount
			cArrowCount += tree.arrowCount
			cKeyBodySize += tree.keyBodySize
			cValBodySize += tree.valBodySize
			cStructBits += tree.structBits
			if i > 0 {
				cStructBits++
			}
		}
		if len(modRefs) > 0 {
			buffer.pinned[pageId] = Forest3{
				refs: modRefs,
				arrows: []*Arrow3{},
				prefix: cPrefix,
				leafCount: cLeafCount,
				arrowCount: cArrowCount,
				keyBodySize: cKeyBodySize,
				valBodySize: cValBodySize,
				structBits: cStructBits,
			}
		}
	}
	t.modifiedPages = make(map[PageID][]Ref3)
	tree := t.root.prepare(t, &buffer)
	tree.ref = t.root
	if tree.pinned {
		buffer.AddPinnedForest(tree.pageId, tree.ToForest())
	} else if tree.pageId == 0 {
		buffer.free = append(buffer.free, tree)
	} else {
		buffer.distinct = append(buffer.distinct, tree)
	}
	containers := buffer.Pack(t)
	var rootPageId PageID
	var rootTree uint32
	// Assign page ids to to the containers
	for _, c := range containers {
		if c.pageId == PageID(0) {
			c.pageId = t.nextPageId()
		}
		for _, a := range c.f.arrows {
			a.pageId = c.pageId
		}
		if rootPageId == PageID(0) {
			for idx, r := range c.f.refs {
				if t.root == r {
					rootPageId = c.pageId
					rootTree = uint32(idx)
					break
				}
			}
		}
	}
	// Commit
	for _, c := range containers {
		t.commitPage(c)
	}

	//currentId := t.commitPage(t.root, &buffer, prefix, keyCount, pageCount, keyBodySize, valBodySize, structBits, pinnedPageId)
	if t.prevRoot != nil {
		var buffer CommitBuffer
		buffer.pinned = make(map[PageID]Forest3)
		tree := t.prevRoot.prepare(t, &buffer)
		tree.ref = t.prevRoot
		if tree.pinned {
			if tree.pageId == PageID(1645) {
				fmt.Printf("t.prevRoot pinned to %d\n", tree.pageId)
			}
			buffer.AddPinnedForest(tree.pageId, tree.ToForest())
		} else if tree.pageId == 0 {
			buffer.free = append(buffer.free, tree)
		} else {
			buffer.distinct = append(buffer.distinct, tree)
		}
		containers := buffer.Pack(t)
		var prevRootPageId PageID
		var prevRootTree uint32
		// Assign page ids to to the containers
		for _, c := range containers {
			if c.pageId == PageID(0) {
				c.pageId = t.nextPageId()
			}
			for _, a := range c.f.arrows {
				a.pageId = c.pageId
			}
			if prevRootPageId == PageID(0) {
				for idx, r := range c.f.refs {
					if t.prevRoot == r {
						prevRootPageId = c.pageId
						prevRootTree = uint32(idx)
						break
					}
				}
			}
		}
		// Commit
		for _, c := range containers {
			t.commitPage(c)
		}
		//prevId := t.commitPage(t.prevRoot, &buffer, prefix, keyCount, pageCount, keyBodySize, valBodySize, structBits, pinnedPageId)
		t.versions[t.currentVersion] = Version3{pageId: prevRootPageId, treeIndex: prevRootTree}
		if t.verFile != nil {
			var verdata [12]byte
			binary.BigEndian.PutUint64(verdata[:], uint64(prevRootPageId))
			binary.BigEndian.PutUint32(verdata[8:], prevRootTree)
			t.verFile.WriteAt(verdata[:], int64(t.currentVersion)*int64(12))
		}
	}
	t.currentVersion++
	t.versions[t.currentVersion] = Version3{pageId: rootPageId, treeIndex: rootTree}
	if t.verFile != nil {
		var verdata [8]byte
		binary.BigEndian.PutUint64(verdata[:], uint64(rootPageId))
		binary.BigEndian.PutUint32(verdata[8:], rootTree)
		t.verFile.WriteAt(verdata[:], int64(t.currentVersion)*int64(12))
	}
	prevRootArrow := &Arrow3{pageId: rootPageId, height: t.root.getheight(), max: t.root.getmax()}
	t.root = &Arrow3{pageId: rootPageId, height: t.root.getheight(), max: t.root.getmax(), arrow: prevRootArrow}
	t.prevRoot = prevRootArrow
	return t.commitedCounter - startCounter
}

func (t *Avl3) commitPage(c *PageContainer) {
	trace := t.trace
	nodeCount := c.f.leafCount + c.f.arrowCount
	size := PageSize - c.space
	data := make([]byte, size)
	offset := uint32(0)
	binary.BigEndian.PutUint32(data[offset:], nodeCount)
	offset += 4
	pageBitsOffset := offset
	offset += 4*((nodeCount+31)/32)
	prefixOffsetOffset := offset
	offset += 4
	keyHeaderOffset := offset
	offset += 4*nodeCount /* key offset per node */ + 4 /* end key offset */
	arrowHeaderOffset := offset
	offset += (12+t.hashLength)*c.f.arrowCount /* (pageId, size - minSize, hash) per arrow */
	valueHeaderOffset := offset
	offset += 4*c.f.leafCount /* value length per leaf */
	structBitsOffset := offset
	if trace {
		//fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	offset += 4*((c.f.structBits+31)/32) // Structure encoding
	// key prefix begins here
	binary.BigEndian.PutUint32(data[prefixOffsetOffset:], uint32(offset))
	copy(data[offset:], c.f.prefix)
	offset += uint32(len(c.f.prefix))
	keyBodyOffset := offset
	offset += c.f.keyBodySize - nodeCount*uint32(len(c.f.prefix))
	valBodyOffset := offset

	var nodeIndex uint32
	var structBit uint32
	if trace {
		//fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	for i, r := range c.f.refs {
		if i > 0 {
			structBit++ // Insert separator bit "0"
		}
		t.serialisePass2(r, c.pageId, data, len(c.f.prefix), false, pageBitsOffset, structBitsOffset,
			&nodeIndex, &structBit, &keyHeaderOffset, &arrowHeaderOffset, &valueHeaderOffset, &keyBodyOffset, &valBodyOffset)
	}
	// end key offset
	if trace {
		fmt.Printf("valBodyOffset %d\n", keyBodyOffset)
	}
	binary.BigEndian.PutUint32(data[keyHeaderOffset:], keyBodyOffset)
	if valBodyOffset != size {
		panic(fmt.Sprintf("valBodyOffset %d (%d) != size %d, valBodySize %d, nodeCount %d, len(prefix): %d",
			valBodyOffset, offset, size, c.f.valBodySize, nodeCount, len(c.f.prefix)))
	}
	if nodeIndex != nodeCount {
		panic("n != nodeCount")
	}
	if structBit != c.f.structBits {
		panic("sb != structBits")
	}
	fmt.Printf("commitPage %d, len(refs) %d, nodeCount %d, structBits %d\n", c.pageId, len(c.f.refs), nodeCount, c.f.structBits)
	if t.trace {
		fmt.Printf("Committed page %d, nodeCount %d, prefix %s\n", c.pageId, nodeCount, c.f.prefix)
	}
	if t.pageFile != nil {
		t.pageFile.WriteAt(data, int64(c.pageId)*int64(PageSize))
	} else {
		t.pageMap[c.pageId] = data
	}
	t.commitedCounter++
	t.pageSpace += uint64(size)
}

// Computes all the dynamic parameters that allow calculation of the page length and pre-allocation of all buffers
func (l *Leaf3) prepare(t *Avl3, buffer *CommitBuffer) (tree Tree3) {
	tree.leafCount = 1
	tree.keyBodySize = uint32(len(l.key))
	if l.valueLen > InlineValueMax {
		tree.valBodySize = 8 + HashLength // Size of value id + valueHash
	} else {
		tree.valBodySize = l.valueLen
	}
	tree.structBits = 1 // one bit per leaf
	tree.prefix = l.key
	if l.pinnedPageId != PageID(0) {
		// Old pin
		tree.pageId = l.pinnedPageId
		tree.pinned = true
	} else if l.arrow != nil {
		// New pin
		buffer.nextDistinctId++
		tree.pageId = buffer.nextDistinctId
		tree.pinned = false
	}
	return
}

func (f *Fork3) prepare(t *Avl3, buffer *CommitBuffer) (tree Tree3) {
	if f.pinnedPageId != PageID(0) {
		// Old pin
		tree.pageId = f.pinnedPageId
		tree.pinned = true
		if f.pinnedPageId == 1645 {
			fmt.Printf("Returning pinned fork to page %d: %s %d\n", f.pinnedPageId, f.max, f.height)
		}
	} else if f.arrow != nil {
		// New pin
		buffer.nextDistinctId++
		tree.pageId = buffer.nextDistinctId
		tree.pinned = false
	}

	treeL := f.left.prepare(t, buffer)
	treeR := f.right.prepare(t, buffer)
	// Fork and both children fit in the page
	var mergable3 bool
	var mergePageId PageID
	var mergePinned bool
	if treeL.pageId == 0 && treeR.pageId == 0 {
		mergable3 = true
		mergePageId = tree.pageId
		mergePinned = tree.pinned
	} else if treeL.pageId == 0 && tree.pageId == 0 {
		mergable3 = true
		mergePageId = treeR.pageId
		mergePinned = treeR.pinned
	} else if treeR.pageId == 0 && tree.pageId == 0 {
		mergable3 = true
		mergePageId = treeL.pageId
		mergePinned = treeL.pinned
	} else if treeL.pageId == treeR.pageId && treeL.pinned == treeR.pinned && treeR.pageId == tree.pageId && treeR.pinned == tree.pinned {
		mergable3 = true
		mergePageId = tree.pageId
		mergePinned = tree.pinned
	}
	if mergable3 {
		leafCountLFR := treeL.leafCount+treeR.leafCount
		arrowCountLFR := treeL.arrowCount+treeR.arrowCount
		keyBodySizeLFR := treeL.keyBodySize+treeR.keyBodySize
		valBodySizeLFR := treeL.valBodySize+treeR.valBodySize
		structBitsLFR := treeL.structBits+treeR.structBits+2 // 2 bits for the fork
		prefixLFR := commonPrefix(treeL.prefix, treeR.prefix)
		sizeLFR := t.pageSize3(leafCountLFR, arrowCountLFR, keyBodySizeLFR, valBodySizeLFR, structBitsLFR, prefixLFR)
		if sizeLFR < PageSize {
			return Tree3{
				prefix: prefixLFR,
				leafCount: leafCountLFR,
				arrowCount: arrowCountLFR,
				keyBodySize: keyBodySizeLFR,
				valBodySize: valBodySizeLFR,
				structBits: structBitsLFR,
				pageId: mergePageId,
				pinned: mergePinned,
			}
		}
	}
	// Choose the biggest child and make a page out of it
	sizeL := t.pageSize3(treeL.leafCount, treeL.arrowCount, treeL.keyBodySize, treeL.valBodySize, treeL.structBits, treeL.prefix)
	sizeR := t.pageSize3(treeR.leafCount, treeR.arrowCount, treeR.keyBodySize, treeR.valBodySize, treeR.structBits, treeR.prefix)
	if sizeL > sizeR {
		var mergableR bool
		if treeR.pageId == 0 {
			mergableR = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		} else if tree.pageId == 0 {
			mergableR = true
			mergePageId = treeR.pageId
			mergePinned = treeR.pinned
		} else if treeR.pageId == tree.pageId && treeR.pinned == tree.pinned {
			mergableR = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		}
		var lArrow *Arrow3
		if la, ok := f.left.(*Arrow3); ok {
			lArrow = la
		} else {
			lArrow = &Arrow3{height: f.left.getheight(), max: f.left.getmax()}
			treeL.ref = f.left
			treeL.arrow = lArrow
			if treeL.pinned {
				if treeL.pageId == 1645 {
					fmt.Printf("Pinned %d from %s\n", treeL.pageId, debug.Stack())
				}
				buffer.AddPinnedForest(treeL.pageId, treeL.ToForest())
			} else if treeL.pageId == 0 {
				buffer.free = append(buffer.free, treeL)
			} else {
				buffer.distinct = append(buffer.distinct, treeL)
			}
			f.left = lArrow
		}
		// Check if the fork and the right child still fit into a page
		arrowCountFR := treeR.arrowCount+1 // 1 for the left arrow
		keyBodySizeFR := treeR.keyBodySize+uint32(len(lArrow.max))
		structBitsFR := treeR.structBits+3 // 2 for the fork and 1 for the left arrow
		prefixFR := commonPrefix(treeR.prefix, lArrow.max)
		sizeFR := t.pageSize3(treeR.leafCount, arrowCountFR, keyBodySizeFR, treeR.valBodySize, structBitsFR, prefixFR)
		if mergableR && sizeFR < PageSize {
			return Tree3{
				prefix: prefixFR,
				leafCount: treeR.leafCount,
				arrowCount: arrowCountFR,
				keyBodySize: keyBodySizeFR,
				valBodySize: treeR.valBodySize,
				structBits: structBitsFR,
				pageId: mergePageId,
				pinned: mergePinned,
			}
		} else {
			// Have to commit right child too
			var rArrow *Arrow3
			if ra, ok := f.right.(*Arrow3); ok {
				rArrow = ra
			} else {
				//rid := t.commitPage(f.right, buffer, prefixR, keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR, pinnedPageR)
				rArrow = &Arrow3{height: f.right.getheight(), max: f.right.getmax()}
				treeR.ref = f.right
				treeR.arrow = rArrow
				if treeR.pinned {
					if treeR.pageId == 1645 {
						fmt.Printf("Pinned %d from %s\n", treeR.pageId, debug.Stack())
					}
					buffer.AddPinnedForest(treeR.pageId, treeR.ToForest())
				} else if treeR.pageId == 0 {
					buffer.free = append(buffer.free, treeR)
				} else {
					buffer.distinct = append(buffer.distinct, treeR)
				}
				f.right = rArrow
			}
			return Tree3{
				prefix: commonPrefix(rArrow.max, lArrow.max),
				leafCount: 0,
				arrowCount: 2,
				keyBodySize: uint32(len(lArrow.max))+uint32(len(rArrow.max)),
				valBodySize: 0,
				structBits: 4 /* 2 bits for arrows, 2 for the fork */,
				pageId: tree.pageId,
				pinned: tree.pinned,
			}
		}
	} else {
		var mergableL bool
		if treeL.pageId == 0 {
			mergableL = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		} else if tree.pageId == 0 {
			mergableL = true
			mergePageId = treeL.pageId
			mergePinned = treeL.pinned
		} else if treeL.pageId == tree.pageId && treeL.pinned == tree.pinned {
			mergableL = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		}
		var rArrow *Arrow3
		if ra, ok := f.right.(*Arrow3); ok {
			rArrow = ra
		} else {
			//rid := t.commitPage(f.right, buffer, prefixR, keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR, pinnedPageR)
			rArrow = &Arrow3{height: f.right.getheight(), max: f.right.getmax()}
			treeR.ref = f.right
			treeR.arrow = rArrow
			if treeR.pinned {
				if treeR.pageId == 1645 {
					fmt.Printf("Pinned %d from %s\n", treeR.pageId, debug.Stack())
				}
				buffer.AddPinnedForest(treeR.pageId, treeR.ToForest())
			} else if treeR.pageId == 0 {
				buffer.free = append(buffer.free, treeR)
			} else {
				buffer.distinct = append(buffer.distinct, treeR)
			}
			f.right = rArrow
		}
		// Check if the fork and the let child still fit into a page
		arrowCountFL := treeL.arrowCount+1 // 1 for the left arrow
		keyBodySizeFL := treeL.keyBodySize+uint32(len(rArrow.max))
		structBitsFL := treeL.structBits+3 // 2 for the fork and 1 for the left arrow
		prefixFL := commonPrefix(treeL.prefix, rArrow.max)
		sizeFL := t.pageSize3(treeL.leafCount, arrowCountFL, keyBodySizeFL, treeL.valBodySize, structBitsFL, prefixFL)
		if mergableL && sizeFL < PageSize {
			return Tree3{
				prefix: prefixFL,
				leafCount: treeL.leafCount,
				arrowCount: arrowCountFL,
				keyBodySize: keyBodySizeFL,
				valBodySize: treeL.valBodySize,
				structBits: structBitsFL,
				pageId: mergePageId,
				pinned: mergePinned,
			}
		} else {
			// Have to commit left child too
			var lArrow *Arrow3
			if la, ok := f.left.(*Arrow3); ok {
				lArrow = la
			} else {
				//lid := t.commitPage(f.left, buffer, prefixL, keyCountL, pageCountL, keyBodySizeL, valBodySizeL, structBitsL, pinnedPageL)
				lArrow = &Arrow3{height: f.left.getheight(), max: f.left.getmax()}
				treeL.ref = f.left
				treeL.arrow = lArrow
				if treeL.pinned {
					if treeL.pageId == 1645 {
						fmt.Printf("Pinned %d from %s\n", treeL.pageId, debug.Stack())
					}
					buffer.AddPinnedForest(treeL.pageId, treeL.ToForest())
				} else if treeL.pageId == 0 {
					buffer.free = append(buffer.free, treeL)
				} else {
					buffer.distinct = append(buffer.distinct, treeL)
				}
				f.left = lArrow
			}
			return Tree3{
				prefix: commonPrefix(rArrow.max, lArrow.max),
				leafCount: 0,
				arrowCount: 2,
				keyBodySize: uint32(len(lArrow.max))+uint32(len(rArrow.max)),
				valBodySize: 0,
				structBits: 4 /* 2 bits for arrows, 2 for the fork */,
				pageId: tree.pageId,
				pinned: tree.pinned,
			}
		}
	}
}

func (a *Arrow3) prepare(t *Avl3, buffer *CommitBuffer) (tree Tree3) {
	tree.arrowCount = 1
	tree.structBits = 1 // one bit for page reference
	tree.keyBodySize = uint32(len(a.max))
	tree.prefix = a.max
	return
}

func (t *Avl3) serialiseKey(key, data []byte, keyHeaderOffset, keyBodyOffset *uint32) {
	binary.BigEndian.PutUint32(data[*keyHeaderOffset:], *keyBodyOffset)
	*keyHeaderOffset += 4
	copy(data[*keyBodyOffset:], key)
	*keyBodyOffset += uint32(len(key))
}

func (t *Avl3) serialiseVal(value []byte, valueId uint64, valueLen uint32, data []byte, valueHeaderOffset, valBodyOffset *uint32) uint64 {
	binary.BigEndian.PutUint32(data[*valueHeaderOffset:], valueLen)
	*valueHeaderOffset += 4
	if valueLen > InlineValueMax {
		var valueHash Hash
		if valueId == 0 {
			valueId = t.nextValueId()
			//valueHash = sha256.Sum256(value)
			//t.valueHashes[valueId] = valueHash
			t.addValue(valueId, value)
		} else {
			valueHash = t.valueHashes[valueId]
		}
		binary.BigEndian.PutUint64(data[*valBodyOffset:], valueId)
		*valBodyOffset += 8
		copy(data[*valBodyOffset:], valueHash[:])
		*valBodyOffset += HashLength
	} else {
		copy(data[*valBodyOffset:], value)
		*valBodyOffset += valueLen
	}
	return valueId
}

func (t *Avl3) serialisePass2(r Ref3, pageId PageID, data []byte, prefixLen int, subtreePinned bool, pageBitsOffset, structBitsOffset uint32,
	nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset *uint32) {
	switch r := r.(type) {
	case *Leaf3:
		t.serialiseKey(r.key[prefixLen:], data, keyHeaderOffset, keyBodyOffset)
		r.valueId = t.serialiseVal(r.value, r.valueId, r.valueLen, data, valueHeaderOffset, valBodyOffset)
		// Update page bits
		*nodeIndex++
		// Update struct bits
		pinned := subtreePinned || r.pinnedPageId == pageId || r.arrow != nil
		if !pinned {
			// If the subtree containing this leaf of the leaf itself is pinned, we write "0" structural bit
			data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		} else {
			if t.trace {
				//fmt.Printf("Pinned leaf %s in page %d\n", r.key, pageId)
			}
		}
		*structBit++
		if r.arrow != nil {
			r.arrow.pageId = pageId
		}
	case *Fork3:
		pinned := subtreePinned || r.pinnedPageId == pageId || r.arrow != nil
		// Write opening parenthesis "0" (noop)
		t.serialisePass2(r.left, pageId, data, prefixLen, pinned, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Update struct bit
		*structBit++
		if r.arrow != nil {
			r.arrow.pageId = pageId
		}
		t.serialisePass2(r.right, pageId, data, prefixLen, pinned, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Write closing parenthesis "1"
		data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		*structBit++
	case *Arrow3:
		binary.BigEndian.PutUint64(data[*arrowHeaderOffset:], uint64(r.pageId))
		*arrowHeaderOffset += 8
		binary.BigEndian.PutUint32(data[*arrowHeaderOffset:], uint32(r.height))
		*arrowHeaderOffset += 4
		t.serialiseKey(r.max[prefixLen:], data, keyHeaderOffset, keyBodyOffset)
		// TODO: write page hash
		*arrowHeaderOffset += t.hashLength
		// Update page bit
		data[pageBitsOffset+(*nodeIndex>>3)] |= (uint8(1)<<(*nodeIndex&7))
		*nodeIndex++
		// Write closing parenthesis "1"
		if !subtreePinned {
			data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		} else {
			if t.trace {
				//fmt.Printf("Pinned arrow %s in page %d\n", r.max, pageId)
			}
		}
		*structBit++
	}
}

func (t *Avl3) deserialiseKey(data []byte, keyHeaderOffset *uint32, prefix []byte) []byte {
	keyStart := binary.BigEndian.Uint32(data[*keyHeaderOffset:])
	keyEnd := binary.BigEndian.Uint32(data[*keyHeaderOffset+4:]) // Start of the next key (or end offset)
	*keyHeaderOffset += 4
	return append(prefix, data[keyStart:keyEnd]...)
}

func (t *Avl3) deserialiseVal(data []byte, valueHeaderOffset, valBodyOffset *uint32) (value []byte, valueId uint64, valLen uint32) {
	valLen = binary.BigEndian.Uint32(data[*valueHeaderOffset:])
	*valueHeaderOffset += 4
	if valLen > InlineValueMax {
		valueId = binary.BigEndian.Uint64(data[*valBodyOffset:])
		*valBodyOffset += 8
		// Read the hash here
		*valBodyOffset += HashLength
	} else {
		value = make([]byte, valLen)
		copy(value, data[*valBodyOffset:])
		*valBodyOffset += valLen
	}
	return
}

func (t *Avl3) returnModPage(pageId PageID, modRefs []Ref3, key []byte, height uint32) Ref3 {
	for i, r := range modRefs {
		if point, found := t.walkToArrowPoint(r, key, height); found {
			// Release reference if requested
			modRefs = append(modRefs[:i], modRefs[i+1:]...)
			t.modifiedPages[pageId] = modRefs
			if pageId == 1645 {
				fmt.Printf("Released reference %s %d for %d, left %d\n", key, height, pageId, len(t.modifiedPages[pageId]))
			}
			return point
		}
	}
	return nil
}

func (t *Avl3) deserialisePage(pageId PageID, key []byte, height uint32, treeIndex uint32, releaseRef bool) Ref3 {
	trace := t.trace
	if trace {
		fmt.Printf("Deserialising page %d %s %d\n", pageId, key, height)
	}
	if releaseRef {
		modRefs, modPageFound := t.modifiedPages[pageId]
		if modPageFound {
			return t.returnModPage(pageId, modRefs, key, height)
		}
	}
	var data []byte
	if t.pageFile != nil {
		data = make([]byte, PageSize)
		if _, err := t.pageFile.ReadAt(data, int64(pageId)*int64(PageSize)); err != nil && err != io.EOF {
			panic(err)
		}
	} else {
		data = t.pageMap[pageId]
	}
	if data == nil {
		return nil
	}
	offset := uint32(0)
	// read node count
	nodeCount := binary.BigEndian.Uint32(data[offset:])
	if nodeCount == 0 {
		return nil
	}
	offset += 4
	arrowBitsOffset := offset
	// Calculate number of arrows
	var arrowCount uint32
	arrowBitsLen := 4*((nodeCount+31)/32)
	for i := uint32(0); i < arrowBitsLen; i += 4 {
		arrowCount += uint32(bits.OnesCount32(binary.BigEndian.Uint32(data[arrowBitsOffset+i:])))
	}
	offset += arrowBitsLen
	leafCount := nodeCount - arrowCount
	prefixOffset := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	keyHeaderOffset := offset
	prefixLen := binary.BigEndian.Uint32(data[keyHeaderOffset:]) - prefixOffset
	prefix := make([]byte, prefixLen, prefixLen) // To prevent this to be appended to
	copy(prefix, data[prefixOffset:])
	offset += 4*nodeCount
	valBodyOffset := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	arrowHeaderOffset := offset
	offset += (12+t.hashLength)*arrowCount
	valueHeaderOffset := offset
	offset += 4*leafCount
	structBitsOffset := offset
	if trace {
		//fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	if trace {
		//fmt.Printf("valBodyOffset %d\n", valBodyOffset)
	}
	if trace {
		//fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	var point Ref3
	var forkStack []Ref3
	var pinnedStack []bool
	var freeStack []bool
	var stackTop int
	var nodeIndex uint32
	var structBit uint32
	var noLeaf bool
	var maxStructBits uint32 = (prefixOffset - structBitsOffset) << 3
	for nodeIndex < nodeCount || structBit < maxStructBits  {
		sbit := (data[structBitsOffset+(structBit>>3)] & (uint8(1)<<(structBit&7))) != 0
		if pageId == 1645 {
			fmt.Printf("nodeIndex %d, nodeCount %d, structBit %d, maxStructBits %d, sbit %t, noLeaf %t, stackTop %d\n",
				nodeIndex, nodeCount, structBit, maxStructBits, sbit, noLeaf, stackTop)
		}
		if !sbit && nodeIndex == nodeCount {
			// We got to the padding "0" bits of the structure
			break
		}
		// Interpret the structural bit
		var r Ref3
		if noLeaf {
			if sbit {
				stackTop--
				right := forkStack[stackTop]
				rPinned := pinnedStack[stackTop]
				rFree := freeStack[stackTop]
				left := forkStack[stackTop-1]
				lPinned := pinnedStack[stackTop-1]
				lFree := freeStack[stackTop-1]
				y := &Fork3{left: left, right: right, height: 1+maxu32(left.getheight(), right.getheight()), max: right.getmax()}
				if y.height == height && bytes.Equal(y.max, key) {
					point = y
				}
				if rPinned && !lPinned {
					switch rt := right.(type) {
					case *Leaf3:
						rt.pinnedPageId = pageId
					case *Fork3:
						rt.pinnedPageId = pageId
					}
				} else if lPinned && !rPinned {
					switch lt := left.(type) {
					case *Leaf3:
						lt.pinnedPageId = pageId
					case *Fork3:
						lt.pinnedPageId = pageId
					}
				}
				forkStack[stackTop-1] = y
				freeStack[stackTop-1] = lFree && rFree
				pinnedStack[stackTop-1] = lPinned && rPinned
				noLeaf = true
				if trace {
					fmt.Printf("Fork finishing %s %d\n", y.max, y.height)
				}
			} else {
				noLeaf = false
			}
		} else {
			isPage := (data[arrowBitsOffset+(nodeIndex>>3)] & (uint8(1)<<(nodeIndex&7))) != 0
			if isPage {
				id := PageID(binary.BigEndian.Uint64(data[arrowHeaderOffset:]))
				arrowHeaderOffset += 8
				height := binary.BigEndian.Uint32(data[arrowHeaderOffset:])
				arrowHeaderOffset += 4
				// TODO read the page hash
				arrowHeaderOffset += t.hashLength
				max := t.deserialiseKey(data, &keyHeaderOffset, prefix)
				arrow := &Arrow3{pageId: id, height: height, max: max}
				r = arrow
				if pageId == 1645 {
					if !sbit {
						fmt.Printf("Deserialised PIN arrow max %s, pageId %d\n", arrow.max, pageId)
					} else {
						fmt.Printf("Deserialised arrow max %s, pageId %d\n", arrow.max, pageId)
					}
				}
			} else {
				l := &Leaf3{}
				l.key = t.deserialiseKey(data, &keyHeaderOffset, prefix)
				l.value, l.valueId, l.valueLen = t.deserialiseVal(data, &valueHeaderOffset, &valBodyOffset)
				if height == 1 && bytes.Equal(l.key, key) {
					point = l
				}
				if pageId == 1645 {
					if !sbit {
						fmt.Printf("Deserialised PIN leaf key %s, pageId %d\n", l.key, pageId)
					} else {
						fmt.Printf("Deserialised leaf key %s, pageId %d\n", l.key, pageId)
					}
				}
				r = l
			}
			nodeIndex++
			noLeaf = true
		}
		if r != nil {
			// Push onto the stack
			if stackTop >= len(forkStack) {
				forkStack = append(forkStack, r)
				pinnedStack = append(pinnedStack, !sbit)
				freeStack = append(freeStack, sbit)
			} else {
				forkStack[stackTop] = r
				pinnedStack[stackTop] = !sbit
				freeStack[stackTop] = sbit
			}
			stackTop++
		}
		structBit++
	}
	for i := 0; i < stackTop; i++ {
		if pinnedStack[i] {
			switch rt := forkStack[i].(type) {
				case *Leaf3:
					rt.pinnedPageId = pageId
				case *Fork3:
					rt.pinnedPageId = pageId
			}
		}
	}
	if releaseRef {
		var modRefs []Ref3
		for i := 0; i < stackTop; i++ {
			modRefs = append(modRefs, forkStack[i])
		}
		fmt.Printf("Deserialised page %d, got %d refs\n", pageId, len(modRefs))
		if len(modRefs) > 0 {
			t.modifiedPages[pageId] = modRefs
		}
		return t.returnModPage(pageId, modRefs, key, height)
	}
	if key == nil && height == 0 && point == nil {
		point = forkStack[treeIndex]
	}
	return point
}

// Checks whether WBT without pages is equivalent to one with pages
func equivalent33(t *Avl3, path string, r1 Ref3, r2 Ref3) bool {
	switch r2 := r2.(type) {
	case nil:
		if r1 != nil {
			fmt.Printf("At path %s, expected n1 nil, but it was %s\n", path, r1.nkey())
			return false
		}
		return true
	case *Leaf3:
		if l1, ok := r1.(*Leaf3); ok {
			if !bytes.Equal(l1.key, r2.key) {
				fmt.Printf("At path %s, l1.key %s, r2.key %s\n", path, l1.nkey(), r2.nkey())
				return false
			}
			if !bytes.Equal(l1.nvalue(t), r2.nvalue(t)) {
				fmt.Printf("At path %s, l1.value %s, r2.value %s\n", path, l1.nvalue(t), r2.nvalue(t))
				return false
			}
		} else {
			fmt.Printf("At path %s, expected leaf, got %T\n", path, r1)
			return false
		}
		return true
	case *Fork3:
		if t.trace {
			fmt.Printf("equivalent33 path %s, at fork %s, height %d\n", path, r2.max, r2.height)
		}
		if f1, ok := r1.(*Fork3); ok {
			if !bytes.Equal(f1.max, r2.max) {
				fmt.Printf("At path %s, f1.max %s, r2.max %s\n", path, f1.max, r2.max)
				return false
			}
			if f1.height != r2.height {
				fmt.Printf("At path %s, f1.height %d, r2.height %d\n", path, f1.height, r2.height)
				return false
			}
			eqL := equivalent33(t, path + "l", f1.left, r2.left)
			eqR := equivalent33(t, path + "r", f1.right, r2.right)
			return eqL && eqR
		}
	case *Arrow3:
		if t.trace {
			fmt.Printf("equivalent33 path %s, at arrow P.%d[%s], height %d\n", path, r2.pageId, r2.max, r2.height)
		}
		if !bytes.Equal(r1.getmax(), r2.max) {
			fmt.Printf("At path %s, r1.max %s, r2(arrow).max %s\n", path, r1.getmax(), r2.max)
			return false
		}
		if r1 != nil && r2 != nil && r1.getheight() != r2.height {
			fmt.Printf("At path %s, r1.height %d, r2(arrow).height %d\n", path, r1.getheight(), r2.height)
			return false
		}
		point := t.deserialisePage(r2.pageId, r2.max, r2.height, 0, false)
		if point == nil {
			panic("")
		}
		return equivalent33(t, path, r1, point)
	}
	return false
}

func (t *Avl3) PrintStats() {
	var totalValueLens uint64
	if t.valueFile == nil {
		for _, l := range t.valueLens {
			totalValueLens += uint64(l)
		}
	} else {
		totalValueLens = t.maxValueId
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Total pages: %d, Mb %0.3f, page space: Mb %0.3f large vals: %d, Mb %0.3f, mem alloc %0.3fMb, sys %0.3fMb\n",
		t.maxPageId,
		float64(t.maxPageId)*float64(PageSize)/1024.0/1024.0,
		float64(t.pageSpace)/1024.0/1024.0,
		len(t.valueLens),
		float64(totalValueLens)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		)
}