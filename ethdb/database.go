// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"bytes"
	"errors"
	"os"
	"path"
	"sync"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/log"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/boltdb/bolt"
	"github.com/petar/GoLLRB/llrb"
)

var OpenFileLimit = 64
var ErrKeyNotFound = errors.New("boltdb: key not found in range")
var SuffixBucket = []byte("SUFFIX")

const HeapSize = 32*1024*1024

type LDBDatabase struct {
	fn string      // filename for reporting
	db *bolt.DB // BoltDB instance

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path

	hashfile     *os.File
	hashdata     []byte
}

func openHashFile(file string) (*os.File, []byte, error) {
	hashfile, err := os.OpenFile(file+".hash", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, err
	}
	stat, err := hashfile.Stat()
	if err != nil {
		hashfile.Close()
		return nil, nil, err
	}
	if stat.Size() < HeapSize {
		var buf [4096]byte
		for i := 0; i < HeapSize; i+=len(buf) {
			if _, err := hashfile.Write(buf[:]); err != nil {
				hashfile.Close()
				return nil, nil, err
			}
		}
	} else if stat.Size() > HeapSize {
		if err := hashfile.Truncate(HeapSize); err != nil {
			hashfile.Close()
			return nil, nil, err
		}
	}
	hashdata, err := mmap(hashfile, HeapSize)
	if err != nil {
		hashfile.Close()
		return nil, nil, err
	}
	return hashfile, hashdata, nil
}

// NewLDBDatabase returns a LevelDB wrapped object.
func NewLDBDatabase(file string, cache int, varKeys bool) (*LDBDatabase, error) {
	logger := log.New("database", file)

	// Ensure we have some minimal caching and file guarantees
	if cache < 16 {
		cache = 16
	}
	logger.Info("Allocated cache and file handles", "cache", cache)

	// Create necessary directories
	if err := os.MkdirAll(path.Dir(file), os.ModePerm); err != nil {
		return nil, err
	}
	hashfile, hashdata, err := openHashFile(file)
	if err != nil {
		return nil, err
	}
	// Open the db and recover any potential corruptions
	db, err := bolt.Open(file, 0600, &bolt.Options{InitialMmapSize: cache*1024*1024})
	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
		return nil, err
	}
	return &LDBDatabase{
		fn:  file,
		db:  db,
		log: logger,
		hashfile: hashfile,
		hashdata: hashdata,
	}, nil
}

// Path returns the path to the database directory.
func (db *LDBDatabase) Path() string {
	return db.fn
}

// Put puts the given key / value to the queue
func (db *LDBDatabase) Put(bucket, key []byte, value []byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
	return err
}

func compositeKeySuffix(key []byte, timestamp uint64) (composite, suffix []byte) {
	suffix = encodeTimestamp(timestamp)
	composite = make([]byte, len(key) + len(suffix))
	copy(composite, key)
	copy(composite[len(key):], suffix)
	return composite, suffix
}

// Put puts the given key / value to the queue
func (db *LDBDatabase) PutS(bucket, key, value []byte, timestamp uint64) error {
	composite, suffix := compositeKeySuffix(key, timestamp)
	suffixkey := make([]byte, len(suffix) + len(bucket))
	copy(suffixkey, suffix)
	copy(suffixkey[len(suffix):], bucket)
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		if err = b.Put(composite, value); err != nil {
			return err
		}
		sb, err := tx.CreateBucketIfNotExists(SuffixBucket)
		if err != nil {
			return err
		}
		dat := sb.Get(suffixkey)
		var l int
		if dat == nil {
			l = 4
		} else {
			l = len(dat)
		}
		dv := make([]byte, l+1+len(key))
		copy(dv, dat)
		binary.BigEndian.PutUint32(dv, 1 + binary.BigEndian.Uint32(dv)) // Increment the counter of keys
		dv[l] = byte(len(key))
		copy(dv[l+1:], key)
		return sb.Put(suffixkey, dv)
	})
	return err
}

func (db *LDBDatabase) MultiPut(tuples ...[]byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		for bucketStart := 0; bucketStart < len(tuples); {
			bucketEnd := bucketStart
			for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
			}
			b, err := tx.CreateBucketIfNotExists(tuples[bucketStart])
			if err != nil {
				return err
			}
			l := (bucketEnd-bucketStart)/3
			pairs := make([][]byte, 2*l)
			for i := 0; i < l; i++ {
				pairs[2*i] = tuples[bucketStart+3*i+1]
				pairs[2*i+1] = tuples[bucketStart+3*i+2]
			}
			if b.MultiPut(pairs...); err != nil {
				return err
			}
			bucketStart = bucketEnd
		}
		return nil
	})
	return err
}

func (db *LDBDatabase) Has(bucket, key []byte) (bool, error) {
	var has bool
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			has = false
		} else {
			has = b.Get(key) != nil
		}
		return nil
	})
	return has, err
}

func (db *LDBDatabase) Size() int {
	return db.db.Size()
}

// Get returns the given key if it's present.
func (db *LDBDatabase) Get(bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

// GetAsOf returns the first pair (k, v) where key is a prefix of key, or nil
// if there are not such (k, v)
func (db *LDBDatabase) GetAsOf(bucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := compositeKeySuffix(key, timestamp)
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			c := b.Cursor()
			k, v := c.Seek(composite)
			if k != nil && bytes.HasPrefix(k, key) {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		}
		return ErrKeyNotFound
	})
	return dat, err
}

func bytesmask(fixedbits uint) (fixedbytes int, mask byte) {
	fixedbytes = int((fixedbits+7)/8)
	shiftbits := fixedbits&7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff<<(8-shiftbits)
	}
	return fixedbytes, mask
}

func (db *LDBDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker WalkerFunc) error {
	fixedbytes, mask := bytesmask(fixedbits)
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		var k, v []byte
		if fixedbits == 0 {
			k, v = c.First()
		} else {
			k, v = c.Seek(startkey)
		}
		for k != nil && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask)==(startkey[fixedbytes-1]&mask)) {
			nextkey, action, err := walker(k, v)
			if err != nil {
				return err
			}
			if action == WalkActionStop {
				break
			} else if action == WalkActionNext {
				k, v = c.Next()
			} else if action == WalkActionSeek {
				k, v = c.SeekTo(nextkey)
			}
		}
		return nil
	})
	return err
}

func (db *LDBDatabase) WalkAsOf(bucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	return walkAsOf(db, bucket, startkey, fixedbits, timestamp, walker)
}

func (db *LDBDatabase) MultiWalkAsOf(bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	return multiWalkAsOf(db, bucket, startkeys, fixedbits, timestamp, walker)
}

// Delete deletes the key from the queue and database
func (db *LDBDatabase) Delete(bucket, key []byte) error {
	// Execute the actual operation
	err := db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			return b.Delete(key)
		} else {
			return nil
		}
	})
	return err
}

// Deletes all keys with specified suffix from all the buckets
func (db *LDBDatabase) DeleteTimestamp(timestamp uint64) error {
	suffix := encodeTimestamp(timestamp)
	err := db.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(SuffixBucket)
		if sb == nil {
			return nil
		}
		c := sb.Cursor()
		for k, v := c.Seek(suffix); k != nil && bytes.HasPrefix(k, suffix); k, v = c.Next() {
			b := tx.Bucket(k[len(suffix):])
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				kk := make([]byte, l+len(suffix))
				copy(kk, v[i:i+l])
				copy(kk[l:], suffix)
				if err := b.Delete(kk); err != nil {
					return err
				}
				i += l
			}
			sb.Delete(k)
		}
		return nil
	})
	return err
}


func (db *LDBDatabase) DeleteBucket(bucket []byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucket); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (db *LDBDatabase) GetHash(index uint32) []byte {
	hash := make([]byte, 32)
	copy(hash, db.hashdata[32*index:32*index+32])
	return hash
}

func (db *LDBDatabase) PutHash(index uint32, hash []byte) {
	copy(db.hashdata[32*index:], hash[:32])
}

func (db *LDBDatabase) Close() {
	// Stop the metrics collection to avoid internal database races
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	err := db.hashfile.Close()
	if err == nil {
		db.log.Info("Hashfile closed")
	} else {
		db.log.Error("Failed to close hashfile", "err", err)
	}
	db.hashfile = nil
	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
	}
	err = db.db.Close()
	if err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}


func (db *LDBDatabase) LDB() *leveldb.DB {
	return nil
}

type PutItem struct {
	bucket, key, value []byte
}

func (a *PutItem) Less(b llrb.Item) bool {
	bi := b.(*PutItem)
	c := bytes.Compare(a.bucket, bi.bucket)
	if c == 0 {
		return bytes.Compare(a.key, bi.key) < 0
	} else {
		return c < 0
	}
}

type Hash struct {
	hash [32]byte
}

type mutation struct {
	puts *llrb.LLRB
	hashes map[uint32]Hash

	mu sync.RWMutex
	db Database
}

func (db *LDBDatabase) NewBatch() Mutation {
	m := &mutation{
		db: db,
		puts: llrb.New(),
		hashes: make(map[uint32]Hash),
	}
	return m
}

func (m *mutation) getMem(bucket, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	i := m.puts.Get(&PutItem{bucket: bucket, key: key})
	if i == nil {
		return nil, false
	}
	if item, ok := i.(*PutItem); ok {
		if item.value == nil {
			return nil, true
		}
		v := make([]byte, len(item.value))
		copy(v, item.value)
		return v, true
	}
	return nil, false
}

// Can only be called from the worker thread
func (m *mutation) Get(bucket, key []byte) ([]byte, error) {
	if value, ok := m.getMem(bucket, key); ok {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(bucket, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.puts.Has(&PutItem{bucket: bucket, key: key})
}

func (m *mutation) Has(bucket, key []byte) (bool, error) {
	if m.hasMem(bucket, key) {
		return true, nil
	}
	if m.db != nil {
		return m.db.Has(bucket, key)
	}
	return false, nil
}

func (m *mutation) Size() int {
	if m.db == nil {
		return 0
	}
	return m.db.Size()
}

func (m *mutation) Put(bucket, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.puts.ReplaceOrInsert(&PutItem{bucket: bb, key: k, value: v})
	return nil
}

func (m *mutation) PutS(bucket, key, value []byte, timestamp uint64) error {
	//fmt.Printf("PutS bucket %x key %x value %x timestamp %d\n", bucket, key, value, timestamp)
	composite, suffix := compositeKeySuffix(key, timestamp)
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	v := make([]byte, len(value))
	copy(v, value)
	m.puts.ReplaceOrInsert(&PutItem{bucket: bb, key: composite, value: v})
	suffixkey := make([]byte, len(suffix) + len(bucket))
	copy(suffixkey, suffix)
	copy(suffixkey[len(suffix):], bucket)
	dat, err := m.Get(SuffixBucket, suffixkey)
	if err != nil && err != ErrKeyNotFound {
		return err
	}
	var l int
	if dat == nil {
		l = 4
	} else {
		l = len(dat)
	}
	dv := make([]byte, l+1+len(key))
	copy(dv, dat)
	binary.BigEndian.PutUint32(dv, 1+binary.BigEndian.Uint32(dv))
	dv[l] = byte(len(key))
	copy(dv[l+1:], key)
	m.puts.ReplaceOrInsert(&PutItem{bucket: SuffixBucket, key: suffixkey, value: dv})
	return nil
}

func (m *mutation) MultiPut(tuples ...[]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		m.puts.ReplaceOrInsert(&PutItem{bucket: tuples[i], key: tuples[i+1], value: tuples[i+2]})
	}
	return nil
}

func (m *mutation) getAsOfMem(bucket, key []byte, timestamp uint64) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	composite, _ := compositeKeySuffix(key, timestamp)
	var dat []byte
	m.puts.AscendGreaterOrEqual1(&PutItem{bucket: bucket, key: composite}, func(i llrb.Item) bool {
		item := i.(*PutItem)
		if !bytes.Equal(item.bucket, bucket) {
			return false
		}
		if !bytes.HasPrefix(item.key, key) {
			return false
		}
		if item.value == nil {
			return true
		}
		dat = make([]byte, len(item.value))
		copy(dat, item.value)
		return false
	})
	if dat != nil {
		return dat, true
	}
	return nil, false
}

func (m *mutation) GetAsOf(bucket, key []byte, timestamp uint64) ([]byte, error) {
	if value, ok := m.getAsOfMem(bucket, key, timestamp); ok {
		return value, nil
	} else {
		if m.db != nil {
			return m.db.GetAsOf(bucket, key, timestamp)
		}
	}
	return nil, nil
}

func (m *mutation) walkMem(bucket, startkey []byte, fixedbits uint, walker WalkerFunc) error {
	fixedbytes, mask := bytesmask(fixedbits)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for nextkey := startkey; nextkey != nil; {
		from := nextkey
		nextkey = nil
		var extErr error
		m.puts.AscendGreaterOrEqual1(&PutItem{bucket: bucket, key: from}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			if !bytes.Equal(item.bucket, bucket) {
				return false
			}
			if item.value == nil {
				return true
			}
			if fixedbits > 0 && (!bytes.Equal(item.key[:fixedbytes-1], startkey[:fixedbytes-1]) || (item.key[fixedbytes-1]&mask)!=(startkey[fixedbytes-1]&mask)) {
				return true
			}
			wr, action, err := walker(item.key, item.value)
			if err != nil {
				extErr = err
				return false
			}
			switch action {
			case WalkActionStop:
				return false
			case WalkActionNext:
				return true
			case WalkActionSeek:
				nextkey = wr
				return false
			default:
				panic("Wrong action")
			}
		})
		if extErr != nil {
			return extErr
		}
	}
	return nil
}

func (m *mutation) Walk(bucket, startkey []byte, fixedbits uint, walker WalkerFunc) error {
	if m.db == nil {
		return m.walkMem(bucket, startkey, fixedbits, walker)
	} else {
		fixedbytes, mask := bytesmask(fixedbits)
		m.mu.RLock()
		defer m.mu.RUnlock()
		start := startkey
		stop := false
		err := m.db.Walk(bucket, startkey, fixedbits, func (k, v []byte) ([]byte, WalkAction, error) {
			nextkey := start
			putsIt := m.puts.NewSeekIterator()
			for i := putsIt.SeekTo(&PutItem{bucket: bucket, key: nextkey}); i != nil; i = putsIt.SeekTo(&PutItem{bucket: bucket, key: nextkey}) {
				item := i.(*PutItem)
				if !bytes.Equal(item.bucket, bucket) {
					break
				}
				if item.value == nil {
					continue
				}
				if bytes.Compare(item.key, k) > 0 {
					break
				}
				if fixedbits > 0 && (!bytes.Equal(item.key[:fixedbytes-1], startkey[:fixedbytes-1]) || (item.key[fixedbytes-1]&mask)!=(startkey[fixedbytes-1]&mask)) {
					continue
				}
				wr, action, err := walker(item.key, item.value)
				if err != nil {
					return nil, WalkActionStop, err
				}
				if action == WalkActionStop {
					stop = true
					return nil, WalkActionStop, nil
				} else if action == WalkActionNext {
					continue
				} else if action == WalkActionSeek {
					nextkey = wr
					continue
				} else {
					panic("Wrong action")
				}
			}
			var err error
			var action WalkAction
			start, action, err = walker(k, v)
			if action == WalkActionNext {
				start = k
			} else if action == WalkActionStop {
				stop = true
			}
			return start, action, err
		})
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
		putsIt := m.puts.NewSeekIterator()
		nextkey := start
		for i := putsIt.SeekTo(&PutItem{bucket: bucket, key: nextkey}); i != nil; i = putsIt.SeekTo(&PutItem{bucket: bucket, key: nextkey}) {
			item := i.(*PutItem)
			if !bytes.Equal(item.bucket, bucket) {
				break
			}
			if item.value == nil {
				continue
			}
			if fixedbits > 0 && (!bytes.Equal(item.key[:fixedbytes-1], startkey[:fixedbytes-1]) || (item.key[fixedbytes-1]&mask)!=(startkey[fixedbytes-1]&mask)) {
				continue
			}
			wr, action, err := walker(item.key, item.value)
			if err != nil {
				return err
			}
			if action == WalkActionStop {
				break
			} else if action == WalkActionNext {
				// When nextkey does not change, it will automatically go to the next item
				continue
			} else if action == WalkActionSeek {
				nextkey = wr
				continue
			} else {
				panic("Wrong action")
			}
		}
		return nil
	}
}

func (m *mutation) WalkAsOf(bucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	return walkAsOf(m, bucket, startkey, fixedbits, timestamp, walker)
}

func (m *mutation) MultiWalkAsOf(bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	return multiWalkAsOf(m, bucket, startkeys, fixedbits, timestamp, walker)
}

func (m *mutation) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	k := make([]byte, len(key))
	copy(k, key)
	m.puts.ReplaceOrInsert(&PutItem{bucket: bb, key: k, value: nil})
	return nil
}

// Deletes all keys with specified suffix from all the buckets
func (m *mutation) DeleteTimestamp(timestamp uint64) error {
	suffix := encodeTimestamp(timestamp)
	err := m.Walk(SuffixBucket, suffix, uint(8*len(suffix)), func(k, v []byte) ([]byte, WalkAction, error) {
		bucket := k[len(suffix):]
		keycount := int(binary.BigEndian.Uint32(v))
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			bb := make([]byte, len(bucket))
			copy(bb, bucket)
			kk := make([]byte, l+len(suffix))
			copy(kk, v[i:i+l])
			copy(kk[l:], suffix)
			m.puts.ReplaceOrInsert(&PutItem{bucket: bb, key: kk, value: nil})
			i += l
		}
		kk := make([]byte, len(k))
		copy(kk, k)
		m.puts.ReplaceOrInsert(&PutItem{bucket: SuffixBucket, key: kk, value: nil})
		return nil, WalkActionNext, nil
	})
	return err
}

func (m *mutation) Commit() error {
	if m.db == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tuples := make([][]byte, m.puts.Len()*3)
	var index int
	m.puts.AscendGreaterOrEqual1(&PutItem{}, func (i llrb.Item) bool {
		item := i.(*PutItem)
		tuples[index] = item.bucket
		index++
		tuples[index] = item.key
		index++
		tuples[index] = item.value
		index++
		return true
	})
	if putErr := m.db.MultiPut(tuples...); putErr != nil {
		return putErr
	}
	m.puts = llrb.New()
	for index, h := range m.hashes {
		m.db.PutHash(index, h.hash[:])
	}
	m.hashes = make(map[uint32]Hash)
	return nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = llrb.New()
}

func (m *mutation) Keys() [][]byte {
	pairs := make([][]byte, 2*m.puts.Len())
	idx := 0
	m.puts.AscendGreaterOrEqual1(&PutItem{}, func(i llrb.Item) bool {
		item := i.(*PutItem)
		pairs[idx] = item.bucket
		idx++
		pairs[idx] = item.key
		idx++
		return true
	})
	return pairs
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) NewBatch() Mutation {
	mm := &mutation{
		db: m,
		puts: llrb.New(),
		hashes: make(map[uint32]Hash),
	}
	return mm
}

var emptyHash [32]byte

func (m *mutation) GetHash(index uint32) []byte {
	h, ok := m.hashes[index]
	if ok {
		return h.hash[:]
	}
	if m.db == nil {
		return emptyHash[:]
	}
	return m.db.GetHash(index)
}

func (m *mutation) PutHash(index uint32, hash []byte) {
	var h Hash
	copy(h.hash[:], hash)
	m.hashes[index] = h
}

type table struct {
	db     Database
	prefix string
}

// NewTable returns a Database object that prefixes all keys with a given
// string.
func NewTable(db Database, prefix string) Database {
	return &table{
		db:     db,
		prefix: prefix,
	}
}

func (dt *table) Put(bucket, key []byte, value []byte) error {
	return dt.db.Put(bucket, append([]byte(dt.prefix), key...), value)
}

func (dt *table) PutS(bucket, key, value []byte, timestamp uint64) error {
	return dt.db.PutS(bucket, append([]byte(dt.prefix), key...), value, timestamp)
}

func (dt *table) MultiPut(tuples ...[]byte) error {
	panic("Not supported")
}

func (dt *table) Has(bucket, key []byte) (bool, error) {
	return dt.db.Has(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) Get(bucket, key []byte) ([]byte, error) {
	return dt.db.Get(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) GetAsOf(bucket, key []byte, timestamp uint64) ([]byte, error) {
	return dt.db.GetAsOf(bucket, append([]byte(dt.prefix), key...), timestamp)
}

func (dt *table) Walk(bucket, startkey []byte, fixedbits uint, walker WalkerFunc) error {
	return dt.db.Walk(bucket, append([]byte(dt.prefix), startkey...), fixedbits+uint(8*len(dt.prefix)), walker)
}

func (dt *table) WalkAsOf(bucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	return dt.db.WalkAsOf(bucket, append([]byte(dt.prefix), startkey...), fixedbits+uint(8*len(dt.prefix)), timestamp, walker)
}

func (dt *table) MultiWalkAsOf(bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	return dt.db.MultiWalkAsOf(bucket, startkeys, fixedbits, timestamp, walker)
}

func (dt *table) Delete(bucket, key []byte) error {
	return dt.db.Delete(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) DeleteTimestamp(timestamp uint64) error {
	return dt.db.DeleteTimestamp(timestamp)
}

func (dt *table) Close() {
	// Do nothing; don't close the underlying DB.
}

func (dt *table) NewBatch() Mutation {
	panic("Not supported")
}

func (dt *table) Size() int {
	return dt.db.Size()
}

func (dt *table) GetHash(index uint32) []byte {
	return dt.db.GetHash(index)
}

func (dt *table) PutHash(index uint32, hash []byte) {
	dt.db.PutHash(index, hash)
}

var EndSuffix []byte = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

func walkAsOf(db Getter, bucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	suffix := encodeTimestamp(timestamp)
	l := len(startkey)
	keyBuffer := make([]byte, l+len(EndSuffix))
	sl := l + len(suffix)
	err := db.Walk(bucket, startkey, fixedbits, func(k, v []byte) ([]byte, WalkAction, error) {
		if bytes.Compare(k[l:], suffix) >=0 {
			// Current key inserted at the given block suffix or earlier
			goOn, err := walker(k[:l], v)
			if err != nil || !goOn {
				return nil, WalkActionStop, err
			}
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], EndSuffix)
			return keyBuffer[:], WalkActionSeek, nil
		} else {
			// Current key inserted after the given block suffix, seek to it
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], suffix)
			return keyBuffer[:sl], WalkActionSeek, nil
		}
	})
	return err
}

// keys is sorted, prefixes strightly containing each other removed
func multiWalkAsOf(db Getter, bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	if len(startkeys) == 0 {
		return nil
	}
	suffix := encodeTimestamp(timestamp)
	l := len(startkeys[0])
	keyBuffer := make([]byte, l+len(EndSuffix))
	sl := l + len(suffix)
	keyIdx := 0 // What is the current key we are extracting
	fixedbytes, mask := bytesmask(fixedbits[keyIdx])
	if err := db.Walk(bucket, startkeys[0], 0, func (k, v []byte) ([]byte, WalkAction, error) {
		// Skip the keys preceeding the current keyIdx
		if fixedbits[keyIdx] > 0 {
			c := int(-1)
			for c != 0 {
				c = bytes.Compare(k[:fixedbytes-1], startkeys[keyIdx][:fixedbytes-1])
				if c == 0 {
					k1 := k[fixedbytes-1]&mask
					k2 := startkeys[keyIdx][fixedbytes-1]&mask
					if k1 < k2 {
						c = -1
					} else if k1 > k2 {
						c = 1
					}
				}
				if c < 0 {
					copy(keyBuffer, startkeys[keyIdx])
					copy(keyBuffer[l:], suffix)
					return keyBuffer[:sl], WalkActionSeek, nil
				} else if c > 0 {
					keyIdx++
					if _, err := walker(keyIdx, nil, nil); err != nil {
						return nil, WalkActionStop, err
					}					
					if keyIdx == len(startkeys) {
						return nil, WalkActionStop, nil
					}
					fixedbytes, mask = bytesmask(fixedbits[keyIdx])
				}
			}
		}
		if bytes.Compare(k[l:], suffix) >= 0 {
			// Current key inserted at the given block suffix or earlier
			goOn, err := walker(keyIdx, k[:l], v)
			if err != nil || !goOn {
				return nil, WalkActionStop, err
			}
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], EndSuffix)
			return keyBuffer[:], WalkActionSeek, nil
		} else {
			// Current key inserted after the given block suffix, seek to it
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], suffix)
			return keyBuffer[:sl], WalkActionSeek, nil
		}
	}); err != nil {
		return err
	}
	for keyIdx < len(startkeys) {
		keyIdx++
		if _, err := walker(keyIdx, nil, nil); err != nil {
			return err
		}	
	}
	return nil
}

