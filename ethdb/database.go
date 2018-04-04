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
func NewLDBDatabase(file string, cache int, handles int) (*LDBDatabase, error) {
	logger := log.New("database", file)

	// Ensure we have some minimal caching and file guarantees
	if cache < 16 {
		cache = 16
	}
	if handles < 16 {
		handles = 16
	}
	logger.Info("Allocated cache and file handles", "cache", cache, "handles", handles)

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

// Put puts the given key / value to the queue
func (db *LDBDatabase) PutS(bucket, key, suffix, value []byte) error {
	composite := make([]byte, len(key) + len(suffix))
	copy(composite, key)
	copy(composite[len(key):], suffix)
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

// First returns the first pair (k, v) where key is a prefix of key, or nil
// if there are not such (k, v)
func (db *LDBDatabase) First(bucket, key, suffix []byte) ([]byte, error) {
	start := make([]byte, len(key) + len(suffix))
	copy(start, key)
	copy(start[len(key):], suffix)
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			c := b.Cursor()
			k, v := c.Seek(start)
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

func (db *LDBDatabase) Walk(bucket, key []byte, keybits uint, walker WalkerFunc) error {
	keybytes := int((keybits + 7)/8)
	//start := make([]byte, keybytes)
	//copy(start, key[:keybytes])
	shiftbits := keybits&7
	mask := byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8-shiftbits)
		//start[keybytes-1] &= mask
	}
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		if keybits == 0 {
			for k, v := c.First(); k != nil; {
				nextkey, action, err := walker(k, v)
				if err != nil {
					return err
				}
				switch action {
				case WalkActionStop:
					break
				case WalkActionNext:
					k, v = c.Next()
				case WalkActionSeek:
					k, v = c.SeekTo(nextkey)
				}
			}
		} else {
			for k, v := c.Seek(key); k != nil && bytes.Equal(k[:keybytes-1], key[:keybytes-1]) && (k[keybytes-1]&mask)==(key[keybytes-1]&mask); {
				nextkey, action, err := walker(k, v)
				if err != nil {
					return err
				}
				switch action {
				case WalkActionStop:
					break
				case WalkActionNext:
					k, v = c.Next()
				case WalkActionSeek:
					k, v = c.SeekTo(nextkey)
				}
			}
		}
		return nil
	})
	return err
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
func (db *LDBDatabase) DeleteSuffix(suffix []byte) error {
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

func (m *mutation) PutS(bucket, key, suffix, value []byte) error {
	composite := make([]byte, len(key) + len(suffix))
	copy(composite, key)
	copy(composite[len(key):], suffix)
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

func (m *mutation) firstMem(bucket, key, suffix []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	start := make([]byte, len(key) + len(suffix))
	copy(start, key)
	copy(start[len(key):], suffix)
	var dat []byte
	m.puts.AscendGreaterOrEqual(&PutItem{bucket: bucket, key: start}, func(i llrb.Item) bool {
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

func (m *mutation) First(bucket, key, suffix []byte) ([]byte, error) {
	if value, ok := m.firstMem(bucket, key, suffix); ok {
		return value, nil
	} else {
		if m.db != nil {
			return m.db.First(bucket, key, suffix)
		}
	}
	return nil, nil
}

func (m *mutation) walkMem(bucket, key []byte, keybits uint, walker WalkerFunc) error {
	keybytes := int((keybits + 7)/8)
	//start := make([]byte, keybytes)
	//copy(start, key[:keybytes])
	shiftbits := keybits&7
	mask := byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8-shiftbits)
		//start[keybytes-1] &= mask
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for nextkey := key; nextkey != nil; {
		from := nextkey
		nextkey = nil
		var extErr error
		m.puts.AscendGreaterOrEqual(&PutItem{bucket: bucket, key: from}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			if !bytes.Equal(item.bucket, bucket) {
				return false
			}
			if item.value == nil {
				return true
			}
			if keybits > 0 && (!bytes.Equal(item.key[:keybytes-1], key[:keybytes-1]) || (item.key[keybytes-1]&mask)!=(key[keybytes-1]&mask)) {
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

func (m *mutation) Walk(bucket, key []byte, keybits uint, walker WalkerFunc) error {
	if m.db == nil {
		return m.walkMem(bucket, key, keybits, walker)
	} else {
		keybytes := int((keybits + 7)/8)
		//start := make([]byte, keybytes)
		//copy(start, key[:keybytes])
		shiftbits := keybits&7
		mask := byte(0xff)
		if shiftbits != 0 {
			mask = 0xff << (8-shiftbits)
			//start[keybytes-1] &= mask
		}
		m.mu.RLock()
		defer m.mu.RUnlock()
		start := key
		err := m.db.Walk(bucket, key, keybits, func (k, v []byte) ([]byte, WalkAction, error) {
			for nextkey := start; nextkey != nil; {
				from := nextkey
				nextkey = nil
				var extErr error
				m.puts.AscendRange(&PutItem{bucket: bucket, key: from}, &PutItem{bucket: bucket, key: k}, func (i llrb.Item) bool {
					item := i.(*PutItem)
					if item.value == nil {
						return true
					}
					if keybits > 0 && (!bytes.Equal(item.key[:keybytes-1], key[:keybytes-1]) || (item.key[keybytes-1]&mask)!=(key[keybytes-1]&mask)) {
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
					return nil, WalkActionStop, extErr
				}
			}
			i := m.puts.Get(&PutItem{bucket: bucket, key: k})
			if i != nil {
				// mutation data shadows database data
				item := i.(*PutItem)
				if item.value == nil {
					// item has been deleted in mutation, so we skip it from the database
					start = k
					return nil, WalkActionNext, nil
				}
				var err error
				var action WalkAction
				start, action, err = walker(item.key, item.value)
				if action == WalkActionNext {
					start = item.key
				}
				return start, action, err
			}
			var err error
			var action WalkAction
			start, action, err = walker(k, v)
			if action == WalkActionNext {
				start = k
			}
			return start, action, err
		})
		if err != nil {
			return err
		}
		for nextkey := start; nextkey != nil; {
			from := nextkey
			nextkey = nil
			var extErr error
			m.puts.AscendGreaterOrEqual(&PutItem{bucket: bucket, key: from}, func (i llrb.Item) bool {
				item := i.(*PutItem)
				if !bytes.Equal(item.bucket, bucket) {
					return false
				}
				if item.value == nil {
					return true
				}
				if keybits > 0 && (!bytes.Equal(item.key[:keybytes-1], key[:keybytes-1]) || (item.key[keybytes-1]&mask)!=(key[keybytes-1]&mask)) {
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
func (m *mutation) DeleteSuffix(suffix []byte) error {
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
	m.puts.AscendGreaterOrEqual(&PutItem{}, func (i llrb.Item) bool {
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
	m.puts.AscendGreaterOrEqual(&PutItem{}, func(i llrb.Item) bool {
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

func (dt *table) PutS(bucket, key, suffix, value []byte) error {
	return dt.db.PutS(bucket, append([]byte(dt.prefix), key...), suffix, value)
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

func (dt *table) First(bucket, key, suffix []byte) ([]byte, error) {
	return dt.db.First(bucket, append([]byte(dt.prefix), key...), suffix)
}

func (dt *table) Walk(bucket, key []byte, keybits uint, walker WalkerFunc) error {
	return dt.db.Walk(bucket, append([]byte(dt.prefix), key...), keybits+uint(8*len(dt.prefix)), walker)
}

func (dt *table) Delete(bucket, key []byte) error {
	return dt.db.Delete(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) DeleteSuffix(suffix []byte) error {
	return dt.db.DeleteSuffix(suffix)
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

func SuffixWalk(db Getter, bucket, key []byte, keybits uint, suffix, endSuffix []byte, walker func([]byte, []byte) (bool, error)) error {
	l := len(key)
	keyBuffer := make([]byte, l+len(endSuffix))
	suffixExt := make([]byte, len(endSuffix))
	copy(suffixExt, suffix)
	err := db.Walk(bucket, key, keybits, func(k, v []byte) ([]byte, WalkAction, error) {
		if bytes.Compare(k[l:], suffix) == 1 {
			// Current key inserted at the given block suffix or earlier
			goOn, err := walker(k[:l], v)
			if err != nil || !goOn {
				return nil, WalkActionStop, err
			}
			// Seek to beyond the current key
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], endSuffix)
			return keyBuffer, WalkActionSeek, nil
		} else {
			// Current key inserted after the given block suffix, seek to it
			copy(keyBuffer, k[:l])
			copy(keyBuffer[l:], suffixExt)
			return keyBuffer, WalkActionSeek, nil
		}
	})
	return err
}


func bytesmask(keybits uint) (int, byte) {
	keybytes := int((keybits + 7)/8)
	shiftbits := keybits&7
	mask := byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8-shiftbits)
	}
	return keybytes, mask
}

// keys is sorted, prefixes strightly containing each other removed
func MultiSuffixWalk(db Getter, bucket []byte, keys [][]byte, keybits []uint, suffix, endSuffix []byte, walker func([]byte, []byte) (bool, error)) error {
	l := len(keys[0])
	keyBuffer := make([]byte, l+len(endSuffix))
	newsection := true
	keyIdx := 0 // What is the current key we are extracting
	keybytes, mask := bytesmask(keybits[keyIdx])
	err := db.Walk(bucket, keys[0], 0, func (k, v []byte) ([]byte, WalkAction, error) {
		if newsection || (!newsection && !bytes.Equal(k[:l], keyBuffer[:l])) {
			// Do we need to switch to the next key and keybits
			if keybits[keyIdx] > 0 && (!bytes.Equal(k[:keybytes-1], keys[keyIdx][:keybytes-1]) || (k[keybytes-1]&mask)!=(keys[keyIdx][keybytes-1]&mask)) {
				keyIdx++
				if keyIdx == len(keys) {
					return nil, WalkActionStop, nil
				}
				keybytes, mask = bytesmask(keybits[keyIdx])
				copy(keyBuffer, keys[keyIdx])
				copy(keyBuffer[l:], suffix)
			}
			// New "section", the first entry for a prefix
			if bytes.Compare(k[l:], suffix) == 1 || bytes.HasPrefix(k[l:], suffix) {

			} else {
				newsection = false
				return nil, WalkActionNext, nil
			}
		}
		return nil, WalkActionStop, nil // TODO - REPLACE
	})
	return err
}

