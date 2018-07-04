// Copyright 2018 The go-ethereum Authors
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
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/petar/GoLLRB/llrb"
)

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


// Generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func rewindData(db Getter, timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	// Collect list of buckets and keys that need to be considered
	m := llrb.New()
	suffixSrc := encodeTimestamp(timestampSrc)
	if err := db.Walk(SuffixBucket, suffixSrc, 0, func (k, v []byte) ([]byte, WalkAction, error) {
		timestamp, bucket := decodeTimestamp(k)
		if timestamp <= timestampDst {
			return nil, WalkActionStop, nil
		}
		keycount := int(binary.BigEndian.Uint32(v))
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			m.ReplaceOrInsert(&PutItem{bucket: common.CopyBytes(bucket), key: common.CopyBytes(v[i:i+l]), value: nil})
			i += l
		}
		return nil, WalkActionNext, nil
	}); err != nil {
		return err
	}
	suffixDst := encodeTimestamp(timestampDst)
	it := m.NewSeekIterator()
	min, _ := m.Min().(*PutItem)
	if min == nil {
		return nil
	}
	var item *PutItem = it.SeekTo(min).(*PutItem)
	seeking := false
	for !seeking && item != nil {
		bucket := item.bucket
		startkey := make([]byte, len(item.key) + len(suffixDst))
		copy(startkey[:], item.key)
		copy(startkey[len(item.key):], suffixDst)
		seeking = true
		if err := db.Walk(bucket, startkey, 0, func (k, v []byte) ([]byte, WalkAction, error) {
			// Check if we found the "item" in the database
			if bytes.HasPrefix(k, item.key) {
				item.value = common.CopyBytes(v)
				item, _ = it.SeekTo(item).(*PutItem)
			} else {
				// Find the next item that could match
				for bytes.Compare(item.key, k[:len(item.key)]) < 0 {
					item, _ = it.SeekTo(item).(*PutItem)
					if item == nil || !bytes.Equal(item.bucket, bucket) {
						seeking = false
						return nil, WalkActionStop, nil
					}
				}
				if bytes.HasPrefix(k, item.key) && bytes.Compare(k[len(item.key):], suffixDst) <= 0 {
					item.value = common.CopyBytes(v)
					item, _ = it.SeekTo(item).(*PutItem)
				}
			}
			if item == nil {
				seeking = false
				return nil, WalkActionStop, nil
			}
			if !bytes.Equal(item.bucket, bucket) {
				seeking = false
				return nil, WalkActionStop, nil
			}
			wr := make([]byte, len(item.key) + len(suffixDst))
			copy(wr, item.key)
			copy(wr[len(item.key):], suffixDst)
			seeking = true
			return wr, WalkActionSeek, nil
		}); err != nil {
			return err
		}
	}
	m.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
		item := i.(*PutItem)
		df(item.bucket, item.key, item.value)
		return true
	})
	return nil
}

var testbucket = []byte("B")

func TestRewindData1Bucket() {
	db := NewMemDatabase()
	batch := db.NewBatch()

	htestbucket := append([]byte("h"), testbucket...)
	batch.PutS(testbucket, htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	batch.PutS(testbucket, htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	batch.PutS(testbucket, htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.PutS(testbucket, htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.PutS(testbucket, htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	batch.PutS(testbucket, htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	batch.PutS(testbucket, htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	batch.PutS(testbucket, htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.PutS(testbucket, htestbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.PutS(testbucket, htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	batch.PutS(testbucket, htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	batch.PutS(testbucket, htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	batch.PutS(testbucket, htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)
	if err := batch.Commit(); err != nil {
		fmt.Printf("Could not commit: %v\n", err)
		return
	}

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->2 %v\n", err)
		return
	}
	if count != 2 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 2, count)
		return
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->0 %v\n", err)
		return
	}
	if count != 7 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 7, count)
		return
	}
}

func TestRewindData2Bucket() {
	db := NewMemDatabase()
	batch := db.NewBatch()

	otherbucket := []byte("OB")
	htestbucket := append([]byte("h"), testbucket...)
	hotherbucket := append([]byte("h"), otherbucket...)

	batch.PutS(testbucket, htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	batch.PutS(testbucket, htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	batch.PutS(testbucket, htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.PutS(testbucket, htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.PutS(testbucket, htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	batch.PutS(testbucket, htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	batch.PutS(otherbucket, hotherbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	batch.PutS(otherbucket, hotherbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.PutS(otherbucket, hotherbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.PutS(otherbucket, hotherbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	batch.PutS(otherbucket, hotherbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	batch.PutS(testbucket, htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	batch.PutS(testbucket, htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)
	batch.Commit()

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->2 %v\n", err)
		return
	}
	if count != 2 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 2, count)
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->0 %v\n", err)
		return
	}
	if count != 11 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 11, count)
		return
	}
}
