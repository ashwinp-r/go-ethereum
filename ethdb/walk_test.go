package ethdb

import (
	"fmt"
	"testing"
)

var testbucket = []byte("B")

// multiWalkAsOf(db Getter, varKeys bool, bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error))
func TestEmptyWalk(t *testing.T) {
	db := NewMemDatabase()
	count := 0
	err := multiWalkAsOf(db, false, testbucket, [][]byte{}, []uint{}, 0, func(i int, k []byte, v []byte) (bool, error) {
		count++
		return true, nil
	})
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected no keys, got %d", count)
	}
}

func TestWalk2(t *testing.T) {
	db := NewMemDatabase()
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	count := 0
	err := multiWalkAsOf(
		db,
		false,
		testbucket,
		[][]byte{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
		[]uint{10},
		0,
		func(i int, k []byte, v []byte) (bool, error) {
			count++
			return true, nil
		},
	)
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 keys, got %d", count)
	}
}

func TestWalkN(t *testing.T) {
	db := NewMemDatabase()

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	db.PutS(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	count := make(map[int]int)
	err := multiWalkAsOf(
		db,
		false,
		testbucket,
		[][]byte{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
		[]uint{40, 16},
		0,
		func(i int, k []byte, v []byte) (bool, error) {
			//fmt.Printf("%d %s %s\n", i, k, v)
			count[i]++
			return true, nil
		},
	)
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if len(count) != 2 {
		t.Errorf("Expected 2 key groups, got %d", len(count))
	}
	if count[0] != 2 {
		t.Errorf("Expected 2 keys in group 0, got %d", count[0])
	}
	if count[1] != 3 {
		t.Errorf("Expected 3 keys in group 1, got %d", count[1])
	}
}
