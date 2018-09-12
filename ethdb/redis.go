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
	"sync"

	"github.com/go-redis/redis"

	"github.com/ethereum/go-ethereum/log"
)

type RedisDatabase struct {
	addr string      // network address for reporting
	client *redis.Client  // Redis client handle
	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	log log.Logger // Contextual logger tracking the database path
}

func NewRedisDatabase(address string) (*RedisDatabase, error) {
	logger := log.New("database", address)
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	log.Info("Connected to redis", "address", address, "pong", pong)
	return &RedisDatabase{
		addr:  address,
		client:  client,
		log: logger,
	}, nil
}

func (rd *RedisDatabase) Put(bucket, key []byte, value []byte) error {
	panic("Not supported")
}

func (rd *RedisDatabase) PutS(hBucket, key, value []byte, timestamp uint64) error {
	panic("Not supported")
}

func (rd *RedisDatabase) MultiPut(tuples ...[]byte) error {
	panic("Not supported")
}

func (rd *RedisDatabase) Has(bucket, key []byte) (bool, error) {
	panic("Not supported")
}

func (rd *RedisDatabase) Get(bucket, key []byte) ([]byte, error) {
	panic("Not supported")
}

func (rd *RedisDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	panic("Not supported")
}

func (rd *RedisDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker WalkerFunc) error {
	panic("Not supported")
}

func (rd *RedisDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (rd *RedisDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (rd *RedisDatabase) MultiWalkAsOf(hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not supported")
}

func (rd *RedisDatabase) RewindData(timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	panic("Not supported")
}

func (rd *RedisDatabase) Delete(bucket, key []byte) error {
	panic("Not supported")
}

func (rd *RedisDatabase) DeleteTimestamp(timestamp uint64) error {
	panic("Not supported")
}

func (rd *RedisDatabase) Close() {
	// Do nothing; don't close the underlying DB.
}

func (rd *RedisDatabase) NewBatch() Mutation {
	panic("Not supported")
}

func (rd *RedisDatabase) Size() int {
	panic("Not supported")
}

func (rd *RedisDatabase) GetHash(index uint32) []byte {
	panic("Not supported")
}

func (rd *RedisDatabase) PutHash(index uint32, hash []byte) {
	panic("Not supported")
}
