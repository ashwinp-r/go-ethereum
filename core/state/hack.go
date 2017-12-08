package state

import (
    "errors"
    "fmt"
    "io"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/trie"
)

type RecordingDatabase struct {
    db ethdb.Database
    get map[string][]byte
    put map[string]struct{}
}

func NewRecordingDatabase(db ethdb.Database) (*RecordingDatabase) {
    return &RecordingDatabase{
        db: db,
        get: make(map[string][]byte),
        put: make(map[string]struct{}),
    }
}

func (rdb *RecordingDatabase) Get(key []byte) ([]byte, error) {
    val, err := rdb.db.Get(key)
    if err == nil {
        stringKey := string(key)
        if _, put_exists := rdb.put[stringKey]; !put_exists {
            if _, get_exists := rdb.get[stringKey]; !get_exists {
                rdb.get[stringKey] = val
            }
        }
    }
    return val, err
}

func (rdb *RecordingDatabase) Has(key []byte) (bool, error) {
    //return rdb.db.Has(key)
    panic("Has is not used")
    return false, errors.New("Has is not used")
}

func (rdb *RecordingDatabase) Put(key, value []byte) error {
    err := rdb.db.Put(key, value)
    if err == nil {
        stringKey := string(key)
        if _, put_exists := rdb.put[stringKey]; !put_exists {
            rdb.put[string(key)] = struct{}{}
        }
    }
    return err
}

type RecordingStateDatabase struct {
    rdb *RecordingDatabase
}

func NewRecordingStateDatabase(db ethdb.Database) (*RecordingStateDatabase) {
    return &RecordingStateDatabase{ NewRecordingDatabase(db) }
}

func (rsd *RecordingStateDatabase) OpenTrie(root common.Hash) (Trie, error) {
    return trie.NewSecure(root, rsd.rdb, MaxTrieCacheGen)
}

func (rsd *RecordingStateDatabase) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
    return trie.NewSecure(root, rsd.rdb, 0)
}

func (rsd *RecordingStateDatabase) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
    return rsd.rdb.Get(codeHash[:])
}

func (rsd *RecordingStateDatabase) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
    code, err := rsd.ContractCode(addrHash, codeHash)
    return len(code), err
}

func (rsd *RecordingStateDatabase) CopyTrie(t Trie) Trie {
    switch t := t.(type) {
    case *trie.SecureTrie:
        return t.Copy()
    default:
        panic(fmt.Errorf("unknown trie type %T", t))
    }    
}

func RecordingState(stateRoot common.Hash, rsd *RecordingStateDatabase) (*StateDB, error) {
    s, err := New(stateRoot, rsd)
    if err != nil {
        return nil, err
    }
    return s, nil
}

type PlaybackDatabase struct {
    cache map[string][]byte
}

func (pdb *PlaybackDatabase) EncodeRLP(w io.Writer) (err error) {
    if err = rlp.Encode(w, uint(len(pdb.cache))); err != nil {
        return err
    }
    for k, v := range pdb.cache {
        if err = rlp.Encode(w, k); err != nil {
            return err
        }
        if err = rlp.Encode(w, string(v)); err != nil {
            return err
        }
    }
    return nil
}

func (pdb *PlaybackDatabase) DecodeRLP(s *rlp.Stream) (err error) {
    var size uint
    if err = s.Decode(&size); err != nil {
        return err
    }
    for i := 0; i < int(size); i++ {
        var k string
        if err = s.Decode(&k); err != nil {
            return err
        }
        var v string
        if err = s.Decode(&v); err != nil {
            return err
        }
        pdb.cache[k] = []byte(v)
    }
    return err
}

func NewPlaybackDatabase(rsd *RecordingStateDatabase) *PlaybackDatabase {
    pdb := &PlaybackDatabase{
        cache: make(map[string][]byte),
    }
    for k, v := range rsd.rdb.get {
        pdb.cache[k] = v
    }
    return pdb
}

func EmptyPlaybackDatabase() *PlaybackDatabase {
    return &PlaybackDatabase{ cache: make(map[string][]byte) }
}

func (pdb *PlaybackDatabase) Get(key []byte) (value []byte, err error) {
    stringKey := string(key)
    if val, exists := pdb.cache[stringKey]; exists {
        return val, nil
    }
    return nil, errors.New("Cache miss in playback database")
}

func (pdb *PlaybackDatabase) Has(key []byte) (bool, error) {
    panic("Has is not used")
    return false, errors.New("Has is not used")
}

func (pdb *PlaybackDatabase) Put(key, value []byte) error {
    stringKey := string(key)
    pdb.cache[stringKey] = value
    return nil
}

type PlaybackStateDatabase struct {
    pdb *PlaybackDatabase
}

func NewPlaybackStateDatabase(pdb *PlaybackDatabase) (*PlaybackStateDatabase) {
    return &PlaybackStateDatabase{ pdb }
}

func (psd *PlaybackStateDatabase) OpenTrie(root common.Hash) (Trie, error) {
    return trie.NewSecure(root, psd.pdb, MaxTrieCacheGen)
}

func (psd *PlaybackStateDatabase) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
    return trie.NewSecure(root, psd.pdb, 0)
}

func (psd *PlaybackStateDatabase) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
    return psd.pdb.Get(codeHash[:])
}

func (psd *PlaybackStateDatabase) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
    code, err := psd.ContractCode(addrHash, codeHash)
    return len(code), err
}

func (psd *PlaybackStateDatabase) CopyTrie(t Trie) Trie {
    switch t := t.(type) {
    case *trie.SecureTrie:
        return t.Copy()
    default:
        panic(fmt.Errorf("unknown trie type %T", t))
    }    
}

func PlaybackState(stateRoot common.Hash, pdb *PlaybackDatabase) (*StateDB, error) {
    s, err := New(stateRoot, NewPlaybackStateDatabase(pdb))
    if err != nil {
        return nil, err
    }
    return s, nil
}
