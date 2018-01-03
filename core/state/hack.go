package state

import (
    "bytes"
    "errors"
    "fmt"
    "io"
    "encoding/hex"
    "sort"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/trie"
)

type RecordingDatabase struct {
    db ethdb.Database
    get map[string][]byte
    put map[string]struct{}
    resolve map[string][]byte
}

func NewRecordingDatabase(db ethdb.Database) (*RecordingDatabase) {
    return &RecordingDatabase{
        db: db,
        get: make(map[string][]byte),
        put: make(map[string]struct{}),
        resolve: make(map[string][]byte),
    }
}

type AddressList []common.Address

func (al *AddressList) Len() int {
    return len(*al)
}

func (al *AddressList) Less(i, j int) bool {
    return bytes.Compare((*al)[i][:], (*al)[j][:]) < 0
}

func (al *AddressList) Swap(i, j int) {
    for k := 0; k < common.AddressLength; k++ {
        (*al)[i][k], (*al)[j][k] = (*al)[j][k], (*al)[i][k]
    }
}

func (s *StateDB) sortedDirtyAccounts() AddressList {
    al := make(AddressList, len(s.stateObjectsDirty))
    i := 0
    for k, _ := range s.stateObjectsDirty {
        al[i] = k
        i++
    }
    sort.Sort(&al)
    return al
}

func (s *StateDB) sortedAllAccounts() AddressList {
    al := make(AddressList, len(s.stateObjects))
    i := 0
    for k, _ := range s.stateObjects {
        al[i] = k
        i++
    }
    sort.Sort(&al)
    return al
}

type HashList []common.Hash

func (hl *HashList) Len() int {
    return len(*hl)
}

func (hl *HashList) Less(i, j int) bool {
    return bytes.Compare((*hl)[i][:], (*hl)[j][:]) < 0
}

func (hl *HashList) Swap(i, j int) {
    for k := 0; k < common.HashLength; k++ {
        (*hl)[i][k], (*hl)[j][k] = (*hl)[j][k], (*hl)[i][k]
    }
}

func (so *stateObject) sortedDirtyStorageKeys() HashList {
    hl := make(HashList, len(so.dirtyStorage))
    i := 0
    for k, _ := range so.dirtyStorage {
        hl[i] = k
        i++
    }
    sort.Sort(&hl)
    return hl
}

func (rdb *RecordingDatabase) Get(key []byte) ([]byte, error) {
    val, err := rdb.db.Get(key)
    stringKey := string(key)
    if _, put_exists := rdb.put[stringKey]; !put_exists {
        if _, get_exists := rdb.get[stringKey]; !get_exists {
            rdb.get[stringKey] = val
        }
    }
    return val, err
}

func (rdb *RecordingDatabase) Has(key []byte) (bool, error) {
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

func (rdb *RecordingDatabase) Resolve(start, limit []byte) ([]byte, error) {
    //fmt.Printf("Calling Resolve on recording: %s\n", hex.EncodeToString(start))
    val, err := rdb.db.Resolve(start, limit)
    // Check if it can be found in the puts
    keys := make([]string, len(rdb.put))
    i := 0
    for k, _ := range rdb.put {
        keys[i] = k
        i++
    }
    sort.Strings(keys)
    startStr := string(start)
    limitStr := string(limit)
    index := sort.Search(len(keys), func(i int) bool {
        return keys[i] >= startStr
    })
    for ; index < len(keys) && keys[index] < limitStr; index++ {
        if len(keys[index]) == len(start) {
            //fmt.Printf("Found in the puts: %s\n", hex.EncodeToString([]byte(keys[index])))
            return val, err
        }
    }
    if _, start_exists := rdb.resolve[startStr]; !start_exists {
        rdb.resolve[startStr] = val
        //fmt.Printf("Recorded Resolve: %s\n", hex.EncodeToString(start))
    }
    return val, err
}

type RecordingStateDatabase struct {
    rdb *RecordingDatabase
}

func NewRecordingStateDatabase(db ethdb.Database) (*RecordingStateDatabase) {
    return &RecordingStateDatabase{ NewRecordingDatabase(db) }
}

func (rsd *RecordingStateDatabase) OpenTrie(root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, rsd.rdb, MaxTrieCacheGen, []byte("AT"), blockNr)
}

func (rsd *RecordingStateDatabase) OpenStorageTrie(addr common.Address, root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, rsd.rdb, 0, addr[:], blockNr)
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

func (rsd *RecordingStateDatabase) CleanForNextBlock() {
    rsd.rdb.get = make(map[string][]byte)
    rsd.rdb.put = make(map[string]struct{})
    rsd.rdb.resolve = make(map[string][]byte)
}

func RecordingState(stateRoot common.Hash, rsd *RecordingStateDatabase, blockNr uint32) (*StateDB, error) {
    s, err := New(stateRoot, rsd, blockNr)
    if err != nil {
        return nil, err
    }
    return s, nil
}

func (st *StateDB) PrintTrie() {
    st.trie.PrintTrie()
}

type PlaybackDatabase struct {
    cache   map[string][]byte
}

func (pdb *PlaybackDatabase) EncodeRLP(w io.Writer) (err error) {
    if err = rlp.Encode(w, uint(len(pdb.cache))); err != nil {
        return err
    }
    keys := []string{}
    for k, _ := range pdb.cache {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _, k := range keys {
        v := pdb.cache[k]
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
        cache:  make(map[string][]byte),
    }
    for k, v := range rsd.rdb.resolve {
        pdb.cache[k] = v
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
    panic("Cache miss in playback database")
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

func (pdb *PlaybackDatabase) Resolve(start, limit []byte) ([]byte, error) {
    // Check if it can be found in the get/put cache
    keys := make([]string, len(pdb.cache))
    i := 0
    for k, _ := range pdb.cache {
        keys[i] = k
        i++
    }
    sort.Strings(keys)
    startStr := string(start)
    limitStr := string(limit)
    index := sort.Search(len(keys), func(i int) bool {
        return keys[i] >= startStr
    })
    for ; index < len(keys) && keys[index] < limitStr; index++ {
        if len(keys[index]) == len(start) {
            return pdb.cache[keys[index]], nil
        }
    }
    panic(fmt.Sprintf("Cache miss in playback database: %s\n", hex.EncodeToString(start)))
    return nil, errors.New("Cache miss in playback database")
}

type PlaybackStateDatabase struct {
    pdb *PlaybackDatabase
}

func NewPlaybackStateDatabase(pdb *PlaybackDatabase) (*PlaybackStateDatabase) {
    return &PlaybackStateDatabase{ pdb }
}

func (psd *PlaybackStateDatabase) OpenTrie(root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, psd.pdb, MaxTrieCacheGen, []byte("AT"), blockNr)
}

func (psd *PlaybackStateDatabase) OpenStorageTrie(addr common.Address, root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, psd.pdb, 0, addr[:], blockNr)
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

func PlaybackState(stateRoot common.Hash, pdb *PlaybackDatabase, blockNr uint32) (*StateDB, error) {
    s, err := New(stateRoot, NewPlaybackStateDatabase(pdb), blockNr)
    if err != nil {
        return nil, err
    }
    return s, nil
}
