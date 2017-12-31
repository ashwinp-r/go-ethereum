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
    getfirst map[string]map[string][]byte
}

func NewRecordingDatabase(db ethdb.Database) (*RecordingDatabase) {
    return &RecordingDatabase{
        db: db,
        get: make(map[string][]byte),
        put: make(map[string]struct{}),
        getfirst: make(map[string]map[string][]byte),
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

func (rdb *RecordingDatabase) GetFirst(start []byte, limit []byte, suffix []byte) ([]byte, error) {
    val, err := rdb.db.GetFirst(start, limit, suffix)
    if err == nil {
        // Check if it can be found in the puts
        /*
        keys := make([]string, len(rdb.put))
        i := 0
        for k, _ := range rdb.put {
            keys[i] = k
            i++
        }
        sort.Strings(keys)
        */
        startStr := string(start)
        /*
        limitStr := string(limit)
        index := sort.Search(len(keys), func(i int) bool {
            return keys[i] >= startStr && keys[i] <= limitStr
        })
        if index == -1 {
            return nil, errors.New("key not found in range")
        }
        for i := index; i < len(keys) && keys[i] >= startStr && keys[i] <= limitStr; i++ {
            if len(keys[i])>=len(suffix) && bytes.Compare([]byte(keys[i])[len(keys[i])-len(suffix):], suffix) == 0 {
                // If found in the puts, do not record it
                return val, nil
            }
        }
        */
        var starts map[string][]byte
        var start_exists bool
        if starts, start_exists = rdb.getfirst[startStr]; !start_exists {
            starts = make(map[string][]byte)
            rdb.getfirst[startStr] = starts
        }
        suffixStr := string(suffix)
        if _, suffix_exists := starts[suffixStr]; !suffix_exists {
            starts[suffixStr] = val
        }
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

func (rsd *RecordingStateDatabase) OpenStorageTrie(addrHash, root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, rsd.rdb, 0, addrHash[:], blockNr)
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
    cache2  map[string]map[string][]byte
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
    if err = rlp.Encode(w, uint(len(pdb.cache2))); err != nil {
        return err
    }
    keys = []string{}
    for k, _ := range pdb.cache2 {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _, k := range keys {
        if err = rlp.Encode(w, k); err != nil {
            return err
        }
        v := pdb.cache2[k]
        if err = rlp.Encode(w, uint(len(v))); err != nil {
            return err
        }
        keys2 := []string{}
        for k2, _ := range v {
            keys2 = append(keys2, k2)
        }
        sort.Strings(keys2)
        for _, k2 := range keys2 {
            if err = rlp.Encode(w, k2); err != nil {
                return err
            }
            v2 := v[k2]
            if err = rlp.Encode(w, string(v2)); err != nil {
                return err
            }
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
    if err = s.Decode(&size); err != nil {
        return err
    }
    for i := 0; i < int(size); i++ {
        var k string
        if err = s.Decode(&k); err != nil {
            return err
        }
        v := make(map[string][]byte)
        var size2 uint32
        if err = s.Decode(&size2); err != nil {
            return err
        }
        for j := 0; j < int(size2); j++ {
            var k2 string
            if err = s.Decode(&k2); err != nil {
                return err
            }
            var v2 string
            if err = s.Decode(&v2); err != nil {
                return err
            }
            v[k2] = []byte(v2)
        }
        pdb.cache2[k] = v
    }
    return err
}

func NewPlaybackDatabase(rsd *RecordingStateDatabase) *PlaybackDatabase {
    pdb := &PlaybackDatabase{
        cache:  make(map[string][]byte),
        cache2: make(map[string]map[string][]byte),
    }
    for k, v := range rsd.rdb.get {
        pdb.cache[k] = v
    }
    for k, v := range rsd.rdb.getfirst {
        m := make(map[string][]byte)
        for k2, v2 := range v {
            m[k2] = v2
        }
        pdb.cache2[k] = m
    }
    return pdb
}

func EmptyPlaybackDatabase() *PlaybackDatabase {
    return &PlaybackDatabase{ cache: make(map[string][]byte), cache2: make(map[string]map[string][]byte) }
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

func (pdb *PlaybackDatabase) GetFirst(start []byte, limit []byte, suffix []byte) ([]byte, error) {
    // Check if it can be found in the get/put cache
    /*
    keys := make([]string, len(pdb.cache))
    i := 0
    for k, _ := range pdb.cache {
        keys[i] = k
        i++
    }
    sort.Strings(keys)
    */
    startStr := string(start)
    /*
    limitStr := string(limit)
    index := sort.Search(len(keys), func(i int) bool {
        return keys[i] >= startStr && keys[i] <= limitStr
    })
    if index == -1 {
        return nil, errors.New("key not found in range")
    }
    for i := index; i < len(keys) && keys[i] >= startStr && keys[i] <= limitStr; i++ {
        if len(keys[i])>=len(suffix) && bytes.Compare([]byte(keys[i])[len(keys[i])-len(suffix):], suffix) == 0 {
            return pdb.cache[keys[i]], nil
        }
    }
    */
    if starts, start_exists := pdb.cache2[startStr]; start_exists {
        suffixKey := string(suffix)
        if val, exists := starts[suffixKey]; exists {
            return val, nil
        }
    }
    panic(fmt.Sprintf("Cache miss in playback database: %s %s\n", hex.EncodeToString(start), hex.EncodeToString(suffix)))
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

func (psd *PlaybackStateDatabase) OpenStorageTrie(addrHash, root common.Hash, blockNr uint32) (Trie, error) {
    return trie.NewSecure(root, psd.pdb, 0, addrHash[:], blockNr)
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
