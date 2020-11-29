package main

const (
	offset64 uint64 = 14695981039346656037
	prime64         = 1099511628211
)

func fnvHash64(data []byte) uint64 {
	hash := offset64
	// each byte = uint8 / time
	for _, c := range data {
		hash *= prime64
		hash ^= uint64(c)
	}
	return hash
}

// hashtbl is a simple hashtable build on map[uint64]uint64
type hashtbl struct {
	hashTable map[uint64]uint64
}

// NewHashTable return a hashtable build on map[uint64]uint64 with the size of size
// *hashtbl is a pointer to the hashtable
func NewHashTable(size int) *hashtbl {
	ht := new(hashtbl)
	ht.hashTable = make(map[uint64]uint64, size)
	return ht
}

func (ht *hashtbl) Put(key []byte, value uint64) {
	hashKey := fnvHash64(key)
	// trick for this test
	// need exchange,each match +1
	// no need exchange,each match + relation0.col0 value
	ht.hashTable[hashKey] += value
}

func (ht *hashtbl) Get(key []byte) uint64 {
	hashKey := fnvHash64(key)
	return ht.hashTable[hashKey]
}
