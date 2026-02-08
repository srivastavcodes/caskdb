package index

import wal "github.com/srivastavcodes/write-ahead-log"

// Indexer is an interface for indexing key and position. It is used to store
// the key and position of the data in the Wal. The index will be rebuilt
// when the database is opened.
// You can implement your own Indexer by implementing this interface.
type Indexer interface {
	BasicIndexer
	Iterator
	Traverser
}

// BasicIndexer is an interface for indexing key and position. It is used to store
// the key and position of the data in the Wal. The index will be rebuilt when the
// database is opened.
type BasicIndexer interface {
	// Put puts the key and position into the index, if the exact key/val
	// pair already exists, it gets replaced, and the old one is returned.
	Put(key []byte, pos *wal.ChunkPosition) *wal.ChunkPosition

	// Get gets the position of the key in the wal.
	Get(key []byte) *wal.ChunkPosition

	// Delete deletes the index of the key if exists, otherwise it's a noOp.
	// Returns the position of the key deleted, else returns nil.
	Delete(key []byte) (*wal.ChunkPosition, bool)

	// Size represents the number of keys in the index.
	Size() int
}

// Iterator is an interface for creating an index iterator. It is used to scan
// keys in order, optionally in reverse. Implementations should return a
// read-only cursor over index entries.
type Iterator interface {
	// Iterator returns an index iterator.
	Iterator(reverse bool) IndexIterator
}

// Traverser is an interface for range and full scans over the index. It is used
// to iterate keys in ascending or descending order with optional bounds. The
// handler controls early termination by returning false.
type Traverser interface {
	// AscendRange iterates over items within [start, end], in ascending order
	// invoking the handler function for each item. Stops if handlerFn returns
	// false.
	AscendRange(start, end []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// DescendRange iterates over items within [start, end], in descending order
	// invoking the handler function for each item. Stops if handlerFn returns false.
	DescendRange(start, end []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// Ascend iterates over items in ascending order and invokes the handler
	// function for each item. If the handler function returns false, the
	// iteration stops.
	Ascend(handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// Descend iterates over items in descending order and invokes the handler
	// function for each item. If the handler function returns false, the
	// iteration stops.
	Descend(handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// AscendGreaterOrEqual iterates over items starting from the given key
	// (keys >= givenKey), in ascending order. Stops if handlerFn return false.
	AscendGreaterOrEqual(key []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// DescendLessOrEqual iterates over items starting from the given key
	// (keys >= givenKey), in descending order. Stops if handlerFn return false.
	DescendLessOrEqual(key []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error))
}

type IndexerType byte

const (
	BTree IndexerType = iota
)

// indexType is meant to be changed into the type you implement.
var indexType = BTree

func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}

// IndexIterator represents a generic index iterator interface.
type IndexIterator interface {
	// Rewind resets the iterator to its initial position.
	Rewind()

	// Next moves the element to the next element.
	Next()

	// Seek positions the cursor to the element with the specified key.
	Seek(key []byte)

	// Valid checks if the iterator is still valid for reading.
	Valid() bool

	// Key returns the key of the current element.
	Key() []byte

	// Value returns the value of the current element.
	Value() *wal.ChunkPosition

	// Close releases the resources associated with the iterator.
	Close()
}
