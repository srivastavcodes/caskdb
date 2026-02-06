package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	wal "github.com/srivastavcodes/write-ahead-log"
)

var _ Indexer = (*MemoryBTree)(nil)

// MemoryBTree is a memory-based btree implementation of the Indexer interface.
// It is a wrapper around the google/btree.
type MemoryBTree struct {
	mu    sync.RWMutex
	bTree *btree.BTree
}

type item struct {
	key []byte
	pos *wal.ChunkPosition
}

// memoryBTreeIterator represents a B-Tree index iterator.
type memoryBTreeIterator struct {
	bTree   *btree.BTree // underlying B-Tree implementation.
	current *item        // current element being traversed.
	valid   bool         // indicates if the iterator is valid.
	reverse bool         // indicates whether traversal should be in descending order.
}

func newBTree() *MemoryBTree {
	return &MemoryBTree{bTree: btree.New(32)}
}

func newMemoryBTreeIterator(bTree *btree.BTree, reverse bool) *memoryBTreeIterator {
	var currItem *item
	var valid bool

	if bTree.Len() > 0 {
		if reverse {
			currItem = bTree.Max().(*item)
		} else {
			currItem = bTree.Min().(*item)
		}
		valid = true
	}
	return &memoryBTreeIterator{
		bTree:   bTree.Clone(),
		current: currItem,
		valid:   valid, reverse: reverse,
	}
}

func (i *item) Less(bItem btree.Item) bool {
	if bItem == nil {
		return false
	}
	return bytes.Compare(i.key, bItem.(*item).key) < 0
}

func (mbt *MemoryBTree) Put(key []byte, pos *wal.ChunkPosition) *wal.ChunkPosition {
	mbt.mu.Lock()
	defer mbt.mu.Unlock()

	oldItem := mbt.bTree.ReplaceOrInsert(&item{
		key: key,
		pos: pos,
	})
	if oldItem != nil {
		return oldItem.(*item).pos
	}
	return nil
}

func (mbt *MemoryBTree) Get(key []byte) *wal.ChunkPosition {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	pos := mbt.bTree.Get(&item{key: key})
	if pos != nil {
		return pos.(*item).pos
	}
	return nil
}

func (mbt *MemoryBTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	mbt.mu.Lock()
	defer mbt.mu.Unlock()

	pos := mbt.bTree.Delete(&item{key: key})
	if pos != nil {
		return pos.(*item).pos, true
	}
	return nil, false
}

func (mbt *MemoryBTree) Size() int {
	return mbt.bTree.Len()
}

func (mbt *MemoryBTree) Iterator(reverse bool) IndexIterator {
	if mbt.bTree == nil {
		return nil
	}
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()
	return newMemoryBTreeIterator(mbt.bTree, reverse)
}

func (mbt *MemoryBTree) AscendRange(start, end []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.AscendRange(&item{key: start}, &item{key: end}, func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbt *MemoryBTree) DescendRange(start, end []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.DescendRange(&item{key: start}, &item{key: end}, func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbt *MemoryBTree) Ascend(handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.Ascend(func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbt *MemoryBTree) Descend(handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.Descend(func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbt *MemoryBTree) AscendGreaterOrEqual(key []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbt *MemoryBTree) DescendLessOrEqual(key []byte, handlerFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	mbt.mu.RLock()
	defer mbt.mu.RUnlock()

	mbt.bTree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		ok, err := handlerFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return ok
	})
}

func (mbi *memoryBTreeIterator) Rewind() {
	if mbi.bTree == nil || mbi.bTree.Len() == 0 {
		return
	}
	if mbi.reverse {
		mbi.current = mbi.bTree.Max().(*item)
	} else {
		mbi.current = mbi.bTree.Min().(*item)
	}
	mbi.valid = true
}

func (mbi *memoryBTreeIterator) Seek(key []byte) {
	if mbi.bTree == nil || !mbi.valid {
		return
	}
	// key we are looking for where iteration should stop in bTree.
	seekItem := &item{key: key}
	mbi.valid = false

	if mbi.reverse {
		mbi.bTree.DescendLessOrEqual(seekItem, func(i btree.Item) bool {
			mbi.current = i.(*item)
			mbi.valid = true
			return false
		})
	} else {
		mbi.bTree.AscendGreaterOrEqual(seekItem, func(i btree.Item) bool {
			mbi.current = i.(*item)
			mbi.valid = true
			return false
		})
	}
}

func (mbi *memoryBTreeIterator) Next() {
	if mbi.bTree == nil || !mbi.valid {
		return
	}
	mbi.valid = false

	if mbi.reverse {
		mbi.bTree.DescendLessOrEqual(mbi.current, func(i btree.Item) bool {
			if !i.(*item).Less(mbi.current) {
				return true
			}
			mbi.current = i.(*item)
			mbi.valid = true
			return false
		})
	} else {
		mbi.bTree.AscendGreaterOrEqual(mbi.current, func(i btree.Item) bool {
			if !mbi.current.Less(i.(*item)) {
				return true
			}
			mbi.current = i.(*item)
			mbi.valid = true
			return false
		})
	}
	if !mbi.valid {
		mbi.current = nil
	}
}

func (mbi *memoryBTreeIterator) Key() []byte {
	if !mbi.valid {
		return nil
	}
	return mbi.current.key
}

func (mbi *memoryBTreeIterator) Valid() bool {
	return mbi.valid
}

func (mbi *memoryBTreeIterator) Value() *wal.ChunkPosition {
	if !mbi.valid {
		return nil
	}
	return mbi.current.pos
}

func (mbi *memoryBTreeIterator) Close() {
	mbi.bTree.Clear(true)
	mbi.bTree = nil
	mbi.current = nil
	mbi.valid = false
}
