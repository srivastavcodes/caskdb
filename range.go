package cask

import (
	"time"

	wal "github.com/srivastavcodes/write-ahead-log"
)

// chunkValue checks whether the decoded chunk is valid or not by checking whether
// the rec type is deleted or expired, if not, the value of the rec is returned.
func (cdb *CaskDb) chunkValue(chunk []byte) []byte {
	rec := decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if rec.Type == LogRecordDeleted || rec.IsExpired(now) {
		return nil
	}
	return rec.Val
}

// AscendRange calls callback for each key/val pair between the range [k1, k2)
// in ascending order.
func (cdb *CaskDb) AscendRange(k1, k2 []byte, callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	handler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return true, nil
	}
	cdb.index.AscendRange(k1, k2, handler)
}

// DescendRange calls callback for each key/val pair between the range [k1, k2)
// in descending order.
func (cdb *CaskDb) DescendRange(k1, k2 []byte, callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	handler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return true, nil
	}
	cdb.index.DescendRange(k1, k2, handler)
}

// Ascend calls callback for each key/val pair in the db in ascending order [first, last].
func (cdb *CaskDb) Ascend(callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	cdb.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return true, nil
	})
}

// Descend calls callback for each key/val pair in the db in descending order [last, first].
func (cdb *CaskDb) Descend(callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	cdb.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return true, nil
	})
}

// AscendGreaterOrEqual calls callback for each key/val pair greater or equal
// than the given key [key, last].
func (cdb *CaskDb) AscendGreaterOrEqual(key []byte, callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	handler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return false, nil
	}
	cdb.index.AscendGreaterOrEqual(key, handler)
}

// DescendLessOrEqual calls callback for each key/val pair lesser or equal
// than the given key [key, first].
func (cdb *CaskDb) DescendLessOrEqual(key []byte, callback func(key, val []byte) (bool, error)) {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	handler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := cdb.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		val := cdb.chunkValue(chunk)
		if val != nil {
			return callback(key, val)
		}
		return false, nil
	}
	cdb.index.DescendLessOrEqual(key, handler)
}
