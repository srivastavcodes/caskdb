package cask

import (
	"bytes"
	"log"
	"time"

	"github.com/srivastavcodes/caskdb/index"
)

// Iterator provides a database level iterator that provides methods to traverse
// over the key-value pairs in the database. It wraps the index.Iterator and adds
// functionality to retrieve the actual data from the database.
type Iterator struct {
	indexIter index.IndexIterator // indexIter is for traversing keys.
	db        *CaskDb             // db instance for retrieving values.
	currItem  *Item               // cached current item.
	opts      IteratorOptions     // user-defined configuration options.
	lastError error               // stores the last error encountered during iteration.
}

// Item represents a key-value pair in the database.
type Item struct {
	Key []byte
	Val []byte
}

func (cdb *CaskDb) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := cdb.index.Iterator(opts.Reverse)
	iter := &Iterator{
		db: cdb, indexIter: indexIter, opts: opts,
	}
	iter.skipToNext()
	return iter
}

// Rewind repositions the iterator to its initial state based on the iteration
// order. After repositioning, it automatically skips any invalid or expired
// entries.
func (i *Iterator) Rewind() {
	if i.db == nil || i.indexIter == nil {
		return
	}
	i.indexIter.Rewind()
	i.skipToNext()
}

// Seek positions the iterator at a specific key in the database. After seeking
// it automatically skips any invalid entries.
func (i *Iterator) Seek(key []byte) {
	if i.db == nil || i.indexIter == nil {
		return
	}
	i.indexIter.Seek(key)
	i.skipToNext()
}

// Next advances the iterator to the next valid entry in the database skipping
// any invalid or expired entries.
func (i *Iterator) Next() {
	if i.db == nil || i.indexIter == nil {
		return
	}
	i.indexIter.Next()
	i.skipToNext()
}

// Valid checks if the iterator is currently positioned at a valid entry.
func (i *Iterator) Valid() bool {
	if i.db == nil || i.indexIter == nil {
		return false
	}
	return i.indexIter.Valid()
}

// Item retrieves the current Item and is idempotent.
func (i *Iterator) Item() *Item {
	return i.currItem
}

// Close releases all resources associated with the iterator.
func (i *Iterator) Close() {
	if i.db == nil || i.indexIter == nil {
		return
	}
	i.indexIter.Close()
	i.indexIter = nil
	i.db = nil
}

// Err returns the last error encountered during iteration.
func (i *Iterator) Err() error { return i.lastError }

// skipToNext advances the iterator to the next valid entry that:
//   - matches the prefix filter if one is specified.
//   - has not expired.
//   - has not been marked for deletion.
//
// Returns a LogRecord of the valid entry or an error if any.
func (i *Iterator) skipToNext() {
	prefixLen := len(i.opts.Prefix)

	for i.indexIter.Valid() {
		key := i.indexIter.Key()
		if prefixLen > 0 {
			if prefixLen > len(key) || !bytes.Equal(i.opts.Prefix, key[:prefixLen]) {
				i.indexIter.Next()
				continue
			}
		}
		pos := i.indexIter.Value()
		if pos == nil {
			i.indexIter.Next()
			continue
		}
		// read the rec from the data file
		chunk, err := i.db.dataFiles.Read(pos)
		if err != nil {
			i.lastError = err
			if !i.opts.ContinueOnError {
				i.Close()
				return
			}
			log.Printf("Error reading data file at key %q: %v", key, err)
			i.indexIter.Next()
			continue
		}
		rec := decodeLogRecord(chunk)
		now := time.Now().UnixNano()

		if rec.Type == LogRecordDeleted || rec.IsExpired(now) {
			i.indexIter.Next()
			continue
		}
		i.currItem = &Item{
			Key: key, Val: rec.Val,
		}
		return
	}
}
