package cask

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/srivastavcodes/caskdb/utils"
	"github.com/valyala/bytebufferpool"
)

// Batch is a batch of operations on the database. If readonly is true, you can
// only perform the get operation on Batch by using the Get method. An error
// will be returned if you try to use the Put or Delete method.
//
// The data will be written to the database permanently after you call Commit.
//
// Batch uses sync.RWMutex ensuring single-writer, multiple-readers semantics at
// the same time. You are not allowed other db operations before the batch is
// either committed or rolled back.
//
// Batch is not a transaction, it does not guarantee isolation, but it does
// guarantee Atomicity, Consistency, and Durability (if the Sync option is true).
//
// You must call Commit or Rollback method after using batch operations, if not
// done so, the database will be locked in an undocumented way.
type Batch struct {
	rwm           sync.RWMutex
	db            *CaskDb
	pendingWrites []*LogRecord     // The data to be written.
	logRecords    map[uint64][]int // Map of [hasKey][indexes] for faster lookup to pending writes.
	opts          BatchOptions
	commited      bool // whether the batch has been committed.
	rolledBack    bool // whether the batch has been rolled back.
	batchId       atomic.Uint64
	buffers       []*bytebufferpool.ByteBuffer
}

func (cdb *CaskDb) NewBatch(opts BatchOptions) *Batch {
	b := &Batch{
		db: cdb, opts: opts,
	}
	b.lock()
	return b
}

// defaultNewBatch gets passed to the buffer pool to be returned as a default
// Batch.
func defaultNewBatch() any {
	return &Batch{opts: DefaultBatchOptions}
}

// Put adds a key/val pair to the batch for writing and returns an error if any.
func (b *Batch) Put(key, val []byte) error {
	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	b.lock()
	defer b.unlock()

	record := b.lookupExistingKey(key)
	if record == nil {
		// if the key does not exist in pendingWrites, get a new record
		// from the pool; the record will be put back when the batch is
		// committed or rolled back.
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendToPendingWrites(key, record)
	}
	record.Key, record.Val = key, val

	record.Type, record.Expire = LogRecordNormal, 0
	return nil
}

// PutWithTTL adds a key/val pair to the batch with a ttl for writing and returns
// an error if any.
func (b *Batch) PutWithTTL(key, val []byte, ttl time.Duration) error {
	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	b.lock()
	defer b.unlock()

	record := b.lookupExistingKey(key)
	if record == nil {
		// if the key does not exist in pendingWrites, get a new record
		// from the pool; the record will be put back when the batch is
		// committed or rolled back.
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendToPendingWrites(key, record)
	}
	record.Key, record.Val = key, val

	record.Type = LogRecordNormal
	record.Expire = uint64(time.Now().Add(ttl).UnixNano())

	return nil
}

// Get retrieves the LogRecord associated with the key and validates it; if valid,
// returns the value of the LogRecord for the key.
func (b *Batch) Get(key []byte) ([]byte, error) {
	if b.db.closed {
		return nil, ErrDbClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	now := time.Now().UnixNano()
	// retrieve value from pending writes if exists.
	b.lock()
	record := b.lookupExistingKey(key)
	b.unlock()

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Val, nil
	}
	pos := b.db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	enc, err := b.db.dataFiles.Read(pos)
	if err != nil {
		return nil, fmt.Errorf("couldn't get record from datafile: %w", err)
	}
	record = decodeLogRecord(enc)

	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrRecordExpired
	}
	return record.Val, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	b.lock()
	defer b.unlock()

	// we'll need type and key for deletion later on, remaining mark zero val.
	record := b.lookupExistingKey(key)
	if record != nil {
		record.Type = LogRecordDeleted
		record.Val = nil
		record.Expire = 0
	} else {
		record = &LogRecord{
			Key: key, Type: LogRecordDeleted,
		}
		b.appendToPendingWrites(key, record)
	}
	return nil
}

// Exists checks if a key exists in the database.
func (b *Batch) Exists(key []byte) (bool, error) {
	if b.db.closed {
		return false, ErrDbClosed
	}
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	now := time.Now().UnixNano()

	b.lock()
	defer b.unlock()

	record := b.lookupExistingKey(key)
	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}
	pos := b.db.index.Get(key)
	if pos == nil {
		return false, nil
	}
	enc, err := b.db.dataFiles.Read(pos)
	if err != nil {
		return false, fmt.Errorf("couldn't get record from datafile: %w", err)
	}
	record = decodeLogRecord(enc)

	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// lookupExistingKey checks if the key already exists, if yes, returns the
// LogRecord associated with the key.
func (b *Batch) lookupExistingKey(key []byte) *LogRecord {
	if len(b.logRecords) == 0 {
		return nil
	}
	haskey := utils.MemHash(key)
	// iterate over the bucket of entries that resulted in the same hash.
	for _, entry := range b.logRecords[haskey] {
		if bytes.Equal(b.pendingWrites[entry].Key, key) {
			return b.pendingWrites[entry]
		}
	}
	return nil
}

// appendToPendingWrites adds a new record to pendingWrites, and the map storing
// the records' offsets by hash of the given key.
func (b *Batch) appendToPendingWrites(key []byte, record *LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
	if b.logRecords == nil {
		b.logRecords = make(map[uint64][]int)
	}
	hashkey := utils.MemHash(key)
	// stores the offset of the record to the hashkey of the key.
	// eg: "hashkey" : {7, 21, 34}; haskey will be obviously different.
	b.logRecords[hashkey] = append(b.logRecords[hashkey], len(b.pendingWrites)-1)
}

func (b *Batch) lock() {
	if b.opts.ReadOnly {
		b.rwm.RLock()
	} else {
		b.rwm.Lock()
	}
}

func (b *Batch) unlock() {
	if b.opts.ReadOnly {
		b.rwm.RUnlock()
	} else {
		b.rwm.Unlock()
	}
}

func (b *Batch) init(rdonly, sync bool, db *CaskDb) {
	b.opts.ReadOnly = rdonly
	b.opts.Sync = sync
	b.db = db
	b.lock()
}

func (b *Batch) reset() {
	b.db = nil

	b.pendingWrites = b.pendingWrites[:0]
	b.logRecords = nil

	b.commited = false
	b.rolledBack = false

	for _, buffer := range b.buffers {
		bytebufferpool.Put(buffer)
	}
	b.buffers = b.buffers[:0]
}
