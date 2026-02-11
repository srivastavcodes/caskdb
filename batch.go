package cask

import (
	"bytes"
	"encoding/binary"
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
	recordIndexes map[uint64][]int // Map of [hasKey][indexes] for faster lookup to pending writes.
	opts          BatchOptions
	commited      bool          // whether the batch has been committed.
	rolledBack    bool          // whether the batch has been rolled back.
	batchId       atomic.Uint64 // encoded as big endian uint64 as the key for a finished batch.
	buffers       []*bytebufferpool.ByteBuffer
}

func (cdb *CaskDb) NewBatch(opts BatchOptions) *Batch {
	b := &Batch{db: cdb, opts: opts}
	b.dbLock()
	return b
}

// defaultNewBatch gets passed to the buffer pool to be returned as a default
// Batch.
func defaultNewBatch() any {
	return &Batch{opts: DefaultBatchOptions}
}

// Put adds a key/val pair to the batch for writing and returns an error if any.
func (b *Batch) Put(key, val []byte) error {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
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
	b.rwm.Lock()
	defer b.rwm.Unlock()

	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
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
	b.rwm.RLock()
	defer b.rwm.RUnlock()

	if b.db.closed {
		return nil, ErrDbClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	now := time.Now().UnixNano()
	// retrieve value from pending writes if exists.
	record := b.lookupExistingKey(key)
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
		return nil, ErrKeyExpiredOrDeleted
	}
	return record.Val, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
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
	b.rwm.RLock()
	defer b.rwm.RUnlock()

	if b.db.closed {
		return false, ErrDbClosed
	}
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	now := time.Now().UnixNano()

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

// Expire sets the ttl for the key.
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	now := time.Now()

	record := b.lookupExistingKey(key)
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = uint64(time.Now().Add(ttl).UnixNano())
	}
	pos := b.db.index.Get(key)
	if pos == nil {
		return ErrKeyNotFound
	}
	enc, err := b.db.dataFiles.Read(pos)
	if err != nil {
		return fmt.Errorf("couldn't get record from datafile: %w", err)
	}
	now = time.Now()
	record = decodeLogRecord(enc)
	// if the record is deleted or expired, we can delete the key from index
	// since that makes the record invalid for read/write.
	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(record.Key)
		return ErrKeyExpiredOrDeleted
	}
	record.Expire = uint64(now.Add(ttl).UnixNano())
	// since we got the record from the wal, we need to re-write the record
	// to pendingWrites/db
	b.appendToPendingWrites(key, record)
	return nil
}

// ExpiresIn returns the remaining duration the key is valid for (the ttl uk).
// In case of an error returns -1 and the error.
func (b *Batch) ExpiresIn(key []byte) (time.Duration, error) {
	b.rwm.RLock()
	defer b.rwm.RUnlock()

	if b.db.closed {
		return -1, ErrDbClosed
	}
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	now := time.Now()

	record := b.lookupExistingKey(key)
	if record != nil {
		if record.Expire == 0 {
			return -1, nil
		}
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, ErrKeyExpiredOrDeleted
		}
		return time.Duration(record.Expire - uint64(now.UnixNano())), nil
	}
	pos := b.db.index.Get(key)
	if pos == nil {
		return -1, ErrKeyNotFound
	}
	enc, err := b.db.dataFiles.Read(pos)
	if err != nil {
		return -1, fmt.Errorf("couldn't get record from datafile: %w", err)
	}
	now = time.Now()
	record = decodeLogRecord(enc)

	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(record.Key)
		return -1, ErrKeyExpiredOrDeleted
	}
	if record.Expire > 0 {
		return time.Duration(record.Expire - uint64(now.UnixNano())), nil
	}
	return -1, nil
}

// PersistKey removes the ttl of the key, meaning the key will no longer expire
// and be accessible until deleted.
func (b *Batch) PersistKey(key []byte) error {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	if b.opts.ReadOnly {
		return ErrReadOnlyBatch
	}
	if b.db.closed {
		return ErrDbClosed
	}
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	now := time.Now()

	record := b.lookupExistingKey(key)
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return ErrKeyExpiredOrDeleted
		}
		record.Expire = 0
		return nil
	}
	pos := b.db.index.Get(key)
	if pos == nil {
		return ErrKeyNotFound
	}
	enc, err := b.db.dataFiles.Read(pos)
	if err != nil {
		return fmt.Errorf("couldn't get record from datafile: %w", err)
	}
	now = time.Now()
	record = decodeLogRecord(enc)

	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(record.Key)
		return ErrKeyExpiredOrDeleted
	}
	if record.Expire > 0 {
		record.Expire = 0
		b.appendToPendingWrites(key, record)
	}
	return nil
}

// Commit commits the Batch. If the batch is read-only or empty, it's a noOp.
//
// It will iterate over pending writes and write the records to the database,
// then writes a record with LogRecordBatchFinished type indicating the end
// of the batch to guarantee atomicity.
// Following this, it will write the indexes.
func (b *Batch) Commit() error {
	defer b.dbUnlock()
	b.rwm.Lock()
	defer b.rwm.Unlock()

	switch {
	case b.db.closed:
		return ErrDbClosed
	case len(b.pendingWrites) == 0:
		return nil
	case b.opts.ReadOnly:
		return nil
	case b.commited:
		return ErrBatchCommitted
	case b.rolledBack:
		return ErrBatchRolledBack
	}
	batchId := b.batchId.Add(1)
	now := time.Now().UnixNano()

	for _, rec := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		rec.BatchId = batchId
		enc := encodeLogRecord(rec, b.db.header, buf)
		b.db.dataFiles.AddPendingWrites(enc)
	}
	biBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(biBytes, batchId)

	// write a record that indicates the end of the batch
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)

	rec := &LogRecord{
		Key: biBytes, Type: LogRecordBatchFinished,
	}
	enc := encodeLogRecord(rec, b.db.header, buf)
	b.db.dataFiles.AddPendingWrites(enc)

	// write to wal
	positions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return fmt.Errorf("couldn't write pending writes to wal: %w", err)
	}
	if len(positions) != len(b.pendingWrites)+1 {
		panic("position len is not equal to pending writes")
	}
	if b.opts.Sync && !b.db.opts.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}
	// write key and position to index
	for i, rec := range b.pendingWrites {
		if rec.Type == LogRecordDeleted || rec.IsExpired(now) {
			b.db.index.Delete(rec.Key)
		} else {
			b.db.index.Put(rec.Key, positions[i])
		}
		if b.db.opts.WatchQueueSize == 0 {
			b.db.recordPool.Put(rec)
			continue
		}
		ev := &Event{
			Key: rec.Key, Val: rec.Val,
			BatchId: rec.BatchId,
		}
		if rec.Type != LogRecordDeleted {
			ev.Action = WatchActionPut
		} else {
			ev.Action = WatchActionDelete
		}
		b.db.watcher.putEvent(ev)
		b.db.recordPool.Put(rec)
	}
	b.commited = true
	return nil
}

// Rollback discards an uncommitted batch instance. The discard operation will
// clear the buffered data and release the lock.
func (b *Batch) Rollback() error {
	defer b.dbUnlock()
	b.rwm.Lock()
	defer b.rwm.Unlock()

	switch {
	case b.commited:
		return ErrBatchCommitted
	case b.rolledBack:
		return ErrBatchRolledBack
	case b.db.closed:
		return ErrDbClosed
	}
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	if b.opts.ReadOnly {
		b.rolledBack = true
		return nil
	}
	for _, rec := range b.pendingWrites {
		b.db.recordPool.Put(rec)
	}
	b.pendingWrites = b.pendingWrites[:0]
	for key := range b.recordIndexes {
		delete(b.recordIndexes, key)
	}
	b.rolledBack = true
	return nil
}

// lookupExistingKey checks if the key already exists, if yes, returns the
// LogRecord associated with the key.
func (b *Batch) lookupExistingKey(key []byte) *LogRecord {
	if len(b.recordIndexes) == 0 {
		return nil
	}
	haskey := utils.MemHash(key)
	// iterate over the bucket of entries that resulted in the same hash.
	for _, entry := range b.recordIndexes[haskey] {
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
	if b.recordIndexes == nil {
		b.recordIndexes = make(map[uint64][]int)
	}
	hashkey := utils.MemHash(key)
	// stores the offset of the record to the hashkey of the key.
	// eg: "hashkey" : {7, 21, 34}; haskey will be obviously different.
	b.recordIndexes[hashkey] = append(b.recordIndexes[hashkey], len(b.pendingWrites)-1)
}

func (b *Batch) dbLock() {
	if b.opts.ReadOnly {
		b.db.rwm.RLock()
	} else {
		b.db.rwm.Lock()
	}
}

func (b *Batch) dbUnlock() {
	if b.opts.ReadOnly {
		b.db.rwm.RUnlock()
	} else {
		b.db.rwm.Unlock()
	}
}

func (b *Batch) init(rdonly, sync bool, db *CaskDb) {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	b.opts.ReadOnly = rdonly
	b.opts.Sync = sync
	b.db = db
	b.dbLock()
}

func (b *Batch) reset() {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	b.db = nil

	b.pendingWrites = b.pendingWrites[:0]
	b.recordIndexes = nil

	b.commited = false
	b.rolledBack = false

	for _, buffer := range b.buffers {
		bytebufferpool.Put(buffer)
	}
	b.buffers = b.buffers[:0]
}
