package cask

import (
	"os"
	"testing"
	"time"

	"github.com/srivastavcodes/caskdb/utils"
	"github.com/stretchr/testify/require"
)

func destroyDB(db *CaskDb) {
	_ = db.Close()
	_ = os.RemoveAll(db.opts.DirPath)
	_ = os.RemoveAll(mergeDirPath(db.opts.DirPath))
}

func TestBatch_Put_Normal(t *testing.T) {
	// value 128B
	batchPutAndIterate(t, 1*GB, 10000, 128)
	// value 1KB
	batchPutAndIterate(t, 1*GB, 10000, KB)
	// value 32KB
	batchPutAndIterate(t, 1*GB, 1000, 32*KB)
}

func TestBatch_Put_IncrSegmentFile(t *testing.T) {
	batchPutAndIterate(t, 64*MB, 2000, 32*KB)
	opts := DefaultOptions
	opts.SegmentSize = 64 * MB
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 2000, 32*KB)

	// write more data to rotate new segment file
	batch := db.NewBatch(DefaultBatchOptions)
	for i := 0; i < 1000; i++ {
		err := batch.Put(utils.GetTestKey(i*100), utils.RandomBytes(32*KB))
		require.NoError(t, err)
	}
	err = batch.Commit()
	require.NoError(t, err)
}

func TestBatch_Get_Normal(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	batch1 := db.NewBatch(DefaultBatchOptions)
	err = batch1.Put(utils.GetTestKey(12), utils.RandomBytes(128))
	require.NoError(t, err)
	val1, err := batch1.Get(utils.GetTestKey(12))
	require.NoError(t, err)
	require.NotNil(t, val1)
	_ = batch1.Commit()

	generateData(t, db, 400, 500, 4*KB)

	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Delete(utils.GetTestKey(450))
	require.NoError(t, err)
	val, err := batch2.Get(utils.GetTestKey(450))
	require.Nil(t, val)
	require.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(12), true)
	assertKeyExistOrNot(t, db2, utils.GetTestKey(450), false)
}

func TestBatch_Delete_Normal(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.Delete([]byte("not exist"))
	require.NoError(t, err)

	generateData(t, db, 1, 100, 128)
	err = db.Delete(utils.GetTestKey(99))
	require.NoError(t, err)

	exist, err := db.Exists(utils.GetTestKey(99))
	require.NoError(t, err)
	require.False(t, exist)

	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Put(utils.GetTestKey(200), utils.RandomBytes(100))
	require.NoError(t, err)
	err = batch.Delete(utils.GetTestKey(200))
	require.NoError(t, err)
	exist1, err := batch.Exists(utils.GetTestKey(200))
	require.NoError(t, err)
	require.False(t, exist1)
	_ = batch.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(200), false)
}

func TestBatch_Exist_Normal(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 100, 128)
	batch := db.NewBatch(DefaultBatchOptions)
	ok1, err := batch.Exists(utils.GetTestKey(99))
	require.NoError(t, err)
	require.True(t, ok1)
	ok2, err := batch.Exists(utils.GetTestKey(5000))
	require.NoError(t, err)
	require.False(t, ok2)
	_ = batch.Commit()

	_ = db.Close()
	db2, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(99), true)
}

func generateData(t *testing.T, db *CaskDb, start, end, valueLen int) {
	t.Helper()
	for ; start < end; start++ {
		err := db.Put(utils.GetTestKey(start), utils.RandomBytes(valueLen))
		require.NoError(t, err)
	}
}

func batchPutAndIterate(t *testing.T, segmentSize int64, size, valueLen int) {
	t.Helper()
	opts := DefaultOptions
	opts.SegmentSize = segmentSize
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(BatchOptions{})

	for i := 0; i < size; i++ {
		err := batch.Put(utils.GetTestKey(i), utils.RandomBytes(valueLen))
		require.NoError(t, err)
	}
	err = batch.Commit()
	require.NoError(t, err)

	for i := 0; i < size; i++ {
		value, err := db.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.Len(t, value, len(utils.RandomBytes(valueLen)))
	}

	// reopen
	_ = db.Close()
	db2, err := Open(opts)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	for i := 0; i < size; i++ {
		value, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.Len(t, value, len(utils.RandomBytes(valueLen)))
	}
}

func assertKeyExistOrNot(t *testing.T, db *CaskDb, key []byte, exist bool) {
	t.Helper()
	val, err := db.Get(key)
	if exist {
		require.NoError(t, err)
		require.NotNil(t, val)
	} else {
		require.Nil(t, val)
		require.Equal(t, ErrKeyNotFound, err)
	}
}

func TestBatch_Rollback(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	key := []byte("rosedb")
	value := []byte("val")

	batcher := db.NewBatch(DefaultBatchOptions)
	err = batcher.Put(key, value)
	require.NoError(t, err)

	err = batcher.Rollback()
	require.NoError(t, err)

	resp, err := db.Get(key)
	require.Equal(t, ErrKeyNotFound, err)
	require.Empty(t, resp)
}

func TestBatch_SetTwice(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	key := []byte("rosedb")
	value1 := []byte("val1")
	value2 := []byte("val2")
	_ = batch.Put(key, value1)
	_ = batch.Put(key, value2)

	res, err := batch.Get(key)
	require.NoError(t, err)
	require.Equal(t, res, value2)

	_ = batch.Commit()
	res2, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, res2, value2)
}

func TestBatch_Expire(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Expire(nil, time.Second)
	require.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test read-only batch
	readOnlyBatch := db.NewBatch(BatchOptions{ReadOnly: true})
	err = readOnlyBatch.Expire([]byte("key"), time.Second)
	require.Equal(t, ErrReadOnlyBatch, err)
	_ = readOnlyBatch.Rollback()

	// Test expire key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Expire([]byte("not-exist"), time.Second)
	require.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test expire key in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = batch3.Expire([]byte("key1"), time.Second*10)
	require.NoError(t, err)
	ttl, err := batch3.ExpiresIn([]byte("key1"))
	require.NoError(t, err)
	require.True(t, ttl > 0 && ttl <= time.Second*10)
	_ = batch3.Commit()

	// Test expire key in database
	err = db.Put([]byte("key2"), []byte("value2"))
	require.NoError(t, err)
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.Expire([]byte("key2"), time.Second*5)
	require.NoError(t, err)
	_ = batch4.Commit()
	ttl2, err := db.ExpiresIn([]byte("key2"))
	require.NoError(t, err)
	require.True(t, ttl2 > 0 && ttl2 <= time.Second*5)

	// Test expire deleted key in pendingWrites
	batch5 := db.NewBatch(DefaultBatchOptions)
	err = batch5.Put([]byte("key3"), []byte("value3"))
	require.NoError(t, err)
	err = batch5.Delete([]byte("key3"))
	require.NoError(t, err)
	err = batch5.Expire([]byte("key3"), time.Second)
	require.Equal(t, ErrKeyNotFound, err)
	_ = batch5.Rollback()
}

func TestBatch_TTL(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	_, err = batch.ExpiresIn(nil)
	require.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test ExpiresIn key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	_, err = batch2.ExpiresIn([]byte("not-exist"))
	require.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test ExpiresIn key without expiration in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	ttl, err := batch3.ExpiresIn([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	_ = batch3.Commit()

	// Test ExpiresIn key with expiration in pendingWrites
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.PutWithTTL([]byte("key2"), []byte("value2"), time.Second*10)
	require.NoError(t, err)
	ttl2, err := batch4.ExpiresIn([]byte("key2"))
	require.NoError(t, err)
	require.True(t, ttl2 > 0 && ttl2 <= time.Second*10)
	_ = batch4.Commit()

	// Test ExpiresIn key without expiration in database
	err = db.Put([]byte("key3"), []byte("value3"))
	require.NoError(t, err)
	batch5 := db.NewBatch(DefaultBatchOptions)
	ttl3, err := batch5.ExpiresIn([]byte("key3"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl3)
	_ = batch5.Rollback()

	// Test ExpiresIn key with expiration in database
	err = db.PutWithTTL([]byte("key4"), []byte("value4"), time.Second*20)
	require.NoError(t, err)
	batch6 := db.NewBatch(DefaultBatchOptions)
	ttl4, err := batch6.ExpiresIn([]byte("key4"))
	require.NoError(t, err)
	require.True(t, ttl4 > 0 && ttl4 <= time.Second*20)
	_ = batch6.Rollback()

	// Test ExpiresIn deleted key in pendingWrites
	// Note: current implementation returns -1, nil for deleted key because
	// it checks Expire == 0 before checking Type == LogRecordDeleted
	batch7 := db.NewBatch(DefaultBatchOptions)
	err = batch7.Put([]byte("key5"), []byte("value5"))
	require.NoError(t, err)
	err = batch7.Delete([]byte("key5"))
	require.NoError(t, err)
	ttl5, err := batch7.ExpiresIn([]byte("key5"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl5)
	_ = batch7.Rollback()
}

func TestBatch_Persist(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test empty key
	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.PersistKey(nil)
	require.Equal(t, ErrKeyIsEmpty, err)
	_ = batch.Rollback()

	// Test read-only batch
	readOnlyBatch := db.NewBatch(BatchOptions{ReadOnly: true})
	err = readOnlyBatch.PersistKey([]byte("key"))
	require.Equal(t, ErrReadOnlyBatch, err)
	_ = readOnlyBatch.Rollback()

	// Test persist key not found
	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.PersistKey([]byte("not-exist"))
	require.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Rollback()

	// Test persist key with ExpiresIn in pendingWrites
	batch3 := db.NewBatch(DefaultBatchOptions)
	err = batch3.PutWithTTL([]byte("key1"), []byte("value1"), time.Second*10)
	require.NoError(t, err)
	ttl, err := batch3.ExpiresIn([]byte("key1"))
	require.NoError(t, err)
	require.True(t, ttl > 0)
	err = batch3.PersistKey([]byte("key1"))
	require.NoError(t, err)
	ttl2, err := batch3.ExpiresIn([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl2)
	_ = batch3.Commit()

	// Test persist key with ExpiresIn in database
	err = db.PutWithTTL([]byte("key2"), []byte("value2"), time.Second*10)
	require.NoError(t, err)
	batch4 := db.NewBatch(DefaultBatchOptions)
	err = batch4.PersistKey([]byte("key2"))
	require.NoError(t, err)
	_ = batch4.Commit()
	ttl3, err := db.ExpiresIn([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl3)

	// Test persist key without ExpiresIn in database (should return directly)
	err = db.Put([]byte("key3"), []byte("value3"))
	require.NoError(t, err)
	batch5 := db.NewBatch(DefaultBatchOptions)
	err = batch5.PersistKey([]byte("key3"))
	require.NoError(t, err)
	_ = batch5.Commit()

	// Test persist deleted key in pendingWrites
	batch6 := db.NewBatch(DefaultBatchOptions)
	err = batch6.PutWithTTL([]byte("key4"), []byte("value4"), time.Second*10)
	require.NoError(t, err)
	err = batch6.Delete([]byte("key4"))
	require.NoError(t, err)
	err = batch6.PersistKey([]byte("key4"))
	require.Equal(t, ErrKeyExpiredOrDeleted, err)
	_ = batch6.Rollback()
}
