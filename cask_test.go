package cask

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/srivastavcodes/caskdb/utils"
	"github.com/stretchr/testify/require"
)

func TestDB_Put_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(rand.Int()), utils.RandomBytes(128))
		require.NoError(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomBytes(KB))
		require.NoError(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomBytes(5*KB))
		require.NoError(t, err)
	}

	// reopen
	err = db.Close()
	require.NoError(t, err)
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	require.Equal(t, 300, stat.KeyCount)
}

func TestDB_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// not exist
	val1, err := db.Get([]byte("not-exist"))
	require.Nil(t, val1)
	require.Equal(t, ErrKeyNotFound, err)

	generateData(t, db, 1, 100, 128)
	for i := 1; i < 100; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.Len(t, utils.RandomBytes(128), len(val))
	}
	generateData(t, db, 200, 300, KB)
	for i := 200; i < 300; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.Len(t, utils.RandomBytes(KB), len(val))
	}
	generateData(t, db, 400, 500, 4*KB)
	for i := 400; i < 500; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.Len(t, utils.RandomBytes(4*KB), len(val))
	}
}

func TestDB_Close_Sync(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.Sync()
	require.NoError(t, err)
}

func TestDB_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				m.Store(string(key), struct{}{})
				e := db.Put(key, utils.RandomBytes(128))
				require.NoError(t, e)
			}
		}()
	}
	wg.Wait()

	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	require.Equal(t, count, db.index.Size())
}

func TestDB_Concurrent_Get(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 10000; i < 20000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomBytes(4096))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			db.Ascend(func(key, value []byte) (bool, error) {
				require.NotNil(t, key)
				require.NotNil(t, value)
				return true, nil
			})
		}()
	}
	wg.Wait()
}

func TestDB_Ascend(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test Ascend function
	var result []string
	db.Ascend(func(k, v []byte) (bool, error) {
		result = append(result, string(k))
		return true, nil // No error here
	})

	if err != nil {
		t.Errorf("Ascend returned an error: %v", err)
	}

	expected := []string{"key1", "key2", "key3"}
	if len(result) != len(expected) {
		t.Errorf("Unexpected number of results. Expected: %v, Got: %v", expected, result)
	} else {
		for i, val := range expected {
			if result[i] != val {
				t.Errorf("Unexpected result at index %d. Expected: %v, Got: %v", i, val, result[i])
			}
		}
	}
}

func TestDB_Descend(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test Descend function
	var result []string
	db.Descend(func(k, v []byte) (bool, error) {
		result = append(result, string(k))
		return true, nil
	})

	if err != nil {
		t.Errorf("Descend returned an error: %v", err)
	}

	expected := []string{"key3", "key2", "key1"}
	if len(result) != len(expected) {
		t.Errorf("Unexpected number of results. Expected: %v, Got: %v", expected, result)
	} else {
		for i, val := range expected {
			if result[i] != val {
				t.Errorf("Unexpected result at index %d. Expected: %v, Got: %v", i, val, result[i])
			}
		}
	}
}

func TestDB_AscendRange(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test AscendRange
	var resultAscendRange []string
	db.AscendRange([]byte("banana"), []byte("grape"), func(k, v []byte) (bool, error) {
		resultAscendRange = append(resultAscendRange, string(k))
		return true, nil
	})
	require.Equal(t, []string{"banana", "cherry", "date"}, resultAscendRange)
}

func TestDB_DescendRange(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test DescendRange
	var resultDescendRange []string
	db.DescendRange([]byte("grape"), []byte("cherry"), func(k, v []byte) (bool, error) {
		resultDescendRange = append(resultDescendRange, string(k))
		return true, nil
	})
	require.Equal(t, []string{"grape", "date"}, resultDescendRange)
}

func TestDB_AscendGreaterOrEqual(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test AscendGreaterOrEqual
	var resultAscendGreaterOrEqual []string
	db.AscendGreaterOrEqual([]byte("date"), func(k, v []byte) (bool, error) {
		resultAscendGreaterOrEqual = append(resultAscendGreaterOrEqual, string(k))
		return true, nil
	})
	require.Equal(t, []string{"date", "grape", "kiwi"}, resultAscendGreaterOrEqual)
}

func TestDB_DescendLessOrEqual(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test DescendLessOrEqual
	var resultDescendLessOrEqual []string
	db.DescendLessOrEqual([]byte("grape"), func(k, v []byte) (bool, error) {
		resultDescendLessOrEqual = append(resultDescendLessOrEqual, string(k))
		return true, nil
	})
	require.Equal(t, []string{"grape", "date", "cherry", "banana", "apple"}, resultDescendLessOrEqual)
}

func TestDB_PutWithTTL(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.PutWithTTL(utils.GetTestKey(1), utils.RandomBytes(128), time.Millisecond*100)
	require.NoError(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	require.NoError(t, err)
	require.NotNil(t, val1)
	time.Sleep(time.Millisecond * 200)
	val2, err := db.Get(utils.GetTestKey(1))
	require.Equal(t, ErrKeyExpiredOrDeleted, err)
	require.Nil(t, val2)

	err = db.PutWithTTL(utils.GetTestKey(2), utils.RandomBytes(128), time.Millisecond*200)
	require.NoError(t, err)
	// rewrite
	err = db.Put(utils.GetTestKey(2), utils.RandomBytes(128))
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 200)
	val3, err := db.Get(utils.GetTestKey(2))
	require.NoError(t, err)
	require.NotNil(t, val3)

	err = db.Close()
	require.NoError(t, err)

	db2, err := Open(options)
	require.NoError(t, err)

	val4, err := db2.Get(utils.GetTestKey(1))
	require.Equal(t, ErrKeyNotFound, err)
	require.Nil(t, val4)

	val5, err := db2.Get(utils.GetTestKey(2))
	require.NoError(t, err)
	require.NotNil(t, val5)

	_ = db2.Close()
}

func TestDB_RePutWithTTL(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.Put(utils.GetTestKey(10), utils.RandomBytes(10))
	require.NoError(t, err)
	err = db.PutWithTTL(utils.GetTestKey(10), utils.RandomBytes(10), time.Millisecond*100)
	require.NoError(t, err)
	time.Sleep(time.Second * 1) // wait for expired

	val1, err := db.Get(utils.GetTestKey(10))
	require.Equal(t, err, ErrKeyExpiredOrDeleted)
	require.Nil(t, val1)

	err = db.Merge(true)
	require.NoError(t, err)

	val2, err := db.Get(utils.GetTestKey(10))
	require.Equal(t, ErrKeyNotFound, err)
	require.Nil(t, val2)
}

func TestDB_PutWithTTL_Merge(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)
	for i := 0; i < 100; i++ {
		err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomBytes(10), time.Second*2)
		require.NoError(t, err)
	}
	for i := 100; i < 150; i++ {
		err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomBytes(10), time.Second*20)
		require.NoError(t, err)
	}
	time.Sleep(time.Second * 3)

	err = db.Merge(true)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		require.Nil(t, val)
		require.Equal(t, ErrKeyExpiredOrDeleted, err)
	}
	for i := 100; i < 150; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		require.NoError(t, err, fmt.Sprintf("Failed to get key: %v", i))
		require.NotNil(t, val)
	}
}

func TestDB_Expire(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.Put(utils.GetTestKey(1), utils.RandomBytes(10))
	require.NoError(t, err)

	err = db.Expire(utils.GetTestKey(1), time.Second*100)
	require.NoError(t, err)
	tt1, err := db.ExpiresIn(utils.GetTestKey(1))
	require.NoError(t, err)
	require.Greater(t, tt1.Seconds(), float64(90))

	err = db.PutWithTTL(utils.GetTestKey(2), utils.RandomBytes(10), time.Second*1)
	require.NoError(t, err)

	tt2, err := db.ExpiresIn(utils.GetTestKey(2))
	require.NoError(t, err)
	require.Greater(t, tt2.Microseconds(), int64(500))

	err = db.Close()
	require.NoError(t, err)

	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	tt3, err := db2.ExpiresIn(utils.GetTestKey(1))
	require.NoError(t, err)
	require.Greater(t, tt3.Seconds(), float64(90))

	time.Sleep(time.Second)
	tt4, err := db2.ExpiresIn(utils.GetTestKey(2))
	require.Equal(t, tt4, time.Duration(-1))
	require.Equal(t, ErrKeyExpiredOrDeleted, err)
}

func TestDB_Expire2(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// expire an expired key
	_ = db.PutWithTTL(utils.GetTestKey(1), utils.RandomBytes(10), time.Second*1)
	_ = db.Put(utils.GetTestKey(2), utils.RandomBytes(10))
	err = db.Expire(utils.GetTestKey(2), time.Second*2)
	require.NoError(t, err)

	time.Sleep(time.Second * 2)
	_ = db.Close()

	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	err = db2.Expire(utils.GetTestKey(1), time.Second)
	require.Equal(t, ErrKeyExpiredOrDeleted, err)
	err = db2.Expire(utils.GetTestKey(2), time.Second)
	require.Equal(t, ErrKeyExpiredOrDeleted, err)
}

func TestDB_DeleteExpiredKeys(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100001; i++ {
		err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomBytes(10), time.Second*1)
		require.NoError(t, err)
	}
	// wait for key to expire
	time.Sleep(time.Second * 2)

	err = db.DeleteExpiredKeys(time.Second * 2)
	require.NoError(t, err)
	require.Equal(t, 0, db.Stat().KeyCount)
}

func TestDB_Multi_DeleteExpiredKeys(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 3; i++ {
		for i := 0; i < 10000; i++ {
			err = db.Put(utils.GetTestKey(i), utils.RandomBytes(10))
			require.NoError(t, err)
		}
		for i := 10000; i < 100001; i++ {
			err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomBytes(10), time.Second*1)
			require.NoError(t, err)
		}
		// wait for key to expire
		time.Sleep(time.Second * 2)

		err = db.DeleteExpiredKeys(time.Second * 2)
		require.NoError(t, err)
		require.Equal(t, 10000, db.Stat().KeyCount)
	}
}

func TestDB_Persist(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// not exist
	err = db.PersistKey(utils.GetTestKey(1))
	require.Equal(t, err, ErrKeyNotFound)

	err = db.PutWithTTL(utils.GetTestKey(1), utils.RandomBytes(10), time.Second*1)
	require.NoError(t, err)

	// exist
	err = db.PersistKey(utils.GetTestKey(1))
	require.NoError(t, err)
	time.Sleep(time.Second * 2)
	// check ttl
	ttl, err := db.ExpiresIn(utils.GetTestKey(1))
	require.NoError(t, err)
	require.Equal(t, ttl, time.Duration(-1))
	val1, err := db.Get(utils.GetTestKey(1))
	require.NoError(t, err)
	require.NotNil(t, val1)

	// restart
	err = db.Close()
	require.NoError(t, err)

	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	ttl2, err := db2.ExpiresIn(utils.GetTestKey(1))
	require.NoError(t, err)
	require.Equal(t, ttl2, time.Duration(-1))
	val2, err := db2.Get(utils.GetTestKey(1))
	require.NoError(t, err)
	require.NotNil(t, val2)
}

func TestDB_Invalid_Cron_Expression(t *testing.T) {
	options := DefaultOptions
	options.AutoMergeCronExpr = "*/1 * * * * * *"
	_, err := Open(options)
	require.Error(t, err)
}

func TestDB_Valid_Cron_Expression(t *testing.T) {
	options := DefaultOptions
	{
		options.AutoMergeCronExpr = "* */1 * * * *"
		db, err := Open(options)
		require.NoError(t, err)
		destroyDB(db)
	}

	{
		options.AutoMergeCronExpr = "*/1 * * * *"
		db, err := Open(options)
		require.NoError(t, err)
		destroyDB(db)
	}

	{
		options.AutoMergeCronExpr = "5 0 * 8 *"
		db, err := Open(options)
		require.NoError(t, err)
		destroyDB(db)
	}

	{
		options.AutoMergeCronExpr = "*/2 14 1 * *"
		db, err := Open(options)
		require.NoError(t, err)
		destroyDB(db)
	}

	{
		options.AutoMergeCronExpr = "@hourly"
		db, err := Open(options)
		require.NoError(t, err)
		destroyDB(db)
	}
}

func TestDB_Auto_Merge(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 2000; i++ {
		delKey := utils.GetTestKey(rand.Int())
		err := db.Put(delKey, utils.RandomBytes(128))
		require.NoError(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomBytes(2*KB))
		require.NoError(t, err)
		err = db.Delete(delKey)
		require.NoError(t, err)
	}
	{
		reader := db.dataFiles.NewReader()
		var keyCnt int
		for {
			if _, _, err := reader.Next(); errors.Is(err, io.EOF) {
				break
			}
			keyCnt++
		}
		// each record has one data wal and commit at end of batch with wal
		// so totally is 2000 * 3 * 2 = 12000
		require.Equal(t, 12000, keyCnt)
	}
	mdir := mergeDirPath(options.DirPath)
	if _, err := os.Stat(mdir); err != nil {
		require.True(t, os.IsNotExist(err))
	}
	require.NoError(t, db.Close())
	{
		options.AutoMergeCronExpr = "* * * * * *" // every second
		db2, err := Open(options)
		require.NoError(t, err)

		// wait for auto merge to complete
		<-time.After(time.Second * 2)
		// close the db to stop cron scheduler and ensure merge is done
		_ = db2.Close()

		// reopen without auto merge to safely read data
		options.AutoMergeCronExpr = ""
		db3, err := Open(options)
		require.NoError(t, err)
		{
			reader := db3.dataFiles.NewReader()
			var keyCnt int
			for {
				if _, _, err := reader.Next(); errors.Is(err, io.EOF) {
					break
				}
				keyCnt++
			}
			// after merge records are only valid data, so totally is 2000
			require.Equal(t, 2000, keyCnt)
		}
		_ = db3.Close()
	}
}

func TestDB_Empty_Key(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test Put with empty key
	err = db.Put(nil, []byte("value"))
	require.Equal(t, ErrKeyIsEmpty, err)
	err = db.Put([]byte{}, []byte("value"))
	require.Equal(t, ErrKeyIsEmpty, err)

	// Test Get with empty key
	_, err = db.Get(nil)
	require.Equal(t, ErrKeyIsEmpty, err)

	// Test Delete with empty key
	err = db.Delete(nil)
	require.Equal(t, ErrKeyIsEmpty, err)

	// Test Exist with empty key
	_, err = db.Exists(nil)
	require.Equal(t, ErrKeyIsEmpty, err)

	// Test ExpiresIn() with empty key
	_, err = db.ExpiresIn(nil)
	require.Equal(t, ErrKeyIsEmpty, err)

	// Test Expire with empty key
	err = db.Expire(nil, time.Second)
	require.Equal(t, ErrKeyIsEmpty, err)
}

func TestDB_Large_Value(t *testing.T) {
	options := DefaultOptions
	options.SegmentSize = 64 * MB
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test with 1MB value
	largeValue1MB := utils.RandomBytes(MB)
	err = db.Put([]byte("large-key-1mb"), largeValue1MB)
	require.NoError(t, err)

	val, err := db.Get([]byte("large-key-1mb"))
	require.NoError(t, err)
	require.Equal(t, len(largeValue1MB), len(val))

	// Test with 5MB value
	largeValue5MB := utils.RandomBytes(5 * MB)
	err = db.Put([]byte("large-key-5mb"), largeValue5MB)
	require.NoError(t, err)

	val2, err := db.Get([]byte("large-key-5mb"))
	require.NoError(t, err)
	require.Equal(t, len(largeValue5MB), len(val2))

	// Reopen and verify
	_ = db.Close()
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	val3, err := db2.Get([]byte("large-key-1mb"))
	require.NoError(t, err)
	require.Equal(t, len(largeValue1MB), len(val3))

	val4, err := db2.Get([]byte("large-key-5mb"))
	require.NoError(t, err)
	require.Equal(t, len(largeValue5MB), len(val4))
}

func TestDB_Empty_Iterator(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test iterator on empty database
	iter := db.NewIterator(DefaultIteratorOptions)
	require.NotNil(t, iter)
	require.False(t, iter.Valid())
	require.Nil(t, iter.Item())
	iter.Close()

	// Test Ascend on empty database
	count := 0
	db.Ascend(func(k, v []byte) (bool, error) {
		count++
		return true, nil
	})
	require.Equal(t, 0, count)

	// Test Descend on empty database
	count = 0
	db.Descend(func(k, v []byte) (bool, error) {
		count++
		return true, nil
	})
	require.Equal(t, 0, count)
}

func TestDB_Concurrent_Batch(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Generate initial data
	generateData(t, db, 0, 100, 128)

	// Concurrent batch operations
	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				batch := db.NewBatch(DefaultBatchOptions)
				key := utils.GetTestKey(id*1000 + j)
				err := batch.Put(key, utils.RandomBytes(128))
				require.NoError(t, err)

				val, err := batch.Get(key)
				require.NoError(t, err)
				require.NotNil(t, val)

				err = batch.Commit()
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all data
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < opsPerGoroutine; j++ {
			key := utils.GetTestKey(i*1000 + j)
			val, err := db.Get(key)
			require.NoError(t, err)
			require.NotNil(t, val)
		}
	}
}

func TestDB_Concurrent_ReadWrite(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Generate initial data
	generateData(t, db, 0, 1000, 128)

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := utils.GetTestKey(id*10000 + j)
				err := db.Put(key, utils.RandomBytes(128))
				require.NoError(t, err)
			}
		}(i)
	}
	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := utils.GetTestKey(rand.Intn(1000))
				_, _ = db.Get(key)
			}
		}()
	}
	// Iterators
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			iter := db.NewIterator(DefaultIteratorOptions)
			count := 0
			for iter.Rewind(); iter.Valid() && count < 100; iter.Next() {
				_ = iter.Item()
				count++
			}
			iter.Close()
		}()
	}
	wg.Wait()
}

func TestDB_Reopen_With_Data(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)

	// Write data
	for i := 0; i < 1000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	// Delete some data
	for i := 0; i < 500; i++ {
		err := db.Delete(utils.GetTestKey(i))
		require.NoError(t, err)
	}
	// Get stats before close
	stat := db.Stat()
	require.Equal(t, 500, stat.KeyCount)

	_ = db.Close()

	// Reopen
	db2, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db2)

	// Verify stats after reopen
	stat2 := db2.Stat()
	require.Equal(t, 500, stat2.KeyCount)

	// Verify data
	for i := 0; i < 500; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		require.Equal(t, ErrKeyNotFound, err)
	}
	for i := 500; i < 1000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.NotNil(t, val)
	}
}
