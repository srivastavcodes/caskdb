package cask

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/srivastavcodes/caskdb/utils"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stderr)
}

func TestDB_Merge_1_Empty(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	err = db.Merge(false)
	require.NoError(t, err)
}

func TestDB_Merge_2_All_Invalid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		require.NoError(t, err)
	}

	err = db.Merge(false)
	require.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	stat := db2.Stat()
	require.Equal(t, 0, stat.KeyCount)
}

func TestDB_Merge_3_All_Valid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	log.Println(db.opts.DirPath)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	err = db.Merge(false)
	require.NoError(t, err)

	_ = db.Close()

	db2, err := Open(options)
	require.NoError(t, err)
	log.Println(db2.opts.DirPath)
	defer func() {
		_ = db2.Close()
	}()
	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err, fmt.Sprintf("value for key: %d", i))
		require.NotNil(t, val)
	}
}

func TestDB_Merge_4_Twice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}

	err = db.Merge(false)
	require.NoError(t, err)
	err = db.Merge(false)
	require.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		require.NotNil(t, val)
	}
}

func TestDB_Merge_5_Mixed(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		require.NoError(t, err)
	}

	err = db.Merge(false)
	require.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	require.Equal(t, 200000, stat.KeyCount)
}

func TestDB_Merge_6_Appending(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomBytes(128))
		require.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	var sm sync.Map
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				sm.Store(string(key), struct{}{})
				e := db.Put(key, utils.RandomBytes(128))
				require.NoError(t, e)
			}
		}()
	}

	err = db.Merge(false)
	require.NoError(t, err)

	wg.Wait()

	_ = db.Close()
	db2, err := Open(options)
	require.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	var count int
	sm.Range(func(key, value any) bool {
		count++
		return true
	})
	require.Equal(t, 200000+count, stat.KeyCount)
}

func TestDB_Multi_Open_Merge(t *testing.T) {
	options := DefaultOptions
	kvs := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		db, err := Open(options)
		require.NoError(t, err)

		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(rand.Int())
			value := utils.RandomBytes(128)
			kvs[string(key)] = value
			err = db.Put(key, value)
			require.NoError(t, err)
		}

		err = db.Merge(false)
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)
	}
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, v)
	}
	require.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_ReopenAfterDone(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	kvs := make(map[string][]byte)
	for i := 0; i < 200000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomBytes(128)
		kvs[string(key)] = value
		err := db.Put(key, value)
		require.NoError(t, err)
	}

	err = db.Merge(true)
	require.NoError(t, err)
	_, err = os.Stat(mergeDirPath(options.DirPath))
	require.True(t, os.IsNotExist(err))

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, v)
	}
	require.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	var wg sync.WaitGroup
	var sm sync.Map
	wg.Add(11)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				value := utils.RandomBytes(128)
				sm.Store(string(key), value)
				e := db.Put(key, value)
				require.NoError(t, e)
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	go func() {
		defer wg.Done()
		err = db.Merge(true)
		require.NoError(t, err)
	}()
	wg.Wait()

	_, err = os.Stat(mergeDirPath(options.DirPath))
	require.True(t, os.IsNotExist(err))

	var count int
	sm.Range(func(key, value any) bool {
		v, err := db.Get([]byte(key.(string)))
		require.NoError(t, err)
		require.Equal(t, value, v)
		count++
		return true
	})
	require.Equal(t, count, db.index.Size())
}
