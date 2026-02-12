package cask

import (
	"bytes"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIterator_Basic(t *testing.T) {
	options := DefaultOptions
	options.DirPath = filepath.Join(options.DirPath, "iterator_basic")
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Test empty database
	iteratorOptions := DefaultIteratorOptions
	iter := db.NewIterator(iteratorOptions)
	require.False(t, iter.Valid())
	iter.Close()

	// Put some key-value pairs
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		require.NoError(t, err)
	}

	// Test forward iteration
	iter = db.NewIterator(iteratorOptions)
	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
		require.True(t, bytes.Equal(item.Key, keys[i]))
		require.True(t, bytes.Equal(item.Val, values[i]))
		i++
	}
	require.Equal(t, len(keys), i)
	iter.Close()

	// Test reverse iteration
	iteratorOptions.Reverse = true
	iter = db.NewIterator(iteratorOptions)

	i = len(keys) - 1
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
		require.True(t, bytes.Equal(item.Key, keys[i]))
		require.True(t, bytes.Equal(item.Val, values[i]))
		i--
	}
	require.Equal(t, -1, i)
	iter.Close()
}

func TestIterator_Seek(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Put some key-value pairs
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		require.NoError(t, err)
	}

	iteratorOptions := DefaultIteratorOptions
	// Test seek in ascending order
	iter := db.NewIterator(iteratorOptions)

	iter.Seek([]byte("key3"))
	log.Println(string(iter.currItem.Key), string(iter.currItem.Val))
	require.True(t, iter.Valid())
	item := iter.Item()
	log.Println(string(item.Key), string(item.Val))
	require.NotNil(t, item)
	require.True(t, bytes.Equal(item.Key, []byte("key3")))
	require.True(t, bytes.Equal(item.Val, []byte("value3")))
	iter.Close()

	// Test seek in descending order
	iteratorOptions.Reverse = true
	iter = db.NewIterator(iteratorOptions)

	iter.Seek([]byte("key3"))
	require.True(t, iter.Valid())
	item = iter.Item()
	require.NotNil(t, item)
	require.True(t, bytes.Equal(item.Key, []byte("key3")))
	require.True(t, bytes.Equal(item.Val, []byte("value3")))
	iter.Close()
}

func TestIterator_Prefix(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Put some key-value pairs with different prefixes
	keys := [][]byte{
		[]byte("a:1"),
		[]byte("a:2"),
		[]byte("b:1"),
		[]byte("b:2"),
		[]byte("c:1"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		require.NoError(t, err)
	}

	iteratorOptions := DefaultIteratorOptions
	// Test prefix iteration
	iteratorOptions.Prefix = []byte("b:")
	iter := db.NewIterator(iteratorOptions)

	expectedKeys := [][]byte{[]byte("b:1"), []byte("b:2")}
	expectedValues := [][]byte{[]byte("value3"), []byte("value4")}
	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
		require.True(t, bytes.Equal(item.Key, expectedKeys[i]))
		require.True(t, bytes.Equal(item.Val, expectedValues[i]))
		i++
	}
	require.Equal(t, len(expectedKeys), i)
	iter.Close()
}

func TestIterator_Expired(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Put a key-value pair with TTL
	err = db.PutWithTTL([]byte("key1"), []byte("value1"), time.Millisecond*10)
	require.NoError(t, err)
	// Put a normal key-value pair
	err = db.Put([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	// Wait for the first key to expire
	time.Sleep(time.Millisecond * 20)

	// Test iteration
	iter := db.NewIterator(DefaultIteratorOptions)

	iter.Rewind()
	require.True(t, iter.Valid())
	item := iter.Item()
	require.NotNil(t, item)
	require.True(t, bytes.Equal(item.Key, []byte("key2")))
	require.True(t, bytes.Equal(item.Val, []byte("value2")))
	iter.Next()
	require.False(t, iter.Valid())
	iter.Close()
}

func TestIterator_Error(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	// Put some key-value pairs
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		require.NoError(t, err)
	}

	// Corrupt the data file to simulate read errors
	db.dataFiles.Close()

	// Test with ContinueOnError = true
	iteratorOptions := DefaultIteratorOptions
	iteratorOptions.ContinueOnError = true
	iter := db.NewIterator(iteratorOptions)

	// Should continue iteration despite errors
	iter.Rewind()
	require.Error(t, iter.Err())
	iter.Close()

	// Test with ContinueOnError = false
	iteratorOptions.ContinueOnError = false
	iter = db.NewIterator(iteratorOptions)

	// Should stop iteration on first error
	iter.Rewind()
	require.Error(t, iter.Err())
	require.False(t, iter.Valid())
	iter.Close()
}

func TestIterator_UseTwice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	db.Put([]byte("aceds"), []byte("value1"))
	db.Put([]byte("eedsq"), []byte("value2"))
	db.Put([]byte("sedas"), []byte("value3"))
	db.Put([]byte("efeds"), []byte("value4"))
	db.Put([]byte("bbdes"), []byte("value5"))

	iteratorOptions := DefaultIteratorOptions
	iter := db.NewIterator(iteratorOptions)

	for iter.Seek([]byte("bbe")); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
	}

	// rewind and iterate again
	iter.Rewind()
	require.True(t, iter.Valid())
	for iter.Seek([]byte("bbe")); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
	}
}

func TestIterator_UseAfterClose(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	db.Put([]byte("aceds"), []byte("value1"))
	db.Put([]byte("eedsq"), []byte("value2"))

	iteratorOptions := DefaultIteratorOptions
	iter := db.NewIterator(iteratorOptions)

	for iter.Seek([]byte("bbe")); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
	}

	iter.Close()
	require.False(t, iter.Valid())
}
