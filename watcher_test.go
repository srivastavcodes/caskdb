package cask

import (
	"math/rand"
	"testing"

	"github.com/srivastavcodes/caskdb/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatch_Insert_Scan(t *testing.T) {
	capacity := 1000
	// There are two spaces to determine whether the queue is full and overwrite the write.
	size := capacity - 2
	q := make([][2][]byte, 0, size)
	w := NewWatcher(uint64(capacity))
	for i := 0; i < size; i++ {
		key := utils.GetTestKey(rand.Int())
		val := utils.RandomBytes(128)
		q = append(q, [2][]byte{key, val})
		w.putEvent(&Event{
			Action:  WatchActionPut,
			Key:     key,
			Val:     val,
			BatchId: 0,
		})
	}

	for i := 0; i < size; i++ {
		event := w.getEvent()
		require.NotEmpty(t, event)
		key := q[i][0]
		require.Equal(t, key, event.Key)
		val := q[i][1]
		require.Equal(t, val, event.Val)
	}
}

func TestWatch_Rotate_Insert_Scan(t *testing.T) {
	capacity := 1000

	q := make([][2][]byte, capacity)
	w := NewWatcher(uint64(capacity))

	for i := 0; i < 2500; i++ {
		key := utils.GetTestKey(rand.Int())
		val := utils.RandomBytes(128)
		w.putEvent(&Event{
			Action:  WatchActionPut,
			Key:     key,
			Val:     val,
			BatchId: 0,
		})
		idx := i % capacity
		q[idx] = [2][]byte{key, val}
	}

	idx := int(w.eventQ.Front)
	for {
		if w.eventQ.isEmpty() {
			break
		}
		event := w.getEvent()

		key := q[idx][0]
		require.Equal(t, key, event.Key)

		val := q[idx][1]
		require.Equal(t, val, event.Val)

		idx = (idx + 1) % capacity
	}
	go func() {
		key := []byte("parth")
		val := utils.RandomBytes(128)

		q[idx] = [2][]byte{key, val}
		w.putEvent(&Event{
			Action:  WatchActionPut,
			Key:     key,
			Val:     val,
			BatchId: 0,
		})
	}()
	event := w.getEvent()

	key := q[idx][0]
	require.Equal(t, key, event.Key)

	val := q[idx][1]
	require.Equal(t, val, event.Val)
}

func TestWatch_Put_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 10
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	w, err := db.Watch()
	assert.NoError(t, err)
	for i := 0; i < 50; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomBytes(128)
		err = db.Put(key, value)
		assert.NoError(t, err)
		event := <-w
		assert.Equal(t, WatchActionPut, event.Action)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, value, event.Val)
	}
}

func TestWatch_Put_Delete_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 10
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	w, err := db.Watch()
	assert.NoError(t, err)

	key := utils.GetTestKey(rand.Int())
	value := utils.RandomBytes(128)
	err = db.Put(key, value)
	assert.NoError(t, err)
	err = db.Delete(key)
	assert.NoError(t, err)

	for i := 0; i < 2; i++ {
		event := <-w
		assert.Equal(t, key, event.Key)
		switch event.Action {
		case WatchActionPut:
			assert.Equal(t, value, event.Val)
		case WatchActionDelete:
			assert.Empty(t, event.Val)
		}
	}
}

func TestWatch_Batch_Put_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 1000
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	w, err := db.Watch()
	require.NoError(t, err)

	times := 100
	batch := db.NewBatch(DefaultBatchOptions)
	for i := 0; i < times; i++ {
		err = batch.Put(utils.GetTestKey(rand.Int()), utils.RandomBytes(128))
		assert.NoError(t, err)
	}
	err = batch.Commit()
	assert.NoError(t, err)

	var batchId uint64
	for i := 0; i < times; i++ {
		event := <-w
		if i == 0 {
			batchId = event.BatchId
		}
		assert.Equal(t, batchId, event.BatchId)
	}
}
