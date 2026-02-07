package cask

import (
	"math/rand"
	"testing"

	"github.com/srivastavcodes/caskdb/utils"
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
