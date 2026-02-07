package cask

import (
	"sync"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

// Event is the event that occurs when the database is modified. It is used to
// synchronize the watch of the database.
type Event struct {
	Action  WatchActionType
	Key     []byte
	Val     []byte
	BatchId uint64
}

// Watcher temporarily stores event information, as it is generated until it is
// synced to CaskDb's watch.
// If Event overflows, it will remove the oldest data in order, even if Event
// hasn't been read yet.
type Watcher struct {
	cond   *sync.Cond
	mu     sync.Mutex
	eventQ eventQueue
	done   chan struct{}
}

// NewWatcher returns a pointer to Watcher with the internal queue initialized
// to the given capacity.
func NewWatcher(cap uint64) *Watcher {
	eq := eventQueue{
		Cap: cap, Events: make([]*Event, cap),
	}
	w := &Watcher{
		eventQ: eq, done: make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// Close closes the done channel stopping the watcher's goroutine.
func (w *Watcher) Close() {
	close(w.done)
	w.cond.Broadcast()
}

// putEvent pushes the event into the event queue and sends a signal, and if the
// queue is full, the oldest event gets discarded.
func (w *Watcher) putEvent(event *Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.eventQ.push(event)
	if w.eventQ.isFull() {
		w.eventQ.incrementFront()
	}
	w.cond.Signal()
}

// getEvent gets the Event from the queue if present or else blocks and waits
// for a signal. If the Watcher gets closed, it returns nil.
func (w *Watcher) getEvent() *Event {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.eventQ.isEmpty() {
		select {
		case <-w.done:
			return nil
		default:
		}
		w.cond.Wait()
	}
	return w.eventQ.pop()
}

// sendEvent sends events to the given channel. It will return when the watcher
// is closed.
func (w *Watcher) sendEvent(ch chan *Event) {
	for {
		ev := w.getEvent()
		if ev == nil {
			return
		}
		select {
		case ch <- ev:
		case <-w.done:
			return
		}
	}
}

type eventQueue struct {
	Events []*Event
	Cap    uint64
	Front  uint64 // read idx
	Back   uint64 // write idx
}

func (eq *eventQueue) incrementFront() {
	eq.Front = (eq.Front + 1) % eq.Cap
}

func (eq *eventQueue) push(event *Event) {
	eq.Events[eq.Back] = event
	eq.Back = (eq.Back + 1) % eq.Cap
}

func (eq *eventQueue) pop() *Event {
	event := eq.Events[eq.Front]
	eq.incrementFront()
	return event
}

func (eq *eventQueue) isEmpty() bool {
	return eq.Front == eq.Back
}

func (eq *eventQueue) isFull() bool {
	return (eq.Back+1)%eq.Cap == eq.Front
}
