package cask

import (
	"math/rand"
	"path/filepath"
	"strconv"
	"time"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// Options represents the available options for opening a database.
type Options struct {
	// SegmentSize specifies the maximum size of a segment file in bytes on
	// disk.
	SegmentSize int64

	// DirPath specifies the directory path where the Wal segment will be
	// stored.
	DirPath string

	// Sync is whether to synchronize writes through os buffer cache and down
	// onto the actual disk.
	// Setting sync is required for durability of a single write operation,
	// but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note: that if it is just the process that crashes (machine does not),
	// then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a 'write'
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	BytesPerSync uint32

	// WatchQueueSize is the cache length of the watch queue. If the size is
	// greater than 0, watch is enabled.
	WatchQueueSize uint64

	// AutoMergeCronExpr enables the auto merge. Auto merge will be triggered
	// when the cron expr is satisfied. Cron expression follows the standard
	// cron expression.
	// eg: "0 0 * * *" means merge at 00:00:00 every day.
	// It also supports seconds optionally. When seconds are enabled, the cron
	// expression will be like:
	// eg: "0/10 * * * * *" (every 10 seconds).
	// When auto merge is enabled, the db will be closed and reopened after
	// merge is done.
	//
	//    Note: do set the schedule too frequently, else it will affect performance.
	AutoMergeCronExpr string
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync
	Sync bool

	// ReadOnly specifies whether the batch is read-only.
	ReadOnly bool
}

// IteratorOptions defines configuration options for creating a new iterator.
type IteratorOptions struct {
	// Prefix specifies the key-prefix for filtering. If set, the iterator
	// will only traverse keys that start with this prefix; default is
	// empty (no filtering).
	Prefix []byte

	// Reverse reverses the traversal order; default is false.
	Reverse bool

	// ContinueOnError determines how the iterator handles error while iterating.
	// If true, the iterator will continue iteration and log the incoming errors;
	// otherwise, it will stop and become invalid when an error occurs.i
	ContinueOnError bool
}

func dbDirTemp() string {
	return filepath.Join("./", "caskdb"+strconv.Itoa(int(randName.Int63())))
}

var DefaultOptions = Options{
	DirPath:           dbDirTemp(),
	SegmentSize:       1 * GB,
	Sync:              false,
	BytesPerSync:      0,
	WatchQueueSize:    0,
	AutoMergeCronExpr: "",
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:          nil,
	Reverse:         false,
	ContinueOnError: false,
}

var randName = rand.NewSource(time.Now().UnixNano())
