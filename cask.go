package cask

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/srivastavcodes/caskdb/index"
	"github.com/srivastavcodes/caskdb/utils"
	wal "github.com/srivastavcodes/write-ahead-log"
)

// watermarkedFileExt is the file name used to track the progress
// of merge/compaction operations.

const (
	watermarkedFileExt = ".watermark"
	dataFileNameSuffix = ".SEG"
	caskLockFileName   = "db.lock"
	hintFileNameSuffix = ".HINT"
)

// CaskDb represents a caskDb instance which is built on the bitcask model, which
// is a log-structured storage. It uses a wal to store the data and uses an in
// memory index to store the key and the position of the data in the wal, the
// index gets rebuilt when the database is opened.
//
// The main advantage of CaskDb is that it is very fast to write, read, and delete
// data because it only needs one disk IO to complete a single operation;
// however, because it stores all the keys and their value's positions in-memory,
// the memory size limits our total capacity.
//
// Which means caskDb fits your needs if you don't need to store a large number
// of keys and need lightning fast operations.
type CaskDb struct {
	lockF            *flock.Flock // lockF prevents multiple processes from using the same dir.
	rwm              sync.RWMutex
	header           []byte
	dataFiles        *wal.Wal // dataFiles are sets of segment files in wal, which holds the data.
	hintFile         *wal.Wal // hintFile is used to store the key and the position for fast startup.
	expiredCursorKey []byte   // location where DeleteExpiredKeys execute.
	opts             Options
	index            index.Indexer
	closed           bool
	cond             *sync.Cond // experimental usage with merging to avoid polling
	merging          bool       // indicates if database is merging
	recordPool       sync.Pool
	batchPool        sync.Pool
	watchCh          chan *Event
	watcher          *Watcher
	cronScheduler    *cron.Cron // cron schedular for auto merge tasks.
}

// Stats provides database metrics.
type Stats struct {
	KeyCount int   // KeyCount represents the number of keys in db.
	DiskSize int64 // Total disk size of the database directory.
}

// Open opens a database with the specified options. If the database directory
// does not exist, it will be created.
//
// Multiple processes cannot use the same database directory at the same time;
// returns ErrDatabaseDirInUse error if tried.
//
// It will open the wal files in the database directory and load the index from
// them. Returns the database instance or an error if any.
func Open(opts Options) (*CaskDb, error) {
	if err := checkOptions(opts); err != nil {
		return nil, err
	}
	if _, err := os.Stat(opts.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// create file lock to prevent multiple processes from using the
	// same database directory.
	lockF := flock.New(filepath.Join(opts.DirPath, caskLockFileName))

	acquired, err := lockF.TryLock()
	if err != nil {
		_ = lockF.Close()
		return nil, err
	}
	if !acquired {
		_ = lockF.Close()
		return nil, ErrDatabaseDirInUse
	}
	cdb := &CaskDb{
		lockF:  lockF,
		header: make([]byte, maxLogRecordHeaderSize),
		opts:   opts,
		index:  index.NewIndexer(),

		batchPool: sync.Pool{
			New: defaultNewBatch,
		},
		recordPool: sync.Pool{
			New: emptyLogRecord,
		},
	}
	if err = loadMergeFiles(opts.DirPath); err != nil {
		return nil, err
	}
	cdb.cond = sync.NewCond(&cdb.rwm)

	if cdb.dataFiles, err = cdb.openWalFiles(); err != nil {
		return nil, err
	}
	if err = cdb.loadIndex(); err != nil {
		return nil, err
	}
	if opts.WatchQueueSize > 0 {
		cdb.watchCh = make(chan *Event, 100)
		cdb.watcher = NewWatcher(opts.WatchQueueSize)
		// run a goroutine to synchronize event info
		go cdb.watcher.sendEvent(cdb.watchCh)
	}
	// enable auto merge task
	if len(opts.AutoMergeCronExpr) > 0 {
		cdb.cronScheduler = cron.New(
			cron.WithParser(
				cron.NewParser(cron.SecondOptional | cron.Minute |
					cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
				),
			),
		)
		_, err = cdb.cronScheduler.AddFunc(opts.AutoMergeCronExpr, func() {
			// todo: we can introduce errCh or something to handle background errors
			_ = cdb.Merge(true)
		})
		if err != nil {
			return nil, err
		}
		cdb.cronScheduler.Start()
	}
	return cdb, nil
}

// Put puts a key value pair into the database. Internally, it will open a new
// batch and commit it; so you can think of a batch operation that has only
// one put operation.
func (cdb *CaskDb) Put(key, val []byte) error {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(false, false, cdb)

	if err := batch.Put(key, val); err != nil {
		if err = batch.Rollback(); err != nil {
			log.Println("rollback failed:", err.Error())
		}
		return err
	}
	return batch.Commit()
}

// PutWithTTL puts a key value pair with ttl into the database. Internally, it will
// open a new batch and commit it; so you can think of a batch operation that has
// only one put operation. It is the callers' responsibility to call Sync if data must
// be synced immediately.
func (cdb *CaskDb) PutWithTTL(key, val []byte, ttl time.Duration) error {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(false, false, cdb)
	if err := batch.PutWithTTL(key, val, ttl); err != nil {
		if err = batch.Rollback(); err != nil {
			log.Println("rollback failed:", err.Error())
		}
		return err
	}
	return batch.Commit()
}

// Get gets the value for the key in the database. Internally, it will open a new
// batch and commit it; so you can think of a batch operation that has only one
// get operation.
func (cdb *CaskDb) Get(key []byte) ([]byte, error) {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		if err := batch.Commit(); err != nil {
			log.Println("commit failed:", err.Error())
		}
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(true, false, cdb)
	return batch.Get(key)
}

// Delete deletes a key value pair from the database. Internally, it will open a
// new batch and commit it; so you can think of a batch operation that has only
// one delete operation. It is the callers' responsibility to call Sync if data
// must be synced immediately.
func (cdb *CaskDb) Delete(key []byte) error {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(false, false, cdb)

	if err := batch.Delete(key); err != nil {
		if err = batch.Rollback(); err != nil {
			log.Println("rollback failed:", err.Error())
		}
		return err
	}
	return batch.Commit()
}

// Exists gets the value for the key in the database. Internally, it will open
// a new batch and commit it; so you can think of a batch operation that has
// only one get operation.
func (cdb *CaskDb) Exists(key []byte) (bool, error) {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		if err := batch.Commit(); err != nil {
			log.Println("commit failed:", err.Error())
		}
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(true, false, cdb)
	return batch.Exists(key)
}

// Expire set an expiration for the key in the database. Internally, it will open
// a new batch and commit it; so you can think of a batch operation that has only
// one delete operation. It is the callers' responsibility to call Sync if data
// must be synced immediately.
func (cdb *CaskDb) Expire(key []byte, ttl time.Duration) error {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(false, false, cdb)

	if err := batch.Expire(key, ttl); err != nil {
		if err = batch.Rollback(); err != nil {
			log.Println("rollback failed:", err.Error())
		}
	}
	return batch.Commit()
}

// ExpiresIn gets the ttl for the key in the database. Internally, it will open
// a new batch and commit it; so you can think of a batch operation that has
// only one get operation.
func (cdb *CaskDb) ExpiresIn(key []byte) (time.Duration, error) {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		if err := batch.Commit(); err != nil {
			log.Println("commit failed:", err.Error())
		}
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(true, false, cdb)
	return batch.ExpiresIn(key)
}

// PersistKey removes the expiration for the key in the database. Internally, it
// will open a new batch and commit it; so you can think of a batch operation that
// has only one delete operation. It is the callers' responsibility to call Sync
// if data must be synced immediately.
func (cdb *CaskDb) PersistKey(key []byte) error {
	batch := cdb.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		cdb.batchPool.Put(batch)
	}()
	batch.init(false, false, cdb)

	if err := batch.PersistKey(key); err != nil {
		if err = batch.Rollback(); err != nil {
			log.Println("rollback failed:", err.Error())
		}
	}
	return batch.Commit()
}

// Watch returns a receiver channel of an Event type which can be used to listen
// for events sent by the Watcher.
func (cdb *CaskDb) Watch() (<-chan *Event, error) {
	if cdb.opts.WatchQueueSize <= 0 {
		return nil, ErrWatchDisabled
	}
	return cdb.watchCh, nil
}

// DeleteExpiredKeys scans the entire index in ascending order to delete expired
// keys. It is a time-consuming operation, so we need to specify a timeout to
// prevent the db from being unavailable for a long time.
func (cdb *CaskDb) DeleteExpiredKeys(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// use channel to pass error safely without race condition.
	errch := make(chan error, 1)

	now := time.Now().UnixNano()
	go func() {
		cdb.rwm.Lock()
		defer cdb.rwm.Unlock()

		for {
			positions := make([]*wal.ChunkPosition, 0, 100)
			// function to stop after 100 keys have been selected
			handlerFn := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
				positions = append(positions, pos)
				return !(len(positions) >= 100), nil
			}
			cdb.index.AscendGreaterOrEqual(cdb.expiredCursorKey, handlerFn)

			// if the keys in db.index have been traversed, len(positions)
			// will be 0.
			if len(positions) == 0 {
				errch <- nil
				cdb.expiredCursorKey = nil
				return
			}
			// delete it from the index if the key is expired.
			for _, pos := range positions {
				enc, err := cdb.dataFiles.Read(pos)
				if err != nil {
					errch <- err
					return
				}
				rec := decodeLogRecord(enc)
				if rec.IsExpired(now) {
					cdb.index.Delete(rec.Key)
				}
				cdb.expiredCursorKey = rec.Key
			}
		}
	}()
	// we return nil if a timeout occurs since that is the expected behavior.
	// The background goroutine will continue running until completion.
	select {
	case <-ctx.Done():
		return nil
	case err := <-errch:
		return err
	}
}

// loadIndex loads the index from the hint file and from the wal respectively;
// returning any errors if encountered.
func (cdb *CaskDb) loadIndex() error {
	if err := cdb.loadIndexFromHintFile(); err != nil {
		return err
	}
	if err := cdb.loadIndexFromWal(); err != nil {
		return err
	}
	return nil
}

// loadIndexFromWal reconstructs the in-memory index by scanning data files from
// the write-ahead log. It skips segments that have already been merged
// (those at or below the compaction watermark) and processes records in batches,
// handling normal puts, deletes, and expired keys. Records are indexed only after
// their batch is marked as finished to ensure atomicity.
func (cdb *CaskDb) loadIndexFromWal() error {
	watermarkedSegId, err := getCompactionWatermark(cdb.opts.DirPath)
	if err != nil {
		return err
	}
	var (
		indexRecords = make(map[uint64][]*IndexRecord)
		now          = time.Now().UnixNano()
		reader       = cdb.dataFiles.NewReader()
	)
	cdb.dataFiles.SetIsStartupTraversal(true)
	for {
		// if the current seg id is less than the watermarked segment id,
		// we can skip this segment because it has been merged, and we
		// can load the index directly from the hint file directly.
		if reader.CurrentSegmentId() <= watermarkedSegId {
			reader.SkipCurrentSegment()
			continue
		}
		enc, pos, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		record := decodeLogRecord(enc)

		switch record.Type {
		// if we get the end of a batch, all the records in this batch are
		// ready to be indexed.
		case LogRecordBatchFinished:
			batchId := binary.BigEndian.Uint64(record.Key)
			for _, rec := range indexRecords[batchId] {
				if rec.recordType == LogRecordNormal {
					cdb.index.Put(rec.key, rec.pos)
				} else if rec.recordType == LogRecordDeleted {
					cdb.index.Delete(rec.key)
				}
			}
			// delete index records according to batch id after indexing.
			delete(indexRecords, batchId)
		case LogRecordNormal:
			// if the record is a normal record and the batch id is 0, it
			// means that the record is involved in a merge operation; so
			// put the record into index directly.
			if record.BatchId == MergeFinishedBatchId {
				cdb.index.Put(record.Key, pos)
			}
			fallthrough
		default:
			if record.IsExpired(now) {
				cdb.index.Delete(record.Key)
				continue
			}
			// put the records into an ephemeral indexRecords map
			indexRecords[record.BatchId] = append(
				indexRecords[record.BatchId],
				&IndexRecord{
					key: record.Key, pos: pos,
					recordType: record.Type,
				},
			)
		}
	}
	cdb.dataFiles.SetIsStartupTraversal(false)
	return nil
}

// openWalFiles returns a pointer to wal.Wal with db options as wal options;
// in case of an error it returns nil and the error.
func (cdb *CaskDb) openWalFiles() (*wal.Wal, error) {
	walFiles, err := wal.Open(wal.Options{
		DirPath:        cdb.opts.DirPath,
		SegmentSize:    cdb.opts.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		Sync:           cdb.opts.Sync,
		BytesPerSync:   cdb.opts.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

// Close gracefully shuts down the CaskDb instance by stopping any running merge operations,
// closing all open files, releasing the database lock, and cleaning up resources like the
// watcher if enabled. It waits for any ongoing merge to complete before proceeding.
func (cdb *CaskDb) Close() error {
	// close auto merge cron schedular first to prevent a new merge from starting
	if cdb.cronScheduler != nil {
		cdb.cronScheduler.Stop()
	}
	/* polling pattern
	for cdb.merging.Load() {
		time.Sleep(100 * time.Millisecond)
	}
	*/
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	// using signals to suspend execution until merge completes
	for cdb.merging {
		cdb.cond.Wait()
	}
	if err := cdb.closeFiles(); err != nil {
		return err
	}
	if err := cdb.lockF.Unlock(); err != nil {
		return err
	}
	if cdb.opts.WatchQueueSize > 0 {
		cdb.watcher.Close()
		close(cdb.watchCh)
	}
	// todo: make sure merge happens under the same mutex when it checks 'closed'?
	cdb.closed = true
	return nil
}

// Stat returns metrics regarding key size and disk consumption.
func (cdb *CaskDb) Stat() *Stats {
	cdb.rwm.RLock()
	defer cdb.rwm.RUnlock()

	size, err := utils.DirSize(cdb.opts.DirPath)
	if err != nil {
		log.Println("error computing directory size:", err)
	}
	return &Stats{
		KeyCount: cdb.index.Size(),
		DiskSize: size,
	}
}

// closeFiles closes the underlying data files as well as hint files if exists.
func (cdb *CaskDb) closeFiles() error {
	if err := cdb.dataFiles.Close(); err != nil {
		return err
	}
	if cdb.hintFile == nil {
		return nil
	}
	return cdb.hintFile.Close()
}

// Sync all data files to the underlying storage.
func (cdb *CaskDb) Sync() error {
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	return cdb.dataFiles.Sync()
}

func checkOptions(opts Options) error {
	if strings.TrimSpace(opts.DirPath) == "" {
		return errors.New("options.DirPath cannot be empty")
	}
	if opts.SegmentSize <= 0 {
		return errors.New("options.SegmentSize (data file) must be greater than 0")
	}
	if len(opts.AutoMergeCronExpr) > 0 {
		_, err := cron.NewParser(
			cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		).Parse(opts.AutoMergeCronExpr)
		if err != nil {
			return fmt.Errorf("invalid AutoMergeCronExpr: %w", err)
		}
	}
	return nil
}
