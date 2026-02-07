package cask

import "errors"

var (
	ErrKeyIsEmpty      = errors.New("the key is empty")
	ErrKeyNotFound     = errors.New("key not found in database")
	ErrRecordExpired   = errors.New("the record has expired, key will be deleted")
	ErrDbDirInUse      = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch   = errors.New("the batch is read only")
	ErrBatchCommitted  = errors.New("the batch is committed")
	ErrBatchRolledBack = errors.New("the batch is rolled back")
	ErrDbClosed        = errors.New("the database is closed")
	ErrMergeRunning    = errors.New("the merge operation is running")
	ErrWatchDisabled   = errors.New("the watch is disabled")
)
