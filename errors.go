package cask

import "errors"

var (
	ErrKeyIsEmpty          = errors.New("key is empty")
	ErrKeyExpiredOrDeleted = errors.New("key has either expired or been marked for deletion")
	ErrKeyNotFound         = errors.New("not found in database")
	ErrDbDirInUse          = errors.New("database directory is used by another process")
	ErrReadOnlyBatch       = errors.New("batch is read only")
	ErrBatchCommitted      = errors.New("batch has been committed")
	ErrBatchRolledBack     = errors.New("batch has been rolled back")
	ErrDbClosed            = errors.New("database is closed")
	ErrMergeRunning        = errors.New("merge operation is running")
	ErrWatchDisabled       = errors.New("watch is disabled")
)
