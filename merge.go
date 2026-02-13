package cask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/srivastavcodes/caskdb/index"
	"github.com/srivastavcodes/caskdb/utils"
	wal "github.com/srivastavcodes/write-ahead-log"
	"github.com/valyala/bytebufferpool"
)

const (
	MergeDirSuffixName   = "-merge"
	MergeFinishedBatchId = 0
)

// Merge merges all the data files in the database. It will iterate all the data
// files, find the valid data, and re-write the data to a new data file.
//
// Merge operation can be a very time-consuming operation, so it is recommended
// to perform merge when the db is idle.
//
// If reopen is true, the original file will be replaced by the merge file, and
// db's index will be rebuilt after the merge completes.
func (cdb *CaskDb) Merge(reopen bool) (err error) {
	if err = cdb.doMerge(); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}
	if !reopen {
		return nil
	}
	cdb.rwm.Lock()
	defer cdb.rwm.Unlock()

	if err = cdb.closeFiles(); err != nil {
		log.Println("couldn't close old files:", err)
	}
	// replace original files.
	if err = loadMergeFiles(cdb.opts.DirPath); err != nil {
		return err
	}
	// open data files after being replaced, with the same options.
	cdb.dataFiles, err = cdb.openWalFiles()
	if err != nil {
		return err
	}
	// discard the old index.
	cdb.index = index.NewIndexer()
	// rebuild the index.
	if err = cdb.loadIndex(); err != nil {
		return err
	}
	return nil
}

func (cdb *CaskDb) doMerge() (err error) {
	cdb.rwm.Lock()

	if cdb.closed {
		cdb.rwm.Unlock()
		return ErrDbClosed
	}
	if cdb.dataFiles.IsEmpty() {
		size, _ := utils.DirSize(cdb.opts.DirPath)
		cdb.rwm.Unlock()
		log.Printf("Err: datafile is empty. size=%d", size)
		return
	}
	if cdb.merging {
		cdb.rwm.Unlock()
		return ErrMergeRunning
	}
	cdb.merging = true
	// signal all waiting goroutines that merge is completed
	defer func() {
		cdb.merging = false
		cdb.cond.Broadcast()
	}()

	prevSegFileId := cdb.dataFiles.ActiveSegmentId()
	// rotate the internal wal creating a new active segment file
	// because the older segment files will be merged and deleted
	err = cdb.dataFiles.OpenNewActiveSegment()
	if err != nil {
		return err
	}
	// we can unlock the database here because the wal has been rotated and
	// any later writes will preside on the new active segment as the merge
	// operation will only read the older segment files.
	cdb.rwm.Unlock()

	// opens a different db instance than the original one and creates a
	// merge directory to perform any writes, if directory already exists
	// its replaced by a new one.
	mergeDb, err := cdb.openMergeDb()
	if err != nil {
		return err
	}
	defer func() {
		if err := mergeDb.Close(); err != nil {
			log.Println("couldn't close merge db:", err)
		}
	}()
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	reader := cdb.dataFiles.NewReaderWithMax(prevSegFileId)
	now := time.Now().UnixNano()

	for {
		buf.Reset()
		enc, pos, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		record := decodeLogRecord(enc)
		// only LogRecordNormal will be handled; LogRecordDeleted and LogRecordBatchFinished
		// will be ignored because they are watermarks and not valid data.
		valid := record.Expire == 0 || record.Expire > uint64(now)

		if record.Type != LogRecordNormal && !valid {
			continue
		}
		cdb.rwm.Lock()
		kpos := cdb.index.Get(record.Key)
		cdb.rwm.Unlock()

		if kpos == nil || !positionEqual(kpos, pos) {
			continue
		}
		// clear the batch id of the record; since all the data after merge will
		// be valid data, it should be 0.
		record.BatchId = MergeFinishedBatchId

		// since mergeDb will never be used for any read/write operations after
		// the merge completes, we don't need to update the index.
		elr := encodeLogRecord(record, mergeDb.header, buf)

		newPos, err := mergeDb.dataFiles.Write(elr)
		if err != nil {
			return fmt.Errorf("failed to write data in merge db: %w", err)
		}
		// and now we write the new position to the wal, also referred to as a Hint
		// file in the BitCask paper.
		// The Hint file will be used to rebuild the index quickly when the db is
		// restarted.
		ehr := encodeHintRecord(record.Key, newPos)
		if _, err := mergeDb.hintFile.Write(ehr); err != nil {
			return fmt.Errorf("failed to write position in hint file: %w", err)
		}
	}
	// After rewrite all the data, we should add a file to indicate that the merge
	// operation is completed. So when we restart the database, we can know that
	// the merge is completed if the file exists, otherwise, we will delete the merge
	// directory and redo the merge operation again.
	watermarkedFile, err := mergeDb.openWatermarkedFile()
	if err != nil {
		return err
	}
	ecr := encodeCompactionRecord(prevSegFileId)

	if _, err = watermarkedFile.Write(ecr); err != nil {
		return fmt.Errorf("failed to write compaction record: %w", err)
	}
	if err = watermarkedFile.Close(); err != nil {
		return err
	}
	return
}

// openMergeDb creates and opens a new CaskDb instance in a temporary merge directory
// for compaction operations. It removes any existing merge directory, configures the
// database with sync disabled for performance, and opens a hint file to track the new
// positions of merged data. Closing the db is on the caller.
func (cdb *CaskDb) openMergeDb() (*CaskDb, error) {
	mdir := mergeDirPath(cdb.opts.DirPath)

	// remove the merge dir if it exists
	if err := os.RemoveAll(mdir); err != nil {
		return nil, err
	}
	opts := cdb.opts
	// we don't need the origin sync policy because we can sync the data
	// manually after the merge is complete.
	opts.Sync, opts.BytesPerSync = false, 0
	opts.DirPath = mdir

	mergedb, err := Open(opts)
	if err != nil {
		return nil, err
	}
	// open the hint file to write the new position of the data
	hintFile, err := wal.Open(wal.Options{
		// hint files are written once during merge and read in full on
		// startup; so disable seg rotation by setting an effectively
		// infinite segment size.
		//
		// NOTE: not the actual size of the file, but a limit determining
		// when to rotate the seg file; in this case, never
		SegmentSize:    math.MaxInt64,
		DirPath:        opts.DirPath,
		Sync:           false,
		BytesPerSync:   0,
		SegmentFileExt: hintFileNameSuffix,
	})
	if err != nil {
		_ = mergedb.Close()
		return nil, err
	}
	mergedb.hintFile = hintFile
	return mergedb, nil
}

// openWatermarkedFile opens the watermarked file which indicates a completed
// merge operation.
func (cdb *CaskDb) openWatermarkedFile() (*wal.Wal, error) {
	return wal.Open(wal.Options{
		DirPath:        cdb.opts.DirPath,
		SegmentSize:    GB,
		Sync:           false,
		BytesPerSync:   0,
		SegmentFileExt: watermarkedFileExt,
	})
}

// loadIndexFromHintFile reads hint records from the hint file and rebuilds the in-memory
// index with key-position mappings. Hint files are generated during merge operations to
// enable fast startup by avoiding a full scan of data files. Returns an error if the hint
// file cannot be opened or if reading fails (except for EOF which indicates completion).
func (cdb *CaskDb) loadIndexFromHintFile() error {
	hintFile, err := wal.Open(wal.Options{
		SegmentSize:    math.MaxInt64,
		DirPath:        cdb.opts.DirPath,
		SegmentFileExt: hintFileNameSuffix,
	})
	if err != nil {
		return err
	}
	defer func() { _ = hintFile.Close() }()

	reader := hintFile.NewReader()
	hintFile.SetIsStartupTraversal(true)

	// read all the hint records from the hint file.
	for {
		encRec, _, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		key, pos := decodeHintRecord(encRec)
		// putting it directly into the index without checking because
		// the hint records are generated by the merge operations and
		// are known to be valid (cause if they ain't...hehe, shit's gonna blow up).
		cdb.index.Put(key, pos)
	}
	hintFile.SetIsStartupTraversal(false)
	return nil
}

// loadMergeFiles loads all the merge files and copies the data to the original data
// directory. If there are no merge files, or the merge operation is not completed
// yet, it will return nil.
func loadMergeFiles(dirPath string) error {
	mdir := mergeDirPath(dirPath)

	if _, err := os.Stat(mdir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = os.RemoveAll(mdir) }()

	copyFile := func(suffix string, fileId uint32, force bool) {
		srcFile := wal.SegmentFileName(mdir, suffix, fileId)

		info, err := os.Stat(srcFile)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			panic(fmt.Sprintf(
				"loadMergeFiles: failed to stat src file %s: %v",
				srcFile, err,
			))
		}
		if !force && info.Size() == 0 {
			return
		}
		dstFile := wal.SegmentFileName(dirPath, suffix, fileId)
		// workingDir/dbdir-merge/000000001.SEG -> workingDir/dbdir/000000001.SEG
		if err = os.Rename(srcFile, dstFile); err != nil {
			slog.Error(fmt.Sprintf("loadMergeFiles-copyFile: failed to rename %s to %s: %v",
				srcFile, dstFile, err,
			))
		}
	}
	watermarkedSegId, err := getCompactionWatermark(mdir)
	if err != nil {
		return err
	}
	// now we have the watermarked segment id, so all the segment files with id
	// lesser than that should be moved to the original data directory, and the
	// original data files should be deleted.
	for id := uint32(1); id <= watermarkedSegId; id++ {
		dstFile := wal.SegmentFileName(dirPath, dataFileNameSuffix, id)
		// remove the original data file
		if _, err = os.Stat(dstFile); nil == err {
			if err = os.Remove(dstFile); err != nil {
				return err
			}
		}
		// move the merge data file to the original data directory.
		copyFile(dataFileNameSuffix, id, false)
	}
	// copy merge and hint files to the original data directory; there is only
	// one merge file, so the id is always 1, the same as the hint file.
	copyFile(watermarkedFileExt, 1, true)
	copyFile(hintFileNameSuffix, 1, true)
	return nil
}

// getCompactionWatermark reads and returns the compaction watermark segment ID
// from the merge directory. The watermark indicates the last segment that was
// successfully processed during a merge operation.
//
// It reads the segment ID from the compaction watermark file in the specified
// mergePath. If the file doesn't exist, it returns 0 with no error, indicating
// no prior merge operation. The segment ID is stored as a 4-byte little-endian
// unsigned integer after a 7-byte chunk header.
func getCompactionWatermark(mergePath string) (wal.SegmentId, error) {
	const chunkHeaderSize int64 = 7
	// check if the merge operation is completed
	cwFile, err := os.Open(wal.SegmentFileName(mergePath, watermarkedFileExt, 1))
	if err != nil {
		return 0, nil
	}
	defer func() { _ = cwFile.Close() }()
	// only 4 bytes are needed to store the segment id and the bytes
	// skipped are headers.
	cwBuf := make([]byte, 4)
	if _, err = cwFile.ReadAt(cwBuf, chunkHeaderSize); err != nil {
		return 0, err
	}
	segId := binary.LittleEndian.Uint32(cwBuf)
	return segId, nil
}

// mergeDirPath concatenates the MergeDirSuffixName into the base of dirPath and
// returns the new path.
// eg: dirPath = working/dbdir; returns working/dbdir-merge
func mergeDirPath(dirPath string) string {
	dirP := filepath.Dir(dirPath)
	base := filepath.Base(dirPath)
	return filepath.Join(dirP, base+MergeDirSuffixName)
}

func positionEqual(a, b *wal.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber && a.ChunkOffset == b.ChunkOffset
}
