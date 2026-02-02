package cask

import (
	"encoding/binary"

	wal "github.com/srivastavcodes/caskdb/write-ahead-log"
	"github.com/valyala/bytebufferpool"
)

// LogRecordType is the type of the Log Record.
type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordBatchFinished
)

// maxLogRecordHeaderSize is the maximum size of a log record header.
//
// type  batchId  keySize  valSize  expire
//
// 1   +   10   +   5   +     5   +   10  = 31
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2 + 1

// LogRecord is the log record of the key/val pair. It contains the key, the val,
// the record type, and the batch id. It will be encoded to byte slice and
// written to the wal.
type LogRecord struct {
	Key     []byte
	Val     []byte
	BatchId uint64
	Type    LogRecordType
	Expire  int64
}

// IsExpired checks whether the log record is expired.
func (lr *LogRecord) IsExpired(now int64) bool {
	return lr.Expire > 0 && lr.Expire <= now
}

// IndexRecord is the index record of the key. It contains the key, the position,
// and the record type in the wal.
// Only used in startup to rebuild the index.
type IndexRecord struct {
	key        []byte
	recordType LogRecordType
	pos        *wal.ChunkPosition
}

// encodeLogRecord encodes the LogRecord to a byte slice.
// +-------------+-------------+-------------+--------------+---------------+---------+---------+
// |    type     |  batch id   |   key size  |   value size |     expire    |  key    |   val   |
// +-------------+-------------+-------------+--------------+---------------+---------+---------+
//
//	1 byte	varint(max 10) varint(max 5)  varint(max 5) varint(max 10) --------data--------
func encodeLogRecord(lr *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	header[0] = byte(lr.Type)
	var index = 1

	index += binary.PutUvarint(header[index:], lr.BatchId)
	index += binary.PutUvarint(header[index:], uint64(len(lr.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(lr.Val)))
	index += binary.PutVarint(header[index:], lr.Expire)

	_, _ = buf.Write(header[:index])
	_, _ = buf.Write(lr.Key)
	_, _ = buf.Write(lr.Val)

	return buf.Bytes()
}

// decodeLogRecord decodes the log record from the given byte slice.
func decodeLogRecord(buf []byte) *LogRecord {
	recordType := buf[0]
	var index = 1

	batchId, n := binary.Uvarint(buf[index:])
	index += n
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	valSize, n := binary.Uvarint(buf[index:])
	index += n
	expire, n := binary.Varint(buf[index:])
	index += n

	key := make([]byte, keySize)
	copy(key, buf[index:index+int(keySize)])
	index += int(keySize)

	val := make([]byte, valSize)
	copy(val, buf[index:index+int(valSize)])
	index += int(valSize)

	return &LogRecord{Key: key, Val: val,
		BatchId: batchId,
		Type:    LogRecordType(recordType),
		Expire:  expire,
	}
}

// encodeHintRecord encodes the key and its position in the wal to a byte slice.
// +-------------+-------------+-------------+--------------+
// |  segmentId  | BlockNumber | ChunkOffset |  ChunkSize   |
// +-------------+-------------+-------------+--------------+
//
//	5                 5            10             5
func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	buf := make([]byte, 25)
	var index int

	index += binary.PutUvarint(buf[index:], uint64(pos.SegmentId))
	index += binary.PutUvarint(buf[index:], uint64(pos.BlockNumber))
	index += binary.PutUvarint(buf[index:], uint64(pos.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(pos.ChunkSize))

	result := make([]byte, index+len(key))
	copy(result, buf[:index])
	copy(result[index:], key)

	return result
}

// decodeHintRecord decodes the key and its position in the wal from the given byte
// slice.
func decodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var index int

	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	key := buf[index:]

	pos := &wal.ChunkPosition{
		SegmentId:   wal.SegmentId(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset), ChunkSize: uint32(chunkSize),
	}
	return key, pos
}

// encodeMergeFinRecord encodes the segment id in a little endian format and
// returns the result.
func encodeMergeFinRecord(segmentId wal.SegmentId) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
