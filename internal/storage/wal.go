package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

// WAL defines the contract for write-ahead log storage.
type WAL interface {
	Append(record *raftv1.WALRecord) error
	AppendBatch(records []*raftv1.WALRecord) ([]int64, error)
	ReplayAll() ([]*raftv1.WALRecord, error)
	ReplayWithOffsets() ([]ReplayedRecord, error)
	TruncateAt(offset int64) error
	Close() error
	Sync() error
}

// ReplayedRecord pairs a recovered WALRecord with its byte offset in the WAL.
type ReplayedRecord struct {
	Offset int64
	Record *raftv1.WALRecord
}

// FileWAL implements WAL backed by an append-only file.
type FileWAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

var _ WAL = (*FileWAL)(nil)

// OpenFileWAL opens or creates a WAL file at path.
func OpenFileWAL(path string) (*FileWAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}
	return &FileWAL{
		file: f,
		path: path,
	}, nil
}

// Append writes a single WALRecord and fsyncs.
func (w *FileWAL) Append(record *raftv1.WALRecord) error {
	_, err := w.AppendBatch([]*raftv1.WALRecord{record})
	return err
}

// AppendBatch writes a slice of WALRecords sequentially and fsyncs once.
// Returns the starting byte offset for each record.
func (w *FileWAL) AppendBatch(records []*raftv1.WALRecord) ([]int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(records) == 0 {
		return nil, nil
	}

	curOffset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek wal end: %w", err)
	}

	offsets := make([]int64, len(records))
	var buf []byte
	runningOffset := curOffset

	for i, rec := range records {
		payload, err := proto.Marshal(rec)
		if err != nil {
			return nil, fmt.Errorf("marshal wal record: %w", err)
		}
		if uint32(len(payload)) > MaxWALRecordSize {
			return nil, fmt.Errorf("wal record exceeds max size %d: got %d", MaxWALRecordSize, len(payload))
		}
		offsets[i] = runningOffset
		frame := encodeWALFrame(payload)
		buf = append(buf, frame...)
		runningOffset += int64(len(frame))
	}

	n, err := w.file.Write(buf)
	if err != nil {
		return nil, fmt.Errorf("write wal records: %w", err)
	}
	if n != len(buf) {
		return nil, io.ErrShortWrite
	}
	if err := w.file.Sync(); err != nil {
		return nil, fmt.Errorf("sync wal: %w", err)
	}
	return offsets, nil
}

// ReplayWithOffsets sequentially scans the WAL from offset 0, returning valid records
// and their start offsets. On any torn record, oversized length, or corruption,
// it truncates the WAL to the last valid record and terminates scan.
func (w *FileWAL) ReplayWithOffsets() ([]ReplayedRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek wal start: %w", err)
	}

	var results []ReplayedRecord
	offset := int64(0)
	lenHeader := make([]byte, 4)

	truncateAndSync := func(validLen int64) error {
		if err := w.file.Truncate(validLen); err != nil {
			return fmt.Errorf("truncate wal to %d: %w", validLen, err)
		}
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal after truncate: %w", err)
		}
		if _, err := w.file.Seek(validLen, io.SeekStart); err != nil {
			return fmt.Errorf("seek wal to %d: %w", validLen, err)
		}
		return nil
	}

	for {
		startOffset := offset
		_, err := io.ReadFull(w.file, lenHeader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Torn length header: truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		payloadLen := binary.LittleEndian.Uint32(lenHeader)
		if payloadLen == 0 || payloadLen > MaxWALRecordSize {
			// Zero length or oversized length prefix (> 1 MiB): reject before allocating,
			// truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			// Torn payload: truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		crcHeader := make([]byte, 4)
		if _, err := io.ReadFull(w.file, crcHeader); err != nil {
			// Torn CRC: truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		expectedCRC := binary.LittleEndian.Uint32(crcHeader)
		actualCRC := crc32.ChecksumIEEE(payload)
		if expectedCRC != actualCRC {
			// Corrupt checksum: truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		var record raftv1.WALRecord
		if err := proto.Unmarshal(payload, &record); err != nil {
			// Corrupt protobuf: truncate at startOffset and terminate scan.
			if truncErr := truncateAndSync(startOffset); truncErr != nil {
				return results, truncErr
			}
			break
		}

		results = append(results, ReplayedRecord{
			Offset: startOffset,
			Record: &record,
		})
		offset = startOffset + 4 + int64(payloadLen) + 4
	}

	return results, nil
}

// ReplayAll sequentially scans the WAL, returning only the recovered WALRecords.
func (w *FileWAL) ReplayAll() ([]*raftv1.WALRecord, error) {
	replayed, err := w.ReplayWithOffsets()
	if err != nil {
		return nil, err
	}
	records := make([]*raftv1.WALRecord, len(replayed))
	for i, r := range replayed {
		records[i] = r.Record
	}
	return records, nil
}

// TruncateAt truncates the WAL file to offset and fsyncs.
func (w *FileWAL) TruncateAt(offset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate wal at %d: %w", offset, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal after truncate: %w", err)
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal to %d: %w", offset, err)
	}
	return nil
}

// Sync flushes buffered writes to disk.
func (w *FileWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Close flushes and closes the WAL file.
func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func encodeWALFrame(payload []byte) []byte {
	buf := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	binary.LittleEndian.PutUint32(buf[4+len(payload):], crc32.ChecksumIEEE(payload))
	return buf
}
