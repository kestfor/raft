// Package wal provides a synchronous, append-only log of opaque records.
// It supports one open writer per file on Unix systems.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	magic         = "RAFTWAL1"
	headerSize    = 12
	MaxRecordSize = 16 << 20
)

var (
	ErrCorrupt = errors.New("wal: corrupt log")
	ErrClosed  = errors.New("wal: log is closed")
	ErrLocked  = errors.New("wal: log is already open")
)

// Log serializes its methods. After a write or sync failure it rejects further
// operations until closed and reopened, since the last write may have reached disk.
type Log struct {
	mu   sync.Mutex
	file *os.File
	err  error
}

// Open validates the file, discards an incomplete final record, and syncs the
// recovered contents before they can be acknowledged again. Checksum failures
// are reported, never repaired. The parent directory must exist.
func Open(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	l := &Log{file: file}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%w: %w", ErrLocked, err)
	}

	if err := l.initialize(); err != nil {
		_ = file.Close()
		return nil, err
	}

	// A complete record may survive a process crash in the OS cache even if
	// Append never reached Sync. Replay must expose only durable records.
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: sync recovered log: %w", err)
	}

	// Make creation durable as well as writes to the file itself.
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		err = errors.Join(dir.Sync(), dir.Close())
	}

	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: sync directory: %w", err)
	}

	return l, nil
}

func (l *Log) initialize() error {
	info, err := l.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() < int64(len(magic)) {
		// No record can have been acknowledged before the header was synced.
		prefix := make([]byte, info.Size())
		if _, err := l.file.ReadAt(prefix, 0); err != nil {
			return err
		}

		if string(prefix) != magic[:len(prefix)] {
			return ErrCorrupt
		}

		if _, err := l.file.WriteAt([]byte(magic), 0); err != nil {
			return err
		}

		if err := l.file.Sync(); err != nil {
			return err
		}
	}

	var header [len(magic)]byte
	if _, err := l.file.ReadAt(header[:], 0); err != nil {
		return err
	}

	if string(header[:]) != magic {
		return ErrCorrupt
	}

	return l.scan(nil, true)
}

// Append writes and syncs one record before returning success. A record is the
// recovery unit: Replay returns it whole or, for an incomplete tail, not at all.
func (l *Log) Append(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.err != nil {
		return l.err
	}

	if len(data) > MaxRecordSize {
		return fmt.Errorf("wal: record exceeds %d bytes", MaxRecordSize)
	}

	frame := make([]byte, headerSize+len(data))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[:4]))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(data))
	copy(frame[headerSize:], data)

	n, err := l.file.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}

	if err == nil {
		err = l.file.Sync()
	}

	if err != nil {
		l.err = fmt.Errorf("wal: append: %w", err)
	}

	return l.err
}

// Replay visits records in order. Each slice belongs to the caller. The callback
// must not call methods on this Log. A callback error stops replay unchanged.
func (l *Log) Replay(visit func([]byte) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.err != nil {
		return l.err
	}

	return l.scan(visit, false)
}

func (l *Log) scan(visit func([]byte) error, repair bool) error {
	offset := int64(len(magic))
	for {
		var header [headerSize]byte
		n, err := l.file.ReadAt(header[:], offset)
		if errors.Is(err, io.EOF) {
			if n == 0 {
				_, err = l.file.Seek(offset, io.SeekStart)
				return err
			}

			return l.incompleteTail(offset, repair)
		}

		if err != nil {
			return fmt.Errorf("wal: read header: %w", err)
		}

		size := binary.LittleEndian.Uint32(header[:4])
		if crc32.ChecksumIEEE(header[:4]) != binary.LittleEndian.Uint32(header[4:8]) || size > MaxRecordSize {
			return fmt.Errorf("%w: header at offset %d", ErrCorrupt, offset)
		}

		data := make([]byte, int(size))
		if _, err := l.file.ReadAt(data, offset+headerSize); errors.Is(err, io.EOF) {
			return l.incompleteTail(offset, repair)
		} else if err != nil {
			return fmt.Errorf("wal: read record: %w", err)
		}

		if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[8:12]) {
			return fmt.Errorf("%w: record at offset %d", ErrCorrupt, offset)
		}

		if visit != nil {
			if err := visit(data); err != nil {
				return err
			}
		}

		offset += headerSize + int64(size)
	}
}

func (l *Log) incompleteTail(offset int64, repair bool) error {
	if !repair {
		return fmt.Errorf("%w: incomplete record at offset %d", ErrCorrupt, offset)
	}

	if err := l.file.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate incomplete tail: %w", err)
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync repaired tail: %w", err)
	}

	_, err := l.file.Seek(offset, io.SeekStart)
	return err
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrClosed
	}

	err := l.file.Close() // Closing releases the file lock, even after a failed write.
	l.file = nil
	l.err = ErrClosed

	return err
}
