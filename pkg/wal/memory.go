package wal

import (
	"fmt"
	"slices"
	"sync"
)

// Memory holds journal records for the lifetime of this instance.
// Its zero value is an empty journal.
type Memory struct {
	mu      sync.Mutex
	records [][]byte
}

func (m *Memory) Append(data []byte) error {
	if len(data) > MaxRecordSize {
		return fmt.Errorf("wal: record exceeds %d bytes", MaxRecordSize)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, slices.Clone(data))
	return nil
}

// Replay visits a snapshot of the journal. Each slice belongs to the caller.
func (m *Memory) Replay(visit func([]byte) error) error {
	m.mu.Lock()
	records := m.records
	m.mu.Unlock()

	// Append only writes beyond the captured length. Existing records are immutable.
	for _, data := range records {
		if err := visit(slices.Clone(data)); err != nil {
			return err
		}
	}
	return nil
}
