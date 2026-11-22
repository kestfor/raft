package wal

import (
	"errors"
	"testing"
)

func TestMemoryReplayOwnsRecords(t *testing.T) {
	var journal Memory
	data := []byte("a")
	for _, ch := range []byte("ab") {
		data[0] = ch
		if err := journal.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	stop := errors.New("stop replay")
	if err := journal.Replay(func(record []byte) error {
		record[0] = 'x'
		return stop
	}); err != stop {
		t.Fatalf("replay returned %v, want %v", err, stop)
	}

	var got []byte
	if err := journal.Replay(func(record []byte) error {
		got = append(got, record...)
		return nil
	}); err != nil || string(got) != "ab" {
		t.Fatalf("replayed %q, err = %v", got, err)
	}
}
