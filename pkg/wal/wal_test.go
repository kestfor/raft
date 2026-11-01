package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func openLog(t *testing.T, path string) *Log {
	t.Helper()
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = l.Close() })
	return l
}

func records(t *testing.T, l *Log) [][]byte {
	t.Helper()
	var got [][]byte
	if err := l.Replay(func(data []byte) error { got = append(got, data); return nil }); err != nil {
		t.Fatal(err)
	}

	return got
}

func TestAppendReplayAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l := openLog(t, path)
	want := [][]byte{[]byte("first"), {}, {0, 255, 1}, []byte("last")}
	for _, data := range want {
		if err := l.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	if got := records(t, l); !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l = openLog(t, path)
	if got := records(t, l); !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen = %v", got)
	}

	stop := errors.New("stop")
	if err := l.Replay(func([]byte) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error = %v", err)
	}

	if err := l.Append([]byte("after replay")); err != nil {
		t.Fatal(err)
	}

	want = append(want, []byte("after replay"))
	if got := records(t, l); !reflect.DeepEqual(got, want) {
		t.Fatalf("append after replay = %v", got)
	}
}

func TestEveryIncompleteTailIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	l := openLog(t, path)
	for _, data := range [][]byte{[]byte("committed"), []byte("incomplete final record")} {
		if err := l.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	prefix := len(magic) + headerSize + len("committed")
	for cut := prefix; cut < len(full); cut++ {
		t.Run(strconv.Itoa(cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			if err := os.WriteFile(path, full[:cut], 0o600); err != nil {
				t.Fatal(err)
			}

			l := openLog(t, path)
			if got := records(t, l); len(got) != 1 || string(got[0]) != "committed" {
				t.Fatalf("recovered = %q", got)
			}

			if err := l.Append([]byte("replacement")); err != nil {
				t.Fatal(err)
			}

			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			got := records(t, openLog(t, path))
			if len(got) != 2 || string(got[1]) != "replacement" {
				t.Fatalf("after repair = %q", got)
			}
		})
	}
}

func TestCorruptionIsNotSilentlyRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	l := openLog(t, path)
	if err := l.Append([]byte("payload")); err != nil {
		t.Fatal(err)
	}

	_ = l.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, offset := range []int{0, len(magic), len(magic) + 4, len(magic) + 8, len(full) - 1} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			corrupt := bytes.Clone(full)
			corrupt[offset] ^= 1
			path := filepath.Join(t.TempDir(), "log")
			if err := os.WriteFile(path, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}

			if l, err := Open(path); !errors.Is(err, ErrCorrupt) {
				if l != nil {
					_ = l.Close()
				}

				t.Fatalf("open corrupt file = %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, corrupt) {
				t.Fatalf("corrupt file was modified: %v", err)
			}
		})
	}
}

func TestExclusiveOpenAndFailedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l := openLog(t, path)
	if second, err := Open(path); !errors.Is(err, ErrLocked) {
		if second != nil {
			_ = second.Close()
		}

		t.Fatalf("second open = %v", err)
	}

	if err := l.Append(make([]byte, MaxRecordSize+1)); err == nil {
		t.Fatal("accepted oversized record")
	}

	if err := l.Append([]byte("good")); err != nil {
		t.Fatal(err)
	}

	// Force a real I/O error, then verify the log stays unusable.
	_ = l.file.Close()
	err := l.Append([]byte("bad"))
	if err == nil || l.Append([]byte("retry")) != err || l.Replay(nil) != err {
		t.Fatal("failed WAL accepted another operation")
	}

	_ = l.Close()
	if err := l.Append(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close = %v", err)
	}

	if got := records(t, openLog(t, path)); len(got) != 1 || string(got[0]) != "good" {
		t.Fatalf("reopened records = %q", got)
	}
}

func TestIncompleteFileHeader(t *testing.T) {
	for cut := range len(magic) {
		t.Run(strconv.Itoa(cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			if err := os.WriteFile(path, []byte(magic[:cut]), 0o600); err != nil {
				t.Fatal(err)
			}

			l := openLog(t, path)
			if err := l.Append([]byte("first")); err != nil {
				t.Fatal(err)
			}

			if got := records(t, l); len(got) != 1 || string(got[0]) != "first" {
				t.Fatalf("records=%q", got)
			}
		})
	}
}
