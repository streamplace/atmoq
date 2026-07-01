package atmoq

import (
	"bytes"
	"io"
	"testing"
)

func TestReadSized(t *testing.T) {
	data := bytes.Repeat([]byte{0xab}, 200_000) // spans multiple grow chunks
	got, err := readSized(bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatalf("readSized: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("readSized returned %d bytes, want %d", len(got), len(data))
	}

	// Truncated input reports ErrUnexpectedEOF instead of hanging or
	// returning short data.
	_, err = readSized(bytes.NewReader(data[:100]), uint64(len(data)))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated readSized err = %v, want ErrUnexpectedEOF", err)
	}

	// Zero size reads nothing.
	if got, err := readSized(bytes.NewReader(nil), 0); err != nil || len(got) != 0 {
		t.Fatalf("zero-size readSized = %v, %v", got, err)
	}

	// The up-front allocation is bounded regardless of the claimed size: a
	// huge claim against an empty reader must error, not OOM.
	_, err = readSized(bytes.NewReader(nil), 1<<61)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("huge-claim readSized err = %v, want ErrUnexpectedEOF", err)
	}
}
