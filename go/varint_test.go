package atmoq

import (
	"bytes"
	"testing"
)

func TestAppendUvarintRoundTrip(t *testing.T) {
	for _, x := range []uint64{0, 1, 63, 64, 16383, 16384, 1 << 29, 1 << 30, 1 << 61} {
		b := appendUvarint(nil, x)
		got, err := readUvarint(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("readUvarint(%d): %v", x, err)
		}
		if got != x {
			t.Fatalf("roundtrip %d: got %d", x, got)
		}
	}
}

// appendOptionUvarint must match moq-lite's Option<u64> coding: None -> 0,
// Some(v) -> v+1.
func TestAppendOptionUvarint(t *testing.T) {
	// None encodes as a single 0 byte.
	if b := appendOptionUvarint(nil, nil); !bytes.Equal(b, []byte{0}) {
		t.Fatalf("None: got %v, want [0]", b)
	}
	// Some(v) decodes back to v+1 on the wire (the relay subtracts 1).
	for _, v := range []uint64{0, 1, 5000, 1 << 30} {
		b := appendOptionUvarint(nil, &v)
		wire, err := readUvarint(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("readUvarint(Some(%d)): %v", v, err)
		}
		if wire != v+1 {
			t.Fatalf("Some(%d): wire %d, want %d", v, wire, v+1)
		}
	}
}
