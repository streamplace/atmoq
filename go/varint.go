package atmoq

import "io"

// QUIC-style variable-length integers (RFC 9000 §16), the encoding moq-lite
// uses for every length, id, and sequence number on the wire. The two most
// significant bits of the first byte select a 1, 2, 4, or 8 byte form.

// maxUvarint is the largest value a QUIC varint can carry.
const maxUvarint = 1<<62 - 1

// appendUvarint appends x to b in QUIC varint form.
func appendUvarint(b []byte, x uint64) []byte {
	switch {
	case x < 1<<6:
		return append(b, byte(x))
	case x < 1<<14:
		return append(b, byte(0x40|(x>>8)), byte(x))
	case x < 1<<30:
		return append(b, byte(0x80|(x>>24)), byte(x>>16), byte(x>>8), byte(x))
	case x < 1<<62:
		return append(b,
			byte(0xc0|(x>>56)), byte(x>>48), byte(x>>40), byte(x>>32),
			byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
	default:
		panic("atmoq: varint value exceeds 2^62-1")
	}
}

// appendString appends a varint length prefix followed by s's bytes.
func appendString(b []byte, s string) []byte {
	b = appendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// appendOptionUvarint encodes an Option<u64> the way moq-lite does
// (coding/encode.rs): None is the varint 0, and Some(v) is the varint v+1. The
// +1 cannot overflow in practice (group sequences are far below 2^62-1).
func appendOptionUvarint(b []byte, x *uint64) []byte {
	if x == nil {
		return appendUvarint(b, 0)
	}
	return appendUvarint(b, *x+1)
}

// readUvarint decodes a QUIC varint from r. A clean io.EOF on the first byte is
// returned verbatim (callers at a stream boundary rely on it); a truncated
// value mid-varint is reported as io.ErrUnexpectedEOF.
func readUvarint(r io.Reader) (uint64, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return 0, err
	}
	length := 1 << (first[0] >> 6) // 1, 2, 4, or 8
	val := uint64(first[0] & 0x3f)
	if length == 1 {
		return val, nil
	}
	rest := make([]byte, length-1)
	if _, err := io.ReadFull(r, rest); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	for _, c := range rest {
		val = val<<8 | uint64(c)
	}
	return val, nil
}
