package atmoq

// DRISL validation: https://dasl.ing/drisl.html
//
// DRISL is a deterministic CBOR profile (a subset of CBOR/c,
// draft-rundgren-cbor-core) — the encoding atproto records and at-sync frames
// are supposed to use. atmoq takes the opinionated position that everything
// across the stack only works on valid DRISL: the relay rejects invalid DRISL
// at ingest, and clients reject it at decode.
//
// The rules enforced here, per the DRISL spec and CBOR/c which it inherits:
//   - definite lengths only (no indefinite-length items, no break code)
//   - minimal-length ("preferred") encoding of every int and length argument
//   - map keys must be text strings, unique, and sorted in bytewise
//     lexicographic order of their encoded bytes (for text keys this is the
//     same order as DAG-CBOR's length-first rule)
//   - floats must be 64-bit (never half/single precision); NaN and ±Infinity
//     are rejected (negative zero is the only allowed special value)
//   - tag 42 (CID) is the only allowed tag; its content must be a byte string
//     with the historical 0x00 multibase prefix
//   - the only allowed simple values are false, true, and null
//   - text strings must be valid UTF-8
//
// Validation is a single pass over the raw bytes that also returns the end
// offset of each item, which is what at-sync frame parsing needs to locate the
// header/payload boundary.
//
// This is a line-for-line sibling of rust/crates/atmoq/src/drisl.rs and
// ts/src/drisl.ts; keep the three in sync. Canonical implementation to
// cross-check against: https://github.com/hyphacoop/go-dasl.

import (
	"bytes"
	"fmt"
	"math"
	"unicode/utf8"
)

// DrislError is a DRISL violation, with the byte offset where it was found.
type DrislError struct {
	Offset  int
	Message string
}

func (e *DrislError) Error() string {
	return fmt.Sprintf("invalid DRISL at byte %d: %s", e.Offset, e.Message)
}

func drislErr(offset int, format string, args ...any) error {
	return &DrislError{Offset: offset, Message: fmt.Sprintf(format, args...)}
}

// maxDrislDepth rejects deeply nested documents rather than risking stack
// exhaustion (the validator recurses per level). Real atproto records nest a
// handful of levels; 128 matches serde_ipld_dagcbor's recursion limit. Keep
// in sync with the Rust and TS siblings.
const maxDrislDepth = 128

// ValidateDrisl validates one complete DRISL item starting at offset and
// returns the offset just past the item.
func ValidateDrisl(data []byte, offset int) (int, error) {
	return validateDrislItem(data, offset, 0)
}

// ValidateDrislExact validates that data is exactly one complete DRISL item —
// no trailing bytes.
func ValidateDrislExact(data []byte) error {
	end, err := ValidateDrisl(data, 0)
	if err != nil {
		return err
	}
	if end != len(data) {
		return drislErr(end, "%d trailing byte(s) after item", len(data)-end)
	}
	return nil
}

// readDrislArg reads the argument (value or length) for an initial byte,
// enforcing minimal encoding. Returns the value and the offset just past it.
func readDrislArg(data []byte, offset int, what string) (uint64, int, error) {
	ai := data[offset] & 0x1f
	if ai < 24 {
		return uint64(ai), offset + 1, nil
	}
	if ai > 27 {
		// 28-30 are reserved; 31 is indefinite-length / break.
		if ai == 31 {
			return 0, 0, drislErr(offset, "indefinite-length %s is not allowed", what)
		}
		return 0, 0, drislErr(offset, "reserved additional-info value %d", ai)
	}
	width := 1 << (ai - 24) // 24→1, 25→2, 26→4, 27→8 bytes
	if offset+1+width > len(data) {
		return 0, 0, drislErr(offset, "truncated %s argument", what)
	}
	var value uint64
	for i := 0; i < width; i++ {
		value = value<<8 | uint64(data[offset+1+i])
	}
	var minimal uint64
	switch ai {
	case 24:
		minimal = 24
	case 25:
		minimal = 256
	case 26:
		minimal = 65536
	default:
		minimal = 1 << 32
	}
	if value < minimal {
		return 0, 0, drislErr(offset, "non-minimal encoding of %s %d (%d-byte argument)", what, value, width)
	}
	return value, offset + 1 + width, nil
}

func validateDrislItem(data []byte, offset, depth int) (int, error) {
	if depth > maxDrislDepth {
		return 0, drislErr(offset, "nesting deeper than %d", maxDrislDepth)
	}
	if offset >= len(data) {
		return 0, drislErr(offset, "truncated: expected an item")
	}
	initial := data[offset]
	major := initial >> 5

	switch major {
	case 0, 1: // unsigned int / negative int
		what := "uint"
		if major == 1 {
			what = "negint"
		}
		_, end, err := readDrislArg(data, offset, what)
		return end, err

	case 2, 3: // byte string / text string
		what := "byte string length"
		if major == 3 {
			what = "text string length"
		}
		length, end, err := readDrislArg(data, offset, what)
		if err != nil {
			return 0, err
		}
		if length > uint64(len(data)) || end+int(length) > len(data) {
			return 0, drislErr(end, "truncated string body")
		}
		if major == 3 && !utf8.Valid(data[end:end+int(length)]) {
			return 0, drislErr(end, "text string is not valid UTF-8")
		}
		return end + int(length), nil

	case 4: // array
		count, cursor, err := readDrislArg(data, offset, "array length")
		if err != nil {
			return 0, err
		}
		for i := uint64(0); i < count; i++ {
			cursor, err = validateDrislItem(data, cursor, depth+1)
			if err != nil {
				return 0, err
			}
		}
		return cursor, nil

	case 5: // map
		count, cursor, err := readDrislArg(data, offset, "map length")
		if err != nil {
			return 0, err
		}
		prevStart, prevEnd := -1, -1
		for i := uint64(0); i < count; i++ {
			keyStart := cursor
			if keyStart >= len(data) {
				return 0, drislErr(keyStart, "truncated: expected a map key")
			}
			if data[keyStart]>>5 != 3 {
				return 0, drislErr(keyStart, "map key is not a text string")
			}
			keyEnd, err := validateDrislItem(data, keyStart, depth+1)
			if err != nil {
				return 0, err
			}
			if prevStart >= 0 {
				switch bytes.Compare(data[prevStart:prevEnd], data[keyStart:keyEnd]) {
				case 0:
					return 0, drislErr(keyStart, "duplicate map key")
				case 1:
					return 0, drislErr(keyStart, "map keys are not in bytewise lexicographic order")
				}
			}
			prevStart, prevEnd = keyStart, keyEnd
			cursor, err = validateDrislItem(data, keyEnd, depth+1)
			if err != nil {
				return 0, err
			}
		}
		return cursor, nil

	case 6: // tag
		tag, end, err := readDrislArg(data, offset, "tag")
		if err != nil {
			return 0, err
		}
		if tag != 42 {
			return 0, drislErr(offset, "tag %d is not allowed (only tag 42/CID)", tag)
		}
		if end >= len(data) || data[end]>>5 != 2 {
			return 0, drislErr(min(end, len(data)), "tag 42 content must be a byte string")
		}
		contentEnd, err := validateDrislItem(data, end, depth+1)
		if err != nil {
			return 0, err
		}
		// The byte string body starts after its own head; check the 0x00 prefix.
		_, bodyStart, err := readDrislArg(data, end, "byte string length")
		if err != nil {
			return 0, err
		}
		if contentEnd == bodyStart || data[bodyStart] != 0x00 {
			return 0, drislErr(bodyStart, "tag 42 CID must start with the 0x00 prefix")
		}
		return contentEnd, nil

	default: // major 7: simple values and floats
		switch initial {
		case 0xf4, 0xf5, 0xf6: // false, true, null
			return offset + 1, nil
		case 0xfb: // 64-bit float — the only float width DRISL allows
			if offset+9 > len(data) {
				return 0, drislErr(offset, "truncated float64")
			}
			var bits uint64
			for i := 0; i < 8; i++ {
				bits = bits<<8 | uint64(data[offset+1+i])
			}
			f := math.Float64frombits(bits)
			if math.IsNaN(f) {
				return 0, drislErr(offset, "NaN is not allowed")
			}
			if math.IsInf(f, 0) {
				return 0, drislErr(offset, "infinity is not allowed")
			}
			return offset + 9, nil
		case 0xf9:
			return 0, drislErr(offset, "half-precision float is not allowed (floats must be 64-bit)")
		case 0xfa:
			return 0, drislErr(offset, "single-precision float is not allowed (floats must be 64-bit)")
		case 0xf7:
			return 0, drislErr(offset, "undefined is not allowed")
		case 0xff:
			return 0, drislErr(offset, "unexpected break code")
		default:
			v := initial & 0x1f
			if v == 24 && offset+1 < len(data) {
				v = data[offset+1]
			}
			return 0, drislErr(offset, "simple value %d is not allowed (only false/true/null)", v)
		}
	}
}
