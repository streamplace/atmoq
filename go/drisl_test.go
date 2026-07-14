package atmoq

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

func f64Bytes(f float64) []byte {
	out := make([]byte, 9)
	out[0] = 0xfb
	binary.BigEndian.PutUint64(out[1:], math.Float64bits(f))
	return out
}

// Mirrors ts/test/drisl.test.ts and rust drisl.rs tests — shared vectors.
func TestValidateDrislAccepts(t *testing.T) {
	valid := []struct {
		name string
		data []byte
	}{
		{"uint 0", []byte{0x00}},
		{"uint 23", []byte{0x17}},
		{"uint 24", []byte{0x18, 0x18}},
		{"uint 256", []byte{0x19, 0x01, 0x00}},
		{"uint 65536", []byte{0x1a, 0x00, 0x01, 0x00, 0x00}},
		{"uint 2^32", []byte{0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
		{"negint -1", []byte{0x20}},
		{"bytes", []byte{0x43, 1, 2, 3}},
		{"text abc", []byte{0x63, 0x61, 0x62, 0x63}},
		{"array", []byte{0x82, 0x01, 0x02}},
		{"empty map", []byte{0xa0}},
		{"sorted map", []byte{0xa2, 0x61, 0x61, 0x01, 0x61, 0x62, 0x02}},
		{"length-first keys {t, op}", []byte{0xa2, 0x61, 0x74, 0x01, 0x62, 0x6f, 0x70, 0x02}},
		{"false", []byte{0xf4}},
		{"true", []byte{0xf5}},
		{"null", []byte{0xf6}},
		{"float64 1.5", f64Bytes(1.5)},
		{"float64 -0.0", f64Bytes(math.Copysign(0, -1))},
		{"tag 42 CID", []byte{0xd8, 0x2a, 0x45, 0x00, 0x01, 0x71, 0x12, 0x20}},
	}
	for _, tc := range valid {
		if err := ValidateDrislExact(tc.data); err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}
	}
}

func TestValidateDrislRejects(t *testing.T) {
	invalid := []struct {
		name   string
		data   []byte
		needle string
	}{
		{"non-minimal uint 1-byte", []byte{0x18, 0x17}, "non-minimal"},
		{"non-minimal uint 2-byte", []byte{0x19, 0x00, 0xff}, "non-minimal"},
		{"non-minimal string length", []byte{0x78, 0x03, 0x61, 0x62, 0x63}, "non-minimal"},
		{"indefinite array", []byte{0x9f, 0x01, 0xff}, "indefinite"},
		{"indefinite map", []byte{0xbf, 0x61, 0x61, 0x01, 0xff}, "indefinite"},
		{"bare break", []byte{0xff}, "break"},
		{"float16", []byte{0xf9, 0x3c, 0x00}, "half-precision"},
		{"float32", []byte{0xfa, 0x3f, 0xc0, 0x00, 0x00}, "single-precision"},
		{"float64 NaN", f64Bytes(math.NaN()), "NaN"},
		{"float64 +Inf", f64Bytes(math.Inf(1)), "infinity"},
		{"undefined", []byte{0xf7}, "undefined"},
		{"simple 19", []byte{0xf3}, "simple value"},
		{"unsorted keys {b, a}", []byte{0xa2, 0x61, 0x62, 0x01, 0x61, 0x61, 0x02}, "order"},
		{"longer key first {op, t}", []byte{0xa2, 0x62, 0x6f, 0x70, 0x01, 0x61, 0x74, 0x02}, "order"},
		{"duplicate keys", []byte{0xa2, 0x61, 0x61, 0x01, 0x61, 0x61, 0x02}, "duplicate"},
		{"int map key", []byte{0xa1, 0x01, 0x02}, "not a text string"},
		{"tag 0", []byte{0xc0, 0x60}, "tag 0"},
		{"tag 2 bignum", []byte{0xc2, 0x41, 0x01}, "tag 2"},
		{"tag 42 non-bytes", []byte{0xd8, 0x2a, 0x61, 0x61}, "byte string"},
		{"tag 42 no 0x00 prefix", []byte{0xd8, 0x2a, 0x42, 0x01, 0x71}, "0x00 prefix"},
		{"invalid UTF-8", []byte{0x62, 0xc3, 0x28}, "UTF-8"},
		{"truncated arg", []byte{0x19, 0x01}, "truncated"},
		{"truncated string", []byte{0x63, 0x61, 0x62}, "truncated"},
		{"truncated array", []byte{0x82, 0x01}, "truncated"},
		{"empty input", []byte{}, "truncated"},
		{"trailing bytes", []byte{0x01, 0x02}, "trailing"},
	}
	for _, tc := range invalid {
		err := ValidateDrislExact(tc.data)
		if err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.needle) {
			t.Errorf("%s: error %q does not contain %q", tc.name, err, tc.needle)
		}
	}
}

func TestValidateDrislOffsets(t *testing.T) {
	// {"a": 1} then null — the header/payload boundary use case.
	doc := []byte{0xa1, 0x61, 0x61, 0x01, 0xf6}
	end, err := ValidateDrisl(doc, 0)
	if err != nil || end != 4 {
		t.Fatalf("first item end = %d, %v; want 4, nil", end, err)
	}
	end, err = ValidateDrisl(doc, 4)
	if err != nil || end != 5 {
		t.Fatalf("second item end = %d, %v; want 5, nil", end, err)
	}
}

func TestValidateDrislNestingCap(t *testing.T) {
	deep := append(bytes.Repeat([]byte{0x81}, 2000), 0x00)
	err := ValidateDrislExact(deep)
	if err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("expected nesting error, got %v", err)
	}
}
