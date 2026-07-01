import { describe, it, expect } from "vitest";
import { validateDrisl, assertDrisl, InvalidDrislError } from "../src/drisl.js";

function bytes(...b: number[]): Uint8Array {
  return new Uint8Array(b);
}

function float64(f: number): Uint8Array {
  const out = new Uint8Array(9);
  out[0] = 0xfb;
  new DataView(out.buffer, 1).setFloat64(0, f);
  return out;
}

describe("validateDrisl: valid documents", () => {
  const valid: [string, Uint8Array][] = [
    ["uint 0", bytes(0x00)],
    ["uint 23 (direct)", bytes(0x17)],
    ["uint 24 (1-byte arg)", bytes(0x18, 0x18)],
    ["uint 255", bytes(0x18, 0xff)],
    ["uint 256 (2-byte arg)", bytes(0x19, 0x01, 0x00)],
    ["uint 65536 (4-byte arg)", bytes(0x1a, 0x00, 0x01, 0x00, 0x00)],
    [
      "uint 2^32 (8-byte arg)",
      bytes(0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00),
    ],
    [
      "uint 2^64-1",
      bytes(0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
    ],
    ["negint -1", bytes(0x20)],
    ["empty byte string", bytes(0x40)],
    ["byte string", bytes(0x43, 1, 2, 3)],
    ["empty text string", bytes(0x60)],
    ["text 'abc'", bytes(0x63, 0x61, 0x62, 0x63)],
    ["empty array", bytes(0x80)],
    ["array [1,2]", bytes(0x82, 0x01, 0x02)],
    ["empty map", bytes(0xa0)],
    // {"a": 1, "b": 2} — keys sorted
    ["sorted map", bytes(0xa2, 0x61, 0x61, 0x01, 0x61, 0x62, 0x02)],
    // {"t": 1, "op": 2} — length-first order ("t" encodes 0x61.. < "op" 0x62..)
    ["length-first keys", bytes(0xa2, 0x61, 0x74, 0x01, 0x62, 0x6f, 0x70, 0x02)],
    ["false", bytes(0xf4)],
    ["true", bytes(0xf5)],
    ["null", bytes(0xf6)],
    ["float64 1.5", float64(1.5)],
    ["float64 -0.0 (allowed special)", float64(-0)],
    // tag 42 CID: 0xd8 0x2a, byte string 0x00-prefixed
    [
      "tag 42 CID",
      bytes(0xd8, 0x2a, 0x45, 0x00, 0x01, 0x71, 0x12, 0x20),
    ],
  ];

  for (const [name, data] of valid) {
    it(`accepts ${name}`, () => {
      expect(() => assertDrisl(data)).not.toThrow();
    });
  }

  it("returns the end offset of the first item", () => {
    // {"a": 1} followed by trailing data
    const doc = bytes(0xa1, 0x61, 0x61, 0x01, 0xf6);
    expect(validateDrisl(doc)).toBe(4);
    expect(validateDrisl(doc, 4)).toBe(5);
  });
});

describe("validateDrisl: rejections", () => {
  const invalid: [string, Uint8Array, RegExp][] = [
    ["non-minimal uint (1-byte arg < 24)", bytes(0x18, 0x17), /non-minimal/],
    ["non-minimal uint (2-byte arg < 256)", bytes(0x19, 0x00, 0xff), /non-minimal/],
    [
      "non-minimal uint (4-byte arg < 65536)",
      bytes(0x1a, 0x00, 0x00, 0xff, 0xff),
      /non-minimal/,
    ],
    [
      "non-minimal uint (8-byte arg < 2^32)",
      bytes(0x1b, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff),
      /non-minimal/,
    ],
    [
      "non-minimal string length",
      bytes(0x78, 0x03, 0x61, 0x62, 0x63),
      /non-minimal/,
    ],
    ["indefinite byte string", bytes(0x5f, 0x41, 0x01, 0xff), /indefinite/],
    ["indefinite text string", bytes(0x7f, 0x61, 0x61, 0xff), /indefinite/],
    ["indefinite array", bytes(0x9f, 0x01, 0xff), /indefinite/],
    ["indefinite map", bytes(0xbf, 0x61, 0x61, 0x01, 0xff), /indefinite/],
    ["bare break code", bytes(0xff), /break/],
    ["float16", bytes(0xf9, 0x3c, 0x00), /half-precision/],
    ["float32", bytes(0xfa, 0x3f, 0xc0, 0x00, 0x00), /single-precision/],
    ["float64 NaN", float64(Number.NaN), /NaN/],
    ["float64 Infinity", float64(Number.POSITIVE_INFINITY), /infinity/],
    ["float64 -Infinity", float64(Number.NEGATIVE_INFINITY), /infinity/],
    ["undefined", bytes(0xf7), /undefined/],
    ["simple value 19", bytes(0xf3), /simple value/],
    ["simple value 32 (via 0xf8)", bytes(0xf8, 0x20), /simple value/],
    // {"b": 1, "a": 2} — out of order
    [
      "unsorted map keys",
      bytes(0xa2, 0x61, 0x62, 0x01, 0x61, 0x61, 0x02),
      /order/,
    ],
    // {"op": 1, "t": 2} — bytewise/length-first violation ("t" must sort first)
    [
      "longer key before shorter",
      bytes(0xa2, 0x62, 0x6f, 0x70, 0x01, 0x61, 0x74, 0x02),
      /order/,
    ],
    // {"a": 1, "a": 2}
    [
      "duplicate map keys",
      bytes(0xa2, 0x61, 0x61, 0x01, 0x61, 0x61, 0x02),
      /duplicate/,
    ],
    // {1: 2}
    ["integer map key", bytes(0xa1, 0x01, 0x02), /not a text string/],
    ["tag 0 (datetime)", bytes(0xc0, 0x60), /tag 0/],
    ["tag 2 (bignum)", bytes(0xc2, 0x41, 0x01), /tag 2/],
    // tag 42 with a text string content
    [
      "tag 42 with non-bytes content",
      bytes(0xd8, 0x2a, 0x61, 0x61),
      /byte string/,
    ],
    // tag 42 whose bytes lack the 0x00 prefix
    [
      "tag 42 without 0x00 prefix",
      bytes(0xd8, 0x2a, 0x42, 0x01, 0x71),
      /0x00 prefix/,
    ],
    ["tag 42 with empty bytes", bytes(0xd8, 0x2a, 0x40), /0x00 prefix/],
    ["invalid UTF-8", bytes(0x62, 0xc3, 0x28), /UTF-8/],
    ["truncated uint arg", bytes(0x19, 0x01), /truncated/],
    ["truncated string body", bytes(0x63, 0x61, 0x62), /truncated/],
    ["truncated array", bytes(0x82, 0x01), /truncated/],
    ["truncated float64", bytes(0xfb, 0x00, 0x00), /truncated/],
    ["reserved additional info 28", bytes(0x1c), /reserved/],
    ["empty input", bytes(), /truncated/],
  ];

  for (const [name, data, pattern] of invalid) {
    it(`rejects ${name}`, () => {
      expect(() => assertDrisl(data)).toThrow(InvalidDrislError);
      expect(() => assertDrisl(data)).toThrow(pattern);
    });
  }

  it("rejects trailing bytes in assertDrisl", () => {
    expect(() => assertDrisl(bytes(0x01, 0x02))).toThrow(/trailing/);
  });

  it("rejects pathological nesting depth", () => {
    // 2000 nested single-element arrays around a 0.
    const deep = new Uint8Array(2001);
    deep.fill(0x81, 0, 2000);
    deep[2000] = 0x00;
    expect(() => assertDrisl(deep)).toThrow(/nesting/);
  });

  it("reports the violation offset", () => {
    // valid uint, then a float16 at offset 1 — validate the second item.
    const doc = bytes(0x01, 0xf9, 0x3c, 0x00);
    try {
      validateDrisl(doc, 1);
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(InvalidDrislError);
      expect((err as InvalidDrislError).offset).toBe(1);
    }
  });
});
