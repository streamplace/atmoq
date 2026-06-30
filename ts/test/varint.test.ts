import { describe, it, expect } from "vitest";
import {
  encode,
  encodeString,
  encodeOption,
  decode,
  size,
} from "../src/varint.js";

describe("varint round-trip", () => {
  // The same values go/varint_test.go tests — the shared wire contract.
  const values: bigint[] = [
    0n,
    1n,
    63n,
    64n,
    16383n,
    16384n,
    1n << 29n,
    1n << 30n,
    1n << 61n,
  ];

  for (const v of values) {
    it(`round-trips ${v}`, () => {
      const encoded = encode([], v);
      const [decoded, bytesRead] = decode(encoded);
      expect(decoded).toBe(v);
      expect(bytesRead).toBe(encoded.length);
    });
  }
});

describe("varint size", () => {
  it("selects the minimal encoding length", () => {
    expect(size(0n)).toBe(1);
    expect(size(63n)).toBe(1);
    expect(size(64n)).toBe(2);
    expect(size(16383n)).toBe(2);
    expect(size(16384n)).toBe(4);
    expect(size(1n << 30n)).toBe(8);
  });
});

describe("encodeString", () => {
  it("prefixes the string with a varint length", () => {
    const encoded = encodeString([], "hello");
    // 5-byte string: varint(5) + "hello"
    expect(encoded).toEqual(
      new Uint8Array([5, 0x68, 0x65, 0x6c, 0x6c, 0x6f]),
    );
  });

  it("round-trips via decode of the length prefix", () => {
    const s = "atproto";
    const encoded = encodeString([], s);
    const [len, off] = decode(encoded);
    expect(len).toBe(BigInt(s.length));
    expect(off).toBe(1);
    expect(new TextDecoder().decode(encoded.subarray(off))).toBe(s);
  });
});

describe("encodeOption (Option<u64>)", () => {
  // Must match moq-lite's Option<u64> coding: None -> 0, Some(v) -> v+1.
  it("encodes None as a single 0 byte", () => {
    const encoded = encodeOption([], undefined);
    expect(encoded).toEqual(new Uint8Array([0]));
  });

  it("encodes Some(v) as v+1 on the wire", () => {
    for (const v of [0n, 1n, 5000n, 1n << 30n]) {
      const encoded = encodeOption([], v);
      const [wire] = decode(encoded);
      expect(wire).toBe(v + 1n);
    }
  });
});

describe("varint encoding matches RFC 9000 §16", () => {
  // Known-answer vectors from RFC 9000 §16 Table 4.
  it("encodes 151288809941952652 as 8 bytes", () => {
    // RFC 9000 §16 example: 0xc2197c5eff14e88c
    const v = 151288809941952652n;
    const encoded = encode([], v);
    expect(Array.from(encoded)).toEqual([
      0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c,
    ]);
  });

  it("encodes 494878333 as 4 bytes", () => {
    const v = 494878333n;
    const encoded = encode([], v);
    expect(Array.from(encoded)).toEqual([0x9d, 0x7f, 0x3e, 0x7d]);
  });
});
