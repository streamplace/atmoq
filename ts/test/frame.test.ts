import { describe, it, expect } from "vitest";
import { encode, decode } from "@atproto/lex-cbor";
import { decodeFrame } from "../src/frame.js";

/**
 * Build a raw at-sync frame: two consecutive DAG-CBOR objects (header + payload),
 * the same shape `com.atproto.sync.subscribeRepos` delivers over WebSocket and
 * `atmoq` republishes over MoQ.
 */
function makeFrame(header: unknown, payload: unknown): Uint8Array {
  const h = encode(header);
  const p = encode(payload);
  const out = new Uint8Array(h.length + p.length);
  out.set(h, 0);
  out.set(p, h.length);
  return out;
}

describe("decodeFrame", () => {
  it("decodes a #commit header and returns the raw payload", () => {
    const header = {
      t: "#commit",
      repo: "did:plc:example123",
      commit: "bafyreighehtvnfl",
      rev: "3l4xqp5xyzabc",
      since: "3l4xqp5xyzab",
      tooBig: false,
    };
    const payload = { blocks: new Uint8Array([0, 1, 2, 3]), ops: [] };

    const frame = makeFrame(header, payload);
    const msg = decodeFrame(frame, 42, 7);

    expect(msg.header.t).toBe("#commit");
    expect(msg.header.repo).toBe("did:plc:example123");
    expect(msg.group).toBe(42);
    expect(msg.frame).toBe(7);

    // Payload is the raw second CBOR object's bytes, passed through untyped.
    expect(msg.payload).toBeInstanceOf(Uint8Array);
    expect(msg.payload.length).toBeGreaterThan(0);
    // Round-trip the payload to confirm it's intact.
    expect(decode(msg.payload)).toEqual(payload);
  });

  it("decodes a #identity message", () => {
    const header = {
      t: "#identity",
      did: "did:plc:example123",
      seq: 1000,
      time: "2026-06-30T12:00:00.000Z",
    };
    const payload = { handle: "example.bsky.social" };

    const frame = makeFrame(header, payload);
    const msg = decodeFrame(frame);

    expect(msg.header.t).toBe("#identity");
    expect(msg.header.did).toBe("did:plc:example123");
    expect(msg.header.seq).toBe(1000);
  });

  it("decodes an #account message", () => {
    const header = {
      t: "#account",
      seq: 5000,
      did: "did:plc:example123",
      time: "2026-06-30T12:00:00.000Z",
    };
    const payload = { active: true, status: "valid" };

    const frame = makeFrame(header, payload);
    const msg = decodeFrame(frame);

    expect(msg.header.t).toBe("#account");
    expect(msg.payload.length).toBeGreaterThan(0);
  });

  it("preserves the full payload bytes (not just a prefix)", () => {
    // A payload with enough bytes that the header boundary is non-trivial.
    const header = { t: "#seq", seq: 1 };
    const payload = { large: new Uint8Array(256).fill(0xab) };

    const frame = makeFrame(header, payload);
    const msg = decodeFrame(frame);

    const decodedPayload = decode(msg.payload);
    expect(decodedPayload).toEqual(payload);
    expect((decodedPayload as any).large.length).toBe(256);
  });

  it("defaults group and frame sequence to 0", () => {
    const frame = makeFrame({ t: "#seq", seq: 1 }, null);
    const msg = decodeFrame(frame);
    expect(msg.group).toBe(0);
    expect(msg.frame).toBe(0);
  });
});
