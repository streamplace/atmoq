import { describe, it, expect } from "vitest";
import * as Moq from "@moq/net";
import { Subscription } from "../src/transport.js";

function frame(n: number): Uint8Array {
  return new Uint8Array([n]);
}

async function readAll(
  sub: Subscription,
): Promise<{ group: number; frame: number; byte: number }[]> {
  const out: { group: number; frame: number; byte: number }[] = [];
  for (;;) {
    const next = await sub.readFrame();
    if (!next) break;
    out.push({ group: next.group, frame: next.frame, byte: next.data[0] });
  }
  return out;
}

describe("Subscription.readFrame", () => {
  it("drains an older group to completion when a newer group is already buffered", async () => {
    // Regression test for the readFrameSequence latest-wins loss: group 0's
    // tail frames arrive *after* group 1 has started buffering. A lossless
    // firehose consumer must still deliver all of group 0 before group 1.
    const track = new Moq.Track("atproto");
    const sub = new Subscription(track);

    const g0 = track.appendGroup();
    g0.writeFrame(frame(0));

    // Group 1 opens (and even finishes) while group 0 is still mid-flight.
    const g1 = track.appendGroup();
    g1.writeFrame(frame(10));
    g1.close();

    // Read group 0's first frame, then let its tail arrive late.
    const first = await sub.readFrame();
    expect(first).toMatchObject({ group: 0, frame: 0 });

    g0.writeFrame(frame(1));
    g0.writeFrame(frame(2));
    g0.close();
    track.close();

    const rest = await readAll(sub);
    expect(rest).toEqual([
      { group: 0, frame: 1, byte: 1 },
      { group: 0, frame: 2, byte: 2 },
      { group: 1, frame: 0, byte: 10 },
    ]);
  });

  it("delivers every frame of every group in order", async () => {
    const track = new Moq.Track("atproto");
    const sub = new Subscription(track);

    for (let g = 0; g < 3; g++) {
      const group = track.appendGroup();
      for (let f = 0; f < 4; f++) group.writeFrame(frame(g * 10 + f));
      group.close();
    }
    track.close();

    const all = await readAll(sub);
    expect(all).toHaveLength(12);
    expect(all.map((f) => f.byte)).toEqual([
      0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23,
    ]);
  });

  it("skips a group that arrives late (sequence order preserved)", async () => {
    const track = new Moq.Track("atproto");
    const sub = new Subscription(track);

    // Groups delivered out of order: 1 completes first, then 0 shows up late.
    const g1 = new Moq.Group(1);
    g1.writeFrame(frame(10));
    g1.close();
    track.writeGroup(g1);

    const first = await sub.readFrame();
    expect(first).toMatchObject({ group: 1 });

    const g0 = new Moq.Group(0);
    g0.writeFrame(frame(0));
    g0.close();
    track.writeGroup(g0);
    track.close();

    // The late group 0 is skipped, matching the Rust model's monotonic
    // next_group(); the subscription simply ends.
    const rest = await readAll(sub);
    expect(rest).toEqual([]);
  });

  it("returns undefined when the track closes cleanly", async () => {
    const track = new Moq.Track("atproto");
    const sub = new Subscription(track);
    track.close();
    expect(await sub.readFrame()).toBeUndefined();
  });
});
