// @streamplace/atmoq — TypeScript client for the atproto firehose over MoQ.
//
// Consumer (subscribe) path only: connect, subscribe to a track, and read
// frames / decoded at-sync messages from the live edge. No publishing.
//
// @example
// ```typescript
// import { connect } from "@streamplace/atmoq";
//
// const sess = await connect("moqt://streamplace.network");
// const sub = sess.subscribe();
//
// for await (const msg of sub) {
//   // msg.header.t is the type ("#commit", "#identity", ...)
//   // msg.payload is the raw CBOR payload bytes
//   console.log(msg.header.t, msg.payload.length, "group=", msg.group);
// }
// ```

export { connect, Session, Subscription } from "./transport.js";
export type { ConnectOptions } from "./transport.js";
export { decodeFrame, InvalidFrameError } from "./frame.js";
export type {
  AtSyncMessage,
  FrameHeader,
  MessageType,
} from "./frame.js";
export { validateDrisl, assertDrisl, InvalidDrislError } from "./drisl.js";
export { parseCarBlocks } from "./car.js";
export type { CarBlocks } from "./car.js";
export { DefaultBroadcast, DefaultTrack } from "./constants.js";
export * as Varint from "./varint.js";
