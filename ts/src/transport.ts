// Transport layer: a thin wrapper over @moq/net that dials a relay, subscribes
// to the atproto broadcast/track, and pumps frames. This is the only module
// that touches @moq/net — the rest of the package is pure decode.
//
// @moq/net handles WebTransport (browser-native, or a polyfill on Node), the
// moq-lite/moq-transport version negotiation, ALPN, and stream multiplexing.
// We just consume broadcasts and read frames — the same role the Go client's
// quic-go + hand-rolled moq-lite plays, but delegated.

import * as Moq from "@moq/net";
import { DefaultBroadcast, DefaultTrack } from "./constants.js";
import { decodeFrame, type AtSyncMessage } from "./frame.js";

/** Options for {@link connect}. */
export interface ConnectOptions {
  /**
   * Skip TLS certificate verification. Useful for self-signed dev servers.
   * On the browser this uses `serverCertificateHashes` pinning if a cert is
   * available; on Node it sets `rejectUnauthorized: false` via the polyfill.
   */
  insecure?: boolean;
  /**
   * A pre-configured WebTransport instance. Pass one if you need to customize
   * the transport beyond what {@link insecure} covers (e.g. custom certs).
   * When omitted, @moq/net's `Connection.connect` creates one for you.
   */
  transport?: WebTransport;
}

/** A live MoQ session to a relay or server. */
export class Session {
  /** The underlying @moq/net established session. */
  readonly established: Moq.Connection.Established;

  constructor(established: Moq.Connection.Established) {
    this.established = established;
  }

  /** The negotiated moq-lite/moq-transport version string. */
  get version(): string {
    return this.established.version;
  }

  /**
   * Subscribe to a broadcast/track and return a {@link Subscription} to read
   * frames from. The subscription starts at the publisher's latest group (the
   * live edge), matching the default `goat firehose` tail behavior and the Go
   * client's `Subscribe`.
   */
  subscribe(
    broadcast: string = DefaultBroadcast,
    track: string = DefaultTrack,
  ): Subscription {
    // @moq/net: consume() takes a broadcast path (Path.Valid, a branded string
    // built by Path.from); subscribe() on the returned Broadcast takes a track
    // name + priority. We use priority 0 (default) — the firehose is a single
    // track, so prioritization is irrelevant.
    const path = Moq.Path.from(broadcast);
    const broadcastObj = this.established.consume(path);
    const trackObj = broadcastObj.subscribe(track, 0);
    return new Subscription(trackObj);
  }

  /** Close the session and tear down the connection. */
  close(): void {
    this.established.close();
  }

  /** A promise that resolves when the session closes. */
  get closed(): Promise<void> {
    return this.established.closed;
  }
}

/** A stream of frames for one subscribed track. */
export class Subscription {
  private readonly track: Moq.Track;
  private closed = false;

  constructor(track: Moq.Track) {
    this.track = track;
  }

  /**
   * Read the next raw frame's bytes and the sequence number of the group it
   * belongs to. For an atmoq firehose, the bytes are a complete at-sync message
   * (CBOR header object + CBOR payload object), identical to a subscribeRepos
   * WebSocket message.
   *
   * Returns `undefined` when the subscription ends (track closed, relay
   * disconnected, etc.).
   */
  async readFrame(): Promise<
    { data: Uint8Array; group: number; frame: number } | undefined
  > {
    // readFrameSequence returns { group, frame, data } or undefined when done.
    return this.track.readFrameSequence();
  }

  /**
   * Read the next decoded at-sync message. Convenience wrapper that decodes
   * the raw frame via {@link decodeFrame}.
   */
  async readMessage(): Promise<AtSyncMessage | undefined> {
    const raw = await this.readFrame();
    if (!raw) return undefined;
    return decodeFrame(raw.data, raw.group, raw.frame);
  }

  /**
   * Async iterator over decoded at-sync messages. Ends when the subscription
   * closes.
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<AtSyncMessage> {
    try {
      for (;;) {
        const msg = await this.readMessage();
        if (msg === undefined) break;
        yield msg;
      }
    } finally {
      this.close();
    }
  }

  /** End the subscription and release its resources. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.track.close();
  }
}

/**
 * Establish a MoQ session to a relay.
 *
 * @param url - The relay URL. Canonical scheme is `moqt://` (the MoQ Transport
 *   URI scheme); `moql`/`moq`/`moqs` and bare `host[:port]` are also accepted,
 *   defaulting to port 443. This mirrors the Go client's `parseDialURL`.
 * @param opts - Connection options.
 * @returns A {@link Session} ready to subscribe.
 *
 * @example
 * ```typescript
 * const sess = await connect("moqt://streamplace.network");
 * const sub = sess.subscribe();
 * for await (const msg of sub) {
 *   console.log(msg.header.t, msg.payload.length);
 * }
 * ```
 */
export async function connect(
  url: string,
  opts: ConnectOptions = {},
): Promise<Session> {
  const parsed = parseDialURL(url);
  const props: Moq.Connection.ConnectProps = {};

  if (opts.transport) {
    props.transport = opts.transport;
  } else if (opts.insecure) {
    // @moq/net's connect accepts a WebTransport instance. For insecure dev
    // servers, the caller should construct one with the appropriate polyfill
    // options and pass it via `transport`. We surface a clear error if they
    // set `insecure` without providing a transport, since the browser
    // WebTransport API has no global "skip verification" flag.
    throw new Error(
      "atmoq: `insecure` requires a custom WebTransport instance — pass one via `opts.transport`. " +
        "The browser WebTransport API has no global TLS-skip flag; use serverCertificateHashes " +
        "or a Node polyfill with rejectUnauthorized.",
    );
  }

  const established = await Moq.Connection.connect(parsed, props);
  return new Session(established);
}

/**
 * Parse a relay URL into a `URL` object, accepting the scheme aliases the Go
 * client accepts (moqt, moql, moq, moqs, or bare host[:port]). @moq/net's
 * `Connection.connect` expects a URL object.
 */
function parseDialURL(rawURL: string): URL {
  // Try as-is first; if there's no scheme, prepend moqt://.
  let u: URL;
  try {
    u = new URL(rawURL);
    if (!u.hostname) throw new Error("no host");
  } catch {
    u = new URL("moqt://" + rawURL);
  }

  switch (u.protocol) {
    case "moqt:":
    case "moql:":
    case "moq:":
    case "moqs:":
      break;
    default:
      throw new Error(`atmoq: unsupported scheme ${u.protocol} (use moqt://)`);
  }

  // @moq/net expects an https:// URL (it translates to WebTransport internally).
  // The moqt: scheme is our hint; we rewrite to https for the underlying call.
  const https = new URL("https://" + u.host + u.pathname);
  return https;
}
