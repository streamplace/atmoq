// Transport layer: a thin wrapper over @moq/net that dials a relay, subscribes
// to the atproto broadcast/track, and pumps frames. This is the only module
// that touches @moq/net — the rest of the package is pure decode.
//
// @moq/net handles the moq-lite/moq-transport wire protocol, stream multiplexing,
// and version negotiation. On the transport side it needs either:
//   - browser: native WebTransport (Chrome/Edge 97+, Firefox behind flag)
//   - Node: a WebTransport polyfill — Node has no native WebTransport yet, and
//     @moq/net's WebSocket fallback can't reach a raw MoQ relay (it speaks
//     WebTransport-over-QUIC, not WebSocket). We install the polyfill below.

import * as Moq from "@moq/net";
import { DefaultBroadcast, DefaultTrack } from "./constants.js";
import {
  decodeFrame,
  InvalidDrislError,
  InvalidFrameError,
  type AtSyncMessage,
} from "./frame.js";

// Install a WebTransport polyfill on Node, where globalThis.WebTransport is
// undefined. We try @fails-components/webtransport (the standard Node option);
// if it's not installed, we fall through and @moq/net will error with a clear
// message when it can't find any transport. The import is dynamic + optional
// so the browser (which has native WebTransport) never pays this cost.
//
// CRITICAL: the polyfill's quiche (HTTP/3) native library loads asynchronously.
// We must await `quicheLoaded` before any WebTransport connection is attempted,
// or the connection silently fails with "Lib quiche loading attempt did not end".
// See https://github.com/fails-components/webtransport#usage
if (typeof globalThis.WebTransport === "undefined") {
  try {
    const polyfill = await import("@fails-components/webtransport");
    globalThis.WebTransport = polyfill.WebTransport;
    // Wait for the native QUIC library to finish loading before proceeding.
    // Without this, @moq/net's connect() races ahead and the WebTransport
    // constructor throws because quiche isn't ready yet.
    await polyfill.quicheLoaded;
  } catch {
    // No polyfill installed — @moq/net will surface the error when connect()
    // is called. We don't throw here so the pure-decode parts of this package
    // (varint, frame) still work without the transport dependency.
  }
}

// @fails-components/webtransport's WebTransport accepts a `rejectUnauthorized`
// option that the DOM WebTransport type doesn't have. We extend the type so
// our `--insecure` path type-checks.
declare global {
  interface WebTransportOptions {
    rejectUnauthorized?: boolean;
  }
}

/** Options for {@link connect}. */
export interface ConnectOptions {
  /**
   * Skip TLS certificate verification. Useful for self-signed dev servers.
   * On Node (via the polyfill) this sets `rejectUnauthorized: false` — an
   * experimental polyfill path that fails against some servers; prefer
   * {@link certHashes}, which works on both Node and browsers.
   */
  insecure?: boolean;
  /**
   * Pin the server certificate by SHA-256 hash (WebTransport
   * `serverCertificateHashes`) — the standard dev-server path on both Node
   * and browsers. moq relays typically serve their current hash over HTTP
   * (e.g. `http://host:4443/certificate.sha256`). Takes precedence over
   * {@link insecure}.
   */
  certHashes?: WebTransportHash[];
  /**
   * A pre-configured WebTransport instance. Pass one if you need to customize
   * the transport beyond what the other options cover.
   * When omitted, one is created automatically.
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
    //
    // Known disparity: the SUBSCRIBE goes out with ordered=false, because
    // @moq/net 0.1.6 hardcodes it (subscribe() only exposes priority). The
    // Rust and Go clients send ordered=true. Against `atmoq serve` this is
    // inert (the publisher serves every group regardless), but a generic
    // relay honoring the flag may skip old groups for us under congestion.
    // Needs an upstream @moq/net API addition; not worth monkey-patching a
    // prototype shared with other @moq/net users in the same process.
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
  private group: Moq.Group | undefined;
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
   * Every frame of every group is delivered, in order: each group is drained
   * to completion before advancing to the next (matching the Rust consumer's
   * next_group/read_frame loop). We deliberately avoid Track.readFrameSequence,
   * whose latest-wins semantics discard the un-read tail of an older group the
   * moment a newer one is buffered — correct for live video, silent data loss
   * for a firehose.
   *
   * Returns `undefined` when the subscription ends (track closed, relay
   * disconnected, etc.). Throws if the track or the current group is aborted
   * with an error — a mid-group abort means frames were lost, and a lossless
   * consumer should see that as a failure, not a silent gap.
   */
  async readFrame(): Promise<
    { data: Uint8Array; group: number; frame: number } | undefined
  > {
    for (;;) {
      // Work on a local reference: a concurrent close() clears `this.group`
      // while we're suspended on an await below, and that must read as a
      // clean end of the subscription, not a TypeError on undefined.
      let group = this.group;
      if (!group) {
        if (this.closed) return undefined;
        // Sequence-ordered: a group arriving late (seq <= last delivered) is
        // skipped, same as the Rust model's monotonic next_group().
        group = await this.track.nextGroupOrdered();
        if (!group) return undefined;
        if (this.closed) {
          // close() ran during the await and never saw this group.
          group.close();
          return undefined;
        }
        this.group = group;
      }
      const next = await group.readFrameSequence();
      if (this.closed) return undefined;
      if (next) {
        return {
          data: next.data,
          group: group.sequence,
          frame: next.sequence,
        };
      }
      // Clean end of group: release it and move to the next.
      group.close();
      this.group = undefined;
    }
  }

  /**
   * Read the next decoded at-sync message. Convenience wrapper that decodes
   * the raw frame via {@link decodeFrame}.
   *
   * atmoq is DRISL-strict: a frame that isn't valid DRISL is rejected with an
   * {@link InvalidDrislError} (an {@link InvalidFrameError} for frames that
   * are valid DRISL but not at-sync-shaped). These are *recoverable*: the bad
   * frame has been consumed and the subscription stays usable, so a caller
   * that wants to reject-and-continue can catch and call again.
   */
  async readMessage(): Promise<AtSyncMessage | undefined> {
    const raw = await this.readFrame();
    if (!raw) return undefined;
    return decodeFrame(raw.data, raw.group, raw.frame);
  }

  /**
   * Async iterator over decoded at-sync messages. Ends when the subscription
   * closes.
   *
   * A rejected frame ({@link InvalidDrislError} / {@link InvalidFrameError})
   * terminates the loop with that error but leaves the subscription open —
   * the bad frame is already consumed, so the caller can log the rejection
   * and start a new `for await` on the same subscription to continue from the
   * next frame. Any other error (or a clean end) closes the subscription.
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<AtSyncMessage> {
    let recoverable = false;
    try {
      for (;;) {
        const msg = await this.readMessage();
        if (msg === undefined) break;
        yield msg;
      }
    } catch (err) {
      recoverable =
        err instanceof InvalidDrislError || err instanceof InvalidFrameError;
      throw err;
    } finally {
      if (!recoverable) this.close();
    }
  }

  /** End the subscription and release its resources. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.group?.close();
    this.group = undefined;
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

  // @moq/net's connect() accepts a pre-built WebTransport via `props.transport`,
  // or creates one itself. For `--insecure` we build one with the polyfill's
  // rejectUnauthorized: false; otherwise we let @moq/net handle it.
  const props: Moq.Connection.ConnectProps = {};

  if (opts.transport) {
    props.transport = opts.transport;
  } else if (opts.certHashes?.length) {
    if (typeof globalThis.WebTransport !== "function") {
      throw new Error(
        "atmoq: certHashes requires WebTransport (native or the " +
          "@fails-components/webtransport polyfill)",
      );
    }
    props.transport = new WebTransport(parsed, {
      serverCertificateHashes: opts.certHashes,
    });
  } else if (opts.insecure) {
    // Build a WebTransport with cert verification disabled. On Node this uses
    // the polyfill's options; on the browser there's no equivalent (use
    // serverCertificateHashes + a custom transport instead).
    if (typeof globalThis.WebTransport !== "function") {
      throw new Error(
        "atmoq: --insecure requires a WebTransport polyfill " +
          "(@fails-components/webtransport) — install it: npm install @fails-components/webtransport",
      );
    }
    props.transport = new WebTransport(parsed, { rejectUnauthorized: false });
  }

  if (props.transport) {
    // @moq/net's custom-transport path (connectTransport) neither awaits
    // `ready` nor attaches a catch to `closed`, and it chains bare .then()s
    // off `closed` internally — with the Node polyfill, which *rejects* both
    // on connection failure or abnormal close, each of those chains is a
    // process-killing unhandled rejection. Shadow `closed` with a promise
    // that settles instead, and await `ready` ourselves so a refused
    // connection surfaces as a clean error here rather than a crash.
    guardTransport(props.transport);
    try {
      await props.transport.ready;
    } catch (err) {
      throw new Error(
        `atmoq: WebTransport connection to ${parsed} failed: ${
          err instanceof Error ? err.message : err
        }`,
        { cause: err },
      );
    }
  }

  const established = await Moq.Connection.connect(parsed, props);
  return new Session(established);
}

/**
 * Replace a WebTransport's `closed` promise with one that settles (never
 * rejects) so downstream `.then()` chains without a catch can't become
 * unhandled rejections. The resolved value carries the close reason either way.
 */
function guardTransport(wt: WebTransport): void {
  const settled = wt.closed.catch((err) => ({
    closeCode: 0,
    reason: err instanceof Error ? err.message : String(err),
  }));
  Object.defineProperty(wt, "closed", { value: settled, configurable: true });
}

/**
 * Parse a relay URL into an https:// URL for @moq/net's connect().
 *
 * @moq/net expects an https:// URL (it creates the WebTransport internally).
 * We accept the moqt/moql/moq/moqs schemes (or bare host) and rewrite to https,
 * mirroring the Go client's parseDialURL.
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
    case "https:":
      // Already https — accept as-is (a caller may pass the final form).
      return u;
    default:
      throw new Error(`atmoq: unsupported scheme ${u.protocol} (use moqt://)`);
  }

  // Rewrite moqt: → https: for the underlying WebTransport call.
  return new URL("https://" + u.host + u.pathname);
}
