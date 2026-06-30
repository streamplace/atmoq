// Type shim for @fails-components/webtransport — the package ships types at
// ./dist/lib/index.types.d.ts but doesn't declare them in an `exports` map
// that TypeScript's `moduleResolution: bundler` can resolve. We declare the
// minimal surface we use: the WebTransport constructor, matching the DOM
// WebTransport interface so it's assignable to globalThis.WebTransport.

declare module "@fails-components/webtransport" {
  // Extend the global DOM WebTransport so the assignment in transport.ts
  // (globalThis.WebTransport = polyfill.WebTransport) type-checks. We declare
  // it as the same shape the browser provides.
  export class WebTransport {
    constructor(url: string | URL, options?: WebTransportOptions);
    readonly ready: Promise<void>;
    readonly closed: Promise<WebTransportCloseInfo>;
    close(closeInfo?: WebTransportCloseInfo): void;
    createBidirectionalStream(): Promise<WebTransportBidirectionalStream>;
    createUnidirectionalStream(): Promise<WebTransportSendStream>;
    readonly incomingBidirectionalStreams: ReadableStream<WebTransportBidirectionalStream>;
    readonly incomingUnidirectionalStreams: ReadableStream<WebTransportReceiveStream>;
    readonly datagrams: WebTransportDatagramDuplexStream;
  }
  export class Http3Server {
    constructor(options: unknown);
  }
  export const quicheLoaded: Promise<boolean>;
}
