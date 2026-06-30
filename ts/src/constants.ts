// Defaults matching `atmoq serve`: a single broadcast named "atproto" carrying
// a single track also named "atproto". Mirrors the Go client's DefaultBroadcast
// / DefaultTrack (go/client.go).

/** Default broadcast name — the atproto firehose. */
export const DefaultBroadcast = "atproto";

/** Default track name within the firehose broadcast. */
export const DefaultTrack = "atproto";

// We don't negotiate ALPN ourselves: @moq/net's Connection.connect() handles
// the WebTransport handshake and moq-lite/moq-transport version negotiation
// internally. The Go client offers moq-lite-03/04 over raw QUIC; the TS client
// rides on @moq/net, which supports the same versions over WebTransport.
