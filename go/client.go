// Package atmoq is a Go client for the atproto firehose carried over MoQ
// transport (https://github.com/streamplace/atmoq).
//
// It speaks kixelated's moq-lite protocol (draft 03/04) directly over raw QUIC
// — no WebTransport/HTTP3 layer — which is what `atmoq serve` and public
// moq-lite relays accept alongside h3. A consumer subscribes to a broadcast's
// track and receives a stream of "groups", each carrying length-prefixed
// frames. For atmoq, every frame is one at-sync firehose message: the exact
// same bytes (a DRISL header object followed by a DRISL payload object) that
// com.atproto.sync.subscribeRepos delivers over WebSocket. So a frame can be
// fed straight into indigo's existing event decoder. See ValidateDrisl for
// atmoq's stack-wide DRISL strictness.
//
// This package implements the consumer (subscribe) path only.
package atmoq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"

	quic "github.com/quic-go/quic-go"
)

// moq-lite control-stream message types (the first varint on a client-opened
// bidirectional stream). See moq-net lite/stream.rs.
const (
	ctrlSubscribe = 2
)

// moq-lite data-stream types (the first varint on a server-opened
// unidirectional stream). Only Group exists today.
const (
	dataGroup = 0
)

// ALPN identifiers for the moq-lite versions this client speaks. Offered in
// preference order; the server selects one during the TLS handshake. The
// subscribe and group/frame wire formats are identical across 03 and 04, which
// is all the consumer path touches.
const (
	alpnLite04 = "moq-lite-04"
	alpnLite03 = "moq-lite-03"
)

// Defaults matching `atmoq serve`: a single broadcast named "atproto" carrying
// a single track also named "atproto".
const (
	DefaultBroadcast = "atproto"
	DefaultTrack     = "atproto"
)

// Wire-size limits. All sizes on the wire are varints chosen by the peer, so
// every read must be capped before allocating: an 8-byte varint can claim an
// exabyte.
const (
	// maxFrameSize matches moq-net's MAX_FRAME_SIZE (model/frame.rs): the
	// Rust stack rejects larger frames, so accepting them here would only
	// mask a misbehaving publisher. (at-sync §4.2.1 caps messages at 5 MB;
	// the transport allows headroom.)
	maxFrameSize = 16 << 20
	// maxControlSize caps control-stream message bodies (SUBSCRIBE_OK/DROP
	// are a handful of varints).
	maxControlSize = 1 << 16
	// maxGroupHeaderSize caps the group stream header ({subscribe id,
	// sequence} — at most 18 bytes in practice).
	maxGroupHeaderSize = 1 << 12
	// initialReadAlloc bounds the up-front allocation for a sized read; the
	// buffer grows as bytes actually arrive (mirrors moq-net's reader).
	initialReadAlloc = 64 << 10
)

// Options configures a Dial.
type Options struct {
	// TLSConfig, if set, is used as-is except that the moq-lite ALPNs and the
	// ServerName (from the dial URL host) are filled in when empty.
	TLSConfig *tls.Config
	// Insecure disables TLS certificate verification. Useful for self-signed
	// dev servers; ignored when TLSConfig is provided.
	Insecure bool
	// Log receives debug/info logs. Defaults to slog.Default().
	Log *slog.Logger
}

// Session is a live moq-lite connection to a relay or server.
type Session struct {
	conn    quic.Connection
	version string
	log     *slog.Logger

	mu     sync.Mutex
	subs   map[uint64]*Subscription
	nextID uint64

	acceptOnce sync.Once
}

// Dial establishes a moq-lite session over QUIC. The URL host (and optional
// port, defaulting to 443) identifies the relay; the canonical scheme is
// "moqt" (the MoQ Transport URI scheme), with "moql"/"moq"/"moqs" accepted as
// aliases, or the scheme may be omitted. The scheme is only a client-side hint
// to use raw-QUIC MoQ — the server negotiates the actual protocol via ALPN.
// Any path is ignored: broadcasts are addressed by name in Subscribe, not by
// URL path.
func Dial(ctx context.Context, rawURL string, opts *Options) (*Session, error) {
	if opts == nil {
		opts = &Options{}
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	host, addr, err := parseDialURL(rawURL)
	if err != nil {
		return nil, err
	}

	tlsConf := opts.TLSConfig
	if tlsConf == nil {
		tlsConf = &tls.Config{InsecureSkipVerify: opts.Insecure}
	} else {
		tlsConf = tlsConf.Clone()
	}
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{alpnLite04, alpnLite03}
	}
	if tlsConf.ServerName == "" {
		tlsConf.ServerName = host
	}

	conn, err := quic.DialAddr(ctx, addr, tlsConf, dialQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("atmoq: dialing %s: %w", addr, err)
	}

	version := conn.ConnectionState().TLS.NegotiatedProtocol
	switch version {
	case alpnLite04, alpnLite03:
		// supported
	case "":
		conn.CloseWithError(0, "")
		return nil, fmt.Errorf("atmoq: server did not negotiate a moq-lite ALPN (offered %v)", tlsConf.NextProtos)
	default:
		conn.CloseWithError(0, "")
		return nil, fmt.Errorf("atmoq: unsupported negotiated protocol %q", version)
	}

	log.Info("atmoq connected", "addr", addr, "version", version)
	return &Session{
		conn:    conn,
		version: version,
		log:     log,
		subs:    make(map[uint64]*Subscription),
	}, nil
}

// Version returns the negotiated moq-lite ALPN (e.g. "moq-lite-04").
func (s *Session) Version() string { return s.version }

// Close tears down the session. All subscriptions are failed first, so
// readers blocked in ReadFrame (and internal goroutines blocked delivering to
// a full buffer) unblock immediately rather than waiting on transport
// timeouts.
func (s *Session) Close() error {
	s.failAll(fmt.Errorf("atmoq: session closed"))
	return s.conn.CloseWithError(0, "")
}

// Subscribe requests every future frame of the given track within the given
// broadcast and returns a Subscription to read them from. The subscription
// starts at the publisher's latest group (the live edge), matching the default
// `goat firehose` tail behavior.
func (s *Session) Subscribe(ctx context.Context, broadcast, track string) (*Subscription, error) {
	return s.subscribe(ctx, broadcast, track, nil)
}

// SubscribeFrom is like Subscribe but requests replay starting at the given MoQ
// group sequence rather than the live edge. The relay serves from startGroup if
// it is still within the relay's retention window, otherwise from the oldest
// group it still retains that is >= startGroup (a forward jump). The caller
// detects that jump — and the resulting gap — by comparing the first delivered
// group/at-seq against what it expected; deeper recovery is out of scope for the
// transport (re-sync from the PDS).
//
// The group sequence to pass is one previously observed from ReadFrame: a
// consumer resumes by remembering the last group it fully processed.
func (s *Session) SubscribeFrom(ctx context.Context, broadcast, track string, startGroup uint64) (*Subscription, error) {
	return s.subscribe(ctx, broadcast, track, &startGroup)
}

func (s *Session) subscribe(ctx context.Context, broadcast, track string, startGroup *uint64) (*Subscription, error) {
	if startGroup != nil && *startGroup > maxUvarint-1 {
		// Some(g) encodes as g+1 on the wire; reject rather than overflow.
		return nil, fmt.Errorf("atmoq: start group %d exceeds the wire maximum", *startGroup)
	}
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	sub := &Subscription{
		sess:   s,
		id:     id,
		frames: make(chan frameItem, 1024),
		closed: make(chan struct{}),
		kick:   make(chan struct{}, 1),
	}
	s.subs[id] = sub
	s.mu.Unlock()

	if err := s.openSubscribe(ctx, sub, broadcast, track, startGroup); err != nil {
		s.mu.Lock()
		delete(s.subs, id)
		s.mu.Unlock()
		return nil, err
	}

	// Each subscription drains its own group streams, so one stalled
	// subscription can't block the others (or the accept loop).
	go sub.run()

	// Start routing server-pushed group streams once a subscription exists.
	s.acceptOnce.Do(func() { go s.acceptLoop() })

	startLog := any("live")
	if startGroup != nil {
		startLog = *startGroup
	}
	s.log.Info("atmoq subscribed", "broadcast", broadcast, "track", track, "id", id, "startGroup", startLog)
	return sub, nil
}

// openSubscribe opens the control stream, sends SUBSCRIBE, and waits for the
// mandatory SUBSCRIBE_OK. The control stream is held open for the lifetime of
// the subscription: closing our send side would signal the publisher to tear
// the subscription down.
func (s *Session) openSubscribe(ctx context.Context, sub *Subscription, broadcast, track string, startGroup *uint64) error {
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("atmoq: opening subscribe stream: %w", err)
	}
	// quic-go stream reads don't observe ctx on their own; cancel the stream
	// if ctx ends so a hung relay can't block Subscribe forever. On any error
	// return, cancel both directions so the server tears the subscription
	// down instead of serving groups nobody reads.
	stopWatchdog := context.AfterFunc(ctx, func() {
		stream.CancelRead(0)
		stream.CancelWrite(0)
	})
	defer stopWatchdog()
	fail := func(err error) error {
		stream.CancelWrite(0)
		stream.CancelRead(0)
		if ctx.Err() != nil {
			return fmt.Errorf("atmoq: subscribe: %w", ctx.Err())
		}
		return err
	}

	// Body of the SUBSCRIBE message (moq-net lite/subscribe.rs, Lite03/04 form).
	body := appendUvarint(nil, sub.id)
	body = appendString(body, broadcast) // Path: a length-prefixed string
	body = appendString(body, track)
	body = append(body, 0)        // priority (raw u8)
	body = append(body, 1)        // ordered = true (raw u8)
	body = appendUvarint(body, 0) // max_latency: 0ms
	// start_group / end_group are Option<u64> on the wire: 0 = None (live edge),
	// otherwise value-1 = Some (moq-net coding/encode.rs). So Some(g) -> g+1.
	body = appendOptionUvarint(body, startGroup) // start_group
	body = appendUvarint(body, 0)                // end_group: None

	// Control stream: [ControlType][size-prefixed SUBSCRIBE body].
	msg := appendUvarint(nil, ctrlSubscribe)
	msg = appendUvarint(msg, uint64(len(body)))
	msg = append(msg, body...)
	if _, err := stream.Write(msg); err != nil {
		return fail(fmt.Errorf("atmoq: writing subscribe: %w", err))
	}

	// First response must be SUBSCRIBE_OK. On Lite03/04 a response is a varint
	// discriminator (0 = OK, 1 = DROP) followed by a size-prefixed body.
	br := bufio.NewReader(stream)
	kind, err := readUvarint(br)
	if err != nil {
		return fail(fmt.Errorf("atmoq: reading subscribe response: %w", err))
	}
	if kind != 0 {
		return fail(fmt.Errorf("atmoq: subscription rejected (response type %d)", kind))
	}
	okSize, err := readUvarint(br)
	if err != nil {
		return fail(fmt.Errorf("atmoq: reading subscribe-ok size: %w", err))
	}
	if okSize > maxControlSize {
		return fail(fmt.Errorf("atmoq: subscribe-ok body of %d bytes exceeds the %d-byte cap", okSize, maxControlSize))
	}
	if _, err := io.CopyN(io.Discard, br, int64(okSize)); err != nil {
		return fail(fmt.Errorf("atmoq: reading subscribe-ok body: %w", err))
	}

	sub.ctrl = stream
	// Watch the control stream: DROP notifications are surfaced there, and a
	// clean close or reset ends the subscription even while the connection
	// itself stays up.
	go sub.watchControl(br)
	return nil
}

// acceptLoop accepts server-pushed unidirectional streams and routes each to
// its subscription's queue. Only the small fixed-size group header is read
// here; the frames themselves are drained by the subscription's own goroutine,
// so a slow or stalled subscription can't block stream acceptance (or the
// other subscriptions on the session).
func (s *Session) acceptLoop() {
	ctx := s.conn.Context()
	for {
		stream, err := s.conn.AcceptUniStream(ctx)
		if err != nil {
			s.failAll(fmt.Errorf("atmoq: connection closed: %w", err))
			return
		}
		s.dispatchGroupStream(stream)
	}
}

// dispatchGroupStream reads a stream's type and group header, then hands it to
// the owning subscription. Streams for unknown subscriptions or with malformed
// headers are cancelled so they don't pin flow-control credit.
func (s *Session) dispatchGroupStream(stream quic.ReceiveStream) {
	br := bufio.NewReader(stream)

	kind, err := readUvarint(br)
	if err != nil {
		stream.CancelRead(0)
		return
	}
	if kind != dataGroup {
		s.log.Debug("atmoq: ignoring unknown data stream", "type", kind)
		stream.CancelRead(0)
		return
	}

	// Group header is a size-prefixed message: { subscribe id, sequence }.
	hdrSize, err := readUvarint(br)
	if err != nil || hdrSize > maxGroupHeaderSize {
		stream.CancelRead(0)
		return
	}
	hdr := make([]byte, hdrSize)
	if _, err := io.ReadFull(br, hdr); err != nil {
		stream.CancelRead(0)
		return
	}
	hr := bytes.NewReader(hdr)
	subID, err := readUvarint(hr)
	if err != nil {
		stream.CancelRead(0)
		return
	}
	seq, err := readUvarint(hr)
	if err != nil {
		stream.CancelRead(0)
		return
	}

	s.mu.Lock()
	sub := s.subs[subID]
	s.mu.Unlock()
	if sub == nil {
		stream.CancelRead(0)
		return
	}
	sub.enqueue(groupStream{stream: stream, br: br, seq: seq})
}

func (s *Session) failAll(err error) {
	s.mu.Lock()
	subs := make([]*Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.fail(err)
	}
}

// frameItem is one delivered frame and the sequence of the group it came from.
type frameItem struct {
	data  []byte
	group uint64
}

// groupStream is an accepted group data stream, header already consumed.
type groupStream struct {
	stream quic.ReceiveStream
	br     *bufio.Reader
	seq    uint64
}

// Subscription is a stream of frames for one subscribed track.
type Subscription struct {
	sess   *Session
	id     uint64
	ctrl   quic.Stream
	frames chan frameItem

	// Pending group streams, drained in accept order by run(). The queue
	// holds stream handles, not data — the amount of buffered wire data is
	// bounded by QUIC flow control, not by the queue length.
	qmu   sync.Mutex
	queue []groupStream
	kick  chan struct{} // 1-buffered wakeup for run()

	closeOnce sync.Once
	closed    chan struct{}
	err       error
}

// ReadFrame returns the next frame's raw bytes and the sequence number of the
// group it belongs to. For an atmoq firehose, the bytes are a complete at-sync
// message (CBOR header object + CBOR payload object), identical to a
// subscribeRepos WebSocket message. It blocks until a frame is available, the
// context is cancelled, or the subscription ends.
func (sub *Subscription) ReadFrame(ctx context.Context) (data []byte, group uint64, err error) {
	select {
	case it := <-sub.frames:
		return it.data, it.group, nil
	case <-sub.closed:
		// Drain any frames buffered before the close.
		select {
		case it := <-sub.frames:
			return it.data, it.group, nil
		default:
		}
		return nil, 0, sub.err
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// Close ends the subscription and releases its resources.
func (sub *Subscription) Close() error {
	sub.sess.mu.Lock()
	delete(sub.sess.subs, sub.id)
	sub.sess.mu.Unlock()
	sub.fail(context.Canceled)
	if sub.ctrl != nil {
		sub.ctrl.CancelWrite(0)
		sub.ctrl.CancelRead(0)
	}
	return nil
}

// enqueue adds an accepted group stream to the subscription's queue. If the
// subscription has already ended, the stream is cancelled instead.
func (sub *Subscription) enqueue(gs groupStream) {
	sub.qmu.Lock()
	select {
	case <-sub.closed:
		sub.qmu.Unlock()
		gs.stream.CancelRead(0)
		return
	default:
	}
	sub.queue = append(sub.queue, gs)
	sub.qmu.Unlock()
	select {
	case sub.kick <- struct{}{}:
	default:
	}
}

// run drains the subscription's group streams sequentially in accept order,
// which preserves frame ordering across group boundaries (the publisher opens
// group streams in sequence order). Exits once the subscription ends,
// cancelling anything still queued.
func (sub *Subscription) run() {
	for {
		sub.qmu.Lock()
		var gs groupStream
		ok := len(sub.queue) > 0
		if ok {
			gs = sub.queue[0]
			sub.queue = sub.queue[1:]
		}
		sub.qmu.Unlock()

		if !ok {
			select {
			case <-sub.kick:
				continue
			case <-sub.closed:
				sub.drainQueue()
				return
			}
		}
		sub.readGroup(gs)

		select {
		case <-sub.closed:
			sub.drainQueue()
			return
		default:
		}
	}
}

// readGroup delivers a group stream's frames until its FIN.
func (sub *Subscription) readGroup(gs groupStream) {
	log := sub.sess.log
	for {
		size, err := readUvarint(gs.br)
		if err == io.EOF {
			return // clean end of group
		}
		if err != nil {
			log.Debug("atmoq: group read error", "seq", gs.seq, "err", err)
			return
		}
		if size > maxFrameSize {
			// Protocol violation, and continuing would silently skip data:
			// fail the subscription loudly.
			gs.stream.CancelRead(0)
			sub.fail(fmt.Errorf("atmoq: frame of %d bytes in group %d exceeds the %d-byte cap", size, gs.seq, maxFrameSize))
			return
		}
		data, err := readSized(gs.br, size)
		if err != nil {
			log.Debug("atmoq: truncated frame", "seq", gs.seq, "err", err)
			return
		}
		if !sub.deliver(frameItem{data: data, group: gs.seq}) {
			gs.stream.CancelRead(0)
			return
		}
	}
}

// drainQueue cancels every stream still queued after the subscription ends.
func (sub *Subscription) drainQueue() {
	sub.qmu.Lock()
	q := sub.queue
	sub.queue = nil
	sub.qmu.Unlock()
	for _, gs := range q {
		gs.stream.CancelRead(0)
	}
}

// deliver hands a frame to the reader, returning false if the subscription has
// ended (so the caller stops feeding it).
func (sub *Subscription) deliver(it frameItem) bool {
	select {
	case sub.frames <- it:
		return true
	case <-sub.closed:
		return false
	}
}

func (sub *Subscription) fail(err error) {
	sub.closeOnce.Do(func() {
		sub.err = err
		close(sub.closed)
	})
}

// watchControl reads the subscribe control stream for the subscription's
// lifetime. SUBSCRIBE_DROP messages (type 1) announce server-dropped group
// ranges — in moq-lite they do not end the subscription, so we surface them in
// the log (the gap is also observable as a group-sequence jump in ReadFrame).
// A FIN or reset of the control stream ends the subscription.
func (sub *Subscription) watchControl(br *bufio.Reader) {
	for {
		kind, err := readUvarint(br)
		if err != nil {
			sub.fail(fmt.Errorf("atmoq: subscription ended: %w", err))
			return
		}
		size, err := readUvarint(br)
		if err != nil || size > maxControlSize {
			sub.fail(fmt.Errorf("atmoq: malformed control message (type %d, size %d): %v", kind, size, err))
			return
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(br, body); err != nil {
			sub.fail(fmt.Errorf("atmoq: subscription ended: %w", err))
			return
		}
		if kind == 1 {
			// SUBSCRIBE_DROP: { start, end (inclusive), error } varints
			// (moq-net lite/subscribe.rs).
			r := bytes.NewReader(body)
			start, _ := readUvarint(r)
			end, _ := readUvarint(r)
			code, _ := readUvarint(r)
			sub.sess.log.Warn("atmoq: server dropped groups",
				"id", sub.id, "start", start, "end", end, "error", code)
		} else {
			sub.sess.log.Debug("atmoq: control message", "id", sub.id, "type", kind, "bytes", size)
		}
	}
}

// readSized reads exactly size bytes, growing the buffer as bytes arrive
// rather than trusting the peer's claimed size with one up-front allocation.
// Callers must have validated size against a cap already.
func readSized(r io.Reader, size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, 0, min(size, initialReadAlloc))
	for uint64(len(buf)) < size {
		chunk := min(size-uint64(len(buf)), initialReadAlloc)
		start := len(buf)
		buf = append(buf, make([]byte, chunk)...)
		if _, err := io.ReadFull(r, buf[start:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return buf, nil
}

func dialQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod:       15_000_000_000, // 15s
		MaxIdleTimeout:        60_000_000_000, // 60s
		MaxIncomingUniStreams: 1 << 16,
	}
}

// parseDialURL extracts the host and the "host:port" dial address (default
// port 443) from a moq:// URL, a bare host, or a host:port.
func parseDialURL(rawURL string) (host, addr string, err error) {
	u, perr := url.Parse(rawURL)
	if perr != nil || u.Host == "" {
		// Maybe it's a bare "host" or "host:port" with no scheme.
		u, perr = url.Parse("moqt://" + rawURL)
		if perr != nil || u.Host == "" {
			return "", "", fmt.Errorf("atmoq: invalid relay URL %q", rawURL)
		}
	}
	switch u.Scheme {
	case "moqt", "moql", "moq", "moqs", "":
		// raw QUIC + TLS
	default:
		return "", "", fmt.Errorf("atmoq: unsupported scheme %q (use moqt://)", u.Scheme)
	}
	host = u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return host, host + ":" + port, nil
}
