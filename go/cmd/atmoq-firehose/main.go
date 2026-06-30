// Command atmoq-firehose is a minimal consumer that pulls the atproto firehose
// from a MoQ relay and prints one line per frame. It exists to exercise the
// atmoq-go client on its own, independent of indigo/goat.
//
//	atmoq-firehose moqt://streamplace.network
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"github.com/fxamacker/cbor/v2"
	"github.com/streamplace/atmoq/go"
)

func main() {
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	broadcast := flag.String("broadcast", atmoq.DefaultBroadcast, "broadcast name")
	track := flag.String("track", atmoq.DefaultTrack, "track name")
	limit := flag.Int("limit", 0, "exit after this many frames (0 = run forever)")
	flag.Parse()

	target := flag.Arg(0)
	if target == "" {
		target = "moqt://streamplace.network"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, target, *broadcast, *track, *insecure, *limit); err != nil && ctx.Err() == nil {
		slog.Error("firehose failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, target, broadcast, track string, insecure bool, limit int) error {
	sess, err := atmoq.Dial(ctx, target, &atmoq.Options{Insecure: insecure})
	if err != nil {
		return err
	}
	defer sess.Close()

	sub, err := sess.Subscribe(ctx, broadcast, track)
	if err != nil {
		return err
	}
	defer sub.Close()

	count := 0
	for {
		raw, group, err := sub.ReadFrame(ctx)
		if err != nil {
			return err
		}
		typ, seq := peek(raw)
		out, _ := json.Marshal(map[string]any{
			"group": group,
			"type":  typ,
			"seq":   seq,
			"bytes": len(raw),
		})
		fmt.Println(string(out))

		count++
		if limit > 0 && count >= limit {
			return nil
		}
	}
}

// peek decodes just the header's message type and the payload's seq from a
// frame (two concatenated CBOR objects), for human-friendly output.
func peek(raw []byte) (msgType string, seq int64) {
	dec := cbor.NewDecoder(bytes.NewReader(raw))

	var header struct {
		Op int64  `cbor:"op"`
		T  string `cbor:"t"`
	}
	if err := dec.Decode(&header); err != nil {
		return "?", 0
	}
	var payload struct {
		Seq int64 `cbor:"seq"`
	}
	if err := dec.Decode(&payload); err != nil && err != io.EOF {
		return header.T, 0
	}
	return header.T, payload.Seq
}
