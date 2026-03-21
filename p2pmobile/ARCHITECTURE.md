# p2pmobile native P2P — architecture notes

## Direct messaging (`/ridechain/dm/1.0.0`, `/ridechain/dm/2.0.0`)

- **v2 (preferred):** One long-lived stream per peer (pool). Each *application* payload is one **frame**: `[flags:1][len:4 BE][payload]`, optional zlib when `flags&1`.
- **v1 (legacy):** One stream per message, `[len:4 BE][payload]`.

Incoming direct data is delivered only via **`DirectMessageHandler.OnDirectMessage`** — not duplicated through `MessageHandler` (GossipSub remains on `OnMessage` with real topic names).

## Binary blob chunks (`RCB1`)

Kotlin sends large file/video ciphertext with magic **`RCB1`** (see `BlobChunkBinaryWire` in `centurion-connect`). Go does not parse chunk bodies; v2 write path **skips zlib** for `RCB1` (see `jsonDmPayloadSkipsZlib` in `node.go`). Legacy JSON `chat_blob_chunk` + Base64 is still supported on receive in the app.

## Batched send (`SendBatchToPeer`)

For many small payloads (e.g. encrypted blob chunks), **one JNI call** unpacks a batch and writes **N v2 frames** on the same pooled stream:

Wire (Kotlin packs, Go unpacks), big-endian:

1. `uint32` frame count `N` (1…512)
2. Repeat `N` times: `uint32` length, then `length` bytes

Limits: each frame ≤ 20 MiB; sum of lengths ≤ 24 MiB per batch (see `maxMessageSize`, `maxBatchTotalBytes` in `node.go`).

`SendToPeer` is implemented as `sendDirectFrames(peer, [][]byte{data})`.

### Pooled batch failure

If a **multi-frame** write fails partway on a pooled stream, the pool is evicted and the error is returned **without** opening a new stream for the rest (avoids re-sending frames that already flushed). Callers may retry the whole batch (same trade-off as N separate sends).

## Future voice / video (WebRTC signaling)

Real-time media is usually **WebRTC** (SRTP) with **libp2p for signaling** (offer/answer/ICE). Kotlin JSON envelopes and protocol id **`/ridechain/webrtc-signal/1.0.0`** live in **`libraries/p2p-webrtc-signaling`**. You can send those payloads over DM v2 until a dedicated Go `SetStreamHandler` is added.

## Group chat / MLS

Group E2E is **not** implemented in `p2pmobile`. API seams: **`libraries/p2p-group-mls`** (MLS or other engine TBD).

## Performance & security (mobile)

- **Compression:** Payloads above ~2.5 KB may use zlib unless magic bytes look like JPEG/PNG/WebP/GIF/Ogg (less CPU on media).
- **DoS:** Per-peer inbound limits on DM streams (`ingress.go`): bytes/sec + frames/sec token buckets; excess → stream reset. Lines with **`SECURITY`** still print when verbose is off.
- **Logging:** **`SetVerboseLogging(false)`** (JNI) silences `p2p_transport:` and most `p2pmobile:` diagnostics; release builds should set **`CenturionConfig.verboseNativeP2PLogging = false`** (rider uses `BuildConfig.DEBUG`).

## Build

From repo: `cd bootstrap && bash p2pmobile/build.sh` → `apps/rider-android/libs/p2pmobile.aar`.
