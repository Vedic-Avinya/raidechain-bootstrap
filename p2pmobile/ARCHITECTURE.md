# p2pmobile native P2P — architecture notes

## Direct messaging (`/ridechain/dm/1.0.0`, `/ridechain/dm/2.0.0`)

- **v2 (preferred):** One long-lived stream per peer (pool). Each *application* payload is one **frame**: `[flags:1][len:4 BE][payload]`, optional zlib when `flags&1`.
- **v1 (legacy):** One stream per message, `[len:4 BE][payload]`.

Incoming direct data is delivered only via **`DirectMessageHandler.OnDirectMessage`** — not duplicated through `MessageHandler` (GossipSub remains on `OnMessage` with real topic names).

## Batched send (`SendBatchToPeer`)

For many small payloads (e.g. encrypted blob chunks), **one JNI call** unpacks a batch and writes **N v2 frames** on the same pooled stream:

Wire (Kotlin packs, Go unpacks), big-endian:

1. `uint32` frame count `N` (1…512)
2. Repeat `N` times: `uint32` length, then `length` bytes

Limits: each frame ≤ 20 MiB; sum of lengths ≤ 24 MiB per batch (see `maxMessageSize`, `maxBatchTotalBytes` in `node.go`).

`SendToPeer` is implemented as `sendDirectFrames(peer, [][]byte{data})`.

### Pooled batch failure

If a **multi-frame** write fails partway on a pooled stream, the pool is evicted and the error is returned **without** opening a new stream for the rest (avoids re-sending frames that already flushed). Callers may retry the whole batch (same trade-off as N separate sends).

## Future voice / video

Real-time media is usually **WebRTC** (SRTP) with **libp2p used for signaling** (offer/answer/ICE), not raw DM frames. Reserve a separate protocol id (e.g. `/ridechain/signal/webrtc/1.0.0`) or a small JSON envelope over DM; keep DM for control + chat as today.

## Build

From repo: `cd bootstrap && bash p2pmobile/build.sh` → `apps/rider-android/libs/p2pmobile.aar`.
