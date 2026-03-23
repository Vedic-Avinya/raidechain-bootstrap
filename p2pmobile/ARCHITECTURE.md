# p2pmobile native P2P — architecture notes

## Hybrid Transport (QUIC + TCP)

The node uses a **hybrid QUIC-first, TCP-fallback** strategy for optimal connectivity:

### Transport Stack
- **QUIC (UDP)**: Low latency, better for mobile networks. Uses `/quic-v1` multiaddr format.
- **TCP**: Reliable fallback for NAT/unreachable scenarios.

### Listen Addresses
```
/ip4/0.0.0.0/udp/0/quic-v1      # IPv4 QUIC (preferred)
/ip6/::/udp/0/quic-v1           # IPv6 QUIC
/ip4/0.0.0.0/tcp/0              # IPv4 TCP (fallback)
/ip6/::/tcp/0                   # IPv6 TCP (fallback)
```

### Dial Strategy (Hybrid)
When connecting to a peer:

1. **QUIC first**: Try QUIC (UDP) addresses for lower latency
2. **TCP fallback**: If QUIC fails, try TCP addresses
3. **IPv6 priority**: IPv6 addresses are tried before IPv4 when available
4. **DHT fallback**: If peerstore addresses fail, query DHT and retry

Priority order:
```
IPv6-QUIC > IPv6-TCP > QUIC > TCP
```

### Bootstrap Connections
Bootstrap connections also use hybrid strategy:
1. Try QUIC first (lower latency, better for mobile)
2. Fall back to TCP if QUIC fails

### Why Hybrid?
- **QUIC benefits**: Lower latency, better NAT traversal on mobile networks
- **TCP fallback**: Reliable when QUIC is blocked by firewalls/carriers
- **IPv6 preference**: Many 4G/5G carriers give public IPv6, enabling direct connections

---

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

## Dedicated file transfer (`/ridechain/file/1.0.0`)

Large file transfers (500 MB+) use a **separate stream** to avoid Head-of-Line blocking on `dm/2.0.0`:

- **`filetransfer.go`**: Sender opens ephemeral stream per transfer, sends header + framed chunks.
- **Flow control**: Receiver sends ACK every 32 chunks (8 MB). Sender pauses until ACK arrives.
- **Concurrency**: Max 4 simultaneous file transfers per node (`activeTransferSemaphore`).
- **Wire**: `[hdr: 4B totalChunks + 8B totalBytes]` then per-chunk `[flags:1][len:4][payload]`.
- **ACK frame**: `[0x02][4B acked_count]` — receiver → sender.

Kotlin calls `SendFile()` or `SendFileFromBytes()` (gomobile-compatible packed format).

## Buffer pooling (`bufpool.go`)

Zero-allocation hot path for high-throughput transfers:

| Pool | Size | Purpose |
|------|------|---------|
| `readBufPool` | 64 KB slabs | Stream readers (v1/v2/file handlers) |
| `frameBufPool` | Growable | v1 frame writes, decompression output |
| `zlibBufPool` | `bytes.Buffer` | Zlib compression/decompression scratch |
| `zlibWriterPool` | `zlib.Writer` | Reuse ~300 KB Huffman tables per writer |

Diagnostic counters: `PoolStats()` returns (hits, misses). Call `LogTransferStats()` from Kotlin for runtime diagnostics. Target: >95% hit rate after warmup.

## Transport security audit (go-libp2p v0.48.0)

| Transport | Security | Forward Secrecy | Authentication |
|-----------|----------|-----------------|----------------|
| TCP | **Noise** (`flynn/noise v1.1.0`) | ✅ Ephemeral DH | ✅ Peer ID (Ed25519) |
| QUIC | **TLS 1.3** (built into quic-go) | ✅ TLS 1.3 | ✅ Peer ID (Ed25519) |

No explicit `libp2p.Security()` needed — defaults are correct. No unencrypted transports exist.

## Encryption layer analysis

Three layers exist, each serving a distinct purpose:

1. **Transport encryption** (Noise/TLS 1.3): Protects data in transit, authenticates transport.
2. **Signal Protocol** (Double Ratchet): E2E for chat metadata/keys — forward secrecy + deniability.
3. **AES-256-GCM**: Bulk blob encryption — keys carried inside Signal envelope.

**On relay path**: All 3 layers are **required** (relay sees transport-decrypted data).
**On direct path**: Layers 2+3 are technically redundant for confidentiality, but:
- Provide integrity verification (SHA-256 hash check catches corrupt transfers)
- Maintain protocol uniformity (relay vs direct use identical wire format)
- AES-GCM on ARM with hardware AES: ~0.3 GB/s — negligible overhead
- **Recommendation: Keep all layers.** CPU cost is minimal; removing creates fragile divergent paths.

## Performance & security (mobile)

- **Compression:** Payloads above ~2.5 KB may use zlib unless magic bytes look like JPEG/PNG/WebP/GIF/Ogg (less CPU on media).
- **DoS:** Per-peer inbound limits on DM streams (`ingress.go`): bytes/sec + frames/sec token buckets; excess → stream reset. Lines with **`SECURITY`** still print when verbose is off.
- **Logging:** **`SetVerboseLogging(false)`** (JNI) silences `p2p_transport:` and most `p2pmobile:` diagnostics; release builds should set **`CenturionConfig.verboseNativeP2PLogging = false`** (rider uses `BuildConfig.DEBUG`).
- **Transfer stats:** Call `LogTransferStats()` from Kotlin for pool hit/miss rates and active file transfer count.

## Build

From repo: `cd bootstrap && bash p2pmobile/build.sh` → `apps/rider-android/libs/p2pmobile.aar`.
