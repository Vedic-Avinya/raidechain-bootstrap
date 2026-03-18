# RideChain Bootstrap — Full Architecture & Security Audit

> **Auditor:** Senior Go / P2P Systems Architect  
> **Date:** March 2026  
> **Scope:** `services/bootstrap` — all Go source, infra docs, env config, Dockerfile  
> **Stack:** Go 1.24, libp2p v0.38, GossipSub v1.1, Kademlia DHT, Redis, Firebase FCM, Caddy, GCP

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Scalability at 1 Million Concurrent Users](#3-scalability-at-1-million-concurrent-users)
4. [Security Audit](#4-security-audit)
5. [P2P Offline Message Delivery & Fallback](#5-p2p-offline-message-delivery--fallback)
6. [Concurrency & Race Conditions](#6-concurrency--race-conditions)
7. [Code Quality & Operational Issues](#7-code-quality--operational-issues)
8. [Risk Matrix](#8-risk-matrix)
9. [Remediation Roadmap](#9-remediation-roadmap)

---

## 1. Executive Summary

The bootstrap server is a well-structured MVP-quality Go service that implements P2P rendezvous, GossipSub messaging, and WebSocket bridging. It is **production-viable at low scale (hundreds of concurrent users)** but has **critical architectural blockers** that will cause cascading failures well before 1 million users. The most severe issues are:

| Severity | Count | Category |
|----------|-------|----------|
| 🔴 CRITICAL | 7 | Scalability blockers, security exploits |
| 🟠 HIGH | 9 | Data loss, auth bypass, reliability |
| 🟡 MEDIUM | 8 | Operational risk, performance degradation |
| 🟢 LOW | 6 | Code quality, observability |

**TL;DR for production at 1M scale: this service, as-is, will hit hard connection limits at 500–1,500 users, has no authentication (any client can impersonate any peer), a committed Firebase service account key, and drops messages for offline riders with no persistence. These must be fixed before public launch.**

---

## 2. Architecture Overview

```
Internet
    │
(Optional) Cloudflare (edge TLS, DDoS)
    │
Caddy (TLS termination, reverse proxy, rate limit)
    │
    ├─── /rider*   → :4003  RiderBridge (WebSocket, city-sharded GossipSub relay)
    ├─── /driver*  → :4004  DriverBridge (WebSocket, GossipSub relay + PeerQueue)
    └─── /*        → :4005  HTTP REST API (register, discover, search, FCM, latLng)
                                │
                    ┌───────────┴───────────┐
                    │  Bootstrap Process    │
                    │  libp2p host          │  :4001 (QUIC+TCP), :4002 (WS)
                    │  Kademlia DHT         │  server mode
                    │  GossipSub            │  FloodPublish=true
                    │  RiderBridge          │  max 500 connections
                    │  DriverBridge         │  max 1000 connections
                    └───────────┬───────────┘
                                │
                         Redis (single instance)
                         Firebase FCM (optional)
```

**Message flow (Rider → Driver):**
1. Rider WS → RiderBridge → `topic.Publish()` → GossipSub mesh
2. RiderBridge calls `onLocalMsg` → DriverBridge.Forward() (cross-bridge relay, bypasses GossipSub self-filter)
3. GossipSub native peers also receive via `sub.Next()` → both bridges forwarded

**Key design decisions observed:**
- City-based GossipSub topic sharding for riders (`/ridechain/{city}/p2p/v1`)
- Per-peer in-memory queue for **drivers only** (not riders)
- FCM push as the only fallback for offline **riders**
- All state in Redis (peer metadata, geo index, search index)

---

## 3. Scalability at 1 Million Concurrent Users

### 🔴 CRITICAL-1: Hard-Coded Connection Limits — System Caps at 1,500 Users

**Files:** `internal/riderbridge/bridge.go:18`, `internal/driverbridge/bridge.go:17`

```go
// riderbridge/bridge.go
const maxRiderConnections = 500

// driverbridge/bridge.go
const maxDriverConnections = 1000
```

**Impact:** The entire platform is capped at **1,500 concurrent WebSocket connections per bootstrap node**. At 1M users, 99.85% of clients get HTTP 503 before they can even connect. These constants are hard-coded — no config, no env override.

**Fix:**
```go
// Make configurable via env
maxRiderConns := envIntOr("MAX_RIDER_CONNECTIONS", 10000)
maxDriverConns := envIntOr("MAX_DRIVER_CONNECTIONS", 10000)
```
And scale horizontally (see CRITICAL-2).

---

### 🔴 CRITICAL-2: Single-Node SPOF — No Horizontal Scaling

**File:** `cmd/main.go`, entire infra design

The entire platform is a **single Go process** on a single VM (e2-medium: 2 vCPU, 4 GB RAM). There is:
- No load balancer across multiple bootstrap instances for WebSocket
- No shared session affinity layer (WebSocket connections are process-local)
- No separation of concerns (P2P host + rider bridge + driver bridge + HTTP API all in one process)
- One bootstrap node going down = **entire platform offline**

**Impact at 1M:** A single VM with 4 GB RAM can hold ~10K–50K WebSocket connections depending on message volume. Even with raised limits, one node cannot serve 1M concurrent users.

**Required architecture change:**

```
                    L4 Load Balancer (WebSocket-aware, sticky sessions)
                    /                    \
           Bootstrap-1               Bootstrap-2..N
           (RiderBridge)             (RiderBridge)
           (DriverBridge)            (DriverBridge)
                    \                    /
                     Redis Cluster (shared state)
                     GossipSub mesh (bootstrap nodes peer with each other)
```

---

### 🔴 CRITICAL-3: `FloodPublish=true` — O(N) Fan-out at All Times

**File:** `cmd/main.go:136`

```go
gs, err := pubsub.NewGossipSub(ctx, h,
    pubsub.WithFloodPublish(true),  // ← CRITICAL at scale
)
```

`FloodPublish` bypasses GossipSub's mesh topology and **sends every message to every connected peer**, even those outside the D-mesh. This was intentionally added for "small networks" (as the comment says) but is **catastrophically expensive at scale**:

- At 1,000 peers in a city topic: every published message = 1,000 direct sends
- At 10,000 peers: 10,000 sends per message, per publisher
- CPU and network saturate instantly under real traffic

**Fix:**
```go
// Remove FloodPublish for production; tune mesh parameters instead
gs, err := pubsub.NewGossipSub(ctx, h,
    pubsub.WithPeerScore(peerScoreParams, peerScoreThresholds),
    pubsub.WithMessageSigning(true),
    pubsub.WithStrictMessageSigning(true),
    // D=6, Dlo=5, Dhi=12 — defaults are fine for moderate scale
)
```

---

### 🔴 CRITICAL-4: O(N) Broadcast in Forward() — Synchronous Under Lock

**Files:** `internal/riderbridge/bridge.go:436-510`, `internal/driverbridge/bridge.go:248-321`

Both bridge `Forward()` methods iterate all connections **synchronously**, acquiring a per-connection mutex for each write:

```go
// Every non-targeted broadcast:
for _, rc := range riders {       // ← iterates ALL connections
    rc.mu.Lock()
    rc.conn.SetWriteDeadline(...)
    rc.conn.WriteMessage(...)     // ← blocking write per connection
    rc.mu.Unlock()
}
```

**At 1,000 riders:** One broadcast message = 1,000 sequential blocking writes.
**At 10,000 riders:** A single `driver_online` broadcast stalls for hundreds of milliseconds.
**At 1M:** This is unmaintainable — a single goroutine serializes all writes globally.

**Fix:** Use per-connection buffered channels with goroutines per connection (fan-out pattern):

```go
type riderConn struct {
    conn    *websocket.Conn
    peerId  string
    city    string
    send    chan []byte  // buffered, goroutine drains it
    mu      sync.Mutex  // only for control frames
}
```

---

### 🟠 HIGH-5: No libp2p Connection Manager or Resource Manager

**File:** `cmd/main.go:72-86`

```go
h, err = libp2p.New(
    libp2p.Identity(priv),
    libp2p.ListenAddrStrings(...),
    libp2p.EnableRelay(),
    libp2p.EnableRelayService(),
    // ← No ConnectionManager
    // ← No ResourceManager
)
```

Without a connection manager, the libp2p host will attempt to maintain connections to **every peer that dials in** with no upper bound. Memory will grow linearly with inbound connections. At scale, this causes OOM.

**Fix:**
```go
import connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"

cm, _ := connmgr.NewConnManager(100, 400, connmgr.WithGracePeriod(time.Minute))
h, err = libp2p.New(
    ...
    libp2p.ConnectionManager(cm),
    libp2p.ResourceManager(&network.NullResourceManager{}), // or rcmgr
)
```

---

### 🟠 HIGH-6: Redis Single Instance — No HA, No Cluster, Deprecated API

**File:** `internal/redis/store.go:168`

```go
locs, err := s.client.GeoRadius(ctx, keyPeersGeo, longitude, latitude, &rdb.GeoRadiusQuery{...})
```

Three problems:
1. **`GEORADIUS` is deprecated** since Redis 6.2 and **removed in Redis 7.4**. Production Redis will fail silently or return errors. Use `GEOSEARCH` / `GeoSearchLocation` instead.
2. **Single Redis instance** — no replica, no sentinel, no cluster. Redis going down = full platform outage.
3. **No connection pool tuning** — default pool size may be exhausted under 1M concurrent requests.

**Fix:**
```go
// Replace GeoRadius with GeoSearchLocation
locs, err := s.client.GeoSearchLocation(ctx, keyPeersGeo, &rdb.GeoSearchLocationQuery{
    GeoSearchQuery: rdb.GeoSearchQuery{
        Longitude:  longitude,
        Latitude:   latitude,
        Radius:     radiusKm,
        RadiusUnit: "km",
        Sort:       "ASC",
        Count:      maxDiscoverPeers,
    },
    WithDist: true,
})
// Deploy Redis Sentinel or Cluster for HA
```

---

### 🟠 HIGH-7: No HTTP Server Timeouts — Slowloris Attack Surface

**Files:** `cmd/main.go:191`, `internal/riderbridge/bridge.go:267`, `internal/driverbridge/bridge.go:85`

```go
// All three HTTP servers created with no timeouts:
apiServer := &http.Server{Addr: ":" + httpAPIPort, Handler: apiMux}
server := &http.Server{Addr: ":" + port, Handler: mux}
```

Without `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`, a single slow HTTP client can hold a goroutine and file descriptor indefinitely. At scale this exhausts the goroutine pool.

**Fix:**
```go
server := &http.Server{
    Addr:              ":" + port,
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,
    ReadTimeout:       15 * time.Second,
    WriteTimeout:      15 * time.Second,
    IdleTimeout:       60 * time.Second,
}
```

---

### 🟠 HIGH-8: No Request Body Size Limit in HTTP Handlers

**File:** `internal/api/http.go:40, 176, 238`

```go
// No size limit before decoding:
if err := json.NewDecoder(r.Body).Decode(&req); err != nil { ... }
```

Without `http.MaxBytesReader`, a client can send a 100 MB body and the server will buffer it all into memory before decoding fails. The Caddy `request_body { max_size 64kb }` exists only in docs/examples — it is **not enforced** and Caddy is optional.

**Fix:**
```go
r.Body = http.MaxBytesReader(w, r.Body, 32*1024) // 32 KB max
if err := json.NewDecoder(r.Body).Decode(&req); err != nil { ... }
```

---

### 🟡 MEDIUM-9: No GossipSub Score Parameters — Sybil Attack Vector

**File:** `cmd/main.go:135-141`

No peer scoring is configured. Without scoring:
- Malicious peers can send invalid/spam messages without penalty
- No mechanism to evict misbehaving peers from the mesh
- Sybil attack: many fake peers can flood the GossipSub topic

**Fix:** Configure `pubsub.WithPeerScore()` and `pubsub.WithStrictMessageSigning(true)` to require signed messages and penalize bad actors.

---

### 🟡 MEDIUM-10: No Backpressure on WebSocket Writes

If a rider or driver client is slow to read (e.g., poor mobile network), the TCP send buffer fills up. The `WriteDeadline` of 5 seconds will eventually trigger a write error and drop the connection — but until then, writes to that single slow connection block the forward loop for other connections (due to synchronous iteration under per-connection mutex).

---

## 4. Security Audit

### 🔴 CRITICAL-S1: Firebase Service Account Key Committed to Repository

**File:** `/ridechain-90ebd-47639048842a.json` (repo root), `.env:4`

```bash
# .env
FIREBASE_SERVICE_ACCOUNT_JSON=./ridechain-90ebd-47639048842a.json
```

A **Google Firebase service account JSON key file** is committed to the source repository. This file grants full Firebase Admin SDK access including:
- Sending push notifications to **any FCM token**
- Accessing Firestore, Firebase Auth, Firebase Storage
- Potential billing abuse

**This is a P0 security incident.** The key must be revoked immediately.

**Immediate actions:**
1. Rotate the key: GCP Console → IAM → Service Accounts → delete/revoke key `47639048842a`
2. Remove `ridechain-90ebd-47639048842a.json` from the repo AND rewrite git history (`git filter-repo` or BFG)
3. Add `*.json` (or at minimum `*serviceAccount*.json`, `*firebase*.json`) to `.gitignore`
4. Use GCP Secret Manager or ADC (Application Default Credentials) on the VM — no key file needed

---

### 🔴 CRITICAL-S2: No Peer Identity Verification — Peer ID Hijacking

**File:** `internal/riderbridge/bridge.go:393-399`, `internal/driverbridge/bridge.go:200-205`

```go
// riderbridge/bridge.go — peer ID is trusted from message payload with zero verification
if rc.peerId == "" && peerIdFromMsg != "" {
    rc.peerId = peerIdFromMsg
    b.mu.Lock()
    b.peerByID[rc.peerId] = rc   // ← Attacker sets this to victim's peer ID
    b.mu.Unlock()
}
```

Any WebSocket client can claim **any peer ID** by sending `{"peer_id": "victim_peer_id"}` as their first message. Once registered, the attacker receives all messages targeted to the victim, including ride offers, location data, and chat messages.

**Attack scenario:**
1. Attacker knows driver's peer ID (visible in `/discover` response)
2. Attacker connects to `/rider` WebSocket
3. Attacker sends `{"type":"rider_online","peer_id":"<driver_peer_id>"}`
4. All messages meant for the driver are now delivered to the attacker

**Fix:** Peer identity must be cryptographically verified. The libp2p peer ID is derived from a public key — require clients to sign a challenge with their private key during WebSocket handshake:

```go
// Handshake challenge flow:
// 1. Server sends: {"type":"challenge","nonce":"<random_32_bytes_hex>"}
// 2. Client responds: {"type":"identify","peer_id":"...","pubkey":"...","sig":"<sign(nonce)>"}
// 3. Server verifies sig against pubkey and that peer_id == hash(pubkey)
```

---

### 🔴 CRITICAL-S3: `CheckOrigin: return true` — CSRF/WebSocket Hijacking

**Files:** `internal/riderbridge/bridge.go:27-31`, `internal/driverbridge/bridge.go:23-27`

```go
var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool { return true }, // ← Any origin allowed
    ...
}
```

Any website can open a WebSocket connection to the bridge endpoints. A malicious page visited by a rider/driver can establish a connection on their behalf (Cross-Site WebSocket Hijacking). This is especially dangerous combined with the lack of peer ID verification.

**Fix:**
```go
var allowedOrigins = map[string]bool{
    "https://app.ridechain.in": true,
    "https://bootstrap.ridechain.in": true,
}

var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool {
        origin := r.Header.Get("Origin")
        return allowedOrigins[origin]
    },
}
```

---

### 🔴 CRITICAL-S4: Hardcoded Dev Identity Seed — Bootstrap Key Predictable

**File:** `cmd/main.go:392-393`

```go
// Dev fallback: deterministic seed so peer ID is stable across restarts.
seed := sha256.Sum256([]byte("ridechain-mvp-bootstrap-seed-v1"))
return seed[:]
```

If `BOOTSTRAP_IDENTITY_SEED` is not set in production (easy to forget), the bootstrap node's **Ed25519 private key is derived from a public constant**. Anyone who reads this source code can:
1. Derive the exact same private key
2. Impersonate the bootstrap node in the DHT
3. Intercept peer routing (man-in-the-middle on the DHT)

The startup log emits a `WARN` but does **not** abort — the node happily starts with the insecure key.

**Fix:** In production builds, make this a hard exit:
```go
if seedHex := os.Getenv("BOOTSTRAP_IDENTITY_SEED"); seedHex == "" {
    if os.Getenv("BOOTSTRAP_DEV_MODE") != "true" {
        slog.Error("BOOTSTRAP_IDENTITY_SEED must be set in production")
        os.Exit(1)
    }
    // dev seed only allowed when BOOTSTRAP_DEV_MODE=true
}
```

---

### 🟠 HIGH-S5: HTTP API Fully Unauthenticated — Peer Record Manipulation

**File:** `internal/api/http.go`

All REST endpoints (`/register`, `PUT /register/fcm`, `PUT /register/display-name`, `PUT /register/lat-lng`) are **publicly accessible with no authentication**:

```go
// Any client can overwrite any peer's FCM token:
// PUT /register/fcm {"peerId": "victim_id", "fcmToken": "attacker_controlled_token"}
func (h *HTTPServer) PutFCM(w http.ResponseWriter, r *http.Request) {
    // ← No auth check
    h.store.SetFCMToken(r.Context(), req.PeerID, token)
}
```

An attacker can:
- Register a display name for any peer ID (including impersonation)
- **Replace any rider's FCM token** with their own — ride notifications go to attacker
- Update location (`/lat-lng`) for any peer — geolocation poisoning

**Fix:** Require JWT or a signed challenge for all write operations. Minimum: require that the request's `peer_id` matches a signed bearer token.

---

### 🟠 HIGH-S6: FCM Push Exposes Raw P2P Payload

**File:** `cmd/main.go:231`

```go
dataMap := map[string]string{
    "type":    "p2p_message",
    "peer_id": peerID,
    "payload": string(data),  // ← Raw P2P message (may contain lat/lng, ride details)
}
pushSender.Send(ctx, meta.FCMToken, dataMap)
```

The complete raw P2P message payload — which may contain GPS coordinates, ride prices, personal info — is sent in the FCM data payload. FCM data messages are:
- Logged by Firebase
- Visible in the FCM delivery report
- Potentially visible to the device's notification tray if the app is backgrounded

**Fix:** Send only a `{"type": "new_message", "peer_id": "..."}` wake-up signal via FCM. The app should reconnect over WebSocket to fetch the actual message. Never put sensitive data in push payloads.

---

### 🟠 HIGH-S7: Redis Has No Authentication or TLS

**File:** `.env:2`, `internal/redis/store.go:49-63`

```
REDIS_URL=redis://localhost:6379
```

Redis runs unauthenticated on `localhost`. While `localhost` is protected from external access:
- Any process on the same VM can read/modify all peer data including FCM tokens
- Redis URL uses `redis://` (plaintext) — if Redis is moved to an external host (e.g. Upstash), credentials must be in URL
- The `.env` file is committed to the repo (without real credentials, but future contributors may add them)

**Fix:**
- Add `requirepass` to Redis config and set `REDIS_URL=redis://:password@localhost:6379`
- Use `rediss://` (TLS) for external Redis
- Add `.env` to `.gitignore` and use `.env.example` only for documentation

---

### 🟡 MEDIUM-S8: No Peer ID Format Validation — Potential Key Injection

**File:** `internal/api/http.go:44-49`

```go
req.PeerID = strings.TrimSpace(req.PeerID)
if req.PeerID == "" {
    http.Error(w, "peerId required", http.StatusBadRequest)
    return
}
```

No validation of peer ID length or format. Redis keys are constructed as `"peer:meta:" + peerID`. While the Redis client library escapes most special characters, an unbounded peer ID could:
- Create arbitrarily long Redis keys consuming memory
- Cause issues if peer IDs contain Redis glob patterns used in key scanning
- Allow display name indexing with 1000s of tokens (no max length on `displayName`)

**Fix:**
```go
const maxPeerIDLen = 128
if len(req.PeerID) > maxPeerIDLen || !isValidPeerID(req.PeerID) {
    http.Error(w, "invalid peerId", http.StatusBadRequest)
    return
}
// isValidPeerID: libp2p peer IDs are base58btc encoded multihash, ~46–53 chars
```

---

### 🟡 MEDIUM-S9: No Rate Limiting in Application Layer

**File:** `internal/api/http.go`, `internal/riderbridge/bridge.go`

Zero rate limiting at the Go application layer. All protection relies on:
1. Caddy being deployed (optional, not enforced)
2. Caddy being configured with `rate_limit` (shown only in docs, not default)
3. Direct access to ports 4003/4004/4005 (bypasses Caddy entirely if firewall allows)

On a fresh deployment, anyone can call `POST /register` millions of times per second or open thousands of WebSocket connections until limits are hit.

**Fix:** Add `golang.org/x/time/rate` limiter per IP at the handler level:
```go
// Per-IP rate limiter middleware
limiter := rate.NewLimiter(rate.Every(time.Second), 100) // 100 req/s
```

---

### 🟡 MEDIUM-S10: Discover Endpoint Leaks All Peer Locations Without Auth

**File:** `internal/api/http.go:110-164`

```go
// GET /discover?lat=12.9716&lng=77.5946&radius_km=99999
// Returns up to 200 peer IDs and their distances — no auth required
```

Anyone can enumerate all registered peers within any radius (no upper bound on `radius_km`). By sweeping lat/lng across India, an attacker can extract the full peer registry with approximate locations.

**Fix:**
- Cap `radius_km` to a reasonable maximum (e.g., 50 km)
- Require authentication for `/discover`
- Consider returning only coarse location (geohash level 5) rather than exact distance

---

## 5. P2P Offline Message Delivery & Fallback

### 🔴 CRITICAL-P1: Riders Have No Persistent Offline Queue — Messages Silently Dropped

**Files:** `internal/riderbridge/bridge.go:209-213`, `cmd/main.go:219-240`

**Driver bridge** has `PeerQueue` (in-memory, bounded at 50 messages). **Rider bridge has no equivalent.**

When a targeted message arrives for an offline rider:

```go
// forwardToCity — riderbridge/bridge.go:209-213
if b.onPeerOffline != nil {
    b.onPeerOffline(targetPeerID, data)  // ← Only FCM push
}
// ← Message is DROPPED. No storage. No retry. Gone forever.
return
```

The FCM push is a **best-effort wake-up signal**, not a message delivery mechanism. FCM:
- Can be dropped by Android Doze mode
- Can be rate-limited by Firebase (currently 240 messages/device/day)
- Is not guaranteed delivery (QoS 0)
- Does not store the actual message

**Result:** If a rider is offline (app background/killed), **ride offers, chat messages, and booking confirmations are permanently lost**. The FCM push may wake the app, but the actual message content is gone — the app has nothing to display.

**Fix — Minimum viable persistent offline queue:**
```go
// Store in Redis with TTL when rider is offline
func (s *Store) EnqueueOfflineMessage(ctx context.Context, peerID string, data []byte, ttl time.Duration) error {
    key := "offline_msgs:" + peerID
    return s.client.RPush(ctx, key, data).Err()
    // With EXPIRE and max list length cap
}

// On rider reconnect, flush queued messages
func (b *Bridge) handleRider(w http.ResponseWriter, r *http.Request) {
    // After peer identifies:
    msgs, _ := store.DrainOfflineMessages(ctx, rc.peerId)
    for _, msg := range msgs {
        rc.conn.WriteMessage(websocket.TextMessage, msg)
    }
}
```

---

### 🟠 HIGH-P2: PeerQueue (Driver) Is In-Memory Only — Lost on Restart

**File:** `internal/driverbridge/peer_queue.go`

```go
type PeerQueue struct {
    mu     sync.Mutex
    queues map[string][][]byte  // ← Entirely in memory
}
```

Any bootstrap restart (deploy, crash, OOM) loses all queued messages for offline drivers. This means:
- Rolling deployments lose in-flight messages
- A crash during a ride offer transaction = driver misses the booking
- At 1M scale, restarts are more frequent (OOM, deploy cycles)

**Fix:** Back `PeerQueue` with Redis (sorted set by timestamp, LPOP on reconnect):
```go
// Redis key: "driver_queue:{peerID}"
// Value: RPUSH serialized message, EXPIRE 24h, LLEN cap at 50
```

---

### 🟠 HIGH-P3: PeerQueue Has No TTL — Stale Messages Accumulate Forever

**File:** `internal/driverbridge/peer_queue.go:29-40`

```go
func (q *PeerQueue) Enqueue(peerID string, data []byte) {
    // Drops oldest when at capacity, but:
    // ← No expiry time on messages
    // ← No cleanup for peers who never reconnect
    q.queues[peerID] = append(list, ...)
}
```

If a driver uninstalls the app or changes their peer ID, their queue entry in the map grows to 50 messages and stays there forever. Over time with millions of peer IDs cycling through, this is a memory leak.

**Fix:** Add a timestamp to each queued message and evict entries older than a TTL (e.g., 24 hours) in a background goroutine.

---

### 🟠 HIGH-P4: FCM Push Sends Raw Message Payload — No Retry, No Dedup

**File:** `cmd/main.go:219-239`

```go
riderBrid.SetOnPeerOffline(func(peerID string, data []byte) {
    // ← No retry on failure
    // ← No deduplication (same message may trigger multiple FCM sends)
    if err := pushSender.Send(ctx, meta.FCMToken, dataMap); err != nil {
        slog.Warn("fcm_offline", ...)
        return  // ← Silently discarded
    }
})
```

Problems:
1. **No retry** — transient FCM errors silently drop the wake-up
2. **No deduplication** — if the same message arrives twice (GossipSub can deliver duplicates), two FCM pushes are sent
3. **No rate limiting** — a message storm can trigger hundreds of FCM pushes for the same rider within seconds, hitting FCM's device-level rate limit and causing delivery blackout

**Fix:**
```go
// Debounce FCM per peer: allow max 1 push per 30s per peer
// Use Redis key "fcm_cooldown:{peerID}" with 30s TTL
// Use exponential backoff retry for FCM failures
```

---

### 🟡 MEDIUM-P5: Cross-City Message Delivery Is Ambiguous

**File:** `internal/riderbridge/bridge.go:189-213`

When a targeted message is forwarded to a city topic (e.g., "bangalore"), `forwardToCity` checks:

```go
if rc != nil && rc.city == city {
    // deliver
}
// If rc exists but rc.city != city → falls through to onPeerOffline (FCM push)
```

If a rider is connected but on city "mumbai" and receives a message on the "bangalore" topic (e.g., because the sender used a stale city), the FCM fallback fires **even though the rider is online**. This causes a spurious FCM push and the in-app WebSocket message never arrives.

The root issue: there is no canonical "current city" for a connected peer that is checked before routing messages into city topics.

---

### 🟡 MEDIUM-P6: No Offline Fallback for Drivers (No FCM)

**File:** `cmd/main.go:213-214`

```go
riderBrid.SetOnPeerOffline(...)   // ← FCM configured for riders
driverBrid := driverbridge.New(defaultTopic, nil)
// ← No SetOnPeerOffline for drivers
```

Offline drivers only have the in-memory `PeerQueue`. If a driver is offline and a ride booking arrives, the message is queued in memory (lost on restart) and no FCM wake-up is sent to the driver's device. The driver misses the booking silently.

**Fix:** Apply the same `SetOnPeerOffline` pattern to the driver bridge with FCM push.

---

## 6. Concurrency & Race Conditions

### 🟠 HIGH-C1: Data Race on `rc.peerId` Assignment

**File:** `internal/riderbridge/bridge.go:393-399`

```go
// handleRider goroutine writes without rc.mu:
if rc.peerId == "" && peerIdFromMsg != "" {
    rc.peerId = peerIdFromMsg    // ← Written without rc.mu held
    b.mu.Lock()
    b.peerByID[rc.peerId] = rc
    b.mu.Unlock()
}

// forwardToCity reads rc.city and rc.peerId without rc.mu:
if rc != nil && rc.city == city { ... }
// cleanup in forward:
delete(b.peerByID, rc.peerId)   // ← Can read while handleRider writes
```

`rc.peerId` is read by concurrent goroutines in `Forward()` and `forwardToCity()` while it may be written by the read loop goroutine. Go's race detector (`go test -race`) would flag this.

**Fix:** Protect `rc.peerId` with `rc.mu` on both read and write, or make it `atomic.Pointer[string]`.

---

### 🟡 MEDIUM-C2: Ping Goroutine Leaks on `WriteControl` Failure

**Files:** `internal/riderbridge/bridge.go:351-367`, `internal/driverbridge/bridge.go:161-177`

```go
go func() {  // ping goroutine
    for {
        select {
        case <-done:
            return
        case <-ticker.C:
            err := conn.WriteControl(websocket.PingMessage, ...)
            if err != nil {
                return  // ← Returns WITHOUT closing `done`
            }
        }
    }
}()

// Read loop exit:
close(done)   // ← Only closed here, after read loop exits
```

If `WriteControl` fails (network error), the ping goroutine exits cleanly. But the **read loop** continues until it hits its own read error. The `done` channel is never used from this path — this is benign but the goroutine lifetime is not deterministic. During this window, resources (ticker, goroutine stack) are held until the read loop eventually errors.

---

### 🟡 MEDIUM-C3: Double-Lock Pattern in `forwardToCity` Iteration

**File:** `internal/riderbridge/bridge.go:230-246`

```go
b.mu.RLock()
riders := make([]*riderConn, 0, len(b.riders))  // snapshot
b.mu.RUnlock()

for _, rc := range riders {
    rc.mu.Lock()
    err := rc.conn.WriteMessage(...)
    if err != nil {
        rc.mu.Unlock()
        b.mu.Lock()           // ← Write lock re-acquired per failed conn
        delete(b.riders, rc.conn)
        b.mu.Unlock()
    }
}
```

This is functionally correct but causes write lock contention for every failed connection during a broadcast. Under high churn (many disconnecting peers during a flood), this serializes all cleanup under a global write lock, stalling incoming connections.

---

## 7. Code Quality & Operational Issues

### 🟡 MEDIUM-Q1: Dockerfile Uses Wrong Go Version

**File:** `Dockerfile:1`

```dockerfile
FROM golang:1.23-alpine AS builder
# But go.mod says:
# go 1.24
```

The `go.mod` specifies Go 1.24 but the Dockerfile builds with Go 1.23. This could:
- Miss Go 1.24 compiler optimizations and bug fixes
- Fail if any code uses Go 1.24-specific language features
- Create inconsistency between local dev builds and CI/CD

**Fix:** `FROM golang:1.24-alpine AS builder`

---

### 🟡 MEDIUM-Q2: No Prometheus Metrics — Cannot Operate at Scale

The codebase has zero Prometheus/OpenTelemetry instrumentation. The only observability is:
- `atomic.Int64` counters (msgTotal, msgByType) logged every 5 seconds
- Structured `slog` JSON logs (good for debugging, not for alerting)

At 1M scale, you cannot operate without metrics. When something goes wrong you have no dashboards for:
- WebSocket connection rates / errors
- Message throughput per city
- Redis latency percentiles
- GossipSub mesh health
- Memory / goroutine leak detection

**Fix:** Add `prometheus/client_golang` (already transitively in `go.mod`) and expose `/metrics`:
```go
import "github.com/prometheus/client_golang/prometheus/promhttp"
http.Handle("/metrics", promhttp.Handler())

// Key metrics:
// - ws_connections_active{bridge="rider|driver",city="..."}
// - messages_forwarded_total{type="..."}
// - redis_operation_duration_seconds{op="..."}
// - fcm_sent_total, fcm_failed_total
```

---

### 🟡 MEDIUM-Q3: FCM Fatal Exit on Explicit Credentials Failure

**File:** `cmd/main.go:207-210`

```go
if pushSender, err = fcm.NewPushSender(ctx); err != nil {
    slog.Error("step", "n", 10, "msg", "FCM init failed", "err", err)
    os.Exit(1)   // ← Entire bootstrap dies if FCM creds are wrong
}
```

If `FIREBASE_SERVICE_ACCOUNT_JSON` is set but the file is corrupt/expired, the **entire bootstrap server refuses to start** — riders and drivers cannot connect at all, not just FCM being unavailable. FCM is a non-critical optional feature; it should degrade gracefully.

**Fix:**
```go
pushSender, err = fcm.NewPushSender(ctx)
if err != nil {
    slog.Warn("FCM init failed; offline push disabled", "err", err)
    pushSender = fcm.NoopSender{}  // graceful degradation
}
```

---

### 🟡 MEDIUM-Q4: GossipSub Default Topic on India — No Namespace Versioning

**File:** `cmd/main.go:144`

```go
gossipsubTopic := envOr("BOOTSTRAP_GOSSIPSUB_TOPIC", "/ridechain/in/p2p/v1")
```

The default topic `/ridechain/in/p2p/v1` has no environment prefix (`prod/staging/dev`). A staging bootstrap running with default config will join the **same GossipSub topic** as the production bootstrap, polluting production message traffic with test messages.

**Fix:** Require environment-namespaced topics in non-dev deployments: `/ridechain/prod/in/p2p/v1`.

---

### 🟢 LOW-Q5: `normalizeName` Function Defined but Unused

**File:** `internal/redis/store.go:359-368`

```go
func normalizeName(name string) string { ... }  // ← Never called
```

Dead code. Remove to reduce maintenance surface.

---

### 🟢 LOW-Q6: No Request ID / Correlation in HTTP Handlers

All HTTP handlers log errors without a request correlation ID, making it impossible to trace a specific failing request through logs in production.

**Fix:** Add a request ID middleware that generates a UUID and attaches it to the `slog` context.

---

### 🟢 LOW-Q7: `.env` File Committed to Repository

**File:** `.env`

```
REDIS_URL=redis://localhost:6379
FIREBASE_SERVICE_ACCOUNT_JSON=./ridechain-90ebd-47639048842a.json
```

The `.env` file (with real values) is committed. Even if the current values are "safe" (localhost Redis), future commits may add real credentials. `.env` should be in `.gitignore`; only `.env.example` should be tracked.

---

### 🟢 LOW-Q8: Discover Endpoint Returns `district` Based Only on Requester's Geohash

**File:** `internal/api/http.go:144-163`

```go
district := ""
if geohashStr != "" {
    district = redis.DistrictForGeohash(geohashStr)  // ← Based on query point
}
// All returned peers get the same district label as the requester
for _, loc := range locs {
    p := peerResult{PeerID: loc.PeerID, District: district}  // ← Not the peer's district
```

Every peer in the response gets the **requester's district**, not the peer's own district. This is a logic bug — the `District` field in the response is misleading.

---

## 8. Risk Matrix

| ID | Severity | Category | Description | Exploitability | Impact |
|----|----------|----------|-------------|---------------|--------|
| CRITICAL-1 | 🔴 | Scale | Hard-coded 500/1000 conn limits | Automatic at launch | Platform inaccessible |
| CRITICAL-2 | 🔴 | Scale | Single-node SPOF, no H-scaling | Guaranteed at growth | Full outage |
| CRITICAL-3 | 🔴 | Scale | FloodPublish O(N) fan-out | Automatic under load | CPU/network saturation |
| CRITICAL-4 | 🔴 | Scale | Synchronous broadcast loop | Automatic under load | Latency → timeouts |
| CRITICAL-S1 | 🔴 | Security | Firebase key committed to repo | Trivial (public repo) | Full Firebase compromise |
| CRITICAL-S2 | 🔴 | Security | Peer ID hijacking via WebSocket | Easy (no auth) | Ride fraud, data theft |
| CRITICAL-S3 | 🔴 | Security | CheckOrigin=true (CSRF) | Moderate | Account hijacking |
| CRITICAL-S4 | 🔴 | Security | Dev seed in production | Easy (source known) | Bootstrap impersonation |
| CRITICAL-P1 | 🔴 | Delivery | Offline rider messages dropped | Automatic | Lost bookings |
| HIGH-5 | 🟠 | Scale | No libp2p connection/resource manager | Automatic | OOM crash |
| HIGH-6 | 🟠 | Scale | Redis GEORADIUS deprecated + single node | Automatic on Redis 7.4+ | Geo queries fail |
| HIGH-7 | 🟠 | Scale | No HTTP timeouts (Slowloris) | Easy | Port exhaustion |
| HIGH-8 | 🟠 | Scale | No request body size limit | Easy | Memory exhaustion |
| HIGH-S5 | 🟠 | Security | Unauthenticated write APIs | Trivial | Peer record poisoning |
| HIGH-S6 | 🟠 | Security | Sensitive data in FCM payload | Automatic | Data leakage |
| HIGH-S7 | 🟠 | Security | Redis unauthenticated | Moderate | Full data dump |
| HIGH-P2 | 🟠 | Delivery | Driver queue in-memory (lost on restart) | Automatic on deploy | Lost bookings |
| HIGH-P3 | 🟠 | Delivery | No TTL on queued messages | Automatic | Memory leak |
| HIGH-P4 | 🟠 | Delivery | No FCM retry/dedup | Moderate | Missed push + FCM flood |
| HIGH-C1 | 🟠 | Concurrency | Data race on peerId | Automatic under load | Panic / wrong routing |
| MEDIUM-Q1 | 🟡 | Quality | Dockerfile Go 1.23 vs go.mod 1.24 | Automatic on build | Build inconsistency |
| MEDIUM-Q2 | 🟡 | Ops | No Prometheus metrics | Automatic | Cannot operate at scale |
| MEDIUM-Q3 | 🟡 | Reliability | FCM failure exits process | If creds expire | Full outage |
| MEDIUM-P5 | 🟡 | Delivery | Cross-city message ambiguity | Edge case | Lost targeted messages |
| MEDIUM-P6 | 🟡 | Delivery | No FCM fallback for drivers | Automatic | Drivers miss bookings |
| MEDIUM-S8 | 🟡 | Security | No peer ID validation | Moderate | Redis memory abuse |
| MEDIUM-S9 | 🟡 | Security | No app-level rate limiting | Easy | API abuse |
| MEDIUM-S10 | 🟡 | Security | Discover leaks all peer locations | Trivial | Privacy violation |

---

## 9. Remediation Roadmap

### Phase 0 — Immediate (Before Any Public Traffic)

> Do these TODAY. These are active security incidents.

1. **Revoke the Firebase service account key** (`ridechain-90ebd-47639048842a.json`) in GCP Console → IAM → Service Accounts.
2. **Remove the key file from git history** using `git filter-repo --path ridechain-90ebd-47639048842a.json --invert-paths`.
3. **Add `.env` and `*.json` to `.gitignore`**.
4. **Require `BOOTSTRAP_IDENTITY_SEED` in non-dev environments** — hard exit if not set.
5. **Restrict WebSocket origins** (`CheckOrigin` to allowlist).
6. **Add `BOOTSTRAP_DEV_MODE` guard** — dev seed never used in production.

### Phase 1 — Before Beta Launch (~1 week)

| Task | Files | Priority |
|------|-------|----------|
| Add per-connection WebSocket write channels (backpressure) | riderbridge, driverbridge | 🔴 |
| Make connection limits configurable via env | bridge.go constants | 🔴 |
| Add HTTP server timeouts to all 3 HTTP servers | main.go, bridges | 🔴 |
| Add `http.MaxBytesReader` to all API handlers | api/http.go | 🔴 |
| Add Redis-backed offline queue for riders | riderbridge + redis/store.go | 🔴 |
| Add FCM fallback for offline drivers | driverbridge + main.go | 🟠 |
| Add message TTL to PeerQueue | driverbridge/peer_queue.go | 🟠 |
| Fix Dockerfile Go version | Dockerfile | 🟡 |
| Replace deprecated `GeoRadius` with `GeoSearchLocation` | redis/store.go | 🟠 |
| Add `normalizeName` dead code removal | redis/store.go | 🟢 |
| Fix `district` bug in Discover response | api/http.go | 🟢 |

### Phase 2 — Before Production Launch (~1 month)

| Task | Description |
|------|-------------|
| **Peer identity verification** | Cryptographic challenge-response on WebSocket handshake |
| **JWT / API token for write endpoints** | Protect `/register`, `/register/fcm`, `/register/lat-lng` |
| **Remove `FloodPublish=true`** | Add GossipSub peer scoring and message signing |
| **Add libp2p ConnectionManager + ResourceManager** | Prevent unbounded peer connections |
| **Prometheus metrics** | Expose `/metrics`; build Grafana dashboard |
| **Redis Sentinel/Cluster** | Eliminate Redis SPOF |
| **Add app-level rate limiting** | `golang.org/x/time/rate` per IP + per peer ID |
| **Cap `/discover` radius** | Max 100 km, require auth |
| **FCM debounce + retry** | 30s cooldown per peer, 3 retries with backoff |
| **Request ID middleware** | Correlation ID in all log lines |
| **Production topic namespacing** | `/ridechain/prod/in/p2p/v1` vs staging/dev |

### Phase 3 — Scale to 1M (~3 months)

| Task | Description |
|------|-------------|
| **Horizontal bootstrap scaling** | Multiple instances + L4 load balancer with WebSocket sticky sessions |
| **Shared session store** | Redis-backed peer → bootstrap-node affinity |
| **Fan-out worker pool** | Replace synchronous broadcast with N goroutine workers per bridge |
| **Separate service concerns** | Split HTTP API, rider bridge, driver bridge into separate services |
| **Redis Cluster** | Shard peer data by peer ID hash |
| **GossipSub parameter tuning** | Tune D, Dlo, Dhi, Dlazy, heartbeat for city mesh sizes |
| **QUIC-only for mobile clients** | Remove TCP fallback overhead for high-volume connections |
| **Distributed tracing** | OpenTelemetry spans (SDK already in go.mod) |

---

## Summary

The RideChain bootstrap is a solid MVP foundation with thoughtful P2P design (city sharding, cross-bridge relay, GossipSub integration). However, it is **not ready for production at any meaningful scale** in its current form. The critical blockers are:

- **Scale:** Hard cap at 1,500 connections, synchronous O(N) broadcast, FloodPublish
- **Security:** Committed Firebase key (revoke immediately), peer ID hijacking, unauthenticated APIs
- **Reliability:** Offline rider messages permanently lost (no persistent queue), PeerQueue in-memory only
- **Operations:** No metrics, no tracing, cannot debug production at scale

With the Phase 0 and Phase 1 fixes, the service can safely handle thousands of concurrent users. Phase 2 brings it to production-ready. Phase 3 takes it to 1M+.

---

*Report generated from full static analysis of `/services/bootstrap/` — `cmd/main.go`, `internal/api/http.go`, `internal/riderbridge/bridge.go`, `internal/driverbridge/bridge.go`, `internal/driverbridge/peer_queue.go`, `internal/fcm/firebase.go`, `internal/fcm/sender.go`, `internal/redis/store.go`, `go.mod`, `Dockerfile`, `.env`, `.env.example`, and all docs.*
