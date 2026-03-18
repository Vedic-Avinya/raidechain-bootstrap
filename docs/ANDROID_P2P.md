# Android Native P2P Architecture

> **Short answer:** Yes — you can set up native P2P on Android right now.  
> The server is ready. Choose the client approach below based on your timeline.

---

## Three Options, Ranked by Implementation Speed

---

### Option A — WebSocket to Bootstrap (Ready Today, Zero Changes)

**Your current server already supports this fully.**

```
Android App  ──WebSocket──▶  wss://ws.ridechain.in/rider?city=mumbai
                               │
                               ├── nearby_ping  ──▶  geo cell subscribers (~2 km)
                               ├── nearby_chat  ──▶  1:1 DM via target_peer_id
                               └── location_broadcast ──▶  9-cell geohash window
```

#### Android Implementation (Kotlin)
```kotlin
// build.gradle.kts
implementation("com.squareup.okhttp3:okhttp:4.12.0")

// WebSocket client
class RideChainClient(val serverUrl: String) {
    private val client = OkHttpClient.Builder()
        .pingInterval(25, TimeUnit.SECONDS)  // matches server pingInterval
        .build()
    private var ws: WebSocket? = null

    fun connect(city: String, peerId: String) {
        val request = Request.Builder()
            .url("$serverUrl/rider?city=$city")
            .build()
        ws = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                // Identify yourself to the bridge
                ws.send("""{"type":"peer_online","peer_id":"$peerId","city":"$city"}""")
            }
            override fun onMessage(ws: WebSocket, text: String) {
                handleMessage(text)
            }
            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                // Reconnect with exponential backoff
                scheduleReconnect()
            }
        })
    }

    fun sendNearby(peerId: String, message: String) {
        ws?.send("""{"type":"nearby_ping","peer_id":"$peerId","msg":"$message"}""")
    }

    fun sendDM(fromId: String, toId: String, message: String) {
        val msgId = UUID.randomUUID().toString()
        ws?.send("""{"type":"chat_message","peer_id":"$fromId",
            "target_peer_id":"$toId","msg":"$message","id":"$msgId"}""")
    }

    fun updateLocation(peerId: String, lat: Double, lng: Double) {
        // HTTP call to update geo cell subscription (9-cell window)
        // POST https://api.ridechain.in/register/lat-lng
    }
}
```

#### Location Update (Retrofit)
```kotlin
interface RideChainApi {
    @PUT("register/lat-lng")
    suspend fun updateLocation(@Body body: LatLngRequest): Response<StatusResponse>
}

data class LatLngRequest(
    @Json(name = "peerId") val peerId: String,
    val lat: Double,
    val lng: Double,
    val city: String
)

// Call from LocationCallback (every 30–60 seconds or on significant movement)
lifecycleScope.launch {
    api.updateLocation(LatLngRequest(myPeerId, lat, lng, "mumbai"))
}
```

**Pros:** Works immediately, no extra libraries, battle-tested  
**Cons:** Not true P2P — server relays all messages  

---

### Option B — Native libp2p on Android (True P2P, 2–4 weeks)

Compile `go-libp2p` to Android using `gomobile`. Android app gets its own libp2p peer ID and connects to the bootstrap node for discovery, then communicates P2P.

#### Build the Android AAR

```bash
# On your dev machine (macOS/Linux)
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# Create a thin Go wrapper package
mkdir -p libp2p-android/
```

Create `libp2p-android/p2p.go`:
```go
package p2pandroid

import (
    "context"
    "github.com/libp2p/go-libp2p"
    pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Node is the exported type visible to Android via gomobile.
type Node struct {
    cancel context.CancelFunc
    ps     *pubsub.PubSub
}

func NewNode(bootstrapAddr string) (*Node, error) {
    ctx, cancel := context.WithCancel(context.Background())
    h, err := libp2p.New(
        libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
    )
    // ... connect to bootstrap, join pubsub
    return &Node{cancel: cancel}, err
}

func (n *Node) Publish(topic, data string) error { ... }
func (n *Node) Subscribe(topic string) {}         // calls Java callback
func (n *Node) Close() { n.cancel() }
```

Build AAR:
```bash
gomobile bind -target=android -androidapi 21 \
  -o ridechain-libp2p.aar \
  github.com/yourorg/ridechain/libp2p-android
```

**Android app size impact:** ~12–18 MB  
**Pros:** True P2P after discovery, no server relay for chat  
**Cons:** Battery impact, QUIC may be firewalled on some carriers, complexity  

---

### Option C — WebRTC Data Channels (Recommended for v2 Chat, 3–6 weeks)

Bootstrap server handles signaling (SDP exchange). Peers establish direct WebRTC connections for chat. Works through NAT.

`github.com/pion/webrtc/v3` is **already in your go.mod** as a transitive dependency.

#### Server-side (add to bridge.go or new `signaling.go`)

```go
// WS message type: "webrtc_offer"  { from, to, sdp }
// WS message type: "webrtc_answer" { from, to, sdp }
// WS message type: "webrtc_ice"    { from, to, candidate }
// Bridge just relays these targeted messages — no WebRTC logic needed server-side.
```

The bridge already handles targeted DMs via `target_peer_id`. WebRTC signaling is just DM messages with SDP payloads — **no server changes needed** beyond ensuring DM routing works.

#### Android (using Google WebRTC library)
```kotlin
// build.gradle.kts
implementation("io.getstream:stream-webrtc-android:1.1.0")

// Peer A: create offer, send via WS DM
val peerConnection = factory.createPeerConnection(iceServers, observer)
peerConnection.createOffer(sdpObserver, mediaConstraints)

// On createSuccess → ws.send("""{"type":"webrtc_offer","target_peer_id":"B","sdp":"..."}""")

// Peer B: receive offer via WS, create answer
// On webrtc_offer message → peerConnection.setRemoteDescription(offer)
// peerConnection.createAnswer(...)
// ws.send("""{"type":"webrtc_answer","target_peer_id":"A","sdp":"..."}""")

// After handshake: use DataChannel for chat (no relay through server)
val channel = peerConnection.createDataChannel("chat", DataChannel.Init())
channel.send(DataChannel.Buffer(ByteBuffer.wrap(msg.toByteArray()), false))
```

**Pros:** Direct P2P after signaling, battery-efficient, works via Cloudflare  
**Cons:** Need TURN server for strict NAT (add coturn on GCP ~$5/mo)  

---

## Recommended Path for RideChain

```
Now (MVP):      Option A  →  WebSocket to bootstrap
                              Walkie-talkie + nearby discovery works today

v1.5 (1 month): Option C  →  Add WebRTC signaling for 1:1 chat
                              Server handles signaling, peers chat directly

v2 (3 months):  Option B  →  Optional: native libp2p for power users
                              True P2P mesh, offline-capable
```

---

## Nearby Discovery Flow (Option A, works today)

```
1. App starts → connect WebSocket to wss://ws.ridechain.in/rider?city=mumbai
2. App gets location → PUT https://api.ridechain.in/register/lat-lng
   → Server subscribes to 9 geohash-6 cells (~3.6 × 1.8 km)

3. App sends nearby_ping every 30s:
   {"type":"nearby_ping","peer_id":"arjun123","name":"Arjun","city":"mumbai"}
   → All riders within ~2 km receive it

4. "Find People" tab: collect nearby_ping messages → show list (Tinder-style)

5. User taps "Chat with Priya":
   {"type":"chat_message","peer_id":"arjun123","target_peer_id":"priya456","msg":"Hi!"}
   → Delivered directly if online, or via FCM wake-up + Redis inbox if offline
```

---

## Android FCM Integration

To receive wake-up pushes when offline:

```kotlin
// After Firebase.initializeApp(this) in Application.onCreate()
FirebaseMessaging.getInstance().token.addOnCompleteListener { task ->
    if (task.isSuccessful) {
        val token = task.result
        // Register with bootstrap:
        api.putFCMToken(PutFCMRequest(myPeerId, token))
    }
}

// Handle incoming FCM in MyFirebaseMessagingService.onMessageReceived()
override fun onMessageReceived(message: RemoteMessage) {
    if (message.data["type"] == "new_message") {
        // Reconnect WebSocket to fetch queued messages from Redis inbox
        rideChainClient.connect(city, myPeerId)
    }
}
```

---

## TURN Server (for WebRTC, Option C only)

If you go WebRTC, you need a TURN server for users behind strict NAT (~5–10% of users):

```bash
# On a small GCP VM (e2-micro, ~$6/mo):
sudo apt-get install coturn
sudo turnadmin -a -u ridechain -r ridechain.in -p your_turn_password
```

Caddyfile addition:
```caddyfile
turn.ridechain.in {
    reverse_proxy localhost:3478
}
```

ICE servers config in Android:
```kotlin
val iceServers = listOf(
    PeerConnection.IceServer.builder("stun:stun.l.google.com:19302").createIceServer(),
    PeerConnection.IceServer.builder("turn:turn.ridechain.in:3478")
        .setUsername("ridechain")
        .setPassword("your_turn_password")
        .createIceServer()
)
```
