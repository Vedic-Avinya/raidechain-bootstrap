package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ridechain/ridechain/services/bootstrap/internal/analytics"
	"github.com/ridechain/ridechain/services/bootstrap/internal/api"
	deviceauth "github.com/ridechain/ridechain/services/bootstrap/internal/auth"
	"github.com/ridechain/ridechain/services/bootstrap/internal/driverbridge"
	"github.com/ridechain/ridechain/services/bootstrap/internal/fcm"
	"github.com/ridechain/ridechain/services/bootstrap/internal/integrity"
	appmetrics "github.com/ridechain/ridechain/services/bootstrap/internal/metrics"
	"github.com/ridechain/ridechain/services/bootstrap/internal/persist"
	"github.com/ridechain/ridechain/services/bootstrap/internal/redis"
	"github.com/ridechain/ridechain/services/bootstrap/internal/riderbridge"
)

func main() {
	loadEnvFile(".env")

	// LOG_LEVEL=debug for verbose logs (conn_evt, relay, gossipsub_message, peer_stats). Default info keeps GCP log volume low.
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	slog.Info("step", "n", 1, "msg", "bootstrap starting")

	port := envOr("BOOTSTRAP_PORT", "4001")
	wsPort := envOr("BOOTSTRAP_WS_PORT", "4002")
	riderBridgePort := envOr("BOOTSTRAP_RIDER_BRIDGE_PORT", "4003")
	driverBridgePort := envOr("BOOTSTRAP_DRIVER_BRIDGE_PORT", "4004")
	httpAPIPort := envOr("BOOTSTRAP_HTTP_PORT", "4005")
	metricsPort := envOr("BOOTSTRAP_METRICS_PORT", "9090")
	bootstrapEnv := envOr("BOOTSTRAP_ENV", "dev")
	slog.Info("step", "n", 2, "msg", "ports configured",
		"env", bootstrapEnv,
		"tcp", port, "ws", wsPort,
		"rider_bridge", riderBridgePort,
		"driver_bridge", driverBridgePort,
		"http_api", httpAPIPort,
		"metrics", metricsPort,
	)

	// Firebase Analytics (GA4 Measurement Protocol) — optional, degrades gracefully.
	analyticsCli := analytics.New()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Identity: load from BOOTSTRAP_IDENTITY_SEED env (hex-encoded 32 bytes),
	// or fall back to a deterministic dev seed. In production, set BOOTSTRAP_IDENTITY_SEED
	// to a securely generated random hex string to prevent key derivation from source code.
	seed := loadIdentitySeed()
	priv, _, err := crypto.GenerateEd25519Key(bytes.NewReader(seed))
	if err != nil {
		slog.Error("step", "n", 3, "msg", "failed to generate bootstrap identity", "err", err)
		os.Exit(1)
	}

	// Create libp2p host with TCP + WebSocket transports and relay.
	// QUIC REMOVED: quic-go v0.48.2 panics with "crypto/tls bug: where's
	// my session ticket?" on incoming QUIC connections, causing a crash loop
	// (restart counter hit 161+).  TCP + WS is sufficient.
	slog.Info("step", "n", 3, "msg", "creating libp2p host (TCP + WebSocket)")
	var h host.Host
	h, err = libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s/ws", wsPort),
		),
		libp2p.EnableRelay(),
		libp2p.EnableRelayService(
			relayv2.WithLimit(&relayv2.RelayLimit{
				Duration: 5 * time.Minute,
				Data:     20 << 20, // 20 MB — generous headroom for images after double base64
			}),
		),
		libp2p.EnableHolePunching(),
		libp2p.ForceReachabilityPublic(),
	)
	if err != nil {
		slog.Error("step", "n", 3, "msg", "failed to create libp2p host", "err", err)
		os.Exit(1)
	}
	defer h.Close()
	slog.Info("step", "n", 3, "msg", "libp2p host created", "peer_id", h.ID().String())

	// Log every listen address.
	for i, addr := range h.Addrs() {
		full := fmt.Sprintf("%s/p2p/%s", addr, h.ID())
		slog.Info("step", "n", 4, "msg", "listening", "i", i+1, "multiaddr", full)
	}

	// Log connection events (Debug by default to reduce GCP log volume).
	notifiee := &network.NotifyBundle{
		ListenF: func(_ network.Network, a multiaddr.Multiaddr) {
			slog.Debug("conn_evt", "event", "listen", "addr", a.String())
		},
		ListenCloseF: func(_ network.Network, a multiaddr.Multiaddr) {
			slog.Debug("conn_evt", "event", "listen_close", "addr", a.String())
		},
		ConnectedF: func(_ network.Network, c network.Conn) {
			remote := c.RemotePeer()
			addr := c.RemoteMultiaddr()
			slog.Debug("conn_evt", "event", "peer_connected", "peer_id", remote.String(), "remote_addr", addr.String(), "dir", c.Stat().Direction.String())
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			remote := c.RemotePeer()
			slog.Debug("conn_evt", "event", "peer_disconnected", "peer_id", remote.String())
		},
	}
	h.Network().Notify(notifiee)
	slog.Info("step", "n", 5, "msg", "connection notifiee registered")

	// Kademlia DHT in server mode.
	slog.Info("step", "n", 6, "msg", "creating DHT (server mode)")
	d, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		slog.Error("step", "n", 6, "msg", "failed to create DHT", "err", err)
		os.Exit(1)
	}
	defer d.Close()

	if err := d.Bootstrap(ctx); err != nil {
		slog.Error("step", "n", 7, "msg", "failed to bootstrap DHT", "err", err)
		os.Exit(1)
	}
	slog.Info("step", "n", 7, "msg", "DHT server mode active")

	// GossipSub v1.1. FloodPublish removed — it sends every message to ALL peers
	// regardless of mesh topology, causing O(N) fan-out that is catastrophic at scale.
	slog.Info("step", "n", 8, "msg", "creating GossipSub")
	gs, err := pubsub.NewGossipSub(ctx, h, pubsub.WithMaxMessageSize(10*1024*1024))
	if err != nil {
		slog.Error("step", "n", 8, "msg", "failed to create GossipSub", "err", err)
		os.Exit(1)
	}

	// Default topic for driver bridge and broadcast.
	// Riders shard by city: /ridechain/{env}/{city}/p2p/v1 (set by bridge.New()).
	defaultGossipTopic := "/ridechain/" + bootstrapEnv + "/in/p2p/v1"
	gossipsubTopic := envOr("BOOTSTRAP_GOSSIPSUB_TOPIC", defaultGossipTopic)
	if !strings.HasPrefix(gossipsubTopic, "/ridechain/") {
		slog.Error("step", "n", 9, "msg", "BOOTSTRAP_GOSSIPSUB_TOPIC must start with /ridechain/", "topic", gossipsubTopic)
		os.Exit(1)
	}
	slog.Info("step", "n", 9, "msg", "joining default topic (driver + broadcast)", "topic", gossipsubTopic, "region", "India")
	defaultTopic, err := gs.Join(gossipsubTopic)
	if err != nil {
		slog.Error("step", "n", 9, "msg", "failed to join topic", "err", err, "topic", gossipsubTopic)
		os.Exit(1)
	}
	defer defaultTopic.Close()

	sub, err := defaultTopic.Subscribe()
	if err != nil {
		slog.Error("step", "n", 10, "msg", "failed to subscribe to topic", "err", err)
		os.Exit(1)
	}
	defer sub.Cancel()
	slog.Info("step", "n", 10, "msg", "subscribed to default topic; rider bridge shards by city on demand")

	// Redis store for peer registration, discover, and FCM (optional).
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
		slog.Info("step", "n", 9, "msg", "REDIS_URL not set; using default redis://localhost:6379")
	}
	var store *redis.Store
	var errStore error
	store, errStore = redis.NewStore(redisURL)
	if errStore != nil {
		slog.Error("step", "n", 9, "msg", "redis connection failed", "err", errStore, "url", redisURL)
		os.Exit(1)
	}
	slog.Info("step", "n", 9, "msg", "redis store connected")

	// FCM sender for waking offline riders and track request pushes (optional; degrades gracefully on failure).
	var pushSender fcm.PushSender
	if pushSender, err = fcm.NewPushSender(ctx); err != nil {
		slog.Warn("step", "n", 10, "msg", "FCM init failed; offline push disabled", "err", err)
		pushSender = fcm.NoopSender()
	}

	// SQLite persistent store for permanent user records (survives Redis TTL).
	persistDBPath := os.Getenv("PERSIST_DB_PATH")
	if persistDBPath == "" {
		persistDBPath = "/opt/ridechain-bootstrap/users.db"
	}
	persistStore, errPersist := persist.New(persistDBPath)
	if errPersist != nil {
		slog.Warn("persist_store_init_failed", "err", errPersist, "path", persistDBPath)
	} else {
		slog.Info("persist_store_ready", "path", persistDBPath)
		defer persistStore.Close()
	}

	// Play Integrity verifier (optional; set PLAY_INTEGRITY_PACKAGE_NAME to enable).
	// Created early so both auth handler and apiSrv can use it.
	var integrityVerifier *integrity.Verifier
	if pkg := os.Getenv("PLAY_INTEGRITY_PACKAGE_NAME"); pkg != "" {
		if iv, err := integrity.NewVerifier(pkg); err != nil {
			slog.Warn("play_integrity", "msg", "verifier init failed; integrity checks disabled", "err", err)
		} else {
			integrityVerifier = iv
			slog.Info("play_integrity", "msg", "verifier enabled", "package", pkg)
		}
	}

	// JWT auth issuer — protects all API routes except /auth/*
	jwtSecret := os.Getenv("JWT_SECRET")
	jwtIssuer := deviceauth.NewIssuer(jwtSecret)
	if jwtSecret == "" {
		slog.Warn("jwt_secret_not_set", "msg", "using random secret; tokens will NOT survive restarts — set JWT_SECRET env var")
	}
	slog.Info("jwt_auth", "msg", "issuer ready", "access_ttl", deviceauth.AccessTokenTTL, "refresh_ttl", deviceauth.RefreshTokenTTL)

	// HTTP API for register, search-by-name, discover, and live tracking (only when Redis is set).
	var apiSrv *api.HTTPServer
	if store != nil {
		apiSrv = api.NewHTTPServer(store)
		if persistStore != nil {
			apiSrv.SetPersistStore(persistStore)
		}
		if analyticsCli != nil {
			apiSrv.SetAnalyticsClient(analyticsCli)
		}
		trackAPI := api.NewTrackAPI(store, pushSender)
		authHandler := deviceauth.NewHandler(jwtIssuer, integrityVerifier)
		apiMux := http.NewServeMux()
		// Device auth endpoints (not protected by JWT middleware)
		apiMux.HandleFunc("POST /auth/device", authHandler.DeviceAuth)
		apiMux.HandleFunc("POST /auth/refresh", authHandler.RefreshAuth)
		// Existing peer registration & discovery
		apiMux.HandleFunc("POST /register", apiSrv.Register)
		apiMux.HandleFunc("POST /register/", apiSrv.Register)
		apiMux.HandleFunc("GET /search-by-name", apiSrv.SearchByName)
		apiMux.HandleFunc("GET /discover", apiSrv.Discover)
		apiMux.HandleFunc("POST /heartbeat", apiSrv.Heartbeat)
		apiMux.HandleFunc("PUT /register/fcm", apiSrv.PutFCM)
		apiMux.HandleFunc("PUT /register/display-name", apiSrv.PutDisplayName)
		apiMux.HandleFunc("PUT /register/lat-lng", apiSrv.PutLatLng)
		// Report user
		apiMux.HandleFunc("POST /report", apiSrv.ReportPeer)
		// P2P push notification fallback (when native P2P delivery fails)
		apiMux.HandleFunc("POST /notify-peer", apiSrv.NotifyPeer)
		// Recently active peers sorted by last activity
		apiMux.HandleFunc("GET /peers/active", apiSrv.RecentlyActivePeers)
		// Persistent DB: stats and peer recovery
		apiMux.HandleFunc("GET /stats", apiSrv.Stats)
		apiMux.HandleFunc("GET /recover-peer", apiSrv.RecoverPeer)
		// Live tracking session endpoints (register /track/sessions/me before /track/sessions/ prefix)
		apiMux.HandleFunc("GET /track/sessions/me", trackAPI.GetMySession)
		apiMux.HandleFunc("POST /track/sessions", trackAPI.CreateSession)
		apiMux.HandleFunc("/track/sessions/", trackAPI.RouteSession)
		// WebRTC: STUN + ephemeral TURN (coturn REST / shared secret); see TURN_* env vars.
		apiMux.HandleFunc("GET /webrtc/turn", apiSrv.TurnCredentials)
		rl := api.NewIPRateLimiter()
		// Chain: rate limiter → JWT middleware → mux
		jwtMiddleware := deviceauth.Middleware(jwtIssuer)
		apiServer := &http.Server{
			Addr:              ":" + httpAPIPort,
			Handler:           rl.Middleware(jwtMiddleware(apiMux)),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		go func() {
			slog.Info("http_api", "msg", "listening", "port", httpAPIPort,
				"paths", []string{"/register", "/search-by-name", "/discover", "/track/sessions", "/webrtc/turn"})
			if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http_api", "msg", "server error", "err", err)
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = apiServer.Shutdown(shutdownCtx)
		}()
	}

	// Create bridges. Rider bridge joins per-city topics on demand (/ridechain/{city}/p2p/v1). Driver uses default topic.
	riderBrid := riderbridge.New(gs, ctx)
	driverBrid := driverbridge.New(defaultTopic, nil)

	// Wire persistence: offline inbox + message deduplication.
	if store != nil {
		riderBrid.SetOfflineStore(store)
		riderBrid.SetDedupStore(store)
	}

	// Wire analytics + geo updater.
	if analyticsCli != nil {
		riderBrid.SetAnalyticsClient(analyticsCli)
	}
	if apiSrv != nil {
		apiSrv.SetGeoUpdater(riderBrid)
		apiSrv.SetPushSender(pushSender)
	}

	// When a message is targeted at a rider who is offline, try to send FCM push.
	// Trigger: only when a message has "to" or "target_peer_id" set to this peer and they're not on WebSocket.
	if store != nil {
		riderBrid.SetOnPeerOffline(func(peerID string, data []byte) {
			// Persist to Redis inbox (at-least-once delivery on reconnect).
			if len(data) > 0 {
				if err := store.EnqueueOfflineMessage(ctx, peerID, data); err != nil {
					slog.Warn("offline_inbox", "msg", "enqueue failed", "peer_id", peerID, "err", err)
				}
			}
			// Send FCM wake-up (no payload — app reconnects over WS to fetch messages).
			meta, err := store.GetPeer(ctx, peerID)
			if err != nil {
				slog.Warn("fcm_offline", "msg", "get peer failed", "peer_id", peerID, "err", err)
				return
			}
			if meta == nil || meta.FCMToken == "" {
				slog.Info("fcm_offline", "msg", "no FCM token for peer", "peer_id", peerID)
				return
			}
			wakeup := map[string]string{"type": "new_message", "peer_id": peerID}
			if err := pushSender.Send(ctx, meta.FCMToken, wakeup); err != nil {
				appmetrics.FCMPushes.WithLabelValues("failure").Inc()
				slog.Warn("fcm_offline", "msg", "send failed", "peer_id", peerID, "err", err)
			} else {
				appmetrics.FCMPushes.WithLabelValues("success").Inc()
				appmetrics.OfflineMessagesEnqueued.Inc()
				slog.Info("fcm_offline", "msg", "wake-up push sent", "peer_id", peerID)
			}
		})
	}

	// CRITICAL: Cross-bridge relay wiring.
	//
	// go-libp2p-pubsub's sub.Next() filters messages published by the local host.
	// When a rider sends a message via WebSocket → rider bridge publishes to gossipsub
	// → but the bootstrap's own subscription NEVER sees it (self-filter).
	// Without cross-bridge relay, messages between bridge-connected clients are LOST.
	//
	// Solution: Each bridge has a callback that fires when a WS client sends a message.
	// We forward directly to the other bridge and (for rider msgs) to all riders too,
	// so that rider-to-rider discovery works (e.g. two phones in Find peers see each other).
	riderBrid.SetLocalMessageHandler(func(data []byte) {
		msgType := parseMessageType(data)
		riderN := riderBrid.RiderCount()
		driverN := driverBrid.DriverCount()
		slog.Debug("relay", "source", "rider", "type", msgType, "size", len(data), "rider_count", riderN, "driver_count", driverN)
		// Rider sent a message → forward to all drivers AND to all riders (so other riders see peer_online).
		driverBrid.Forward(data)
		riderBrid.Forward(data)
	})
	driverBrid.SetLocalMessageHandler(func(data []byte) {
		msgType := parseMessageType(data)
		riderN := riderBrid.RiderCount()
		driverN := driverBrid.DriverCount()
		slog.Debug("relay", "source", "driver", "type", msgType, "size", len(data), "rider_count", riderN, "driver_count", driverN)
		// Driver sent a message → forward to all connected riders.
		riderBrid.Forward(data)
	})

	// Prometheus metrics endpoint — scrape with GET /metrics.
	// Point Prometheus → http://<host>:metricsPort/metrics; connect Grafana to Prometheus.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	metricsServer := &http.Server{
		Addr:              ":" + metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		slog.Info("metrics", "msg", "prometheus endpoint listening", "port", metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("metrics", "msg", "server error", "err", err)
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutCtx)
	}()

	go riderBrid.Run(ctx, riderBridgePort)
	go driverBrid.Run(ctx, driverBridgePort)

	// Copy-paste friendly WS addr for rider PWA.
	wsAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%s/ws/p2p/%s", wsPort, h.ID().String())
	slog.Info("bootstrap_ready",
		"peer_id", h.ID().String(),
		"topic", gossipsubTopic,
		"tcp_port", port,
		"ws_port", wsPort,
		"rider_bridge_port", riderBridgePort,
		"driver_bridge_port", driverBridgePort,
		"rider_pwa_addr", wsAddr,
	)
	slog.Info("step", "n", 11, "msg", "bootstrap node ready; waiting for peers")

	// Analytics counters.
	var msgTotal atomic.Int64
	var msgByType sync.Map // map[string]*atomic.Int64

	// Relay gossipsub messages from NATIVE libp2p peers to both bridges.
	// NOTE: Messages published by the bridges themselves (via topic.Publish) are
	// NOT received here due to gossipsub self-filtering. That's handled by the
	// cross-bridge relay above.
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				slog.Warn("gossipsub", "msg", "sub.Next failed", "err", err)
				return
			}
			msgTotal.Add(1)

			// Parse message type for analytics.
			var envelope struct {
				Type   string `json:"type"`
				Role   string `json:"role"`
				PeerID string `json:"peerId"`
			}
			msgType := "unknown"
			if json.Unmarshal(msg.Data, &envelope) == nil && envelope.Type != "" {
				msgType = envelope.Type
				if msgType == "rider_online" {
					slog.Debug("rider_registered", "peer_id", envelope.PeerID, "from", msg.ReceivedFrom.String())
				}
			}

			riderN := riderBrid.RiderCount()
			driverN := driverBrid.DriverCount()
			slog.Debug("gossipsub_message",
				"type", msgType,
				"from", truncPeerID(msg.ReceivedFrom.String()),
				"size", len(msg.Data),
				"total_relayed", msgTotal.Load(),
				"forward_to_riders", riderN,
				"forward_to_drivers", driverN,
			)

			// Forward to both bridges (these are messages from native libp2p peers).
			riderBrid.Forward(msg.Data)
			driverBrid.Forward(msg.Data)

			// Increment per-type counter.
			counter, _ := msgByType.LoadOrStore(msgType, &atomic.Int64{})
			counter.(*atomic.Int64).Add(1)
		}
	}()

	// Log peer count + message stats every 5 seconds.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		prevCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				libp2pPeers := len(h.Network().Peers())
				riderCount := riderBrid.RiderCount()
				driverCount := driverBrid.DriverCount()
				totalCount := libp2pPeers + riderCount + driverCount
				if totalCount != prevCount {
					typeCounts := map[string]int64{}
					msgByType.Range(func(k, v any) bool {
						typeCounts[k.(string)] = v.(*atomic.Int64).Load()
						return true
					})
					slog.Debug("peer_stats",
						"libp2p_peers", libp2pPeers,
						"rider_bridge", riderCount,
						"driver_bridge", driverCount,
						"total_messages", msgTotal.Load(),
						"by_type", typeCounts,
					)
					prevCount = totalCount
				}
			}
		}
	}()

	<-ctx.Done()
	slog.Info("bootstrap node shutting down",
		"total_messages_relayed", msgTotal.Load(),
	)
}

// loadIdentitySeed returns a 32-byte seed for Ed25519 key generation.
// In production, BOOTSTRAP_IDENTITY_SEED must be set (64 hex chars).
// Dev fallback only allowed when BOOTSTRAP_DEV_MODE=true.
func loadIdentitySeed() []byte {
	if seedHex := os.Getenv("BOOTSTRAP_IDENTITY_SEED"); seedHex != "" {
		decoded, err := hex.DecodeString(seedHex)
		if err != nil || len(decoded) < 32 {
			slog.Error("BOOTSTRAP_IDENTITY_SEED must be 64 hex chars (32 bytes)", "len", len(decoded), "err", err)
			os.Exit(1)
		}
		slog.Info("step", "n", 3, "msg", "using identity from BOOTSTRAP_IDENTITY_SEED env")
		return decoded[:32]
	}
	if os.Getenv("BOOTSTRAP_DEV_MODE") != "true" {
		slog.Error("BOOTSTRAP_IDENTITY_SEED is not set. Set it to a secure 64-hex-char value in production, or set BOOTSTRAP_DEV_MODE=true for local dev only.")
		os.Exit(1)
	}
	// Dev fallback: deterministic seed — NEVER use in production.
	slog.Warn("step", "n", 3, "msg", "DEV MODE: using insecure deterministic identity seed")
	seed := sha256.Sum256([]byte("ridechain-mvp-bootstrap-seed-v1"))
	return seed[:]
}

// generateSecureSeed prints a secure random seed for production use, then exits.
// Usage: GENERATE_SEED=true go run ./cmd/
func init() {
	if os.Getenv("GENERATE_SEED") == "true" {
		seed := make([]byte, 32)
		if _, err := rand.Read(seed); err != nil {
			fmt.Fprintf(os.Stderr, "failed to generate seed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("BOOTSTRAP_IDENTITY_SEED=%s\n", hex.EncodeToString(seed))
		os.Exit(0)
	}
}

// truncPeerID safely truncates a peer ID string for logging.
func truncPeerID(id string) string {
	if len(id) > 16 {
		return id[:16] + "..."
	}
	return id
}

// parseMessageType extracts the "type" field from a JSON message for logging.
func parseMessageType(data []byte) string {
	if len(data) == 0 {
		return "empty"
	}
	var env struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &env) != nil {
		return "invalid_json"
	}
	if env.Type == "" {
		return "unknown"
	}
	return env.Type
}

// loadEnvFile reads key=value lines from path and sets os env vars (only if not already set).
// So exported env and CLI take precedence over .env. No external deps.
func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.Index(line, "=")
		if i <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:i])
		val := strings.TrimSpace(line[i+1:])
		if key == "" {
			continue
		}
		if os.Getenv(key) != "" {
			continue
		}
		val = strings.Trim(val, "\"'")
		_ = os.Setenv(key, val)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
