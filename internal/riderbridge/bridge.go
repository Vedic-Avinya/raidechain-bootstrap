package riderbridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"golang.org/x/time/rate"

	"github.com/ridechain/ridechain/services/bootstrap/internal/analytics"
	appmetrics "github.com/ridechain/ridechain/services/bootstrap/internal/metrics"
)

const (
	riderBridgePortDefault = "4003"
	defaultMaxConnections  = 5000
	sendBufferSize         = 256
	readDeadline           = 90 * time.Second
	writeDeadline          = 5 * time.Second
	pingInterval           = 25 * time.Second
	topicPrefix            = "/ridechain/"
	topicSuffix            = "/p2p/v1"
	defaultCity            = "in"
	defaultEnv             = "dev"
)

// allowedOrigins returns the set of allowed WebSocket origins from RIDER_ALLOWED_ORIGINS
// (comma-separated). If empty, all origins are allowed (dev mode only).
func allowedOrigins() map[string]bool {
	raw := os.Getenv("RIDER_ALLOWED_ORIGINS")
	if raw == "" {
		return nil
	}
	m := make(map[string]bool)
	for _, o := range strings.Split(raw, ",") {
		if o = strings.TrimSpace(o); o != "" {
			m[o] = true
		}
	}
	return m
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origins := allowedOrigins()
		if len(origins) == 0 {
			return true // dev: allow all origins
		}
		return origins[r.Header.Get("Origin")]
	},
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

// riderConn tracks a single connected rider and their shard (city).
// send is a buffered channel drained by a per-connection writePump goroutine.
// WriteControl (ping/close) is safe to call concurrently with writePump's WriteMessage.
type riderConn struct {
	conn      *websocket.Conn
	peerId    string
	city      string   // normalized city slug for topic sharding
	geoCells  []string // current 9-cell geohash window; nil until first location update
	send      chan []byte
	closeOnce sync.Once
	rateLim   *rate.Limiter // per-connection token bucket: 20 msg/s, burst 40
}

// writePump serialises all data frame writes to the WebSocket connection.
// Runs in its own goroutine per connection; exits when send is closed or a write fails.
func (rc *riderConn) writePump() {
	defer rc.conn.Close()
	for msg := range rc.send {
		rc.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
		if err := rc.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

// deliver enqueues a message for writePump (non-blocking).
// Returns false when the buffer is full (slow/stalled client) or the channel is closed.
func (rc *riderConn) deliver(msg []byte) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false // send on closed channel — connection already torn down
		}
	}()
	select {
	case rc.send <- msg:
		return true
	default:
		return false
	}
}

// closeSend closes the send channel exactly once, causing writePump to drain and exit.
func (rc *riderConn) closeSend() {
	rc.closeOnce.Do(func() { close(rc.send) })
}

// cityTopic holds a joined topic and its subscription for one city.
type cityTopic struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
	cancel context.CancelFunc
}

// OfflineStore persists messages for peers that are temporarily offline.
// Implemented by redis.Store; use a nil store to disable (FCM-only fallback).
type OfflineStore interface {
	DrainOfflineMessages(ctx context.Context, peerID string) ([][]byte, error)
}

// DedupStore provides server-side message deduplication via Redis SETNX.
type DedupStore interface {
	IsDuplicateMessage(ctx context.Context, msgID string) (bool, error)
}

// Bridge manages multiple rider WebSocket connections and bridges them to Gossipsub.
// Shards by city (/ridechain/{city}/p2p/v1) for broad routing;
// also shards by geohash-6 cell (/ridechain/geo/{cell}/p2p/v1) for proximity routing.
type Bridge struct {
	gs              *pubsub.PubSub
	ctx             context.Context
	maxConns        int
	env             string // prod | staging | dev — used in topic names
	store           OfflineStore
	dedupStore      DedupStore
	analyticsClient *analytics.Client
	mu              sync.RWMutex
	riders          map[*websocket.Conn]*riderConn
	peerByID        map[string]*riderConn
	topicByCity     map[string]*cityTopic
	geoTopics       map[string]*cityTopic            // geohash-6 cell → topic+sub
	cellRiders      map[string]map[string]*riderConn // geohash-6 cell → peerID → riderConn
	onLocalMsg      func(data []byte)
	onPeerOffline   func(peerID string, data []byte)
}

// New creates a rider bridge that joins topics per city on demand.
// Pass the GossipSub instance so the bridge can join /ridechain/{city}/p2p/v1 per city.
// MAX_RIDER_CONNECTIONS env overrides the default connection limit (default: 5000).
func New(gs *pubsub.PubSub, ctx context.Context) *Bridge {
	maxConns := defaultMaxConnections
	if v := os.Getenv("MAX_RIDER_CONNECTIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConns = n
		}
	}
	env := strings.TrimSpace(strings.ToLower(os.Getenv("BOOTSTRAP_ENV")))
	if env == "" {
		env = defaultEnv
	}
	return &Bridge{
		gs:          gs,
		ctx:         ctx,
		maxConns:    maxConns,
		env:         env,
		riders:      make(map[*websocket.Conn]*riderConn),
		peerByID:    make(map[string]*riderConn),
		topicByCity: make(map[string]*cityTopic),
		geoTopics:   make(map[string]*cityTopic),
		cellRiders:  make(map[string]map[string]*riderConn),
	}
}

// SetLocalMessageHandler sets a callback invoked when a rider sends a message (for cross-bridge relay to driver bridge).
func (b *Bridge) SetLocalMessageHandler(fn func(data []byte)) {
	b.onLocalMsg = fn
}

// SetOnPeerOffline sets a callback when a message targets a peer who is not connected (e.g. FCM push).
func (b *Bridge) SetOnPeerOffline(fn func(peerID string, data []byte)) {
	b.onPeerOffline = fn
}

// SetOfflineStore provides a store used to flush queued messages when a peer reconnects.
func (b *Bridge) SetOfflineStore(store OfflineStore) {
	b.store = store
}

// SetDedupStore provides a store for server-side message deduplication.
func (b *Bridge) SetDedupStore(store DedupStore) {
	b.dedupStore = store
}

// SetAnalyticsClient wires a Firebase Analytics client for server-side event tracking.
func (b *Bridge) SetAnalyticsClient(c *analytics.Client) {
	b.analyticsClient = c
}

// normalizeCity returns a safe topic segment (lowercase, alphanumeric + underscore). Empty -> defaultCity.
func normalizeCity(city string) string {
	s := strings.TrimSpace(strings.ToLower(city))
	if s == "" {
		return defaultCity
	}
	var out strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			out.WriteRune(r)
		}
	}
	if out.Len() == 0 {
		return defaultCity
	}
	return out.String()
}

// topicForCity returns the GossipSub topic for a city within this deployment env.
// Format: /ridechain/{env}/{city}/p2p/v1  e.g. /ridechain/prod/mumbai/p2p/v1
func (b *Bridge) topicForCity(city string) string {
	return topicPrefix + b.env + "/" + city + topicSuffix
}

// getOrJoinCity ensures we are joined to the topic for city and have a subscription that forwards to riders in that city.
func (b *Bridge) getOrJoinCity(city string) (*pubsub.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ct, ok := b.topicByCity[city]; ok {
		return ct.topic, nil
	}
	topicName := b.topicForCity(city)
	topic, err := b.gs.Join(topicName)
	if err != nil {
		return nil, err
	}
	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(b.ctx)
	ct := &cityTopic{topic: topic, sub: sub, cancel: cancel}
	b.topicByCity[city] = ct
	slog.Debug("rider_bridge", "msg", "joined city topic", "city", city, "topic", topicName)
	go b.runCitySub(ctx, city, sub)
	return topic, nil
}

func (b *Bridge) runCitySub(ctx context.Context, city string, sub *pubsub.Subscription) {
	defer sub.Cancel()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if len(msg.Data) == 0 {
			continue
		}
		b.forwardToCity(city, msg.Data)
	}
}

// chatDMTopicPrefix is the prefix for 1:1 chat topics (chat_dm_peerA_peerB). Used to derive target from topic when envelope has no "to".
const chatDMTopicPrefix = "chat_dm_"

// otherPeerFromChatTopic returns the peer ID that is not the sender from a topic "chat_dm_peerA_peerB".
func otherPeerFromChatTopic(topic, sender string) string {
	rest := strings.TrimPrefix(topic, chatDMTopicPrefix)
	if rest == topic || sender == "" {
		return ""
	}
	if strings.HasPrefix(rest, sender+"_") {
		return rest[len(sender)+1:]
	}
	if strings.HasSuffix(rest, "_"+sender) {
		return rest[:len(rest)-len(sender)-1]
	}
	return ""
}

// forwardToCity sends data to all riders in the given city (and handles targeted messages).
func (b *Bridge) forwardToCity(city string, data []byte) {
	var msgType string
	var targetPeerID string
	if len(data) > 0 {
		var envelope struct {
			Type         string `json:"type"`
			To           string `json:"to"`
			TargetPeerID string `json:"target_peer_id"`
			Topic        string `json:"topic"`
			PeerID       string `json:"peer_id"`
		}
		_ = json.Unmarshal(data, &envelope)
		msgType = envelope.Type
		if msgType == "" {
			msgType = "unknown"
		}
		if envelope.To != "" {
			targetPeerID = envelope.To
		} else if envelope.TargetPeerID != "" {
			targetPeerID = envelope.TargetPeerID
		} else if envelope.Type == "chat_msg" && envelope.Topic != "" && envelope.PeerID != "" && strings.HasPrefix(envelope.Topic, chatDMTopicPrefix) {
			// 1:1 chat: topic is "chat_dm_peerA_peerB" (sorted); sender is PeerID; target is the other peer.
			targetPeerID = otherPeerFromChatTopic(envelope.Topic, envelope.PeerID)
		}
	}

	if targetPeerID != "" {
		b.mu.RLock()
		rc := b.peerByID[targetPeerID]
		b.mu.RUnlock()
		if rc != nil && rc.city == city {
			if rc.deliver(data) {
				slog.Debug("rider_bridge", "msg", "forward_targeted", "type", msgType, "target", targetPeerID, "city", city)
				return
			}
			slog.Warn("rider_bridge", "msg", "forward_targeted buffer full, dropping slow client", "target", targetPeerID)
			b.mu.Lock()
			delete(b.riders, rc.conn)
			delete(b.peerByID, rc.peerId)
			b.mu.Unlock()
			rc.closeSend()
		}
		if b.onPeerOffline != nil {
			b.onPeerOffline(targetPeerID, data)
		}
		slog.Debug("rider_bridge", "msg", "forward_targeted peer offline", "type", msgType, "target", targetPeerID)
		return
	}

	b.mu.RLock()
	riders := make([]*riderConn, 0, len(b.riders))
	for _, rc := range b.riders {
		if rc.city == city {
			riders = append(riders, rc)
		}
	}
	b.mu.RUnlock()

	if len(riders) == 0 {
		return
	}
	slog.Debug("rider_bridge", "msg", "forward_to_city", "type", msgType, "city", city, "count", len(riders))

	for _, rc := range riders {
		if !rc.deliver(data) {
			b.mu.Lock()
			delete(b.riders, rc.conn)
			if rc.peerId != "" {
				delete(b.peerByID, rc.peerId)
			}
			b.mu.Unlock()
			rc.closeSend()
			slog.Warn("rider_bridge", "msg", "forward buffer full, removed slow rider", "peer_id", rc.peerId)
		}
	}
}

// Run starts the HTTP server for rider WebSocket (GET /rider). Blocks until ctx done.
func (b *Bridge) Run(ctx context.Context, port string) {
	if port == "" {
		port = riderBridgePortDefault
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /rider", b.handleRider)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		b.mu.RLock()
		count := len(b.riders)
		b.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":          "ok",
			"rider_connected": count > 0,
			"rider_count":     count,
		})
	})
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		slog.Info("rider_bridge", "msg", "listening", "port", port, "path", "/rider", "sharding", "by_city")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("rider_bridge", "msg", "server error", "err", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)

	b.mu.Lock()
	for _, rc := range b.riders {
		_ = rc.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
			time.Now().Add(2*time.Second),
		)
		rc.closeSend()
	}
	b.riders = make(map[*websocket.Conn]*riderConn)
	b.peerByID = make(map[string]*riderConn)
	b.cellRiders = make(map[string]map[string]*riderConn)
	for _, ct := range b.topicByCity {
		ct.cancel()
		ct.sub.Cancel()
		ct.topic.Close()
	}
	b.topicByCity = make(map[string]*cityTopic)
	for _, ct := range b.geoTopics {
		ct.cancel()
		ct.sub.Cancel()
		ct.topic.Close()
	}
	b.geoTopics = make(map[string]*cityTopic)
	b.mu.Unlock()
}

func (b *Bridge) handleRider(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	count := len(b.riders)
	b.mu.RUnlock()
	if count >= b.maxConns {
		slog.Warn("rider_bridge", "msg", "connection limit reached", "limit", b.maxConns)
		appmetrics.ConnectionsRejected.WithLabelValues("limit_reached").Inc()
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	city := normalizeCity(r.URL.Query().Get("city"))

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("rider_bridge", "msg", "upgrade failed", "err", err)
		return
	}

	rc := &riderConn{
		conn:    conn,
		city:    city,
		send:    make(chan []byte, sendBufferSize),
		rateLim: rate.NewLimiter(rate.Limit(20), 40),
	}
	b.mu.Lock()
	b.riders[conn] = rc
	count = len(b.riders)
	b.mu.Unlock()
	go rc.writePump()
	appmetrics.RidersConnected.Inc()
	slog.Debug("rider_bridge", "msg", "rider connected", "remote", r.RemoteAddr, "city", city, "total_riders", count)
	connectedAt := time.Now()

	topic, err := b.getOrJoinCity(city)
	if err != nil {
		slog.Warn("rider_bridge", "msg", "getOrJoinCity failed", "city", city, "err", err)
		// continue anyway; publish will fail but we can still try
	}

	defer func() {
		b.mu.Lock()
		delete(b.riders, conn)
		if rc.peerId != "" {
			delete(b.peerByID, rc.peerId)
		}
		for _, cell := range rc.geoCells {
			if cm := b.cellRiders[cell]; cm != nil {
				delete(cm, rc.peerId)
			}
		}
		count := len(b.riders)
		b.mu.Unlock()
		rc.closeSend()
		appmetrics.RidersConnected.Dec()
		if rc.peerId != "" && b.analyticsClient != nil {
			b.analyticsClient.Send(b.ctx, rc.peerId,
				analytics.PeerDisconnect(rc.peerId, rc.city, int64(time.Since(connectedAt).Seconds())))
		}
		slog.Debug("rider_bridge", "msg", "rider disconnected", "peer_id", rc.peerId, "city", rc.city, "geo_cells", len(rc.geoCells), "total_riders", count)
	}()

	conn.SetReadDeadline(time.Now().Add(readDeadline))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeDeadline))
				if err != nil {
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			slog.Warn("rider_bridge", "msg", "read failed", "peer_id", rc.peerId, "remote", r.RemoteAddr, "err", err)
			break
		}

		conn.SetReadDeadline(time.Now().Add(readDeadline))

		if len(data) == 0 {
			continue
		}
		if len(data) > 65536 { // 64 KiB max message size
			appmetrics.MessagesDropped.WithLabelValues("oversized").Inc()
			slog.Warn("rider_bridge", "msg", "oversized message dropped", "peer_id", rc.peerId, "size", len(data))
			continue
		}
		if !rc.rateLim.Allow() {
			appmetrics.MessagesDropped.WithLabelValues("rate_limit").Inc()
			if b.analyticsClient != nil {
				b.analyticsClient.Send(b.ctx, rc.peerId, analytics.RateLimitHit(rc.peerId))
			}
			slog.Warn("rider_bridge", "msg", "rate limit exceeded, message dropped", "peer_id", rc.peerId)
			continue
		}
		appmetrics.MessageSizeBytes.Observe(float64(len(data)))

		var envelope struct {
			Type   string `json:"type"`
			PeerID string `json:"peer_id"`
			PeerId string `json:"peerId"`
		}
		_ = json.Unmarshal(data, &envelope)
		peerIdFromMsg := envelope.PeerID
		if peerIdFromMsg == "" {
			peerIdFromMsg = envelope.PeerId
		}

		if rc.peerId == "" && peerIdFromMsg != "" {
			rc.peerId = peerIdFromMsg
			b.mu.Lock()
			b.peerByID[rc.peerId] = rc
			b.mu.Unlock()
			slog.Debug("rider_bridge", "msg", "rider identified", "peer_id", rc.peerId, "city", rc.city, "remote", r.RemoteAddr)
			if b.store != nil {
				go b.flushInbox(rc)
			}
			if b.analyticsClient != nil {
				b.analyticsClient.Send(b.ctx, rc.peerId,
					analytics.PeerConnect(rc.peerId, rc.city, b.RiderCount()))
			}
		}

		if envelope.Type == "ping" {
			pong := map[string]any{"type": "pong", "timestamp": time.Now().Unix()}
			pongBytes, _ := json.Marshal(pong)
			rc.deliver(pongBytes)
			continue
		}
		if envelope.Type == "pong" {
			continue
		}

		if topic != nil {
			if err := topic.Publish(context.Background(), data); err != nil {
				appmetrics.GossipSubPublishErrors.WithLabelValues("city").Inc()
				slog.Warn("rider_bridge", "msg", "publish failed", "type", envelope.Type, "city", rc.city, "err", err)
			} else {
				appmetrics.MessagesRelayed.WithLabelValues(envelope.Type, "city").Inc()
				slog.Debug("rider_bridge", "msg", "published from rider", "type", envelope.Type, "peer_id", rc.peerId, "city", rc.city, "size", len(data))
			}
			// Nearby/geo broadcast: also publish to the rider's geohash-cell topic.
			if strings.HasPrefix(envelope.Type, "nearby_") || envelope.Type == "location_broadcast" {
				b.publishToRiderGeoCell(rc, data)
			}
			// Forward to all riders in this city (self-filter bypass).
			b.forwardToCity(rc.city, data)
			if b.analyticsClient != nil {
				b.analyticsClient.Send(b.ctx, rc.peerId,
					analytics.MessageSent(rc.peerId, envelope.Type, rc.city, len(data)))
			}
		}

		if b.onLocalMsg != nil {
			b.onLocalMsg(data)
		}
	}
	close(done)
	slog.Info("rider_bridge", "msg", "read loop exited", "peer_id", rc.peerId)
}

// Forward broadcasts a message to all connected riders. Used when the message comes from the default/broadcast topic (e.g. from drivers).
// For per-city traffic, the bridge uses forwardToCity from runCitySub instead.
func (b *Bridge) Forward(data []byte) {
	var msgType string
	var targetPeerID string
	if len(data) > 0 {
		var envelope struct {
			Type         string `json:"type"`
			To           string `json:"to"`
			TargetPeerID string `json:"target_peer_id"`
			Topic        string `json:"topic"`
			PeerID       string `json:"peer_id"`
		}
		_ = json.Unmarshal(data, &envelope)
		msgType = envelope.Type
		if msgType == "" {
			msgType = "unknown"
		}
		if envelope.To != "" {
			targetPeerID = envelope.To
		} else if envelope.TargetPeerID != "" {
			targetPeerID = envelope.TargetPeerID
		} else if envelope.Type == "chat_msg" && envelope.Topic != "" && envelope.PeerID != "" && strings.HasPrefix(envelope.Topic, chatDMTopicPrefix) {
			targetPeerID = otherPeerFromChatTopic(envelope.Topic, envelope.PeerID)
		}
	}

	if targetPeerID != "" {
		b.mu.RLock()
		rc := b.peerByID[targetPeerID]
		b.mu.RUnlock()
		if rc != nil {
			if rc.deliver(data) {
				slog.Info("rider_bridge", "msg", "forward_targeted", "type", msgType, "target", targetPeerID)
				return
			}
			slog.Warn("rider_bridge", "msg", "forward_targeted buffer full, dropping slow client", "target", targetPeerID)
			b.mu.Lock()
			delete(b.riders, rc.conn)
			delete(b.peerByID, rc.peerId)
			b.mu.Unlock()
			rc.closeSend()
		}
		if b.onPeerOffline != nil {
			b.onPeerOffline(targetPeerID, data)
		}
		return
	}

	b.mu.RLock()
	riders := make([]*riderConn, 0, len(b.riders))
	for _, rc := range b.riders {
		riders = append(riders, rc)
	}
	b.mu.RUnlock()

	for _, rc := range riders {
		if !rc.deliver(data) {
			b.mu.Lock()
			delete(b.riders, rc.conn)
			if rc.peerId != "" {
				delete(b.peerByID, rc.peerId)
			}
			b.mu.Unlock()
			rc.closeSend()
		}
	}
}

// flushInbox drains persisted offline messages for a reconnected peer and delivers them.
func (b *Bridge) flushInbox(rc *riderConn) {
	msgs, err := b.store.DrainOfflineMessages(b.ctx, rc.peerId)
	if err != nil {
		slog.Warn("rider_bridge", "msg", "inbox drain failed", "peer_id", rc.peerId, "err", err)
		return
	}
	for _, msg := range msgs {
		if !rc.deliver(msg) {
			slog.Warn("rider_bridge", "msg", "inbox delivery dropped, buffer full", "peer_id", rc.peerId)
			break
		}
	}
	if len(msgs) > 0 {
		slog.Info("rider_bridge", "msg", "inbox flushed on reconnect", "peer_id", rc.peerId, "count", len(msgs))
	}
}

// RiderCount returns the number of currently connected riders.
func (b *Bridge) RiderCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.riders)
}
