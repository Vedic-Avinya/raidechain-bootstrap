package driverbridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	driverBridgePortDefault = "4004"
	maxDriverConnections    = 1000
	readDeadline            = 90 * time.Second
	writeDeadline           = 5 * time.Second
	pingInterval            = 25 * time.Second
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

// driverConn tracks a single connected driver.
type driverConn struct {
	conn   *websocket.Conn
	peerId string
	mu     sync.Mutex
}

// Bridge manages multiple driver WebSocket connections and bridges them to Gossipsub.
type Bridge struct {
	topic      *pubsub.Topic
	mu         sync.RWMutex
	drivers    map[*websocket.Conn]*driverConn
	peerByID   map[string]*driverConn // peerId -> conn (one conn per peer; updated on identify)
	peerQueue  *PeerQueue             // optional per-peer queue for handover message delivery
	onLocalMsg func(data []byte)      // cross-bridge callback for messages from WS clients
}

// New creates a driver bridge. Pass nil for queue to disable per-peer queuing.
func New(topic *pubsub.Topic, peerQueue *PeerQueue) *Bridge {
	if peerQueue == nil {
		peerQueue = NewPeerQueue()
	}
	return &Bridge{
		topic:     topic,
		drivers:   make(map[*websocket.Conn]*driverConn),
		peerByID:  make(map[string]*driverConn),
		peerQueue: peerQueue,
	}
}

// SetLocalMessageHandler sets a callback invoked when a driver sends a message
// through the WebSocket bridge. This is CRITICAL for cross-bridge relay:
// go-libp2p-pubsub sub.Next() filters self-published messages, so messages
// published by this bootstrap node never appear in its own subscription.
// The callback allows main.go to forward directly to the rider bridge.
func (b *Bridge) SetLocalMessageHandler(fn func(data []byte)) {
	b.onLocalMsg = fn
}

// Run starts the HTTP server for driver WebSocket (GET /driver). Blocks until ctx done.
func (b *Bridge) Run(ctx context.Context, port string) {
	if port == "" {
		port = driverBridgePortDefault
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /driver", b.handleDriver)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		b.mu.RLock()
		count := len(b.drivers)
		b.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":       "ok",
			"driver_count": count,
		})
	})
	server := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		slog.Info("driver_bridge", "msg", "listening", "port", port, "path", "/driver")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("driver_bridge", "msg", "server error", "err", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)

	// Send graceful close frame to all drivers before closing.
	b.mu.Lock()
	for _, dc := range b.drivers {
		dc.mu.Lock()
		_ = dc.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
			time.Now().Add(2*time.Second),
		)
		dc.mu.Unlock()
		_ = dc.conn.Close()
	}
	b.drivers = make(map[*websocket.Conn]*driverConn)
	b.mu.Unlock()
}

func (b *Bridge) handleDriver(w http.ResponseWriter, r *http.Request) {
	// Connection limit check.
	b.mu.RLock()
	count := len(b.drivers)
	b.mu.RUnlock()
	if count >= maxDriverConnections {
		slog.Warn("driver_bridge", "msg", "connection limit reached", "limit", maxDriverConnections)
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("driver_bridge", "msg", "upgrade failed", "err", err)
		return
	}
	dc := &driverConn{conn: conn}
	b.mu.Lock()
	b.drivers[conn] = dc
	count = len(b.drivers)
	b.mu.Unlock()
	slog.Debug("driver_bridge", "msg", "driver connected", "remote", r.RemoteAddr, "total_drivers", count)

	// Send current driver count to all drivers.
	b.broadcastPeerCount()

	defer func() {
		b.mu.Lock()
		delete(b.drivers, conn)
		if dc.peerId != "" {
			delete(b.peerByID, dc.peerId)
		}
		count := len(b.drivers)
		b.mu.Unlock()
		_ = conn.Close()
		slog.Debug("driver_bridge", "msg", "driver disconnected", "peer_id", dc.peerId, "total_drivers", count)
		b.broadcastPeerCount()
	}()

	// Read deadline: reset on every pong AND every application-level message.
	conn.SetReadDeadline(time.Now().Add(readDeadline))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})

	// Ping ticker — uses mutex to avoid concurrent writes (gorilla/websocket is NOT thread-safe).
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				dc.mu.Lock()
				err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeDeadline))
				dc.mu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	// Read from driver WebSocket and publish to Gossipsub topic.
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			slog.Warn("driver_bridge", "msg", "read failed", "peer_id", dc.peerId, "remote", r.RemoteAddr, "err", err)
			break
		}

		// Extend read deadline on ANY activity (prevents timeout for active drivers).
		conn.SetReadDeadline(time.Now().Add(readDeadline))

		if len(data) == 0 {
			continue
		}
		var envelope struct {
			Type   string `json:"type"`
			PeerID string `json:"peer_id"`
		}
		_ = json.Unmarshal(data, &envelope)

		// Track driver peer ID from first message and flush any queued messages for this peer.
		if dc.peerId == "" && envelope.PeerID != "" {
			dc.peerId = envelope.PeerID
			b.mu.Lock()
			b.peerByID[dc.peerId] = dc
			b.mu.Unlock()
			slog.Debug("driver_bridge", "msg", "driver identified", "peer_id", dc.peerId)
			b.peerQueue.FlushTo(dc.peerId, func(data []byte) error {
				dc.mu.Lock()
				defer dc.mu.Unlock()
				conn.SetWriteDeadline(time.Now().Add(writeDeadline))
				return conn.WriteMessage(websocket.TextMessage, data)
			})
		}

		// Handle ping/pong heartbeat messages (don't publish to topic).
		if envelope.Type == "ping" {
			pong := map[string]any{"type": "pong", "timestamp": time.Now().Unix()}
			pongBytes, _ := json.Marshal(pong)
			dc.mu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			_ = conn.WriteMessage(websocket.TextMessage, pongBytes)
			dc.mu.Unlock()
			continue
		}
		if envelope.Type == "pong" {
			continue
		}

		// Publish to Gossipsub topic (for native libp2p peers).
		if err := b.topic.Publish(context.Background(), data); err != nil {
			slog.Warn("driver_bridge", "msg", "publish failed", "type", envelope.Type, "err", err)
		} else {
			slog.Debug("driver_bridge", "msg", "published from driver", "type", envelope.Type, "peer_id", dc.peerId, "size", len(data))
		}

		// CRITICAL: Cross-bridge relay. sub.Next() filters self-published messages,
		// so we must forward directly to the other bridge (rider bridge).
		if b.onLocalMsg != nil {
			b.onLocalMsg(data)
		}
	}
	close(done)
	slog.Debug("driver_bridge", "msg", "read loop exited", "peer_id", dc.peerId)
}

// Forward sends a Gossipsub message to ALL connected drivers.
// If the message has a "to" or "target_peer_id" field, it is sent only to that peer (and queued if offline).
// Called by main.go when messages arrive from native libp2p peers (via sub.Next()).
func (b *Bridge) Forward(data []byte) {
	var msgType string
	var targetPeerID string
	if len(data) > 0 {
		var envelope struct {
			Type          string `json:"type"`
			To            string `json:"to"`
			TargetPeerID  string `json:"target_peer_id"`
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
		}
	}

	if targetPeerID != "" {
		b.mu.RLock()
		dc := b.peerByID[targetPeerID]
		b.mu.RUnlock()
		if dc != nil {
			dc.mu.Lock()
			dc.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			err := dc.conn.WriteMessage(websocket.TextMessage, data)
			dc.mu.Unlock()
			if err == nil {
				slog.Debug("driver_bridge", "msg", "forward_targeted", "type", msgType, "target", targetPeerID, "size", len(data))
				return
			}
			slog.Warn("driver_bridge", "msg", "forward_targeted write failed", "target", targetPeerID, "err", err)
			b.mu.Lock()
			delete(b.drivers, dc.conn)
			delete(b.peerByID, dc.peerId)
			b.mu.Unlock()
			_ = dc.conn.Close()
		}
		b.peerQueue.Enqueue(targetPeerID, data)
		slog.Debug("driver_bridge", "msg", "forward_targeted queued (peer not connected)", "type", msgType, "target", targetPeerID)
		return
	}

	b.mu.RLock()
	drivers := make([]*driverConn, 0, len(b.drivers))
	for _, dc := range b.drivers {
		drivers = append(drivers, dc)
	}
	n := len(drivers)
	b.mu.RUnlock()

	slog.Debug("driver_bridge", "msg", "forward", "type", msgType, "size", len(data), "target_driver_count", n)

	for _, dc := range drivers {
		dc.mu.Lock()
		dc.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
		if err := dc.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			dc.mu.Unlock()
			b.mu.Lock()
			delete(b.drivers, dc.conn)
			if dc.peerId != "" {
				delete(b.peerByID, dc.peerId)
			}
			b.mu.Unlock()
			_ = dc.conn.Close()
			slog.Warn("driver_bridge", "msg", "forward write failed, removed driver", "peer_id", dc.peerId, "err", err)
			continue
		}
		dc.mu.Unlock()
	}
}

// broadcastPeerCount sends a peer_count control message to all connected drivers.
func (b *Bridge) broadcastPeerCount() {
	b.mu.RLock()
	count := len(b.drivers)
	drivers := make([]*driverConn, 0, count)
	for _, dc := range b.drivers {
		drivers = append(drivers, dc)
	}
	b.mu.RUnlock()

	msg, _ := json.Marshal(map[string]any{
		"type":       "peer_count",
		"peer_count": count,
	})

	var dead []*driverConn
	for _, dc := range drivers {
		dc.mu.Lock()
		dc.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
		if err := dc.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			dead = append(dead, dc)
		}
		dc.mu.Unlock()
	}

	// Clean up dead connections.
	if len(dead) > 0 {
		b.mu.Lock()
		for _, dc := range dead {
			delete(b.drivers, dc.conn)
			_ = dc.conn.Close()
		}
		b.mu.Unlock()
	}
}

// DriverCount returns the number of currently connected drivers.
func (b *Bridge) DriverCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.drivers)
}
