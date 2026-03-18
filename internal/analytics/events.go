// Package analytics sends server-side events to Firebase / Google Analytics 4
// via the GA4 Measurement Protocol.
//
// Required env vars:
//   FIREBASE_MEASUREMENT_ID   — e.g. G-XXXXXXXXXX
//   FIREBASE_ANALYTICS_SECRET — API secret from GA4 property settings
//
// If either var is unset the client is nil and all Send() calls are no-ops.
// Fire-and-forget: all sends run in a goroutine, so the hot path is never blocked.
package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const ga4Endpoint = "https://www.google-analytics.com/mp/collect"

// Client sends GA4 Measurement Protocol events.
type Client struct {
	measurementID string
	apiSecret     string
	http          *http.Client
}

// Event is a single GA4 event with optional parameters.
type Event struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
}

// New returns a Client if FIREBASE_MEASUREMENT_ID and FIREBASE_ANALYTICS_SECRET
// are set, otherwise returns nil (all operations become no-ops).
func New() *Client {
	mid := os.Getenv("FIREBASE_MEASUREMENT_ID")
	secret := os.Getenv("FIREBASE_ANALYTICS_SECRET")
	if mid == "" || secret == "" {
		slog.Info("analytics", "msg", "disabled (FIREBASE_MEASUREMENT_ID or FIREBASE_ANALYTICS_SECRET not set)")
		return nil
	}
	return &Client{
		measurementID: mid,
		apiSecret:     secret,
		http:          &http.Client{Timeout: 3 * time.Second},
	}
}

// Send fires events asynchronously.  clientID should be the peer_id or stable device ID.
func (c *Client) Send(ctx context.Context, clientID string, events ...Event) {
	if c == nil || len(events) == 0 {
		return
	}
	go c.send(ctx, clientID, events)
}

func (c *Client) send(ctx context.Context, clientID string, events []Event) {
	payload := map[string]any{
		"client_id":            clientID,
		"timestamp_micros":     time.Now().UnixMicro(),
		"non_personalized_ads": true,
		"events":               events,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	url := fmt.Sprintf("%s?measurement_id=%s&api_secret=%s", ga4Endpoint, c.measurementID, c.apiSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Debug("analytics", "msg", "send failed", "err", err)
		return
	}
	resp.Body.Close()
}

// ── Event constructors ───────────────────────────────────────────────────────

// PeerConnect fires when a rider establishes a WebSocket connection.
func PeerConnect(peerID, city string, totalRiders int) Event {
	return Event{Name: "peer_connect", Params: map[string]any{
		"peer_id": peerID, "city": city, "total_riders": totalRiders,
	}}
}

// PeerDisconnect fires when a rider disconnects.
func PeerDisconnect(peerID, city string, sessionSec int64) Event {
	return Event{Name: "peer_disconnect", Params: map[string]any{
		"peer_id": peerID, "city": city, "session_seconds": sessionSec,
	}}
}

// MessageSent fires when a rider publishes a message through the bridge.
func MessageSent(peerID, msgType, city string, sizeBytes int) Event {
	return Event{Name: "message_sent", Params: map[string]any{
		"peer_id": peerID, "msg_type": msgType, "city": city, "size_bytes": sizeBytes,
	}}
}

// NearbyBroadcast fires when a rider publishes to a geohash-cell topic.
func NearbyBroadcast(peerID, geoCell string) Event {
	return Event{Name: "nearby_broadcast", Params: map[string]any{
		"peer_id": peerID, "geo_cell": geoCell,
	}}
}

// LocationUpdate fires when a rider updates their lat/lng.
func LocationUpdate(peerID, geoCell string) Event {
	return Event{Name: "location_update", Params: map[string]any{
		"peer_id": peerID, "geo_cell": geoCell,
	}}
}

// OfflinePush fires after an FCM wake-up push attempt.
func OfflinePush(peerID string, success bool) Event {
	result := "success"
	if !success {
		result = "failure"
	}
	return Event{Name: "offline_push", Params: map[string]any{
		"peer_id": peerID, "result": result,
	}}
}

// PeerRegistered fires when POST /register succeeds.
func PeerRegistered(peerID string) Event {
	return Event{Name: "peer_registered", Params: map[string]any{"peer_id": peerID}}
}

// RateLimitHit fires when a rider's message is dropped for rate limiting.
func RateLimitHit(peerID string) Event {
	return Event{Name: "rate_limit_hit", Params: map[string]any{"peer_id": peerID}}
}
