package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ridechain/ridechain/services/bootstrap/internal/analytics"
	appmetrics "github.com/ridechain/ridechain/services/bootstrap/internal/metrics"
	"github.com/ridechain/ridechain/services/bootstrap/internal/persist"
	"github.com/ridechain/ridechain/services/bootstrap/internal/redis"
)

// GeoUpdater is implemented by riderbridge.Bridge. Allows the API server to update
// a rider's geohash cell subscriptions when their location changes.
type GeoUpdater interface {
	UpdateRiderGeohash(peerID string, lat, lng float64)
}

// PushSender sends FCM push notifications (imported from fcm package interface).
type PushSender interface {
	Send(ctx context.Context, token string, data map[string]string) error
	SendDataOnly(ctx context.Context, token string, data map[string]string) error
}

// HTTPServer serves REST API for peer registration, search, and discover.
type HTTPServer struct {
	store              *redis.Store
	persistStore       *persist.Store
	geoUpdater         GeoUpdater
	analyticsClient    *analytics.Client
	pushSender         PushSender
}

// NewHTTPServer creates the HTTP API server.
func NewHTTPServer(store *redis.Store) *HTTPServer {
	return &HTTPServer{store: store}
}

// SetPushSender wires FCM for notify-peer endpoint.
func (h *HTTPServer) SetPushSender(ps PushSender) { h.pushSender = ps }

// SetGeoUpdater wires the rider bridge so that PUT /register/lat-lng also
// updates the rider's geohash-cell GossipSub subscriptions.
func (h *HTTPServer) SetGeoUpdater(u GeoUpdater) { h.geoUpdater = u }

// SetAnalyticsClient wires server-side Firebase Analytics.
func (h *HTTPServer) SetAnalyticsClient(c *analytics.Client) { h.analyticsClient = c }

// SetPersistStore wires the SQLite persistent store for permanent user records.
func (h *HTTPServer) SetPersistStore(ps *persist.Store) { h.persistStore = ps }


// RegisterRequest is the body for POST /register.
type RegisterRequest struct {
	PeerID         string   `json:"peerId"`
	DeviceID       string   `json:"deviceId,omitempty"`
	DisplayName    string   `json:"displayName"`
	Geohash        string   `json:"geohash"`
	Lat            *float64 `json:"lat,omitempty"`
	Lng            *float64 `json:"lng,omitempty"`
	FCMToken       string   `json:"fcmToken,omitempty"`
}

// Register handles POST /register.
func (h *HTTPServer) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	// Reject fake Kotlin-generated peer IDs (30 chars). Real libp2p IDs are 52 chars starting with "12D3KooW".
	if len(req.PeerID) < 46 || !strings.HasPrefix(req.PeerID, "12D3KooW") {
		slog.Warn("register_rejected_fake_peer_id", "peer_id", req.PeerID, "len", len(req.PeerID))
		http.Error(w, "invalid peerId: must be a real libp2p peer ID (52 chars, 12D3KooW prefix)", http.StatusBadRequest)
		return
	}
	fcmToken := strings.TrimSpace(req.FCMToken)
	// If the client omits fcmToken (e.g. geo-only refresh), do not wipe an existing token — otherwise
	// track_request / chat push to this peer silently fails (GetPeer returns empty FCMToken).
	if fcmToken == "" {
		if existing, _ := h.store.GetPeer(r.Context(), req.PeerID); existing != nil && existing.FCMToken != "" {
			fcmToken = existing.FCMToken
		}
	}

	// ── Registration (JWT middleware already verified device identity) ─
	if err := h.store.Register(r.Context(), req.PeerID, req.DisplayName, req.Geohash, req.Lat, req.Lng, fcmToken); err != nil {
		slog.Error("register_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "registration failed", http.StatusInternalServerError)
		return
	}
	appmetrics.PeerRegistrations.Inc()
	// Persist to SQLite (permanent record — survives Redis TTL expiry)
	if h.persistStore != nil {
		var lat, lng float64
		if req.Lat != nil { lat = *req.Lat }
		if req.Lng != nil { lng = *req.Lng }
		if err := h.persistStore.Upsert(r.Context(), persist.UserRecord{
			PeerID:      req.PeerID,
			DeviceID:    strings.TrimSpace(req.DeviceID),
			DisplayName: req.DisplayName,
			Lat:         lat,
			Lng:         lng,
		}); err != nil {
			slog.Warn("persist_upsert_failed", "peer_id", req.PeerID, "err", err)
		}
	}
	if h.analyticsClient != nil {
		h.analyticsClient.Send(r.Context(), req.PeerID, analytics.PeerRegistered(req.PeerID))
	}
	if fcmToken != "" {
		slog.Info("register_fcm_saved", "peer_id", req.PeerID, "token_len", len(fcmToken))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "peerId": req.PeerID})
}

// Heartbeat handles POST /heartbeat — lightweight keep-alive that only updates UpdatedAt.
// Clients should call this every ~3 minutes while the app is in the foreground.
func (h *HTTPServer) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 256)
	var req struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" || len(req.PeerID) < 46 || !strings.HasPrefix(req.PeerID, "12D3KooW") {
		http.Error(w, "invalid peerId", http.StatusBadRequest)
		return
	}
	if err := h.store.TouchPeer(r.Context(), req.PeerID); err != nil {
		slog.Warn("heartbeat_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "heartbeat failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

// Stats handles GET /stats — returns total users, active users, cities from persistent DB.
func (h *HTTPServer) Stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.persistStore == nil {
		http.Error(w, "persistent store not configured", http.StatusServiceUnavailable)
		return
	}
	stats, err := h.persistStore.Stats(r.Context())
	if err != nil {
		slog.Error("stats_failed", "err", err)
		http.Error(w, "stats failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// RecoverPeer handles GET /recover-peer?device_id=... — returns the old peerId for a reinstalled app.
func (h *HTTPServer) RecoverPeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.persistStore == nil {
		http.Error(w, "persistent store not configured", http.StatusServiceUnavailable)
		return
	}
	deviceID := strings.TrimSpace(r.URL.Query().Get("device_id"))
	if deviceID == "" {
		http.Error(w, "device_id query required", http.StatusBadRequest)
		return
	}
	user, err := h.persistStore.GetByDeviceID(r.Context(), deviceID)
	if err != nil {
		slog.Error("recover_peer_failed", "device_id", deviceID, "err", err)
		http.Error(w, "recover failed", http.StatusInternalServerError)
		return
	}
	if user == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"found":false}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"found":       true,
		"peerId":      user.PeerID,
		"displayName": user.DisplayName,
		"city":        user.City,
		"createdAt":   user.CreatedAt,
	})
}

// SearchByName handles GET /search-by-name?name=...
// Returns peers with peerId, displayName, optional lat/lng, and lastActive (UpdatedAt unix seconds).
func (h *HTTPServer) SearchByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		http.Error(w, "name query required", http.StatusBadRequest)
		return
	}
	peerIDs, err := h.store.SearchByName(r.Context(), name)
	if err != nil {
		slog.Error("search_by_name_failed", "name", name, "err", err)
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}
	if len(peerIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"peers": []any{}})
		return
	}
	metas, err := h.store.GetPeerMetas(r.Context(), peerIDs)
	if err != nil {
		slog.Error("search_by_name_metas_failed", "err", err)
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}
	type peerResult struct {
		PeerID      string  `json:"peerId"`
		DisplayName string  `json:"displayName"`
		Lat         float64 `json:"lat,omitempty"`
		Lng         float64 `json:"lng,omitempty"`
		LastActive  int64   `json:"lastActive,omitempty"`
	}
	peers := make([]peerResult, 0, len(peerIDs))
	for i, meta := range metas {
		if meta == nil {
			peers = append(peers, peerResult{PeerID: peerIDs[i], DisplayName: ""})
			continue
		}
		pr := peerResult{PeerID: meta.PeerID, DisplayName: meta.DisplayName}
		if meta.UpdatedAt > 0 {
			pr.LastActive = meta.UpdatedAt
		}
		if meta.Lat != 0 || meta.Lng != 0 {
			pr.Lat = meta.Lat
			pr.Lng = meta.Lng
		}
		peers = append(peers, pr)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"peers": peers})
}

// DiscoverRequest is the query for GET /discover.
// Either geohash or (lat,lng) must be set. radius_km defaults to 50.
func (h *HTTPServer) Discover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	geohashStr := strings.TrimSpace(q.Get("geohash"))
	var lat, lng *float64
	if s := q.Get("lat"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			lat = &f
		}
	}
	if s := q.Get("lng"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			lng = &f
		}
	}
	radiusKm := 50.0
	if s := q.Get("radius_km"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			radiusKm = f
		}
	}
	if geohashStr == "" && (lat == nil || lng == nil) {
		slog.Info("discover_no_geo", "msg", "no lat/lng or geohash — returning fallback (up to 500 peers)")
	}
	locs, err := h.store.Discover(r.Context(), geohashStr, lat, lng, radiusKm)
	if err != nil {
		slog.Error("discover_failed", "err", err)
		http.Error(w, "discover failed", http.StatusInternalServerError)
		return
	}
	// Build response: peerId, distance, district, displayName.
	district := ""
	if geohashStr != "" {
		district = redis.DistrictForGeohash(geohashStr)
	}
	// Fetch display names from Redis in a pipeline
	peerIDs := make([]string, len(locs))
	for i, loc := range locs {
		peerIDs[i] = loc.PeerID
	}
	metas, metaErr := h.store.GetPeerMetas(r.Context(), peerIDs)
	if metaErr != nil {
		slog.Warn("discover_meta_fetch", "err", metaErr)
		// Non-fatal — continue without display names
	}
	type peerResult struct {
		PeerID      string  `json:"peerId"`
		Distance    float64 `json:"distance_km,omitempty"`
		District    string  `json:"district,omitempty"`
		DisplayName string  `json:"displayName,omitempty"`
		LastActive  int64   `json:"lastActive,omitempty"`
	}
	now := time.Now().Unix()
	cutoff48h := now - 48*3600
	cutoff30d := now - 30*24*3600
	var recentPeers, olderPeers []peerResult
	for i, loc := range locs {
		p := peerResult{PeerID: loc.PeerID, District: district}
		if loc.Distance > 0 {
			p.Distance = loc.Distance
		}
		if metas != nil && i < len(metas) && metas[i] != nil {
			p.DisplayName = metas[i].DisplayName
			// Send as epoch millis so Android client (System.currentTimeMillis()) math works.
			p.LastActive = metas[i].UpdatedAt * 1000
		}
		// Skip peers older than 30 days
		updatedAt := int64(0)
		if metas != nil && i < len(metas) && metas[i] != nil {
			updatedAt = metas[i].UpdatedAt
		}
		if updatedAt >= cutoff48h {
			recentPeers = append(recentPeers, p)
		} else if updatedAt >= cutoff30d {
			olderPeers = append(olderPeers, p)
		}
		// Peers older than 30 days are excluded entirely
	}
	// Sort each tier by lastActive descending (most recent first)
	sortByLastActiveDesc := func(s []peerResult) {
		for i := 0; i < len(s); i++ {
			for j := i + 1; j < len(s); j++ {
				if s[j].LastActive > s[i].LastActive {
					s[i], s[j] = s[j], s[i]
				}
			}
		}
	}
	sortByLastActiveDesc(recentPeers)
	sortByLastActiveDesc(olderPeers)
	// Cap: first 100 online/recently-active, then rest up to 200 total
	if len(recentPeers) > 100 {
		recentPeers = recentPeers[:100]
	}
	results := append(recentPeers, olderPeers...)
	if len(results) > 200 {
		results = results[:200]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"peers": results})
}

// PutFCM handles PUT /register/fcm (body: {"peerId","fcmToken"}).
func (h *HTTPServer) PutFCM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req struct {
		PeerID   string `json:"peerId"`
		FCMToken string `json:"fcmToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	token := strings.TrimSpace(req.FCMToken)
	if err := h.store.SetFCMToken(r.Context(), req.PeerID, token); err != nil {
		slog.Error("set_fcm_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}
	slog.Info("fcm_token_saved", "peer_id", req.PeerID, "token_len", len(token))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// PutDisplayName handles PUT /register/display-name (body: {"peerId","displayName"}).
func (h *HTTPServer) PutDisplayName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req struct {
		PeerID      string `json:"peerId"`
		DisplayName string `json:"displayName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	if err := h.store.SetDisplayName(r.Context(), req.PeerID, strings.TrimSpace(req.DisplayName)); err != nil {
		slog.Error("set_display_name_failed", "peer_id", req.PeerID, "display_name", req.DisplayName, "error", err)
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "peerId": req.PeerID})
}


// PutLatLng handles PUT /register/lat-lng (body: {"peerId","lat","lng","city"}).
// City is optional; when sent, it is stored for topic routing (/ridechain/{city}/p2p/v1).
func (h *HTTPServer) PutLatLng(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req struct {
		PeerID string  `json:"peerId"`
		Lat    float64 `json:"lat"`
		Lng    float64 `json:"lng"`
		City   string  `json:"city,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	if err := h.store.SetLatLng(r.Context(), req.PeerID, req.Lat, req.Lng, strings.TrimSpace(req.City)); err != nil {
		slog.Error("set_lat_lng_failed", "peer_id", req.PeerID, "lat", req.Lat, "lng", req.Lng, "error", err)
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}
	// Persist city + lat/lng to SQLite
	if h.persistStore != nil {
		if err := h.persistStore.Upsert(r.Context(), persist.UserRecord{
			PeerID: req.PeerID,
			City:   strings.TrimSpace(req.City),
			Lat:    req.Lat,
			Lng:    req.Lng,
		}); err != nil {
			slog.Warn("persist_upsert_lat_lng_failed", "peer_id", req.PeerID, "err", err)
		}
	}
	// Update rider's geohash-cell GossipSub subscriptions (9-cell window).
	appmetrics.GeoLocationUpdates.Inc()
	if h.geoUpdater != nil {
		h.geoUpdater.UpdateRiderGeohash(req.PeerID, req.Lat, req.Lng)
	}
	if h.analyticsClient != nil {
		h.analyticsClient.Send(r.Context(), req.PeerID, analytics.LocationUpdate(req.PeerID, ""))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "peerId": req.PeerID})
}

// NotifyPeer handles POST /notify-peer.
// Called by the sender's device when native P2P delivery fails (no ACK within timeout).
// Looks up the target peer's FCM token and sends a wake-up push so they reconnect.
//
// P2P Push Notification Flow:
//  1. Sender sends message via native libp2p (direct IPv6 or relay)
//  2. Sender waits ~5s for delivery ACK from receiver
//  3. If no ACK → sender calls POST /notify-peer
//  4. Server looks up target's FCM token → sends push
//  5. Receiver gets FCM → app wakes → reconnects to P2P → receives message
func (h *HTTPServer) NotifyPeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var req struct {
		SenderPeerID string `json:"senderPeerId"`
		TargetPeerID string `json:"targetPeerId"`
		MessageType  string `json:"messageType"`
		Preview      string `json:"preview"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.SenderPeerID = strings.TrimSpace(req.SenderPeerID)
	req.TargetPeerID = strings.TrimSpace(req.TargetPeerID)
	req.MessageType = strings.TrimSpace(req.MessageType)
	if req.TargetPeerID == "" {
		http.Error(w, "targetPeerId required", http.StatusBadRequest)
		return
	}
	if h.pushSender == nil {
		slog.Warn("notify_peer", "msg", "no push sender configured")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "no_push_sender"})
		return
	}

	meta, err := h.store.GetPeer(r.Context(), req.TargetPeerID)
	if err != nil {
		slog.Error("notify_peer", "msg", "get peer failed", "target", req.TargetPeerID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if meta == nil || meta.FCMToken == "" {
		slog.Info("notify_peer", "msg", "no FCM token", "target", req.TargetPeerID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "no_token"})
		return
	}

	senderName := "Someone"
	if req.SenderPeerID != "" {
		if senderMeta, _ := h.store.GetPeer(r.Context(), req.SenderPeerID); senderMeta != nil && senderMeta.DisplayName != "" {
			senderName = senderMeta.DisplayName
		}
	}

	msgType := req.MessageType
	if msgType == "" {
		msgType = "message"
	}
	body := senderName + " sent you a " + msgType
	if req.Preview != "" && len(req.Preview) < 100 {
		body = senderName + ": " + req.Preview
	}

	pushData := map[string]string{
		"type":           "new_message",
		"sender_peer_id": req.SenderPeerID,
		"message_type":   msgType,
		"title":          senderName,
		"body":           body,
	}
	if err := h.pushSender.Send(r.Context(), meta.FCMToken, pushData); err != nil {
		slog.Warn("notify_peer", "msg", "push failed", "target", req.TargetPeerID, "err", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "push_failed"})
		return
	}
	slog.Info("notify_peer", "msg", "push sent", "target", req.TargetPeerID, "sender", req.SenderPeerID, "type", msgType)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "pushed"})
}

// RecentlyActivePeers handles GET /peers/active?lat=...&lng=...&radius_km=...&since_hours=...
// Returns peers sorted by most recent activity (updatedAt desc), filtered by location and recency.
func (h *HTTPServer) RecentlyActivePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	var lat, lng *float64
	if s := q.Get("lat"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			lat = &f
		}
	}
	if s := q.Get("lng"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			lng = &f
		}
	}
	radiusKm := 50.0
	if s := q.Get("radius_km"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			radiusKm = f
		}
	}
	sinceHours := 24.0
	if s := q.Get("since_hours"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			sinceHours = f
		}
	}

	geohash := strings.TrimSpace(q.Get("geohash"))
	locs, err := h.store.Discover(r.Context(), geohash, lat, lng, radiusKm)
	if err != nil {
		slog.Error("active_peers", "msg", "discover failed", "err", err)
		http.Error(w, "discover failed", http.StatusInternalServerError)
		return
	}
	if len(locs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"peers": []any{}})
		return
	}

	peerIDs := make([]string, len(locs))
	for i, loc := range locs {
		peerIDs[i] = loc.PeerID
	}
	metas, err := h.store.GetPeerMetas(r.Context(), peerIDs)
	if err != nil {
		slog.Warn("active_peers", "msg", "meta fetch failed", "err", err)
	}

	cutoff := time.Now().Add(-time.Duration(sinceHours * float64(time.Hour))).Unix()

	type activePeer struct {
		PeerID      string  `json:"peerId"`
		DisplayName string  `json:"displayName,omitempty"`
		Distance    float64 `json:"distance_km,omitempty"`
		LastActive  int64   `json:"lastActive"`
	}
	var results []activePeer
	for i, loc := range locs {
		if metas == nil || i >= len(metas) || metas[i] == nil {
			continue
		}
		m := metas[i]
		if m.UpdatedAt < cutoff {
			continue
		}
		results = append(results, activePeer{
			PeerID:      loc.PeerID,
			DisplayName: m.DisplayName,
			Distance:    loc.Distance,
			// Send as epoch millis so Android client (System.currentTimeMillis()) math works.
			LastActive:  m.UpdatedAt * 1000,
		})
	}

	// Sort by lastActive descending (most recent first)
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].LastActive > results[i].LastActive {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"peers": results})
}

// ReportPeer handles POST /report (body: {"reporterPeerId","reportedPeerId","reason"}).
// Stores the report in Redis and logs it for review.
func (h *HTTPServer) ReportPeer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req struct {
		ReporterPeerID string `json:"reporterPeerId"`
		ReportedPeerID string `json:"reportedPeerId"`
		Reason         string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.ReporterPeerID = strings.TrimSpace(req.ReporterPeerID)
	req.ReportedPeerID = strings.TrimSpace(req.ReportedPeerID)
	if req.ReportedPeerID == "" {
		http.Error(w, "reportedPeerId required", http.StatusBadRequest)
		return
	}
	if err := h.store.SaveReport(r.Context(), req.ReporterPeerID, req.ReportedPeerID, req.Reason); err != nil {
		slog.Error("report_save_failed", "reporter", req.ReporterPeerID, "reported", req.ReportedPeerID, "err", err)
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}
	slog.Info("peer_reported", "reporter", req.ReporterPeerID, "reported", req.ReportedPeerID, "reason_len", len(req.Reason))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
