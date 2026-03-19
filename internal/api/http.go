package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/ridechain/ridechain/services/bootstrap/internal/analytics"
	appmetrics "github.com/ridechain/ridechain/services/bootstrap/internal/metrics"
	"github.com/ridechain/ridechain/services/bootstrap/internal/redis"
)

// GeoUpdater is implemented by riderbridge.Bridge. Allows the API server to update
// a rider's geohash cell subscriptions when their location changes.
type GeoUpdater interface {
	UpdateRiderGeohash(peerID string, lat, lng float64)
}

// HTTPServer serves REST API for peer registration, search, and discover.
type HTTPServer struct {
	store           *redis.Store
	geoUpdater      GeoUpdater
	analyticsClient *analytics.Client
}

// NewHTTPServer creates the HTTP API server.
func NewHTTPServer(store *redis.Store) *HTTPServer {
	return &HTTPServer{store: store}
}

// SetGeoUpdater wires the rider bridge so that PUT /register/lat-lng also
// updates the rider's geohash-cell GossipSub subscriptions.
func (h *HTTPServer) SetGeoUpdater(u GeoUpdater) { h.geoUpdater = u }

// SetAnalyticsClient wires server-side Firebase Analytics.
func (h *HTTPServer) SetAnalyticsClient(c *analytics.Client) { h.analyticsClient = c }

// RegisterRequest is the body for POST /register.
type RegisterRequest struct {
	PeerID      string   `json:"peerId"`
	DisplayName string   `json:"displayName"`
	Geohash     string   `json:"geohash"`
	Lat         *float64 `json:"lat,omitempty"`
	Lng         *float64 `json:"lng,omitempty"`
	FCMToken    string   `json:"fcmToken,omitempty"`
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
	if err := h.store.Register(r.Context(), req.PeerID, req.DisplayName, req.Geohash, req.Lat, req.Lng, fcmToken); err != nil {
		slog.Error("register_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "registration failed", http.StatusInternalServerError)
		return
	}
	appmetrics.PeerRegistrations.Inc()
	if h.analyticsClient != nil {
		h.analyticsClient.Send(r.Context(), req.PeerID, analytics.PeerRegistered(req.PeerID))
	}
	if fcmToken != "" {
		slog.Info("register_fcm_saved", "peer_id", req.PeerID, "token_len", len(fcmToken))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "peerId": req.PeerID})
}

// SearchByName handles GET /search-by-name?name=...
// Returns peers with peerId and displayName (for UI display).
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
		PeerID      string `json:"peerId"`
		DisplayName string `json:"displayName"`
	}
	peers := make([]peerResult, 0, len(peerIDs))
	for i, meta := range metas {
		if meta == nil {
			peers = append(peers, peerResult{PeerID: peerIDs[i], DisplayName: ""})
		} else {
			peers = append(peers, peerResult{PeerID: meta.PeerID, DisplayName: meta.DisplayName})
		}
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
	// Build response: peerId, distance, district (from request geohash if provided).
	district := ""
	if geohashStr != "" {
		district = redis.DistrictForGeohash(geohashStr)
	}
	type peerResult struct {
		PeerID   string  `json:"peerId"`
		Distance float64 `json:"distance_km,omitempty"`
		District string `json:"district,omitempty"`
	}
	results := make([]peerResult, 0, len(locs))
	for _, loc := range locs {
		p := peerResult{PeerID: loc.PeerID, District: district}
		if loc.Distance > 0 {
			p.Distance = loc.Distance
		}
		results = append(results, p)
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
