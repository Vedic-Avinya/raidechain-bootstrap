package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ridechain/ridechain/services/bootstrap/internal/fcm"
	"github.com/ridechain/ridechain/services/bootstrap/internal/redis"
)

// TrackAPI handles live tracking session endpoints.
type TrackAPI struct {
	store      *redis.Store
	pushSender fcm.PushSender
}

// NewTrackAPI creates the track API handler.
func NewTrackAPI(store *redis.Store, pushSender fcm.PushSender) *TrackAPI {
	return &TrackAPI{store: store, pushSender: pushSender}
}

// --- POST /track/sessions ---

type createSessionReq struct {
	PeerID             string `json:"peerId"`
	DurationMinutes    int    `json:"durationMinutes"`
	DirectInvitePeerID string `json:"directInvitePeerId,omitempty"`
}

type createSessionResp struct {
	SessionID       string `json:"sessionId"`
	JoinURL         string `json:"joinUrl"`
	ExpiresAt       int64  `json:"expiresAt"`
	DurationMinutes int    `json:"durationMinutes"`
}

func (t *TrackAPI) CreateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var req createSessionReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	if req.DurationMinutes <= 0 {
		req.DurationMinutes = 15
	}
	if req.DurationMinutes > 120 {
		req.DurationMinutes = 120
	}

	directInvite := strings.TrimSpace(req.DirectInvitePeerID)

	// One non-expired session per owner: reject creating another until the current one ends or expires.
	existing, err := t.store.GetOwnerActiveTrackSession(r.Context(), req.PeerID)
	if err != nil {
		slog.Error("track_create_check_existing_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if existing != nil && time.Now().Unix() <= existing.ExpiresAt {
		slog.Info("track_create_rejected_active_exists",
			"owner", req.PeerID,
			"existing_session_id", existing.SessionID,
			"expires_at", existing.ExpiresAt,
			"requested_direct_invite_len", len(directInvite),
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":       "active_session_exists",
			"sessionId":   existing.SessionID,
			"expiresAt":   existing.ExpiresAt,
			"durationMin": existing.DurationMinutes,
		})
		return
	}

	session, err := t.store.CreateTrackSession(r.Context(), req.PeerID, req.DurationMinutes, directInvite)
	if err != nil {
		slog.Error("track_create_session_failed", "peer_id", req.PeerID, "err", err)
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}
	sessionID := session.SessionID
	if directInvite != "" {
		if err := t.store.PatchTrackSessionDirectInvite(r.Context(), sessionID, directInvite); err != nil {
			slog.Warn("track_patch_direct_invite_failed", "session_id", sessionID, "err", err)
		}
	}

	joinURL := "https://www.ridechain.in/track/join/" + sessionID
	slog.Info("track_session_created",
		"session_id", sessionID,
		"owner", req.PeerID,
		"duration", req.DurationMinutes,
		"direct_invite_len", len(directInvite),
		"direct_invite_suffix", trimPeerSuffix(directInvite),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(createSessionResp{
		SessionID:       sessionID,
		JoinURL:         joinURL,
		ExpiresAt:       session.ExpiresAt,
		DurationMinutes: session.DurationMinutes,
	})
}

// GetMySession handles GET /track/sessions/me?peerId=...
// Returns the owner's single active track session, if any.
func (t *TrackAPI) GetMySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ownerPeerID := strings.TrimSpace(r.URL.Query().Get("peerId"))
	if ownerPeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	session, err := t.store.GetOwnerActiveTrackSession(r.Context(), ownerPeerID)
	if err != nil {
		slog.Error("track_get_my_session_failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if session == nil {
		http.Error(w, "no active session", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

// --- GET /track/sessions/{id} ---

func (t *TrackAPI) GetSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractPathParam(r.URL.Path, "/track/sessions/")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}
	// Strip any sub-path (e.g. /request, /respond, /location)
	if i := strings.Index(sessionID, "/"); i >= 0 {
		sessionID = sessionID[:i]
	}

	session, err := t.store.GetTrackSession(r.Context(), sessionID)
	if err != nil {
		slog.Error("track_get_session_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if session == nil {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

// --- POST /track/sessions/{id}/request ---

type trackRequestReq struct {
	WatcherPeerID string `json:"watcherPeerId"`
	WatcherName   string `json:"watcherName"`
	WatcherCity   string `json:"watcherCity"`
}

func (t *TrackAPI) RequestToTrack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/request")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var req trackRequestReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.WatcherPeerID = strings.TrimSpace(req.WatcherPeerID)
	if req.WatcherPeerID == "" {
		http.Error(w, "watcherPeerId required", http.StatusBadRequest)
		return
	}

	session, err := t.store.GetTrackSession(r.Context(), sessionID)
	if err != nil || session == nil {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}
	if time.Now().Unix() > session.ExpiresAt {
		http.Error(w, "session expired", http.StatusGone)
		return
	}

	ttl := time.Duration(session.DurationMinutes) * time.Minute
	watcherName := strings.TrimSpace(req.WatcherName)
	if watcherName == "" {
		watcherName = "Someone"
	}

	invite := strings.TrimSpace(session.DirectInvitePeerID)
	watcher := strings.TrimSpace(req.WatcherPeerID)
	autoApprove := invite != "" && watcher == invite

	slog.Info("track_request_received",
		"session_id", sessionID,
		"watcher_peer_id_len", len(watcher),
		"watcher_suffix", trimPeerSuffix(watcher),
		"direct_invite_suffix", trimPeerSuffix(invite),
		"auto_approve", autoApprove,
	)

	var watcherStatus redis.WatcherStatus
	if autoApprove {
		watcherStatus = redis.WatcherApproved
	} else {
		watcherStatus = redis.WatcherRequested
	}

	if err := t.store.SetWatcherStatus(r.Context(), sessionID, req.WatcherPeerID, watcherStatus, ttl); err != nil {
		slog.Error("track_set_watcher_failed", "session_id", sessionID, "watcher", req.WatcherPeerID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := t.store.SetWatcherRequestMeta(r.Context(), sessionID, req.WatcherPeerID, watcherName, req.WatcherCity, ttl); err != nil {
		slog.Warn("track_set_watcher_meta_failed", "session_id", sessionID, "watcher", req.WatcherPeerID, "err", err)
	}

	// Direct invite (1:1 chat share): auto-approve — no owner approval notification.
	if autoApprove {
		t.sendWatcherApprovedFCM(r.Context(), sessionID, req.WatcherPeerID)
		slog.Info("track_request_auto_approved", "session_id", sessionID, "watcher", req.WatcherPeerID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "approved"})
		return
	}

	// Send FCM push to the session owner (approval required)
	ownerMeta, err := t.store.GetPeer(r.Context(), session.OwnerPeerID)
	if err != nil {
		slog.Warn("track_request_get_owner_failed", "owner", session.OwnerPeerID, "err", err)
	}
	if ownerMeta != nil && ownerMeta.FCMToken != "" {
		pushData := map[string]string{
			"type":            "track_request",
			"session_id":      sessionID,
			"watcher_peer_id": req.WatcherPeerID,
			"watcher_name":    watcherName,
			"watcher_city":    strings.TrimSpace(req.WatcherCity),
			"title":           "Location Request",
			"body":            watcherName + " wants to see your live location",
		}
		if err := t.pushSender.SendDataOnly(r.Context(), ownerMeta.FCMToken, pushData); err != nil {
			slog.Warn("track_request_fcm_failed", "session_id", sessionID, "owner", session.OwnerPeerID, "err", err)
		} else {
			slog.Info("track_request_fcm_sent", "session_id", sessionID, "owner", session.OwnerPeerID, "watcher", req.WatcherPeerID)
		}
	} else {
		slog.Warn("track_request_no_fcm", "session_id", sessionID, "owner", session.OwnerPeerID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "requested"})
}

// --- POST /track/sessions/{id}/respond ---

type trackRespondReq struct {
	OwnerPeerID string `json:"ownerPeerId"`
	WatcherPeerID string `json:"watcherPeerId"`
	Action      string `json:"action"` // "approve" or "reject"
}

func (t *TrackAPI) RespondToRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/respond")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2048)
	var req trackRespondReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.OwnerPeerID = strings.TrimSpace(req.OwnerPeerID)
	req.WatcherPeerID = strings.TrimSpace(req.WatcherPeerID)
	req.Action = strings.TrimSpace(strings.ToLower(req.Action))

	if req.OwnerPeerID == "" || req.WatcherPeerID == "" {
		http.Error(w, "ownerPeerId and watcherPeerId required", http.StatusBadRequest)
		return
	}
	if req.Action != "approve" && req.Action != "reject" {
		http.Error(w, "action must be 'approve' or 'reject'", http.StatusBadRequest)
		return
	}

	session, err := t.store.GetTrackSession(r.Context(), sessionID)
	if err != nil || session == nil {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}
	// Verify the responder is the session owner
	if session.OwnerPeerID != req.OwnerPeerID {
		http.Error(w, "unauthorized", http.StatusForbidden)
		return
	}

	var status redis.WatcherStatus
	if req.Action == "approve" {
		status = redis.WatcherApproved
	} else {
		status = redis.WatcherRejected
	}

	ttl := time.Duration(session.DurationMinutes) * time.Minute
	if err := t.store.SetWatcherStatus(r.Context(), sessionID, req.WatcherPeerID, status, ttl); err != nil {
		slog.Error("track_respond_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Send FCM push to watcher about the decision
	watcherMeta, err := t.store.GetPeer(r.Context(), req.WatcherPeerID)
	if err != nil {
		slog.Warn("track_respond_get_watcher_failed", "watcher", req.WatcherPeerID, "err", err)
	}
	if watcherMeta != nil && watcherMeta.FCMToken != "" {
		var title, body string
		if req.Action == "approve" {
			title = "Request Approved"
			body = "Your tracking request has been approved. You can now see their live location."
		} else {
			title = "Request Declined"
			body = "Your tracking request was declined."
		}
		pushData := map[string]string{
			"type":       "track_" + req.Action + "d",
			"session_id": sessionID,
			"title":      title,
			"body":       body,
		}
		if err := t.pushSender.SendDataOnly(r.Context(), watcherMeta.FCMToken, pushData); err != nil {
			slog.Warn("track_respond_fcm_failed", "session_id", sessionID, "watcher", req.WatcherPeerID, "err", err)
		} else {
			slog.Info("track_respond_fcm_sent", "session_id", sessionID, "watcher", req.WatcherPeerID, "action", req.Action)
		}
	}

	slog.Info("track_respond", "session_id", sessionID, "action", req.Action, "watcher", req.WatcherPeerID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": string(status)})
}

// --- POST /track/sessions/{id}/location ---

type publishLocationReq struct {
	PeerID string  `json:"peerId"`
	Lat    float64 `json:"lat"`
	Lng    float64 `json:"lng"`
}

func (t *TrackAPI) PublishLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/location")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req publishLocationReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.PeerID = strings.TrimSpace(req.PeerID)
	if req.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}

	session, err := t.store.GetTrackSession(r.Context(), sessionID)
	if err != nil || session == nil {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}
	if session.OwnerPeerID != req.PeerID {
		http.Error(w, "unauthorized: only session owner can publish", http.StatusForbidden)
		return
	}
	if time.Now().Unix() > session.ExpiresAt {
		http.Error(w, "session expired", http.StatusGone)
		return
	}

	ttl := time.Until(time.Unix(session.ExpiresAt, 0))
	if ttl <= 0 {
		http.Error(w, "session expired", http.StatusGone)
		return
	}
	if err := t.store.PublishTrackLocation(r.Context(), sessionID, req.Lat, req.Lng, ttl); err != nil {
		slog.Error("track_publish_location_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// --- GET /track/sessions/{id}/location ---

func (t *TrackAPI) GetLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/location")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}

	watcherPeerID := strings.TrimSpace(r.URL.Query().Get("watcherPeerId"))
	if watcherPeerID == "" {
		http.Error(w, "watcherPeerId query param required", http.StatusBadRequest)
		return
	}

	// Verify watcher is approved
	status, err := t.store.GetWatcherStatus(r.Context(), sessionID, watcherPeerID)
	if err != nil {
		slog.Error("track_get_location_status_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if status != redis.WatcherApproved {
		http.Error(w, "not approved", http.StatusForbidden)
		return
	}

	loc, err := t.store.GetTrackLocation(r.Context(), sessionID)
	if err != nil {
		slog.Error("track_get_location_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if loc == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "waiting", "lat": 0, "lng": 0, "timestamp": 0})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":    "ok",
		"lat":       loc.Lat,
		"lng":       loc.Lng,
		"timestamp": loc.Timestamp,
	})
}

// --- GET /track/sessions/{id}/status?watcherPeerId=... ---

func (t *TrackAPI) GetWatcherStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/status")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}
	watcherPeerID := strings.TrimSpace(r.URL.Query().Get("watcherPeerId"))
	if watcherPeerID == "" {
		http.Error(w, "watcherPeerId required", http.StatusBadRequest)
		return
	}

	status, err := t.store.GetWatcherStatus(r.Context(), sessionID, watcherPeerID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if status == "" {
		status = "unknown"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": string(status)})
}

// ListPendingRequests handles GET /track/sessions/{id}/pending-requests?ownerPeerId=...
func (t *TrackAPI) ListPendingRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/pending-requests")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}
	ownerPeerID := strings.TrimSpace(r.URL.Query().Get("ownerPeerId"))
	if ownerPeerID == "" {
		http.Error(w, "ownerPeerId required", http.StatusBadRequest)
		return
	}
	session, err := t.store.GetTrackSession(r.Context(), sessionID)
	if err != nil || session == nil {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}
	if time.Now().Unix() > session.ExpiresAt {
		http.Error(w, "session expired", http.StatusGone)
		return
	}
	if session.OwnerPeerID != ownerPeerID {
		http.Error(w, "unauthorized", http.StatusForbidden)
		return
	}
	list, err := t.store.ListPendingTrackRequests(r.Context(), sessionID)
	if err != nil {
		slog.Error("track_list_pending_failed", "session_id", sessionID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Always JSON array, never null (clients deserialize into List).
	if list == nil {
		list = []redis.PendingTrackRequest{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"sessionId": sessionID,
		"requests":  list,
	})
}

// EndSession handles POST /track/sessions/{id}/end — owner stops sharing early; invalidates server-side session.
func (t *TrackAPI) EndSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessionID := extractTrackSubPath(r.URL.Path, "/end")
	if sessionID == "" {
		http.Error(w, "sessionId required", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req struct {
		OwnerPeerID string `json:"ownerPeerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	req.OwnerPeerID = strings.TrimSpace(req.OwnerPeerID)
	if req.OwnerPeerID == "" {
		http.Error(w, "ownerPeerId required", http.StatusBadRequest)
		return
	}
	if err := t.store.EndTrackSession(r.Context(), req.OwnerPeerID, sessionID); err != nil {
		slog.Warn("track_end_session_failed", "session_id", sessionID, "owner", req.OwnerPeerID, "err", err)
		msg := err.Error()
		code := http.StatusBadRequest
		if strings.Contains(msg, "not session owner") {
			code = http.StatusForbidden
		} else if strings.Contains(msg, "not found") {
			code = http.StatusNotFound
		}
		http.Error(w, msg, code)
		return
	}
	slog.Info("track_session_ended_by_owner", "session_id", sessionID, "owner", req.OwnerPeerID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ended"})
}

// RouteSession dispatches /track/sessions/{id}[/sub] to the correct handler.
func (t *TrackAPI) RouteSession(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/end"):
		t.EndSession(w, r)
	case strings.HasSuffix(path, "/request"):
		t.RequestToTrack(w, r)
	case strings.HasSuffix(path, "/respond"):
		t.RespondToRequest(w, r)
	case strings.HasSuffix(path, "/pending-requests"):
		t.ListPendingRequests(w, r)
	case strings.HasSuffix(path, "/location") && r.Method == http.MethodPost:
		t.PublishLocation(w, r)
	case strings.HasSuffix(path, "/location") && r.Method == http.MethodGet:
		t.GetLocation(w, r)
	case strings.HasSuffix(path, "/status"):
		t.GetWatcherStatus(w, r)
	default:
		// GET /track/sessions/{id}
		t.GetSession(w, r)
	}
}

func (t *TrackAPI) sendWatcherApprovedFCM(ctx context.Context, sessionID, watcherPeerID string) {
	watcherMeta, err := t.store.GetPeer(ctx, watcherPeerID)
	if err != nil {
		slog.Warn("track_approved_get_watcher_failed", "watcher", watcherPeerID, "err", err)
	}
	if watcherMeta != nil && watcherMeta.FCMToken != "" {
		pushData := map[string]string{
			"type":       "track_approved",
			"session_id": sessionID,
			"title":      "Request Approved",
			"body":       "Your tracking request has been approved. You can now see their live location.",
		}
		if err := t.pushSender.SendDataOnly(ctx, watcherMeta.FCMToken, pushData); err != nil {
			slog.Warn("track_approved_fcm_failed", "session_id", sessionID, "watcher", watcherPeerID, "err", err)
		} else {
			slog.Info("track_approved_fcm_sent", "session_id", sessionID, "watcher", watcherPeerID)
		}
	}
}

// --- Helpers ---

func extractPathParam(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(path, prefix))
}

func trimPeerSuffix(peerID string) string {
	s := strings.TrimSpace(peerID)
	if len(s) <= 12 {
		return s
	}
	return "…" + s[len(s)-12:]
}

// extractTrackSubPath extracts sessionID from paths like /track/sessions/{id}/request
func extractTrackSubPath(path, suffix string) string {
	const prefix = "/track/sessions/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	if !strings.HasSuffix(rest, suffix) {
		return ""
	}
	sessionID := strings.TrimSuffix(rest, suffix)
	sessionID = strings.TrimRight(sessionID, "/")
	return strings.TrimSpace(sessionID)
}
