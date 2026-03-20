package auth

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ridechain/ridechain/services/bootstrap/internal/integrity"
)

// Handler serves /auth/device and /auth/refresh endpoints.
type Handler struct {
	issuer            *Issuer
	integrityVerifier *integrity.Verifier
}

// NewHandler creates a new auth handler.
func NewHandler(issuer *Issuer, integrityVerifier *integrity.Verifier) *Handler {
	return &Handler{issuer: issuer, integrityVerifier: integrityVerifier}
}

// DeviceAuth handles POST /auth/device — issues tokens for a new device.
// Requires a valid Play Integrity token on first auth.
type deviceAuthRequest struct {
	DeviceID       string `json:"deviceId"`
	IntegrityToken string `json:"integrityToken"`
	PeerID         string `json:"peerId"`
}

func (h *Handler) DeviceAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req deviceAuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	deviceID := strings.TrimSpace(req.DeviceID)
	if deviceID == "" {
		http.Error(w, "deviceId is required", http.StatusBadRequest)
		return
	}

	// Verify Play Integrity if available
	if req.IntegrityToken != "" && h.integrityVerifier != nil {
		verdict, err := h.integrityVerifier.Verify(r.Context(), req.IntegrityToken)
		if err != nil {
			slog.Warn("auth_integrity_verify_failed", "device_id", deviceID, "err", err)
			// Soft-fail: allow auth even if verification API errors
		} else if !verdict.MeetsBasicIntegrity() {
			slog.Warn("auth_integrity_failed", "device_id", deviceID, "labels", verdict.DeviceIntegrity)
			http.Error(w, "device integrity check failed", http.StatusForbidden)
			return
		}
	}

	pair, err := h.issuer.Issue(deviceID, strings.TrimSpace(req.PeerID))
	if err != nil {
		slog.Error("auth_issue_failed", "device_id", deviceID, "err", err)
		http.Error(w, "token generation failed", http.StatusInternalServerError)
		return
	}

	slog.Info("auth_device_issued", "device_id", deviceID[:min(16, len(deviceID))]+"...")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pair)
}

// RefreshAuth handles POST /auth/refresh — issues a new access token using a refresh token.
type refreshRequest struct {
	RefreshToken string `json:"refreshToken"`
	PeerID       string `json:"peerId"`
}

func (h *Handler) RefreshAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.RefreshToken == "" {
		http.Error(w, "refreshToken is required", http.StatusBadRequest)
		return
	}

	pair, err := h.issuer.Refresh(req.RefreshToken, strings.TrimSpace(req.PeerID))
	if err != nil {
		slog.Debug("auth_refresh_failed", "err", err)
		http.Error(w, "invalid or expired refresh token", http.StatusUnauthorized)
		return
	}

	slog.Debug("auth_refreshed", "device_id", pair.AccessToken[:8]+"...")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pair)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
