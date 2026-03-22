package api

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ridechain/ridechain/services/bootstrap/internal/auth"
)

const defaultStunURL = "stun:stun.l.google.com:19302"

// TurnCredentials handles GET /webrtc/turn.
//
// Returns a WebRTC iceServers array. When TURN_SHARED_SECRET and TURN_URIS are set,
// ephemeral TURN username/password are minted with the coturn "REST API" / shared-secret
// scheme (HMAC-SHA1 only — no DB). The username embeds Unix expiry and the caller's
// peer id from the JWT (pid claim, else device subject).
func (h *HTTPServer) TurnCredentials(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := auth.ClaimsFromContext(r.Context())
	if claims == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	uid := strings.TrimSpace(claims.PeerID)
	if uid == "" {
		uid = strings.TrimSpace(claims.DeviceID)
	}
	if uid == "" {
		http.Error(w, "token missing identity", http.StatusBadRequest)
		return
	}

	secret := strings.TrimSpace(os.Getenv("TURN_SHARED_SECRET"))
	turnURIs := splitCommaEnv("TURN_URIS")
	ttlSec, _ := strconv.ParseInt(envDefault("TURN_CREDENTIAL_TTL_SEC", "86400"), 10, 64)
	if ttlSec < 300 {
		ttlSec = 300
	}
	if ttlSec > 86400*7 {
		ttlSec = 86400 * 7
	}

	type iceSrv struct {
		URLs       interface{} `json:"urls"` // string or []string for WebRTC clients
		Username   string      `json:"username,omitempty"`
		Credential string      `json:"credential,omitempty"`
	}

	servers := []iceSrv{{URLs: defaultStunURL}}

	if secret != "" && len(turnURIs) > 0 {
		expiry := time.Now().Unix() + ttlSec
		username := fmt.Sprintf("%d:%s", expiry, uid)
		mac := hmac.New(sha1.New, []byte(secret))
		_, _ = mac.Write([]byte(username))
		credential := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		for _, raw := range turnURIs {
			u := strings.TrimSpace(raw)
			if u == "" {
				continue
			}
			servers = append(servers, iceSrv{
				URLs:       u,
				Username:   username,
				Credential: credential,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ttlSeconds": ttlSec,
		"iceServers": servers,
	})
}

func envDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCommaEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
