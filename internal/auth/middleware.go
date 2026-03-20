package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
)

type contextKey string

const claimsKey contextKey = "device_claims"

// Middleware returns an HTTP middleware that validates JWT access tokens.
// Requests without a valid token get 401 Unauthorized.
// The /auth/device and /auth/refresh endpoints are excluded (they issue tokens).
func Middleware(issuer *Issuer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth endpoints themselves
			path := r.URL.Path
			if strings.HasPrefix(path, "/auth/") {
				next.ServeHTTP(w, r)
				return
			}
			// Skip health/metrics
			if path == "/metrics" || path == "/health" {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "missing authorization header", http.StatusUnauthorized)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				http.Error(w, "invalid authorization header", http.StatusUnauthorized)
				return
			}

			claims, err := issuer.ValidateAccess(parts[1])
			if err != nil {
				slog.Debug("jwt_auth_failed", "err", err, "path", path)
				http.Error(w, "invalid or expired token", http.StatusUnauthorized)
				return
			}

			// Attach claims to context for downstream handlers
			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClaimsFromContext extracts DeviceClaims from the request context.
// Returns nil if no claims are present (e.g. auth middleware was skipped).
func ClaimsFromContext(ctx context.Context) *DeviceClaims {
	claims, _ := ctx.Value(claimsKey).(*DeviceClaims)
	return claims
}
