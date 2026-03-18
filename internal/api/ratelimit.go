package api

// Per-IP token-bucket rate limiting middleware for the HTTP API server.
// Default: 100 requests/minute per IP, burst 20.
// Override via BOOTSTRAP_API_RATE_LIMIT (requests/min) and BOOTSTRAP_API_BURST.

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultAPIRateLimit = 100 // requests per minute
	defaultAPIBurst     = 20
	cleanupInterval     = 5 * time.Minute
	ipIdleTTL           = 10 * time.Minute
)

type ipEntry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// IPRateLimiter holds per-IP token bucket limiters with background cleanup.
type IPRateLimiter struct {
	mu      sync.Mutex
	ips     map[string]*ipEntry
	rps     rate.Limit // tokens per second (derived from req/min)
	burst   int
}

// NewIPRateLimiter creates a limiter from env vars (or defaults).
func NewIPRateLimiter() *IPRateLimiter {
	rpm := defaultAPIRateLimit
	burst := defaultAPIBurst
	if v := os.Getenv("BOOTSTRAP_API_RATE_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rpm = n
		}
	}
	if v := os.Getenv("BOOTSTRAP_API_BURST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			burst = n
		}
	}
	rl := &IPRateLimiter{
		ips:   make(map[string]*ipEntry),
		rps:   rate.Limit(float64(rpm) / 60.0),
		burst: burst,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *IPRateLimiter) get(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	entry, ok := rl.ips[ip]
	if !ok {
		entry = &ipEntry{lim: rate.NewLimiter(rl.rps, rl.burst)}
		rl.ips[ip] = entry
	}
	entry.lastSeen = time.Now()
	return entry.lim
}

func (rl *IPRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		for ip, entry := range rl.ips {
			if time.Since(entry.lastSeen) > ipIdleTTL {
				delete(rl.ips, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// Middleware wraps a handler with per-IP rate limiting.
// Returns HTTP 429 when the limit is exceeded.
func (rl *IPRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := realIP(r)
		if !rl.get(ip).Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// realIP extracts the client IP, honoring X-Real-IP / X-Forwarded-For set by Caddy.
func realIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		// X-Forwarded-For may be comma-separated; take the first (leftmost = original client).
		for i := 0; i < len(ip); i++ {
			if ip[i] == ',' {
				return ip[:i]
			}
		}
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
