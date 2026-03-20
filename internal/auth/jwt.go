package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// Token lifetimes.
const (
	AccessTokenTTL  = 24 * time.Hour     // 24 hours
	RefreshTokenTTL = 90 * 24 * time.Hour // 90 days
)

// Claims embedded in every access token.
type DeviceClaims struct {
	jwt.RegisteredClaims
	DeviceID string `json:"did"`
	PeerID   string `json:"pid,omitempty"`
}

// TokenPair returned on successful device auth or refresh.
type TokenPair struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"` // seconds until access token expires
}

// Issuer manages JWT creation and validation.
type Issuer struct {
	secret []byte
}

// NewIssuer creates a JWT issuer with the given HMAC-SHA256 secret.
// If secret is empty, a random 32-byte key is generated (suitable for single-instance).
func NewIssuer(secret string) *Issuer {
	var key []byte
	if secret != "" {
		key = []byte(secret)
	} else {
		key = make([]byte, 32)
		_, _ = rand.Read(key)
	}
	return &Issuer{secret: key}
}

// Issue creates a new access + refresh token pair for the given device.
func (iss *Issuer) Issue(deviceID, peerID string) (*TokenPair, error) {
	now := time.Now()

	// Access token (short-lived, carries claims)
	accessClaims := DeviceClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   deviceID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenTTL)),
			Issuer:    "ridechain",
		},
		DeviceID: deviceID,
		PeerID:   peerID,
	}
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims).SignedString(iss.secret)
	if err != nil {
		return nil, fmt.Errorf("sign access token: %w", err)
	}

	// Refresh token (long-lived, opaque random + signed)
	refreshID := randomHex(32)
	refreshClaims := jwt.RegisteredClaims{
		Subject:   deviceID,
		ID:        refreshID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(RefreshTokenTTL)),
		Issuer:    "ridechain-refresh",
	}
	refreshToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims).SignedString(iss.secret)
	if err != nil {
		return nil, fmt.Errorf("sign refresh token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(AccessTokenTTL.Seconds()),
	}, nil
}

// Refresh validates a refresh token and issues a new access token.
// Returns a new TokenPair (same refresh token is reused until it expires).
func (iss *Issuer) Refresh(refreshToken, peerID string) (*TokenPair, error) {
	claims := &jwt.RegisteredClaims{}
	token, err := jwt.ParseWithClaims(refreshToken, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return iss.secret, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid refresh token: %w", err)
	}
	if claims.Issuer != "ridechain-refresh" {
		return nil, fmt.Errorf("not a refresh token")
	}

	deviceID := claims.Subject
	now := time.Now()

	// Issue new access token only
	accessClaims := DeviceClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   deviceID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenTTL)),
			Issuer:    "ridechain",
		},
		DeviceID: deviceID,
		PeerID:   peerID,
	}
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims).SignedString(iss.secret)
	if err != nil {
		return nil, fmt.Errorf("sign access token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken, // reuse existing refresh token
		ExpiresIn:    int64(AccessTokenTTL.Seconds()),
	}, nil
}

// ValidateAccess parses and validates an access token, returning the claims.
func (iss *Issuer) ValidateAccess(tokenStr string) (*DeviceClaims, error) {
	claims := &DeviceClaims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return iss.secret, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid access token: %w", err)
	}
	if claims.Issuer != "ridechain" {
		return nil, fmt.Errorf("not an access token")
	}
	return claims, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
