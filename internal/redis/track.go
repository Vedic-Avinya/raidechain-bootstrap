package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	rdb "github.com/redis/go-redis/v9"
)

const (
	keyTrackSession  = "track:session:"          // {sessionId} -> JSON
	keyTrackLocation = "track:session:location:"  // {sessionId} -> JSON {lat,lng,ts}
	keyTrackWatcher  = "track:session:watcher:"   // {sessionId}:{watcherPeerId} -> status
	maxSessionTTL    = 3 * time.Hour              // hard cap
)

// TrackSessionStatus represents the lifecycle of a tracking session.
type TrackSessionStatus string

const (
	TrackStatusActive   TrackSessionStatus = "active"
	TrackStatusExpired  TrackSessionStatus = "expired"
)

// WatcherStatus represents a watcher's request state.
type WatcherStatus string

const (
	WatcherRequested WatcherStatus = "requested"
	WatcherApproved  WatcherStatus = "approved"
	WatcherRejected  WatcherStatus = "rejected"
)

// TrackSession is stored in Redis for each live tracking session.
type TrackSession struct {
	SessionID       string `json:"session_id"`
	OwnerPeerID     string `json:"owner_peer_id"`
	DurationMinutes int    `json:"duration_minutes"`
	CreatedAt       int64  `json:"created_at"`       // unix seconds
	ExpiresAt       int64  `json:"expires_at"`        // unix seconds
	Status          string `json:"status"`            // active, expired
}

// TrackLocation is the latest published location for a session.
type TrackLocation struct {
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp int64   `json:"timestamp"` // unix seconds
}

// CreateTrackSession creates a new live tracking session in Redis.
func (s *Store) CreateTrackSession(ctx context.Context, sessionID, ownerPeerID string, durationMinutes int) (*TrackSession, error) {
	now := time.Now().Unix()
	ttl := time.Duration(durationMinutes) * time.Minute
	if ttl > maxSessionTTL {
		ttl = maxSessionTTL
	}
	session := TrackSession{
		SessionID:       sessionID,
		OwnerPeerID:     ownerPeerID,
		DurationMinutes: durationMinutes,
		CreatedAt:       now,
		ExpiresAt:       now + int64(durationMinutes*60),
		Status:          string(TrackStatusActive),
	}
	data, err := json.Marshal(session)
	if err != nil {
		return nil, err
	}
	if err := s.client.Set(ctx, keyTrackSession+sessionID, data, ttl).Err(); err != nil {
		return nil, err
	}
	return &session, nil
}

// GetTrackSession returns the session metadata. Returns nil if expired or not found.
func (s *Store) GetTrackSession(ctx context.Context, sessionID string) (*TrackSession, error) {
	b, err := s.client.Get(ctx, keyTrackSession+sessionID).Bytes()
	if err == rdb.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session TrackSession
	if err := json.Unmarshal(b, &session); err != nil {
		return nil, err
	}
	// Check expiry
	if time.Now().Unix() > session.ExpiresAt {
		session.Status = string(TrackStatusExpired)
	}
	return &session, nil
}

// SetWatcherStatus stores the watcher's request status for a session.
func (s *Store) SetWatcherStatus(ctx context.Context, sessionID, watcherPeerID string, status WatcherStatus, ttl time.Duration) error {
	key := fmt.Sprintf("%s%s:%s", keyTrackWatcher, sessionID, watcherPeerID)
	return s.client.Set(ctx, key, string(status), ttl).Err()
}

// GetWatcherStatus returns the watcher's current status for a session.
func (s *Store) GetWatcherStatus(ctx context.Context, sessionID, watcherPeerID string) (WatcherStatus, error) {
	key := fmt.Sprintf("%s%s:%s", keyTrackWatcher, sessionID, watcherPeerID)
	val, err := s.client.Get(ctx, key).Result()
	if err == rdb.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return WatcherStatus(val), nil
}

// PublishTrackLocation stores the latest location for a session.
func (s *Store) PublishTrackLocation(ctx context.Context, sessionID string, lat, lng float64, ttl time.Duration) error {
	loc := TrackLocation{
		Lat:       lat,
		Lng:       lng,
		Timestamp: time.Now().Unix(),
	}
	data, err := json.Marshal(loc)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, keyTrackLocation+sessionID, data, ttl).Err()
}

// GetTrackLocation returns the latest published location for a session.
func (s *Store) GetTrackLocation(ctx context.Context, sessionID string) (*TrackLocation, error) {
	b, err := s.client.Get(ctx, keyTrackLocation+sessionID).Bytes()
	if err == rdb.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var loc TrackLocation
	if err := json.Unmarshal(b, &loc); err != nil {
		return nil, err
	}
	return &loc, nil
}
