package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	rdb "github.com/redis/go-redis/v9"
)

const (
	keyTrackSession      = "track:session:"         // {sessionId} -> JSON
	keyTrackLocation     = "track:session:location:" // {sessionId} -> JSON {lat,lng,ts}
	keyTrackWatcher      = "track:session:watcher:"  // {sessionId}:{watcherPeerId} -> status
	keyTrackWatcherMeta  = "track:session:watcher_meta:"
	keyTrackOwnerSession = "track:owner_session:" // {ownerPeerId} -> sessionId
	maxSessionTTL        = 3 * time.Hour
)

// TrackSessionStatus represents the lifecycle of a tracking session.
type TrackSessionStatus string

const (
	TrackStatusActive  TrackSessionStatus = "active"
	TrackStatusExpired TrackSessionStatus = "expired"
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
	CreatedAt       int64  `json:"created_at"` // unix seconds
	ExpiresAt       int64  `json:"expires_at"` // unix seconds
	Status          string `json:"status"`     // active, expired
	// DirectInvitePeerID: when set, this watcher is auto-approved (e.g. 1:1 chat share link).
	DirectInvitePeerID string `json:"direct_invite_peer_id,omitempty"`
}

// PendingTrackRequest is a watcher waiting for owner approval.
type PendingTrackRequest struct {
	WatcherPeerID string `json:"watcherPeerId"`
	DisplayName   string `json:"displayName"`
	City          string `json:"city"`
}

// TrackLocation is the latest published location for a session.
type TrackLocation struct {
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp int64   `json:"timestamp"` // unix seconds
}

// CreateTrackSession creates or reuses the owner's single active track session in Redis.
// directInvitePeerID is stored on newly created sessions; when reusing an existing session, call
// PatchTrackSessionDirectInvite so the invite target can still be updated.
func (s *Store) CreateTrackSession(ctx context.Context, ownerPeerID string, durationMinutes int, directInvitePeerID string) (*TrackSession, error) {
	ownerPeerID = strings.TrimSpace(ownerPeerID)
	if ownerPeerID == "" {
		return nil, fmt.Errorf("owner peer id required")
	}
	ttl := time.Duration(durationMinutes) * time.Minute
	if ttl > maxSessionTTL {
		ttl = maxSessionTTL
	}

	// One active session per owner: reuse if still valid.
	if sid, err := s.client.Get(ctx, keyTrackOwnerSession+ownerPeerID).Result(); err == nil && sid != "" {
		if existing, err := s.GetTrackSession(ctx, sid); err == nil && existing != nil {
			if time.Now().Unix() <= existing.ExpiresAt && existing.OwnerPeerID == ownerPeerID {
				return existing, nil
			}
		}
		_ = s.client.Del(ctx, keyTrackOwnerSession+ownerPeerID).Err()
	}

	sessionID := randomTrackSessionID()
	now := time.Now().Unix()
	session := TrackSession{
		SessionID:          sessionID,
		OwnerPeerID:      ownerPeerID,
		DurationMinutes:  durationMinutes,
		CreatedAt:        now,
		ExpiresAt:        now + int64(durationMinutes*60),
		Status:           string(TrackStatusActive),
		DirectInvitePeerID: strings.TrimSpace(directInvitePeerID),
	}
	data, err := json.Marshal(session)
	if err != nil {
		return nil, err
	}
	pipe := s.client.Pipeline()
	pipe.Set(ctx, keyTrackSession+sessionID, data, ttl)
	pipe.Set(ctx, keyTrackOwnerSession+ownerPeerID, sessionID, ttl)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return &session, nil
}

// PatchTrackSessionDirectInvite sets or updates the peer id that is auto-approved when requesting
// (1:1 chat share). Call after CreateTrackSession when reusing an existing session.
func (s *Store) PatchTrackSessionDirectInvite(ctx context.Context, sessionID, directInvitePeerID string) error {
	sessionID = strings.TrimSpace(sessionID)
	directInvitePeerID = strings.TrimSpace(directInvitePeerID)
	if sessionID == "" || directInvitePeerID == "" {
		return nil
	}
	sess, err := s.GetTrackSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess == nil {
		return fmt.Errorf("session not found")
	}
	if time.Now().Unix() > sess.ExpiresAt {
		return fmt.Errorf("session expired")
	}
	sess.DirectInvitePeerID = directInvitePeerID
	data, err := json.Marshal(sess)
	if err != nil {
		return err
	}
	ttl := time.Until(time.Unix(sess.ExpiresAt, 0))
	if ttl <= 0 {
		ttl = time.Minute
	}
	if ttl > maxSessionTTL {
		ttl = maxSessionTTL
	}
	return s.client.Set(ctx, keyTrackSession+sessionID, data, ttl).Err()
}

func randomTrackSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// GetOwnerActiveTrackSession returns the non-expired session mapped to this owner, if any.
func (s *Store) GetOwnerActiveTrackSession(ctx context.Context, ownerPeerID string) (*TrackSession, error) {
	ownerPeerID = strings.TrimSpace(ownerPeerID)
	if ownerPeerID == "" {
		return nil, nil
	}
	sid, err := s.client.Get(ctx, keyTrackOwnerSession+ownerPeerID).Result()
	if err == rdb.Nil || sid == "" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sess, err := s.GetTrackSession(ctx, sid)
	if err != nil || sess == nil {
		return nil, err
	}
	if time.Now().Unix() > sess.ExpiresAt {
		return nil, nil
	}
	if sess.OwnerPeerID != ownerPeerID {
		return nil, nil
	}
	return sess, nil
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

// SetWatcherRequestMeta stores display name and city shown to the session owner (same TTL as status).
func (s *Store) SetWatcherRequestMeta(ctx context.Context, sessionID, watcherPeerID, displayName, city string, ttl time.Duration) error {
	type meta struct {
		DisplayName string `json:"displayName"`
		City        string `json:"city"`
	}
	b, err := json.Marshal(meta{DisplayName: displayName, City: strings.TrimSpace(city)})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s%s:%s", keyTrackWatcherMeta, sessionID, watcherPeerID)
	return s.client.Set(ctx, key, b, ttl).Err()
}

func (s *Store) getWatcherRequestMeta(ctx context.Context, sessionID, watcherPeerID string) (displayName, city string) {
	key := fmt.Sprintf("%s%s:%s", keyTrackWatcherMeta, sessionID, watcherPeerID)
	b, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		return "", ""
	}
	var m struct {
		DisplayName string `json:"displayName"`
		City        string `json:"city"`
	}
	_ = json.Unmarshal(b, &m)
	return m.DisplayName, m.City
}

// ListPendingTrackRequests returns watchers in "requested" state for the session.
func (s *Store) ListPendingTrackRequests(ctx context.Context, sessionID string) ([]PendingTrackRequest, error) {
	pattern := fmt.Sprintf("%s%s:*", keyTrackWatcher, sessionID)
	var keys []string
	var cursor uint64
	for {
		batch, next, err := s.client.Scan(ctx, cursor, pattern, 64).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	prefix := fmt.Sprintf("%s%s:", keyTrackWatcher, sessionID)
	var out []PendingTrackRequest
	for _, key := range keys {
		status, err := s.client.Get(ctx, key).Result()
		if err != nil || status != string(WatcherRequested) {
			continue
		}
		watcherPeerID := strings.TrimPrefix(key, prefix)
		if watcherPeerID == "" {
			continue
		}
		dn, city := s.getWatcherRequestMeta(ctx, sessionID, watcherPeerID)
		out = append(out, PendingTrackRequest{
			WatcherPeerID: watcherPeerID,
			DisplayName:   dn,
			City:          city,
		})
	}
	return out, nil
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

// EndTrackSession removes session data, owner mapping, cached location, and all watcher keys.
// Call when the owner stops sharing before natural expiry.
func (s *Store) EndTrackSession(ctx context.Context, ownerPeerID, sessionID string) error {
	ownerPeerID = strings.TrimSpace(ownerPeerID)
	sessionID = strings.TrimSpace(sessionID)
	if ownerPeerID == "" || sessionID == "" {
		return fmt.Errorf("owner and session id required")
	}
	sess, err := s.GetTrackSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess == nil {
		return fmt.Errorf("session not found")
	}
	if sess.OwnerPeerID != ownerPeerID {
		return fmt.Errorf("not session owner")
	}

	pipe := s.client.Pipeline()
	pipe.Del(ctx, keyTrackSession+sessionID)
	pipe.Del(ctx, keyTrackLocation+sessionID)
	pipe.Del(ctx, keyTrackOwnerSession+ownerPeerID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	deleteByPattern := func(pattern string) error {
		var cursor uint64
		for {
			keys, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				if err := s.client.Del(ctx, keys...).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return nil
	}
	patWatcher := fmt.Sprintf("%s%s:*", keyTrackWatcher, sessionID)
	patMeta := fmt.Sprintf("%s%s:*", keyTrackWatcherMeta, sessionID)
	if err := deleteByPattern(patWatcher); err != nil {
		return err
	}
	return deleteByPattern(patMeta)
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
