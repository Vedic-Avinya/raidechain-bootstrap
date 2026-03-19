package redis

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/mmcloughlin/geohash"
	rdb "github.com/redis/go-redis/v9"
)

const (
	keyPeerMeta   = "peer:meta:"       // peerId -> JSON
	keyPeersGeo   = "peers:geo"        // GEO key for GEORADIUS
	keySearchName = "search:name:"     // token -> set of peerIds (smart search)
	peerTTL       = 7 * 24 * time.Hour // expire inactive peers
	defaultRadius     = 50.0  // km
	maxSearchResults  = 100   // cap search-by-name results
	maxDiscoverPeers      = 100 // cap geo-filtered discover results
	maxDiscoverNonGeo     = 500 // cap when no geo params (return all recent, ~40KB)
)

// PeerMeta is stored in Redis for each peer.
type PeerMeta struct {
	PeerID      string  `json:"peer_id"`
	DisplayName string  `json:"display_name"`
	Geohash     string  `json:"geohash"`
	Lat         float64 `json:"lat,omitempty"`
	Lng         float64 `json:"lng,omitempty"`
	City        string  `json:"city,omitempty"` // optional; sent by client for topic routing
	FCMToken    string  `json:"fcm_token,omitempty"`
	UpdatedAt   int64   `json:"updated_at"`
}

// PeerLocation is a peer within discover radius (avoids exposing go-redis types).
type PeerLocation struct {
	PeerID   string
	Distance float64 // km
}

// Store handles peer registration, geo indexing, and search.
type Store struct {
	client *rdb.Client
}

// NewStore creates a Redis-backed peer store.
func NewStore(addr string) (*Store, error) {
	opt, err := rdb.ParseURL(addr)
	if err != nil {
		// Try as host:port
		opt = &rdb.Options{Addr: addr}
	}
	client := rdb.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	slog.Info("redis_store", "msg", "connected", "addr", addr)
	return &Store{client: client}, nil
}

// Register stores or updates peer metadata and updates geo index.
// If geohash is set, it is decoded to lat/lng and used for GEOADD.
// Search index: peerId is added to each token (word) of display name for smart search.
func (s *Store) Register(ctx context.Context, peerID, displayName, geohashStr string, lat, lng *float64, fcmToken string) error {
	now := time.Now().Unix()
	displayName = strings.TrimSpace(displayName)
	meta := PeerMeta{
		PeerID:      peerID,
		DisplayName: displayName,
		Geohash:     geohashStr,
		UpdatedAt:   now,
		FCMToken:    fcmToken,
	}

	// Resolve lat/lng from geohash if not provided
	if lat != nil && lng != nil {
		meta.Lat, meta.Lng = *lat, *lng
	} else if geohashStr != "" {
		latC, lngC := geohash.DecodeCenter(geohashStr)
		meta.Lat, meta.Lng = latC, lngC
	}

	// Remove peer from old name tokens (if updating)
	existing, _ := s.GetPeer(ctx, peerID)
	if existing != nil && existing.DisplayName != "" {
		for _, tok := range tokenizeDisplayName(existing.DisplayName) {
			s.client.SRem(ctx, keySearchName+tok, peerID)
		}
	}

	pipe := s.client.Pipeline()

	// Store metadata
	metaJSON, _ := json.Marshal(meta)
	pipe.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL)

	// Update geo index (Redis: GEOADD key lng lat member)
	if meta.Lat != 0 || meta.Lng != 0 {
		pipe.GeoAdd(ctx, keyPeersGeo, &rdb.GeoLocation{
			Name:      peerID,
			Longitude: meta.Lng,
			Latitude:  meta.Lat,
		})
	}

	// Search-by-name: index by token (word) for smart multi-word and partial match
	if meta.DisplayName != "" {
		for _, tok := range tokenizeDisplayName(meta.DisplayName) {
			pipe.SAdd(ctx, keySearchName+tok, peerID)
			pipe.Expire(ctx, keySearchName+tok, peerTTL)
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

// SearchByName returns peer IDs matching the query (smart search: tokenized, case-insensitive).
// Single word: peers whose display name contains that word. Multiple words: peers matching all words (AND).
func (s *Store) SearchByName(ctx context.Context, name string) ([]string, error) {
	tokens := tokenizeDisplayName(name)
	if len(tokens) == 0 {
		return nil, nil
	}
	if len(tokens) == 1 {
		peerIDs, err := s.client.SMembers(ctx, keySearchName+tokens[0]).Result()
		if err != nil {
			return nil, err
		}
		if len(peerIDs) > maxSearchResults {
			peerIDs = peerIDs[:maxSearchResults]
		}
		return peerIDs, nil
	}
	// Multiple tokens: SINTER to get peers that have all tokens
	keys := make([]string, len(tokens))
	for i, t := range tokens {
		keys[i] = keySearchName + t
	}
	peerIDs, err := s.client.SInter(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	if len(peerIDs) > maxSearchResults {
		peerIDs = peerIDs[:maxSearchResults]
	}
	return peerIDs, nil
}

// Discover returns peers within radius (km) of the given point.
// If geohash is provided, it is decoded to lat/lng. Otherwise lat/lng must be set.
// When neither is provided, returns up to 500 peers from India center (fallback).
func (s *Store) Discover(ctx context.Context, geohashStr string, lat, lng *float64, radiusKm float64) ([]PeerLocation, error) {
	var latitude, longitude float64
	cap := maxDiscoverPeers
	if lat != nil && lng != nil {
		latitude, longitude = *lat, *lng
	} else if geohashStr != "" {
		latitude, longitude = geohash.DecodeCenter(geohashStr)
	} else {
		// No geo params — return all peers using India center with earth-spanning radius.
		// ~40KB for 500 peers (lightweight). Client should always send lat/lng but this is the fallback.
		latitude, longitude = 20.5937, 78.9629
		radiusKm = 20_000
		cap = maxDiscoverNonGeo
	}
	if radiusKm <= 0 {
		radiusKm = defaultRadius
	}
	locs, err := s.client.GeoRadius(ctx, keyPeersGeo, longitude, latitude, &rdb.GeoRadiusQuery{
		Radius:   radiusKm,
		Unit:     "km",
		WithDist: true,
		Count:    cap,
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PeerLocation, 0, len(locs))
	for _, loc := range locs {
		out = append(out, PeerLocation{PeerID: loc.Name, Distance: loc.Dist})
	}
	return out, nil
}

// GetPeerMetas returns metadata for the given peer IDs (pipeline). Missing peers are nil in the slice.
func (s *Store) GetPeerMetas(ctx context.Context, peerIDs []string) ([]*PeerMeta, error) {
	if len(peerIDs) == 0 {
		return nil, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*rdb.StringCmd, len(peerIDs))
	for i, id := range peerIDs {
		cmds[i] = pipe.Get(ctx, keyPeerMeta+id)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != rdb.Nil {
		return nil, err
	}
	out := make([]*PeerMeta, len(peerIDs))
	for i, cmd := range cmds {
		b, e := cmd.Bytes()
		if e == rdb.Nil || e != nil {
			out[i] = nil
			continue
		}
		var meta PeerMeta
		if json.Unmarshal(b, &meta) == nil {
			out[i] = &meta
		}
	}
	return out, nil
}

// GetPeer returns metadata for a peer.
func (s *Store) GetPeer(ctx context.Context, peerID string) (*PeerMeta, error) {
	b, err := s.client.Get(ctx, keyPeerMeta+peerID).Bytes()
	if err == rdb.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta PeerMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// SetFCMToken updates the FCM token for a peer (and refreshes TTL).
func (s *Store) SetFCMToken(ctx context.Context, peerID, token string) error {
	b, err := s.client.Get(ctx, keyPeerMeta+peerID).Bytes()
	if err == rdb.Nil {
		// No peer yet; store minimal record
		meta := PeerMeta{PeerID: peerID, FCMToken: token, UpdatedAt: time.Now().Unix()}
		metaJSON, _ := json.Marshal(meta)
		return s.client.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL).Err()
	}
	if err != nil {
		return err
	}
	var meta PeerMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return err
	}
	meta.FCMToken = token
	meta.UpdatedAt = time.Now().Unix()
	metaJSON, _ := json.Marshal(meta)
	return s.client.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL).Err()
}

// SetDisplayName updates the display name for a peer (and refreshes TTL) and updates the search index.
func (s *Store) SetDisplayName(ctx context.Context, peerID, displayName string) error {
	displayName = strings.TrimSpace(displayName)
	b, err := s.client.Get(ctx, keyPeerMeta+peerID).Bytes()
	if err == rdb.Nil {
		// No peer yet; store minimal record and index tokens
		meta := PeerMeta{PeerID: peerID, DisplayName: displayName, UpdatedAt: time.Now().Unix()}
		metaJSON, _ := json.Marshal(meta)
		if err := s.client.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL).Err(); err != nil {
			return err
		}
		for _, tok := range tokenizeDisplayName(displayName) {
			s.client.SAdd(ctx, keySearchName+tok, peerID)
			s.client.Expire(ctx, keySearchName+tok, peerTTL)
		}
		return nil
	}
	if err != nil {
		return err
	}
	var meta PeerMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return err
	}
	// Remove from old name tokens
	if meta.DisplayName != "" {
		for _, tok := range tokenizeDisplayName(meta.DisplayName) {
			s.client.SRem(ctx, keySearchName+tok, peerID)
		}
	}
	meta.DisplayName = displayName
	meta.UpdatedAt = time.Now().Unix()
	metaJSON, _ := json.Marshal(meta)
	pipe := s.client.Pipeline()
	pipe.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL)
	for _, tok := range tokenizeDisplayName(displayName) {
		pipe.SAdd(ctx, keySearchName+tok, peerID)
		pipe.Expire(ctx, keySearchName+tok, peerTTL)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// SetLatLng updates the latitude, longitude, and optional city for a peer (and refreshes TTL) and updates the geo index.
// City is stored for topic routing; the rider bridge joins /ridechain/{city}/p2p/v1 per city.
func (s *Store) SetLatLng(ctx context.Context, peerID string, lat, lng float64, city string) error {
	city = strings.TrimSpace(strings.ToLower(city))
	b, err := s.client.Get(ctx, keyPeerMeta+peerID).Bytes()
	if err == rdb.Nil {
		// No peer yet; store minimal record and add to geo index
		meta := PeerMeta{PeerID: peerID, Lat: lat, Lng: lng, City: city, UpdatedAt: time.Now().Unix()}
		metaJSON, _ := json.Marshal(meta)
		if err := s.client.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL).Err(); err != nil {
			return err
		}
		return s.client.GeoAdd(ctx, keyPeersGeo, &rdb.GeoLocation{
			Name:      peerID,
			Longitude: lng,
			Latitude:  lat,
		}).Err()
	}
	if err != nil {
		return err
	}
	var meta PeerMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return err
	}
	meta.Lat = lat
	meta.Lng = lng
	if city != "" {
		meta.City = city
	}
	meta.UpdatedAt = time.Now().Unix()
	metaJSON, _ := json.Marshal(meta)
	pipe := s.client.Pipeline()
	pipe.Set(ctx, keyPeerMeta+peerID, metaJSON, peerTTL)
	pipe.GeoAdd(ctx, keyPeersGeo, &rdb.GeoLocation{
		Name:      peerID,
		Longitude: lng,
		Latitude:  lat,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// DistrictForGeohash returns a district label for a geohash prefix (India-oriented).
// Uses first 4–5 chars of geohash for coarse region.
func DistrictForGeohash(geohashStr string) string {
	if len(geohashStr) < 4 {
		return ""
	}
	prefix := geohashStr[:4]
	// Simplified: map common Indian geohash prefixes to region names (expand as needed).
	districts := map[string]string{
		"tdr2": "Hyderabad", "tdr3": "Secunderabad", "tdr4": "Telangana",
		"tduq": "Bangalore", "tdun": "Karnataka",
		"te7u": "Chennai", "te7v": "Tamil Nadu",
		"ttnu": "Mumbai", "ttnv": "Maharashtra",
		"ttn3": "Pune",
		"tq9r": "Delhi", "tq9s": "NCR",
	}
	if d, ok := districts[prefix]; ok {
		return d
	}
	return prefix
}

const (
	keyInbox    = "inbox:"
	inboxTTL    = 48 * time.Hour
	maxInboxLen = 100

	keyMsgSeen  = "msgseen:"
	msgSeenTTL  = 5 * time.Minute // dedup window
)

// IsDuplicateMessage returns true if msgID has already been seen within the dedup window.
// Uses Redis SET NX (set-if-not-exists) with a 5-minute TTL.
// Returns (false, nil) when Redis is unavailable so messages are never silently dropped.
func (s *Store) IsDuplicateMessage(ctx context.Context, msgID string) (bool, error) {
	if msgID == "" {
		return false, nil
	}
	key := keyMsgSeen + msgID
	set, err := s.client.SetNX(ctx, key, 1, msgSeenTTL).Result()
	if err != nil {
		return false, err // fail-open: treat as new
	}
	return !set, nil // set=true means it was new; !set means already existed
}

// EnqueueOfflineMessage persists a message for a peer that is currently offline.
// Messages are stored in a Redis list (capped at maxInboxLen, TTL 48 h).
func (s *Store) EnqueueOfflineMessage(ctx context.Context, peerID string, data []byte) error {
	key := keyInbox + peerID
	pipe := s.client.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.LTrim(ctx, key, -maxInboxLen, -1)
	pipe.Expire(ctx, key, inboxTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// DrainOfflineMessages atomically fetches and deletes all queued messages for peerID.
// Returns nil slice (no error) when the inbox is empty.
func (s *Store) DrainOfflineMessages(ctx context.Context, peerID string) ([][]byte, error) {
	key := keyInbox + peerID
	pipe := s.client.Pipeline()
	listCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil && err != rdb.Nil {
		return nil, err
	}
	strs, err := listCmd.Result()
	if err == rdb.Nil || len(strs) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	msgs := make([][]byte, len(strs))
	for i, s := range strs {
		msgs[i] = []byte(s)
	}
	return msgs, nil
}

func normalizeName(name string) string {
	s := strings.TrimSpace(strings.ToLower(name))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// tokenizeDisplayName splits display name into lowercase word tokens (alphanumeric only per word).
// Used for smart search: "John Doe" -> ["john", "doe"]; query "john doe" matches peers with both tokens.
func tokenizeDisplayName(name string) []string {
	s := strings.TrimSpace(strings.ToLower(name))
	if s == "" {
		return nil
	}
	var tokens []string
	var word strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			word.WriteRune(r)
		} else {
			if word.Len() > 0 {
				tokens = append(tokens, word.String())
				word.Reset()
			}
		}
	}
	if word.Len() > 0 {
		tokens = append(tokens, word.String())
	}
	return tokens
}
