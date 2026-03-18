package riderbridge

// Geohash-6 cell-level GossipSub topics for proximity routing.
//
// Architecture:
//   - Each geohash-6 cell (~1.2 km × 0.6 km) gets its own topic: /ridechain/geo/{cell}/p2p/v1
//   - A rider subscribes to a 9-cell window (center + 8 neighbours ≈ 3.6 km × 1.8 km)
//   - When a rider broadcasts a "nearby" message (type: "nearby_*"), it is published to
//     their center-cell topic. All riders whose window overlaps that cell receive it.
//   - This is the "car walkie-talkie" and "find nearby people (Tinder-style)" layer.
//   - For 1:1 chat, use targeted messages via peerID (forwardToCity / Forward).
//
// Naming: /ridechain/geo/{geohash6}/p2p/v1  e.g. /ridechain/geo/ttnv52/p2p/v1

import (
	"context"
	"log/slog"

	"github.com/mmcloughlin/geohash"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/ridechain/ridechain/services/bootstrap/internal/analytics"
	appmetrics "github.com/ridechain/ridechain/services/bootstrap/internal/metrics"
)

const (
	geoPrecision   = 6 // each cell ~1.22 km × 0.61 km
	geoTopicPrefix = "/ridechain/geo/" // joined with: {env}/{cell}/p2p/v1
)

// Get9CellWindow returns the geohash-6 cell containing (lat,lng) plus its 8 neighbours.
// Subscribing to all 9 cells ensures seamless reception across cell boundaries.
// Index 0 is always the centre cell.
// geohash.Neighbors returns [N, NE, E, SE, S, SW, W, NW] (8 elements).
func Get9CellWindow(lat, lng float64) []string {
	center := geohash.EncodeWithPrecision(lat, lng, geoPrecision)
	ns := geohash.Neighbors(center) // []string{N, NE, E, SE, S, SW, W, NW}
	cells := make([]string, 0, 9)
	cells = append(cells, center)
	cells = append(cells, ns...)
	return cells
}

// GeoTopicName returns the GossipSub topic name for a geohash-6 cell.
// Format: /ridechain/geo/{env}/{cell}/p2p/v1
func GeoTopicName(env, cell string) string {
	return geoTopicPrefix + env + "/" + cell + topicSuffix
}

// getOrJoinGeoCell ensures the bridge is subscribed to the GossipSub topic for cell.
// Idempotent and goroutine-safe.  Must NOT be called with b.mu held.
func (b *Bridge) getOrJoinGeoCell(cell string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.geoTopics[cell]; ok {
		return nil
	}
	topicName := GeoTopicName(b.env, cell)
	topic, err := b.gs.Join(topicName)
	if err != nil {
		return err
	}
	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		return err
	}
	ctx, cancel := context.WithCancel(b.ctx)
	ct := &cityTopic{topic: topic, sub: sub, cancel: cancel}
	b.geoTopics[cell] = ct
	appmetrics.GeoTopicsActive.Set(float64(len(b.geoTopics)))
	slog.Debug("rider_bridge", "msg", "joined geo cell topic", "cell", cell, "topic", topicName)
	go b.runGeoSub(ctx, cell, sub)
	return nil
}

// runGeoSub reads messages from a geohash-cell subscription and fans out to riders in that cell.
func (b *Bridge) runGeoSub(ctx context.Context, cell string, sub *pubsub.Subscription) {
	defer sub.Cancel()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if len(msg.Data) == 0 {
			continue
		}
		b.forwardToGeoCell(cell, msg.Data)
	}
}

// forwardToGeoCell delivers data to all riders currently in the given geohash cell.
// Riders with a full send buffer are disconnected (slow-client protection).
func (b *Bridge) forwardToGeoCell(cell string, data []byte) {
	b.mu.RLock()
	cellMap := b.cellRiders[cell]
	riders := make([]*riderConn, 0, len(cellMap))
	for _, rc := range cellMap {
		riders = append(riders, rc)
	}
	b.mu.RUnlock()

	for _, rc := range riders {
		if !rc.deliver(data) {
			b.mu.Lock()
			delete(b.riders, rc.conn)
			if rc.peerId != "" {
				delete(b.peerByID, rc.peerId)
			}
			for _, c := range rc.geoCells {
				if cm := b.cellRiders[c]; cm != nil {
					delete(cm, rc.peerId)
				}
			}
			b.mu.Unlock()
			rc.closeSend()
			slog.Warn("rider_bridge", "msg", "geo forward buffer full, removed slow rider", "peer_id", rc.peerId, "cell", cell)
		}
	}
}

// UpdateRiderGeohash subscribes peerID to the 9-cell window around (lat,lng).
// Called by PUT /register/lat-lng.  Safe to call even when the rider is offline
// (will take effect on their next connection if they reconnect after this call).
func (b *Bridge) UpdateRiderGeohash(peerID string, lat, lng float64) {
	newCells := Get9CellWindow(lat, lng)

	// Ensure GossipSub topics exist for all 9 cells (join is idempotent).
	for _, cell := range newCells {
		if err := b.getOrJoinGeoCell(cell); err != nil {
			slog.Warn("rider_bridge", "msg", "geo cell join failed", "cell", cell, "peer_id", peerID, "err", err)
		}
	}

	b.mu.Lock()
	rc := b.peerByID[peerID]

	// Remove rider from old cells.
	var oldCells []string
	if rc != nil {
		oldCells = rc.geoCells
	}
	for _, cell := range oldCells {
		if cm := b.cellRiders[cell]; cm != nil {
			delete(cm, peerID)
		}
	}

	// Register rider in new cells (only if they are currently connected).
	if rc != nil {
		rc.geoCells = newCells
		for _, cell := range newCells {
			if b.cellRiders[cell] == nil {
				b.cellRiders[cell] = make(map[string]*riderConn)
			}
			b.cellRiders[cell][peerID] = rc
		}
	}
	b.mu.Unlock()

	slog.Debug("rider_bridge", "msg", "geo cells updated", "peer_id", peerID,
		"center", newCells[0], "online", rc != nil)
}

// PublishToGeoCell publishes data to the GossipSub topic for the centre cell of (lat,lng).
// Use for "nearby broadcast" messages (car walkie-talkie, nearby ping, etc.).
// Returns the centre geohash-6 cell used.
func (b *Bridge) PublishToGeoCell(lat, lng float64, data []byte) (string, error) {
	cells := Get9CellWindow(lat, lng)
	center := cells[0]

	if err := b.getOrJoinGeoCell(center); err != nil {
		return center, err
	}

	b.mu.RLock()
	ct := b.geoTopics[center]
	b.mu.RUnlock()

	if ct == nil {
		return center, nil
	}
	return center, ct.topic.Publish(context.Background(), data)
}

// publishToRiderGeoCell publishes data to the GossipSub topic for rc's current centre cell,
// then fans out to all riders in that cell via WebSocket (self-filter bypass).
func (b *Bridge) publishToRiderGeoCell(rc *riderConn, data []byte) {
	if len(rc.geoCells) == 0 {
		return // rider has not registered a location yet
	}
	cell := rc.geoCells[0] // centre cell
	b.mu.RLock()
	ct := b.geoTopics[cell]
	b.mu.RUnlock()
	if ct == nil {
		return
	}
	if err := ct.topic.Publish(context.Background(), data); err != nil {
		appmetrics.GossipSubPublishErrors.WithLabelValues("geo").Inc()
		slog.Warn("rider_bridge", "msg", "geo publish failed", "cell", cell, "peer_id", rc.peerId, "err", err)
		return
	}
	appmetrics.MessagesRelayed.WithLabelValues("geo_broadcast", "geo").Inc()
	appmetrics.NearbyBroadcasts.Inc()
	b.forwardToGeoCell(cell, data)
	if b.analyticsClient != nil {
		b.analyticsClient.Send(b.ctx, rc.peerId, analytics.NearbyBroadcast(rc.peerId, cell))
	}
}

// GeoTopicCount returns the number of active geohash-cell GossipSub topics.
func (b *Bridge) GeoTopicCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.geoTopics)
}
