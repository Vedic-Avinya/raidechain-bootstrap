// Package metrics registers all Prometheus metrics for the RideChain bootstrap node.
// Expose via GET /metrics on BOOTSTRAP_METRICS_PORT (default: 9090).
// Connect Prometheus scrape → Grafana dashboard.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ── Connections ─────────────────────────────────────────────────────────────

	RidersConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ridechain_riders_connected",
		Help: "Number of currently connected riders via WebSocket.",
	})

	ConnectionsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ridechain_connections_rejected_total",
		Help: "WebSocket connections rejected, labeled by reason (limit_reached, bad_origin).",
	}, []string{"reason"})

	// ── Messages ─────────────────────────────────────────────────────────────────

	MessagesRelayed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ridechain_messages_relayed_total",
		Help: "Messages relayed by the bootstrap node, labeled by type and routing (city, geo, dm).",
	}, []string{"msg_type", "routing"})

	MessagesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ridechain_messages_dropped_total",
		Help: "Messages dropped, labeled by reason (rate_limit, buffer_full, oversized, duplicate).",
	}, []string{"reason"})

	MessageSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ridechain_message_size_bytes",
		Help:    "Size distribution of P2P messages received from riders.",
		Buckets: prometheus.ExponentialBuckets(64, 2, 11), // 64 B → 64 KiB
	})

	// ── Offline / FCM ────────────────────────────────────────────────────────────

	OfflineMessagesEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ridechain_offline_messages_enqueued_total",
		Help: "Messages enqueued to the Redis offline inbox (peer was offline).",
	})

	OfflineMessagesDelivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ridechain_offline_messages_delivered_total",
		Help: "Offline inbox messages successfully delivered on peer reconnect.",
	})

	FCMPushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ridechain_fcm_pushes_total",
		Help: "FCM wake-up pushes sent, labeled by result (success, failure, no_token).",
	}, []string{"result"})

	// ── Geo / Topics ─────────────────────────────────────────────────────────────

	GeoTopicsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ridechain_geo_topics_active",
		Help: "Number of active geohash-6 cell GossipSub topics.",
	})

	GeoLocationUpdates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ridechain_geo_location_updates_total",
		Help: "Total location updates received via PUT /register/lat-lng.",
	})

	NearbyBroadcasts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ridechain_nearby_broadcasts_total",
		Help: "Total proximity broadcasts published to geohash cell topics.",
	})

	CityTopicsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ridechain_city_topics_active",
		Help: "Number of active city-level GossipSub topics.",
	})

	// ── Peers ────────────────────────────────────────────────────────────────────

	PeerRegistrations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ridechain_peer_registrations_total",
		Help: "Total peer registrations via POST /register.",
	})

	// ── GossipSub / libp2p ───────────────────────────────────────────────────────

	GossipSubPublishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ridechain_gossipsub_publish_errors_total",
		Help: "GossipSub publish errors, labeled by topic_type (city, geo).",
	}, []string{"topic_type"})
)
