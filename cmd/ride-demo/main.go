package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ──────────────────────────────────────────────────────────
// Message types that flow over Gossipsub
// Compatible with rider-android format
// ──────────────────────────────────────────────────────────

// RideRequest — from rider-android via bridge or from CLI rider
type RideRequest struct {
	Type        string `json:"type"`
	RequestID   string `json:"request_id"`
	RiderPeerID string `json:"rider_peer_id,omitempty"` // rider-android format
	RiderID     string `json:"rider_id,omitempty"`      // CLI rider format
	Timestamp   int64  `json:"timestamp"`
	// Flat fields (CLI rider)
	Pickup    string  `json:"pickup,omitempty"`
	Dropoff   string  `json:"dropoff,omitempty"`
	PickupLat float64 `json:"pickup_lat,omitempty"`
	PickupLng float64 `json:"pickup_lng,omitempty"`
}

func (r *RideRequest) GetRiderID() string {
	if r.RiderPeerID != "" {
		return r.RiderPeerID
	}
	return r.RiderID
}

// RideOffer — driver sends back, compatible with rider-android parseRideOffer
type RideOffer struct {
	Type         string `json:"type"`          // "ride_offer"
	RequestID    string `json:"request_id"`
	DriverPeerID string `json:"driver_peer_id"` // rider-android expects this
	DriverID     string `json:"driver_id"`      // CLI compat
	FareINR      int    `json:"fare_inr"`
	EtaMinutes   int    `json:"eta_minutes"` // rider-android expects this
	EtaMin       int    `json:"eta_min"`     // CLI compat
	Vehicle      string `json:"vehicle"`     // flat string accepted by rider-android
	Name         string `json:"name"`
	Timestamp    int64  `json:"timestamp"`
}

// RideAccept — rider accepts
type RideAccept struct {
	Type      string `json:"type"` // "ride_accept"
	RequestID string `json:"request_id"`
	OfferID   string `json:"offer_id,omitempty"` // rider-android format
	RiderID   string `json:"rider_id,omitempty"` // CLI format
	DriverID  string `json:"driver_id,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// TripUpdate — after acceptance, driver publishes trip progress
type TripUpdate struct {
	Type      string  `json:"type"` // "location_update" or "trip_update"
	RequestID string  `json:"request_id"`
	RideID    string  `json:"ride_id"` // rider-android trackTrip listens on this
	FromID    string  `json:"from_id"`
	Status    string  `json:"status"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp int64   `json:"timestamp"`
}

func main() {
	role := os.Getenv("ROLE") // "bootstrap", "rider", "driver"
	if role == "" {
		fmt.Println("RideChain P2P Ride Flow Demo")
		fmt.Println("============================")
		fmt.Println("")
		fmt.Println("Usage (run each in a separate terminal):")
		fmt.Println("")
		fmt.Println("  Terminal 1 — Bootstrap node:")
		fmt.Println("    cd services/bootstrap && go run ./cmd/main.go")
		fmt.Println("")
		fmt.Println("  Terminal 2 — Driver (auto-responds to ride requests):")
		fmt.Println("    ROLE=driver BOOTSTRAP_ADDR=<addr from bootstrap> DRIVER_NAME=Raju go run ./cmd/ride-demo/")
		fmt.Println("")
		fmt.Println("  Terminal 3 — Rider (CLI, or use rider-android app):")
		fmt.Println("    ROLE=rider BOOTSTRAP_ADDR=<addr from bootstrap> go run ./cmd/ride-demo/")
		fmt.Println("")
		fmt.Println("  Rider Android — connect to ws://<HOST>:4003/rider")
		fmt.Println("    The driver will auto-respond to ride_request from the app.")
		fmt.Println("")
		fmt.Println("Flow:")
		fmt.Println("  1. Rider broadcasts ride_request on Gossipsub topic")
		fmt.Println("  2. Driver receives it, sends ride_offer back")
		fmt.Println("  3. Rider picks offer, sends ride_accept")
		fmt.Println("  4. Driver sends location_update (arriving -> trip_started -> trip_ended)")
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch role {
	case "bootstrap":
		runBootstrap(ctx)
	case "driver":
		runDriver(ctx)
	case "rider":
		runRider(ctx)
	default:
		fmt.Printf("Unknown role: %s. Use bootstrap, driver, or rider.\n", role)
		os.Exit(1)
	}
}

// ──────────────────────────────────────────────────────────
// BOOTSTRAP NODE (lightweight, use cmd/main.go for full)
// ──────────────────────────────────────────────────────────
func runBootstrap(ctx context.Context) {
	port := envOr("BOOTSTRAP_PORT", "4001")

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic-v1", port),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port),
		),
		libp2p.EnableRelay(),
		libp2p.EnableRelayService(),
	)
	if err != nil {
		slog.Error("failed to create host", "err", err)
		os.Exit(1)
	}
	defer h.Close()

	connectAddr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%s/p2p/%s", port, h.ID())

	slog.Info("RIDECHAIN BOOTSTRAP NODE")
	slog.Info("copy this address for driver/rider", "BOOTSTRAP_ADDR", connectAddr)

	d, _ := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	defer d.Close()
	d.Bootstrap(ctx)

	gs, _ := pubsub.NewGossipSub(ctx, h)
	topic, _ := gs.Join("/ridechain/hyderabad/demo/v1")
	defer topic.Close()
	sub, _ := topic.Subscribe()

	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			var raw map[string]any
			json.Unmarshal(msg.Data, &raw)
			msgType, _ := raw["type"].(string)
			slog.Info("RELAYING",
				"type", msgType,
				"from", truncID(msg.ReceivedFrom.String()),
				"size", len(msg.Data),
			)
		}
	}()

	go watchPeers(ctx, h)

	<-ctx.Done()
}

// ──────────────────────────────────────────────────────────
// DRIVER NODE — Listens for requests, sends offers
// Auto-responds to ride_request from rider-android or CLI
// ──────────────────────────────────────────────────────────
func runDriver(ctx context.Context) {
	bootstrapAddr := os.Getenv("BOOTSTRAP_ADDR")
	if bootstrapAddr == "" {
		fmt.Println("ERROR: Set BOOTSTRAP_ADDR env var (copy from bootstrap node output)")
		os.Exit(1)
	}
	driverName := envOr("DRIVER_NAME", fmt.Sprintf("Driver-%d", rand.Intn(100)))

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic-v1"),
	)
	if err != nil {
		slog.Error("failed to create host", "err", err)
		os.Exit(1)
	}
	defer h.Close()

	connectToBootstrap(ctx, h, bootstrapAddr)

	d, _ := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	defer d.Close()
	d.Bootstrap(ctx)

	gs, _ := pubsub.NewGossipSub(ctx, h)
	topic, _ := gs.Join("/ridechain/hyderabad/demo/v1")
	defer topic.Close()
	sub, _ := topic.Subscribe()

	slog.Info("RIDECHAIN DRIVER NODE")
	slog.Info("driver online, waiting for ride requests...",
		"name", driverName,
		"peer_id", truncID(h.ID().String()),
	)

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == h.ID() {
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(msg.Data, &raw); err != nil {
			continue
		}

		msgType, _ := raw["type"].(string)

		switch msgType {
		case "ride_request":
			var req RideRequest
			json.Unmarshal(msg.Data, &req)

			fare := 100 + rand.Intn(100)
			eta := 3 + rand.Intn(8)
			riderID := req.GetRiderID()

			slog.Info("RIDE REQUEST RECEIVED",
				"request_id", req.RequestID,
				"rider", truncID(riderID),
			)

			// Send offer in format compatible with rider-android
			offer := RideOffer{
				Type:         "ride_offer",
				RequestID:    req.RequestID,
				DriverPeerID: h.ID().String(),
				DriverID:     h.ID().String(),
				FareINR:      fare,
				EtaMinutes:   eta,
				EtaMin:       eta,
				Vehicle:      "Auto Rickshaw",
				Name:         driverName,
				Timestamp:    time.Now().Unix(),
			}
			data, _ := json.Marshal(offer)
			topic.Publish(ctx, data)

			slog.Info("OFFER SENT",
				"request_id", req.RequestID,
				"fare", fmt.Sprintf("₹%d", fare),
				"eta", fmt.Sprintf("%d min", eta),
			)

		case "ride_accept":
			var accept RideAccept
			json.Unmarshal(msg.Data, &accept)

			// Check if this driver was chosen (by driver_id or offer_id containing our peer ID)
			isForUs := accept.DriverID == h.ID().String() ||
				len(accept.OfferID) > 0 // If offer_id is present, it's from rider-android (we're the only driver in MVP)

			if isForUs {
				requestID := accept.RequestID
				if requestID == "" {
					// rider-android may not send request_id in accept, derive from offer_id
					requestID = accept.OfferID
				}

				slog.Info("RIDE ACCEPTED - MATCHED!",
					"request_id", requestID,
				)

				go simulateTrip(ctx, h, topic, requestID)
			} else {
				slog.Info("Rider chose another driver",
					"request_id", accept.RequestID,
				)
			}
		}
	}
}

// ──────────────────────────────────────────────────────────
// RIDER NODE — Sends request, picks best offer, tracks trip
// ──────────────────────────────────────────────────────────
func runRider(ctx context.Context) {
	bootstrapAddr := os.Getenv("BOOTSTRAP_ADDR")
	if bootstrapAddr == "" {
		fmt.Println("ERROR: Set BOOTSTRAP_ADDR env var (copy from bootstrap node output)")
		os.Exit(1)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic-v1"),
	)
	if err != nil {
		slog.Error("failed to create host", "err", err)
		os.Exit(1)
	}
	defer h.Close()

	connectToBootstrap(ctx, h, bootstrapAddr)

	d, _ := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	defer d.Close()
	d.Bootstrap(ctx)

	gs, _ := pubsub.NewGossipSub(ctx, h)
	topic, _ := gs.Join("/ridechain/hyderabad/demo/v1")
	defer topic.Close()
	sub, _ := topic.Subscribe()

	requestID := fmt.Sprintf("REQ-%d", time.Now().UnixMilli())

	slog.Info("RIDECHAIN RIDER NODE")
	slog.Info("rider connected", "peer_id", truncID(h.ID().String()))

	// Wait for mesh to form
	slog.Info("waiting for mesh to form...")
	time.Sleep(3 * time.Second)

	// Broadcast ride request in rider-android compatible format
	req := map[string]any{
		"type":          "ride_request",
		"request_id":    requestID,
		"rider_peer_id": h.ID().String(),
		"pickup":        map[string]any{"lat": 17.4375, "lng": 78.4483, "address": "Ameerpet Metro Station"},
		"dropoff":       map[string]any{"lat": 17.4430, "lng": 78.3821, "address": "HITEC City, Madhapur"},
		"distance_km":   8.5,
		"timestamp":     time.Now().Unix(),
	}
	data, _ := json.Marshal(req)
	topic.Publish(ctx, data)

	slog.Info("RIDE REQUEST SENT - waiting for offers...",
		"request_id", requestID,
	)

	// Collect offers for 10 seconds, then pick cheapest
	type offerInfo struct {
		driverID string
		name     string
		fare     int
		eta      int
	}
	var offers []offerInfo
	offerTimeout := time.After(10 * time.Second)

collecting:
	for {
		select {
		case <-offerTimeout:
			break collecting
		case <-ctx.Done():
			return
		default:
			msgCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			msg, err := sub.Next(msgCtx)
			cancel()
			if err != nil {
				continue
			}
			if msg.ReceivedFrom == h.ID() {
				continue
			}

			var raw map[string]any
			if err := json.Unmarshal(msg.Data, &raw); err != nil {
				continue
			}

			if raw["type"] == "ride_offer" {
				var offer RideOffer
				json.Unmarshal(msg.Data, &offer)
				if offer.RequestID == requestID {
					offers = append(offers, offerInfo{
						driverID: offer.DriverPeerID,
						name:     offer.Name,
						fare:     offer.FareINR,
						eta:      offer.EtaMinutes,
					})
					slog.Info("OFFER RECEIVED",
						"driver", offer.Name,
						"fare", fmt.Sprintf("₹%d", offer.FareINR),
						"eta", fmt.Sprintf("%d min", offer.EtaMinutes),
						"offers_so_far", len(offers),
					)
				}
			}
		}
	}

	if len(offers) == 0 {
		slog.Warn("no offers received. Make sure a driver is running!")
		<-ctx.Done()
		return
	}

	// Pick cheapest offer
	best := offers[0]
	for _, o := range offers[1:] {
		if o.fare < best.fare {
			best = o
		}
	}

	slog.Info("ACCEPTING BEST OFFER",
		"driver", best.name,
		"fare", fmt.Sprintf("₹%d", best.fare),
		"eta", fmt.Sprintf("%d min", best.eta),
		"total_offers", len(offers),
	)

	// Send acceptance
	accept := RideAccept{
		Type:      "ride_accept",
		RequestID: requestID,
		RiderID:   h.ID().String(),
		DriverID:  best.driverID,
		Timestamp: time.Now().Unix(),
	}
	data, _ = json.Marshal(accept)
	topic.Publish(ctx, data)

	slog.Info("Waiting for driver trip updates...")

	// Listen for trip updates from matched driver
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == h.ID() {
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(msg.Data, &raw); err != nil {
			continue
		}

		msgType, _ := raw["type"].(string)
		if msgType == "location_update" || msgType == "trip_update" {
			var update TripUpdate
			json.Unmarshal(msg.Data, &update)
			rid := update.RequestID
			if rid == "" {
				rid = update.RideID
			}
			if rid == requestID {
				switch update.Status {
				case "arriving":
					slog.Info("Driver is arriving at pickup!")
				case "at_pickup":
					slog.Info("Driver has arrived! Look for the auto.")
				case "trip_started":
					slog.Info("Trip started - on the way to destination!")
				case "trip_ended":
					slog.Info("TRIP COMPLETE!")
					slog.Info(fmt.Sprintf("Pay ₹%d to driver via UPI", best.fare))
					slog.Info("Zero commission. Full amount goes to driver.")
					time.Sleep(2 * time.Second)
					return
				}
			}
		}
	}
}

// ──────────────────────────────────────────────────────────
// Simulate trip progress after driver is matched
// Sends location_update messages (rider-android trackTrip listens for these)
// ──────────────────────────────────────────────────────────
func simulateTrip(ctx context.Context, h host.Host, topic *pubsub.Topic, requestID string) {
	steps := []struct {
		status string
		delay  time.Duration
		lat    float64
		lng    float64
	}{
		{"arriving", 3 * time.Second, 17.4380, 78.4490},
		{"at_pickup", 3 * time.Second, 17.4375, 78.4483},
		{"trip_started", 4 * time.Second, 17.4400, 78.4500},
		{"trip_ended", 4 * time.Second, 17.4430, 78.3821},
	}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			return
		case <-time.After(step.delay):
		}

		// Send as location_update (rider-android trackTrip listens for this type)
		update := TripUpdate{
			Type:      "location_update",
			RequestID: requestID,
			RideID:    requestID,
			FromID:    h.ID().String(),
			Status:    step.status,
			Lat:       step.lat,
			Lng:       step.lng,
			Timestamp: time.Now().Unix(),
		}
		data, _ := json.Marshal(update)
		topic.Publish(ctx, data)

		switch step.status {
		case "arriving":
			slog.Info("Heading to pickup...", "lat", step.lat, "lng", step.lng)
		case "at_pickup":
			slog.Info("Arrived at pickup! Waiting for rider.")
		case "trip_started":
			slog.Info("Rider onboard - trip started!")
		case "trip_ended":
			slog.Info("Arrived at destination - trip complete!")
		}
	}
}

// ──────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────
func connectToBootstrap(ctx context.Context, h host.Host, addr string) {
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		slog.Error("invalid bootstrap address", "err", err, "addr", addr)
		os.Exit(1)
	}
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		slog.Error("failed to parse peer info", "err", err)
		os.Exit(1)
	}
	if err := h.Connect(ctx, *peerInfo); err != nil {
		slog.Error("failed to connect to bootstrap", "err", err)
		os.Exit(1)
	}
	slog.Info("connected to bootstrap node")
}

func watchPeers(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	prev := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count := len(h.Network().Peers())
			if count != prev {
				slog.Info("connected peers", "count", count)
				prev = count
			}
		}
	}
}

func truncID(id string) string {
	if len(id) > 16 {
		return id[:16] + "..."
	}
	return id
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
