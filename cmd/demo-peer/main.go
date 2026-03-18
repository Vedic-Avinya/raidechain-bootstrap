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
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// RideRequest simulates a ride request message on Gossipsub.
type RideRequest struct {
	Type        string  `json:"type"`
	RequestID   string  `json:"request_id"`
	PickupLat   float64 `json:"pickup_lat"`
	PickupLng   float64 `json:"pickup_lng"`
	DropoffLat  float64 `json:"dropoff_lat"`
	DropoffLng  float64 `json:"dropoff_lng"`
	Pickup      string  `json:"pickup"`
	Dropoff     string  `json:"dropoff"`
	FareINR     int     `json:"fare_inr"`
	VehicleType string  `json:"vehicle_type"`
	PeerID      string  `json:"peer_id"`
	Timestamp   int64   `json:"timestamp"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Bootstrap multiaddr: pass via env or use default localhost.
	bootstrapAddr := os.Getenv("BOOTSTRAP_ADDR")
	if bootstrapAddr == "" {
		fmt.Println("Usage: BOOTSTRAP_ADDR=/ip4/127.0.0.1/tcp/4001/p2p/<PEER_ID> go run .")
		fmt.Println("")
		fmt.Println("Get the PEER_ID from the bootstrap node's startup log.")
		os.Exit(1)
	}

	peerName := os.Getenv("PEER_NAME")
	if peerName == "" {
		peerName = fmt.Sprintf("driver-%d", rand.Intn(1000))
	}

	// Whether this peer publishes demo ride requests.
	isPublisher := os.Getenv("PUBLISHER") == "true"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Random port so we can run multiple peers on one machine.
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip4/0.0.0.0/tcp/0",
		),
	)
	if err != nil {
		slog.Error("failed to create host", "err", err)
		os.Exit(1)
	}
	defer h.Close()

	slog.Info("peer started",
		"name", peerName,
		"peer_id", h.ID().String(),
	)

	// Parse bootstrap multiaddr and connect.
	maddr, err := multiaddr.NewMultiaddr(bootstrapAddr)
	if err != nil {
		slog.Error("invalid bootstrap multiaddr", "err", err, "addr", bootstrapAddr)
		os.Exit(1)
	}

	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		slog.Error("failed to parse bootstrap peer info", "err", err)
		os.Exit(1)
	}

	slog.Info("connecting to bootstrap node...", "bootstrap_id", peerInfo.ID.String())

	if err := h.Connect(ctx, *peerInfo); err != nil {
		slog.Error("failed to connect to bootstrap", "err", err)
		os.Exit(1)
	}
	slog.Info("connected to bootstrap node")

	// Start DHT in client mode (driver peers are DHT clients).
	d, err := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		slog.Error("failed to create DHT", "err", err)
		os.Exit(1)
	}
	defer d.Close()

	if err := d.Bootstrap(ctx); err != nil {
		slog.Error("DHT bootstrap failed", "err", err)
		os.Exit(1)
	}
	slog.Info("DHT client joined")

	// Start Gossipsub.
	gs, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		slog.Error("failed to create GossipSub", "err", err)
		os.Exit(1)
	}

	topic, err := gs.Join("/ridechain/hyderabad/demo/v1")
	if err != nil {
		slog.Error("failed to join topic", "err", err)
		os.Exit(1)
	}
	defer topic.Close()

	sub, err := topic.Subscribe()
	if err != nil {
		slog.Error("failed to subscribe to topic", "err", err)
		os.Exit(1)
	}

	slog.Info("subscribed to gossipsub topic",
		"topic", "/ridechain/hyderabad/demo/v1",
		"mode", func() string {
			if isPublisher {
				return "PUBLISHER (sending ride requests)"
			}
			return "SUBSCRIBER (receiving ride requests)"
		}(),
	)

	// Receive messages.
	go func() {
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			// Skip own messages.
			if msg.ReceivedFrom == h.ID() {
				continue
			}

			var req RideRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				slog.Warn("received non-JSON message", "data", string(msg.Data))
				continue
			}

			slog.Info("RIDE REQUEST RECEIVED",
				"from_peer", truncID(msg.ReceivedFrom.String()),
				"request_id", req.RequestID,
				"route", fmt.Sprintf("%s → %s", req.Pickup, req.Dropoff),
				"fare", fmt.Sprintf("₹%d", req.FareINR),
				"vehicle", req.VehicleType,
			)
		}
	}()

	// Publish demo ride requests if this is a publisher.
	if isPublisher {
		go func() {
			// Wait a bit for mesh to form.
			time.Sleep(3 * time.Second)

			routes := []struct {
				pickup, dropoff string
				fare            int
			}{
				{"Ameerpet", "HITEC City", 180},
				{"Secunderabad", "Gachibowli", 250},
				{"Kukatpally", "Charminar", 200},
				{"Madhapur", "LB Nagar", 320},
				{"Banjara Hills", "Shamshabad Airport", 450},
			}

			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				route := routes[i%len(routes)]
				req := RideRequest{
					Type:        "ride_request",
					RequestID:   fmt.Sprintf("REQ-%s-%04d", peerName, i+1),
					PickupLat:   17.4326 + rand.Float64()*0.05,
					PickupLng:   78.3688 + rand.Float64()*0.05,
					DropoffLat:  17.4326 + rand.Float64()*0.05,
					DropoffLng:  78.3688 + rand.Float64()*0.05,
					Pickup:      route.pickup,
					Dropoff:     route.dropoff,
					FareINR:     route.fare + rand.Intn(50),
					VehicleType: "auto",
					PeerID:      h.ID().String(),
					Timestamp:   time.Now().Unix(),
				}

				data, _ := json.Marshal(req)
				if err := topic.Publish(ctx, data); err != nil {
					slog.Error("publish failed", "err", err)
					continue
				}

				slog.Info("RIDE REQUEST PUBLISHED",
					"request_id", req.RequestID,
					"route", fmt.Sprintf("%s → %s", req.Pickup, req.Dropoff),
					"fare", fmt.Sprintf("₹%d", req.FareINR),
				)

				time.Sleep(5 * time.Second)
			}
		}()
	}

	<-ctx.Done()
	slog.Info("peer shutting down", "name", peerName)
}

func truncID(id string) string {
	if len(id) > 16 {
		return id[:16] + "..."
	}
	return id
}
