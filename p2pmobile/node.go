// Package p2pmobile provides a gomobile-bindable libp2p node for Android/iOS.
//
// Features:
//   - GossipSub topic pub/sub (backward-compat with existing chat)
//   - Direct peer-to-peer streams (no relay needed when NAT allows)
//   - DHT peer discovery
//   - NAT traversal: port mapping, hole punching, circuit relay v2 fallback
//
// Build AAR:
//
//	cd bootstrap && gomobile bind -target=android -androidapi 21 -o p2pmobile.aar ./p2pmobile
package p2pmobile

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

const (
	// DirectMessageProtocol is the libp2p stream protocol for 1-to-1 messages.
	DirectMessageProtocol = "/ridechain/dm/1.0.0"
	maxMessageSize        = 10 * 1024 * 1024 // 10 MB
	identityKeyFile       = "p2p_identity.key"
)

// -------- Callback interfaces (gomobile-compatible: simple types only) --------

// MessageHandler receives GossipSub topic messages.
type MessageHandler interface {
	OnMessage(from string, topic string, data []byte)
}

// DirectMessageHandler receives direct peer-to-peer stream messages.
type DirectMessageHandler interface {
	OnDirectMessage(from string, data []byte)
}

// ConnectionHandler receives peer connection lifecycle events.
type ConnectionHandler interface {
	OnPeerConnected(peerID string)
	OnPeerDisconnected(peerID string)
}

// -------- Node --------

// Node is a libp2p peer that runs on Android/iOS.
type Node struct {
	mu      sync.RWMutex
	host    host.Host
	dht     *dht.IpfsDHT
	ps      *pubsub.PubSub
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	topics map[string]*pubsub.Topic
	subs   map[string]*pubsub.Subscription

	msgHandler    MessageHandler
	directHandler DirectMessageHandler
	connHandler   ConnectionHandler

	bootstrapPeers []peer.AddrInfo
}

func init() {
	// Suppress non-fatal "netlinkrib: permission denied" errors on Android.
	// Android 11+ restricts netlink socket access; basichost periodically tries
	// to resolve local interface addresses and fails. This doesn't affect
	// outbound connectivity (bootstrap, DHT, GossipSub all work fine).
	logging.SetLogLevel("basichost", "FATAL")
}

// NewNode creates a new P2P node (not started yet).
func NewNode() *Node {
	return &Node{
		topics: make(map[string]*pubsub.Topic),
		subs:   make(map[string]*pubsub.Subscription),
	}
}

// Start creates the libp2p host, connects to bootstrap, starts DHT + GossipSub.
// dataDir: writable directory for persistent identity (e.g. context.filesDir).
// bootstrapMultiaddrs: comma-separated multiaddrs of the bootstrap node.
func (n *Node) Start(dataDir string, bootstrapMultiaddrs string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running {
		return nil
	}

	n.ctx, n.cancel = context.WithCancel(context.Background())

	priv, err := loadOrCreateIdentity(dataDir)
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}

	// Parse multiaddrs and merge addresses by peer ID so h.Connect() tries
	// all transports (TCP + QUIC) for the same peer simultaneously.
	peerMap := make(map[peer.ID]*peer.AddrInfo)
	for _, ma := range strings.Split(bootstrapMultiaddrs, ",") {
		ma = strings.TrimSpace(ma)
		if ma == "" {
			continue
		}
		addr, err := multiaddr.NewMultiaddr(ma)
		if err != nil {
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			continue
		}
		if existing, ok := peerMap[info.ID]; ok {
			existing.Addrs = append(existing.Addrs, info.Addrs...)
		} else {
			copy := *info
			peerMap[info.ID] = &copy
		}
	}
	var bootstrapInfos []peer.AddrInfo
	for _, info := range peerMap {
		bootstrapInfos = append(bootstrapInfos, *info)
	}
	n.bootstrapPeers = bootstrapInfos

	// MINIMAL host options — avoid anything that calls net.Interfaces() / netlinkRIB().
	// On Android 11+, SELinux denies AF_NETLINK socket bind for untrusted apps.
	// Specifically avoided: EnableRelay, ForceReachabilityPrivate, EnableAutoRelay,
	// EnableHolePunching — all trigger net.Interfaces() via AutoNAT or relay address ads.
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoListenAddrs,
		libp2p.DisableRelay(),
	)
	if err != nil {
		return fmt.Errorf("host: %w", err)
	}
	n.host = h

	h.SetStreamHandler(protocol.ID(DirectMessageProtocol), n.handleDirectStream)
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			if ch := n.connHandler; ch != nil {
				ch.OnPeerConnected(c.RemotePeer().String())
			}
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			if ch := n.connHandler; ch != nil {
				ch.OnPeerDisconnected(c.RemotePeer().String())
			}
		},
	})

	// Connect to bootstrap peers FIRST — before DHT or GossipSub.
	// This is a plain TCP/QUIC dial that does NOT need net.Interfaces().
	var bootstrapConnected bool
	for _, info := range bootstrapInfos {
		fmt.Printf("p2pmobile: dialing bootstrap %s addrs=%v\n", info.ID, info.Addrs)
		ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
		if err := h.Connect(ctx, info); err != nil {
			fmt.Printf("p2pmobile: bootstrap dial failed %s: %v\n", info.ID, err)
			cancel()
			continue
		}
		cancel()
		fmt.Printf("p2pmobile: bootstrap connected %s (peers=%d)\n", info.ID, len(h.Network().Peers()))
		bootstrapConnected = true
		break
	}
	if !bootstrapConnected && len(bootstrapInfos) > 0 {
		for _, info := range bootstrapInfos {
			go func(pi peer.AddrInfo) {
				ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
				defer cancel()
				if err := h.Connect(ctx, pi); err == nil {
					fmt.Printf("p2pmobile: late bootstrap connected %s (peers=%d)\n", pi.ID, len(h.Network().Peers()))
				} else {
					fmt.Printf("p2pmobile: late bootstrap FAILED %s: %v\n", pi.ID, err)
				}
			}(info)
		}
		fmt.Println("p2pmobile: WARNING — no bootstrap peer connected synchronously")
	}

	// GossipSub — only needs a host + at least one connected peer. No DHT required.
	ps, err := pubsub.NewGossipSub(n.ctx, h, pubsub.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		h.Close()
		return fmt.Errorf("gossipsub: %w", err)
	}
	n.ps = ps

	// DHT (optional) — used for FindPeer in SendToPeer. Non-fatal if it triggers SELinux.
	d, err := dht.New(n.ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		fmt.Printf("p2pmobile: DHT creation failed (non-fatal, SendToPeer won't work): %v\n", err)
	} else {
		n.dht = d
		if err := d.Bootstrap(n.ctx); err != nil {
			fmt.Printf("p2pmobile: DHT bootstrap failed (non-fatal): %v\n", err)
		}
	}

	// Protect bootstrap peers from connection manager pruning
	for _, info := range bootstrapInfos {
		h.ConnManager().Protect(info.ID, "bootstrap")
	}

	// Background reconnect loop — re-dial bootstrap every 30s if disconnected
	go n.bootstrapKeepAlive()

	n.running = true
	fmt.Printf("p2pmobile: node started — peerID=%s connectedPeers=%d dht=%v\n",
		h.ID(), len(h.Network().Peers()), n.dht != nil)
	return nil
}

// bootstrapKeepAlive periodically checks bootstrap connectivity and re-dials if lost.
func (n *Node) bootstrapKeepAlive() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.RLock()
			h := n.host
			peers := n.bootstrapPeers
			running := n.running
			n.mu.RUnlock()
			if !running || h == nil {
				return
			}
			for _, info := range peers {
				if h.Network().Connectedness(info.ID) == network.Connected {
					continue
				}
				fmt.Printf("p2pmobile: bootstrap %s disconnected, reconnecting...\n", info.ID)
				ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
				if err := h.Connect(ctx, info); err != nil {
					fmt.Printf("p2pmobile: bootstrap reconnect FAILED %s: %v\n", info.ID, err)
				} else {
					fmt.Printf("p2pmobile: bootstrap reconnected %s (peers=%d)\n", info.ID, len(h.Network().Peers()))
				}
				cancel()
			}
		}
	}
}

// Stop shuts down the node.
func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return nil
	}
	n.running = false
	for _, sub := range n.subs {
		sub.Cancel()
	}
	for _, t := range n.topics {
		t.Close()
	}
	n.topics = make(map[string]*pubsub.Topic)
	n.subs = make(map[string]*pubsub.Subscription)
	if n.dht != nil {
		n.dht.Close()
	}
	if n.cancel != nil {
		n.cancel()
	}
	if n.host != nil {
		n.host.Close()
	}
	return nil
}

// IsRunning returns true if the node is started.
func (n *Node) IsRunning() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.running
}

// PeerID returns this node's libp2p peer ID (e.g. "12D3KooW...").
func (n *Node) PeerID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.host == nil {
		return ""
	}
	return n.host.ID().String()
}

// ListenAddrs returns comma-separated listen multiaddrs.
func (n *Node) ListenAddrs() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.host == nil {
		return ""
	}
	addrs := n.host.Addrs()
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = fmt.Sprintf("%s/p2p/%s", a, n.host.ID())
	}
	return strings.Join(out, ",")
}

// ConnectedPeers returns comma-separated list of connected peer IDs.
func (n *Node) ConnectedPeers() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.host == nil {
		return ""
	}
	peers := n.host.Network().Peers()
	out := make([]string, len(peers))
	for i, p := range peers {
		out[i] = p.String()
	}
	return strings.Join(out, ",")
}

// ConnectedPeerCount returns the number of connected peers.
func (n *Node) ConnectedPeerCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.host == nil {
		return 0
	}
	return len(n.host.Network().Peers())
}

// -------- GossipSub (topic broadcast) --------

// Publish sends data to a GossipSub topic.
func (n *Node) Publish(topicName string, data []byte) error {
	n.mu.Lock()
	topic, ok := n.topics[topicName]
	if !ok {
		if n.ps == nil {
			n.mu.Unlock()
			return fmt.Errorf("not started")
		}
		var err error
		topic, err = n.ps.Join(topicName)
		if err != nil {
			n.mu.Unlock()
			return err
		}
		n.topics[topicName] = topic
	}
	n.mu.Unlock()
	return topic.Publish(n.ctx, data)
}

// SubscribeTopic subscribes to a GossipSub topic.
func (n *Node) SubscribeTopic(topicName string) error {
	n.mu.Lock()
	if _, ok := n.subs[topicName]; ok {
		n.mu.Unlock()
		return nil
	}
	topic, ok := n.topics[topicName]
	if !ok {
		if n.ps == nil {
			n.mu.Unlock()
			return fmt.Errorf("not started")
		}
		var err error
		topic, err = n.ps.Join(topicName)
		if err != nil {
			n.mu.Unlock()
			return err
		}
		n.topics[topicName] = topic
	}
	sub, err := topic.Subscribe()
	if err != nil {
		n.mu.Unlock()
		return err
	}
	n.subs[topicName] = sub
	n.mu.Unlock()

	selfID := n.host.ID()
	go func() {
		for {
			msg, err := sub.Next(n.ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == selfID {
				continue
			}
			if h := n.msgHandler; h != nil {
				h.OnMessage(msg.ReceivedFrom.String(), topicName, msg.Data)
			}
		}
	}()
	return nil
}

// SetMessageHandler sets the callback for incoming GossipSub messages.
func (n *Node) SetMessageHandler(handler MessageHandler) {
	n.msgHandler = handler
}

// -------- Direct peer-to-peer streams --------

// SetDirectMessageHandler sets callback for direct stream messages.
func (n *Node) SetDirectMessageHandler(handler DirectMessageHandler) {
	n.directHandler = handler
}

// SetConnectionHandler sets callback for connection events.
func (n *Node) SetConnectionHandler(handler ConnectionHandler) {
	n.connHandler = handler
}

// SendToPeer sends data directly to a peer via a libp2p stream.
// No server relay — pure device-to-device (falls back to circuit relay if NAT blocks direct).
func (n *Node) SendToPeer(targetPeerID string, data []byte) error {
	n.mu.RLock()
	h := n.host
	ok := n.running
	n.mu.RUnlock()
	if !ok || h == nil {
		return fmt.Errorf("not started")
	}

	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return fmt.Errorf("bad peer id (%d chars): %w", len(targetPeerID), err)
	}

	if h.Network().Connectedness(pid) != network.Connected {
		if err := n.dialPeer(pid); err != nil {
			return fmt.Errorf("dial: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	s, err := h.NewStream(ctx, pid, protocol.ID(DirectMessageProtocol))
	if err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	defer s.Close()

	length := uint32(len(data))
	hdr := []byte{byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)}
	if _, err := s.Write(hdr); err != nil {
		s.Reset()
		return err
	}
	if _, err := s.Write(data); err != nil {
		s.Reset()
		return err
	}
	return nil
}

// ConnectToPeer establishes a connection to a peer (DHT lookup + direct dial + relay fallback).
func (n *Node) ConnectToPeer(targetPeerID string) error {
	n.mu.RLock()
	ok := n.running
	n.mu.RUnlock()
	if !ok {
		return fmt.Errorf("not started")
	}
	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return fmt.Errorf("bad peer id (%d chars): %w", len(targetPeerID), err)
	}
	return n.dialPeer(pid)
}

// FindPeer looks up a peer's multiaddrs via DHT. Returns comma-separated addrs.
func (n *Node) FindPeer(targetPeerID string) (string, error) {
	n.mu.RLock()
	d := n.dht
	n.mu.RUnlock()
	if d == nil {
		return "", fmt.Errorf("dht not started")
	}
	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return "", fmt.Errorf("bad peer id (%d chars): %w", len(targetPeerID), err)
	}
	ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	info, err := d.FindPeer(ctx, pid)
	if err != nil {
		return "", err
	}
	out := make([]string, len(info.Addrs))
	for i, a := range info.Addrs {
		out[i] = a.String()
	}
	return strings.Join(out, ","), nil
}

// IsConnectedToPeer returns true if already connected to the peer.
func (n *Node) IsConnectedToPeer(targetPeerID string) bool {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return false
	}
	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return false
	}
	return h.Network().Connectedness(pid) == network.Connected
}

// -------- internal --------

func (n *Node) dialPeer(pid peer.ID) error {
	h := n.host
	if h == nil {
		return fmt.Errorf("not started")
	}
	// 1) DHT lookup → direct dial (short timeouts for chat responsiveness)
	if n.dht != nil {
		ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
		info, err := n.dht.FindPeer(ctx, pid)
		cancel()
		if err == nil && len(info.Addrs) > 0 {
			ctx2, cancel2 := context.WithTimeout(n.ctx, 3*time.Second)
			err2 := h.Connect(ctx2, info)
			cancel2()
			if err2 == nil {
				return nil
			}
		}
	}
	// 2) Relay fallback through each bootstrap peer
	for _, bp := range n.bootstrapPeers {
		relayMA, err := multiaddr.NewMultiaddr(
			fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", bp.ID, pid),
		)
		if err != nil {
			continue
		}
		relayInfo := peer.AddrInfo{ID: pid, Addrs: []multiaddr.Multiaddr{relayMA}}
		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		err = h.Connect(ctx, relayInfo)
		cancel()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("unreachable: %s", pid)
}

func (n *Node) handleDirectStream(s network.Stream) {
	defer s.Close()
	from := s.Conn().RemotePeer().String()
	fmt.Printf("p2pmobile: incoming direct stream from %s\n", from)

	hdr := make([]byte, 4)
	if _, err := io.ReadFull(s, hdr); err != nil {
		fmt.Printf("p2pmobile: direct stream header read error from %s: %v\n", from, err)
		return
	}
	length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	if length > maxMessageSize {
		fmt.Printf("p2pmobile: direct stream message too large from %s: %d bytes\n", from, length)
		return
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(s, data); err != nil {
		fmt.Printf("p2pmobile: direct stream body read error from %s: %v\n", from, err)
		return
	}
	fmt.Printf("p2pmobile: direct stream received %d bytes from %s\n", length, from)

	if h := n.directHandler; h != nil {
		h.OnDirectMessage(from, data)
	}
	if h := n.msgHandler; h != nil {
		h.OnMessage(from, "direct", data)
	}
}

// -------- identity persistence --------

func loadOrCreateIdentity(dataDir string) (crypto.PrivKey, error) {
	if dataDir == "" {
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		return priv, err
	}
	keyPath := filepath.Join(dataDir, identityKeyFile)
	data, err := os.ReadFile(keyPath)
	if err == nil && len(data) > 0 {
		seedHex := strings.TrimSpace(string(data))
		seed, err := hex.DecodeString(seedHex)
		if err == nil && len(seed) == ed25519.SeedSize {
			edKey := ed25519.NewKeyFromSeed(seed)
			priv, err := crypto.UnmarshalEd25519PrivateKey(edKey)
			if err == nil {
				return priv, nil
			}
		}
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(seed)), 0600); err != nil {
		return nil, err
	}
	edKey := ed25519.NewKeyFromSeed(seed)
	return crypto.UnmarshalEd25519PrivateKey(edKey)
}
