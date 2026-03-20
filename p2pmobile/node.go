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
	"bufio"
	"bytes"
	"compress/zlib"
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
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	circuitv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

const (
	// DirectMessageProtocol is the libp2p stream protocol for 1-to-1 messages.
	DirectMessageProtocol = "/ridechain/dm/1.0.0"
	// OptimizedDMProtocol adds compression + framing. Peers negotiate the best version.
	OptimizedDMProtocol = "/ridechain/dm/2.0.0"
	maxMessageSize      = 20 * 1024 * 1024 // 20 MB
	identityKeyFile     = "p2p_identity.key"
	// compressionThreshold: payloads above this size get zlib-compressed.
	compressionThreshold = 1024 // 1 KB
	// streamPoolTTL: how long an idle pooled stream stays alive.
	streamPoolTTL = 60 * time.Second
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
	relayReady     bool // true after successful relay reservation

	// extraAddrs: dynamically discovered addresses (real IP from TCP conn,
	// relay circuit addr, external IPv6 from Android APIs).
	// These are injected into h.Addrs() via AddrsFactory so that identify
	// and DCUtR can share them with other peers.
	extraAddrsMu sync.RWMutex
	extraAddrs   []multiaddr.Multiaddr

	// streamPool: reuse streams to avoid per-message TCP+protocol negotiation overhead.
	// Key: peer.ID string, Value: *pooledStream
	streamPoolMu sync.Mutex
	streamPool   map[string]*pooledStream
}

// pooledStream wraps a reusable libp2p stream with a buffered writer.
type pooledStream struct {
	stream  network.Stream
	writer  *bufio.Writer
	lastUse time.Time
	mu      sync.Mutex
}

func init() {
	// Suppress non-fatal "netlinkrib: permission denied" errors on Android.
	logging.SetLogLevel("basichost", "FATAL")
	// Enable identify debug — logger name is "net/identify" in go-libp2p v0.38.
	logging.SetLogLevel("net/identify", "DEBUG")
}

// NewNode creates a new P2P node (not started yet).
func NewNode() *Node {
	return &Node{
		topics:     make(map[string]*pubsub.Topic),
		subs:       make(map[string]*pubsub.Subscription),
		streamPool: make(map[string]*pooledStream),
	}
}

// Start initialises the libp2p host, connects to bootstrap, and starts GossipSub.
//
// Architecture — phased startup (fast start, background upgrade):
//
//	Phase 1  Create host                           (instant)
//	Phase 2  DialPeer to bootstrap — NO identify   (2-5 s)
//	Phase 3  Start GossipSub                       (instant)
//	         ═══ NODE READY — pub/sub messaging works ═══
//	Phase 4  Background: identify → relay → DHT    (async)
//	         → direct peer-to-peer streams become available
//
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

	bootstrapInfos := parseBootstrapAddrs(bootstrapMultiaddrs)
	n.bootstrapPeers = bootstrapInfos

	// ── Phase 1: Create host (TCP on IPv4 + IPv6) ──────────────────
	// QUIC is broken on Android/cellular: handshake succeeds but data
	// streams fail (Berty issue #1428 — reuseport breaks QUIC on mobile).
	// Listen on BOTH IPv4 and IPv6:
	//   - IPv4: works everywhere but behind CGNAT → needs relay
	//   - IPv6: many 4G/5G carriers give public IPv6 → DIRECT connections!
	//
	// AddrsFactory: on Android, net.Interfaces() fails (SELinux) so libp2p
	// only sees loopback. We inject real addresses discovered at runtime
	// (from TCP connection local addr, relay circuit, Android IPv6 APIs).
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
		),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			n.extraAddrsMu.RLock()
			extra := make([]multiaddr.Multiaddr, len(n.extraAddrs))
			copy(extra, n.extraAddrs)
			n.extraAddrsMu.RUnlock()
			if len(extra) > 0 {
				return append(addrs, extra...)
			}
			return addrs
		}),
	)
	if err != nil {
		return fmt.Errorf("host: %w", err)
	}
	n.host = h

	h.SetStreamHandler(protocol.ID(DirectMessageProtocol), n.handleDirectStream)
	h.SetStreamHandler(protocol.ID(OptimizedDMProtocol), n.handleOptimizedStream)
	go n.streamPoolJanitor() // evict idle pooled streams
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			remotePeer := c.RemotePeer().String()
			remoteAddr := c.RemoteMultiaddr().String()
			connType := "DIRECT"
			if strings.Contains(remoteAddr, "p2p-circuit") {
				connType = "RELAY"
			}
			isIPv6 := strings.Contains(remoteAddr, "/ip6/")
			fmt.Printf("p2pmobile: ⚡ CONNECTED peer=%s type=%s ipv6=%v addr=%s dir=%s\n",
				remotePeer[:16], connType, isIPv6, remoteAddr, c.Stat().Direction)
			if ch := n.connHandler; ch != nil {
				ch.OnPeerConnected(remotePeer)
			}
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			remotePeer := c.RemotePeer().String()
			remoteAddr := c.RemoteMultiaddr().String()
			fmt.Printf("p2pmobile: ✖ DISCONNECTED peer=%s addr=%s\n", remotePeer[:16], remoteAddr)
			// Evict pooled stream for this peer (connection is dead)
			n.evictPooledStream(remotePeer)
			if ch := n.connHandler; ch != nil {
				ch.OnPeerDisconnected(remotePeer)
			}
			// Immediate reconnect if a bootstrap peer dropped
			n.mu.RLock()
			bsPeers := n.bootstrapPeers
			running := n.running
			n.mu.RUnlock()
			if running {
				for _, info := range bsPeers {
					if info.ID.String() == remotePeer {
						fmt.Printf("p2pmobile: [auto-reconnect] bootstrap peer %s lost — reconnecting immediately\n", remotePeer[:16])
						go func(pi peer.AddrInfo) {
							time.Sleep(2 * time.Second) // brief backoff
							tcpInfo := tcpOnlyPeerInfo(pi)
							if len(tcpInfo.Addrs) == 0 {
								tcpInfo = pi
							}
							ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
							err := n.host.Connect(ctx, tcpInfo)
							cancel()
							if err != nil {
								fmt.Printf("p2pmobile: [auto-reconnect] FAILED %s: %v\n", pi.ID.String()[:16], err)
								return
							}
							fmt.Printf("p2pmobile: [auto-reconnect] reconnected %s\n", pi.ID.String()[:16])
							n.discoverRealAddrs()
							n.reserveRelayOnBootstrap()
							n.addRelayCircuitAddrs()
						}(info)
						break
					}
				}
			}
		},
	})

	// ── Phase 2: Bootstrap via h.Connect over TCP (includes identify) ─
	// Filter to TCP-only addresses (QUIC broken on mobile).
	// h.Connect = DialPeer + IdentifyWait.
	for _, info := range bootstrapInfos {
		h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
		h.ConnManager().Protect(info.ID, "bootstrap")
	}
	var bootstrapConnected bool
	for _, info := range bootstrapInfos {
		// Use TCP-only addresses for bootstrap
		tcpInfo := tcpOnlyPeerInfo(info)
		if len(tcpInfo.Addrs) == 0 {
			tcpInfo = info // fallback to all if no TCP addrs
		}
		fmt.Printf("p2pmobile: [phase2] h.Connect %s addrs=%v (TCP, includes identify)\n", tcpInfo.ID, tcpInfo.Addrs)
		t0 := time.Now()
		ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
		err := h.Connect(ctx, tcpInfo)
		cancel()
		if err != nil {
			fmt.Printf("p2pmobile: [phase2] h.Connect FAILED %s after %v: %v\n", tcpInfo.ID, time.Since(t0).Round(time.Millisecond), err)
			continue
		}
		protos, _ := h.Peerstore().GetProtocols(tcpInfo.ID)
		fmt.Printf("p2pmobile: [phase2] connected+identified %s in %v (peers=%d, protocols=%d)\n",
			tcpInfo.ID, time.Since(t0).Round(time.Millisecond), len(h.Network().Peers()), len(protos))

		// Diagnostic: test raw stream at connection level (bypasses IdentifyWait)
		conns := h.Network().ConnsToPeer(tcpInfo.ID)
		if len(conns) > 0 {
			diagCtx, diagCancel := context.WithTimeout(n.ctx, 10*time.Second)
			s, sErr := conns[0].NewStream(diagCtx)
			diagCancel()
			if sErr != nil {
				fmt.Printf("p2pmobile: [phase2] DIAG raw stream FAILED: %v\n", sErr)
			} else {
				fmt.Printf("p2pmobile: [phase2] DIAG raw stream OK on %s\n", conns[0].RemoteMultiaddr())
				s.Reset()
			}
		}

		bootstrapConnected = true
		break
	}
	if !bootstrapConnected {
		fmt.Println("p2pmobile: [phase2] WARNING — no bootstrap connected; background will retry")
	}

	// ── Phase 3: GossipSub (works immediately — no identify needed) ──
	ps, err := pubsub.NewGossipSub(n.ctx, h, pubsub.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		h.Close()
		return fmt.Errorf("gossipsub: %w", err)
	}
	n.ps = ps

	n.running = true
	// Log all listen addresses (including IPv6)
	for _, addr := range h.Addrs() {
		fmt.Printf("p2pmobile: LISTEN addr=%s\n", addr)
	}
	fmt.Printf("p2pmobile: node READY — peerID=%s bootstrap=%v peers=%d addrs=%d\n",
		h.ID(), bootstrapConnected, len(h.Network().Peers()), len(h.Addrs()))

	// ── Phase 4: Background upgrade ──────────────────────────────────
	go n.backgroundUpgrade()

	return nil
}

// parseBootstrapAddrs parses comma-separated multiaddrs, merging by peer ID
// so DialPeer tries all transports (TCP + QUIC) simultaneously.
func parseBootstrapAddrs(raw string) []peer.AddrInfo {
	peerMap := make(map[peer.ID]*peer.AddrInfo)
	for _, ma := range strings.Split(raw, ",") {
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
			cp := *info
			peerMap[info.ID] = &cp
		}
	}
	var out []peer.AddrInfo
	for _, info := range peerMap {
		out = append(out, *info)
	}
	return out
}

// tcpOnlyPeerInfo returns a copy of info with only TCP multiaddrs.
// QUIC identify has been unreliable on mobile — TCP is tried first.
func tcpOnlyPeerInfo(info peer.AddrInfo) peer.AddrInfo {
	var tcpAddrs []multiaddr.Multiaddr
	for _, a := range info.Addrs {
		// Check if multiaddr contains /tcp/ component
		for _, p := range a.Protocols() {
			if p.Code == multiaddr.P_TCP {
				tcpAddrs = append(tcpAddrs, a)
				break
			}
		}
	}
	return peer.AddrInfo{ID: info.ID, Addrs: tcpAddrs}
}

// backgroundUpgrade runs after Start() returns.  Phase 2 already
// completed identify via h.Connect, so we can go straight to relay.
func (n *Node) backgroundUpgrade() {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return
	}

	// Step 1 — ensure at least one bootstrap is connected
	n.ensureBootstrap()

	// Step 1b — Extract real IP from bootstrap TCP connection.
	// On Android, net.Interfaces() fails (SELinux) so libp2p only sees
	// loopback (127.0.0.1, ::1). The TCP connection to bootstrap uses the
	// phone's REAL network interface IP. We extract it and inject it into
	// our address set so identify/DCUtR can share it with other peers.
	n.discoverRealAddrs()

	// Step 2 — reserve relay (identify already done in Phase 2)
	n.reserveRelayOnBootstrap()

	// Step 2b — After relay reservation, construct relay circuit address
	// and add it to our advertised addresses so other peers can find us.
	n.addRelayCircuitAddrs()

	// Step 3 — DHT (optional — used by FindPeer in SendToPeer)
	d, err := dht.New(n.ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		fmt.Printf("p2pmobile: [bg] DHT failed (non-fatal): %v\n", err)
	} else {
		n.mu.Lock()
		n.dht = d
		n.mu.Unlock()
		d.Bootstrap(n.ctx)
		fmt.Printf("p2pmobile: [bg] DHT started\n")
	}

	// Log all addresses after full setup
	var hasIPv6, hasRelay, hasRealIP bool
	for _, addr := range h.Addrs() {
		addrStr := addr.String()
		if strings.Contains(addrStr, "/ip6/") && !strings.Contains(addrStr, "/ip6/::") && !strings.Contains(addrStr, "/ip6/::1") {
			hasIPv6 = true
		}
		if strings.Contains(addrStr, "p2p-circuit") {
			hasRelay = true
		}
		if !strings.Contains(addrStr, "127.0.0.1") && !strings.Contains(addrStr, "/ip6/::1") && !strings.Contains(addrStr, "p2p-circuit") {
			hasRealIP = true
		}
		fmt.Printf("p2pmobile: [bg] advertised addr=%s\n", addrStr)
	}
	fmt.Printf("p2pmobile: [bg] upgrade complete — relay=%v dht=%v addrs=%d hasRealIP=%v hasIPv6=%v hasRelay=%v\n",
		n.relayReady, n.dht != nil, len(h.Addrs()), hasRealIP, hasIPv6, hasRelay)

	// Step 4 — keep-alive (runs forever)
	n.keepAlive()
}

// discoverRealAddrs extracts real IP addresses from active TCP connections.
// This works around Android SELinux blocking net.Interfaces().
func (n *Node) discoverRealAddrs() {
	n.mu.RLock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	n.mu.RUnlock()
	if h == nil {
		return
	}

	// Get the TCP listen port from one of our listen addresses
	var listenPort string
	for _, la := range h.Network().ListenAddresses() {
		laStr := la.String()
		// Extract port from e.g. /ip4/0.0.0.0/tcp/34985
		parts := strings.Split(laStr, "/")
		for i, p := range parts {
			if p == "tcp" && i+1 < len(parts) {
				if strings.Contains(laStr, "/ip4/") {
					listenPort = parts[i+1]
				}
				break
			}
		}
		if listenPort != "" {
			break
		}
	}
	fmt.Printf("p2pmobile: [discover] TCP listen port=%s\n", listenPort)

	for _, info := range bootstrapInfos {
		conns := h.Network().ConnsToPeer(info.ID)
		for _, c := range conns {
			localAddr := c.LocalMultiaddr().String()
			fmt.Printf("p2pmobile: [discover] connection to bootstrap local=%s remote=%s\n",
				localAddr, c.RemoteMultiaddr())

			// Extract our real IP from the local side of the connection
			// localAddr looks like /ip4/192.168.1.5/tcp/54321 (ephemeral port)
			// We want /ip4/192.168.1.5/tcp/<listen_port>
			parts := strings.Split(localAddr, "/")
			var ipVersion, ipAddr string
			for i, p := range parts {
				if (p == "ip4" || p == "ip6") && i+1 < len(parts) {
					ipVersion = p
					ipAddr = parts[i+1]
					break
				}
			}
			if ipAddr == "" || ipAddr == "127.0.0.1" || ipAddr == "::1" {
				continue
			}

			if listenPort != "" {
				// Construct our real address with the listen port
				realAddr := fmt.Sprintf("/%s/%s/tcp/%s", ipVersion, ipAddr, listenPort)
				ma, err := multiaddr.NewMultiaddr(realAddr)
				if err == nil {
					n.addExtraAddr(ma)
					h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
					fmt.Printf("p2pmobile: [discover] real addr from TCP conn: %s\n", realAddr)
				}
			}

			// Also try IPv6 listen port
			var ipv6Port string
			for _, la := range h.Network().ListenAddresses() {
				laStr := la.String()
				if strings.Contains(laStr, "/ip6/") {
					laParts := strings.Split(laStr, "/")
					for i, p := range laParts {
						if p == "tcp" && i+1 < len(laParts) {
							ipv6Port = laParts[i+1]
							break
						}
					}
				}
			}
			if ipVersion == "ip6" && ipv6Port != "" && ipv6Port != listenPort {
				realAddr := fmt.Sprintf("/ip6/%s/tcp/%s", ipAddr, ipv6Port)
				ma, err := multiaddr.NewMultiaddr(realAddr)
				if err == nil {
					n.addExtraAddr(ma)
					h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
				}
			}
		}
	}
}

// addRelayCircuitAddrs constructs relay circuit addresses from active bootstrap
// connections and adds them so other peers can find us via DHT/identify.
func (n *Node) addRelayCircuitAddrs() {
	n.mu.RLock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	relayReady := n.relayReady
	n.mu.RUnlock()
	if h == nil || !relayReady {
		return
	}

	selfID := h.ID()
	for _, info := range bootstrapInfos {
		if h.Network().Connectedness(info.ID) != network.Connected {
			continue
		}
		// Get the bootstrap's actual address from our connection
		conns := h.Network().ConnsToPeer(info.ID)
		for _, c := range conns {
			remoteAddr := c.RemoteMultiaddr()
			// Construct: <bootstrap_addr>/p2p/<bootstrap_id>/p2p-circuit/p2p/<our_id>
			circuitStr := fmt.Sprintf("%s/p2p/%s/p2p-circuit/p2p/%s", remoteAddr, info.ID, selfID)
			circuitMA, err := multiaddr.NewMultiaddr(circuitStr)
			if err == nil {
				n.addExtraAddr(circuitMA)
				h.Peerstore().AddAddr(selfID, circuitMA, peerstore.TempAddrTTL)
				fmt.Printf("p2pmobile: [relay] circuit addr: %s\n", circuitStr)
			}
			break // one per bootstrap peer
		}
	}
}

// reserveRelayOnBootstrap requests a circuit relay v2 reservation so that
// other peers can reach us via /p2p/<relay>/p2p-circuit/p2p/<us>.
func (n *Node) reserveRelayOnBootstrap() {
	n.mu.RLock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	n.mu.RUnlock()
	if h == nil {
		return
	}
	for _, info := range bootstrapInfos {
		if h.Network().Connectedness(info.ID) != network.Connected {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
		rsv, err := circuitv2.Reserve(ctx, h, info)
		cancel()
		if err == nil {
			n.mu.Lock()
			n.relayReady = true
			n.mu.Unlock()
			fmt.Printf("p2pmobile: [bg] relay reserved on %s (expires=%v, addrs=%d)\n",
				info.ID, rsv.Expiration.Sub(time.Now()).Round(time.Second), len(rsv.Addrs))
			return
		}
		fmt.Printf("p2pmobile: [bg] relay reservation FAILED on %s: %v\n", info.ID, err)
	}
}

// ensureBootstrap ensures at least one bootstrap peer is connected.
// Retries with backoff.
func (n *Node) ensureBootstrap() {
	n.mu.RLock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	n.mu.RUnlock()
	if h == nil {
		return
	}
	for attempt := 1; attempt <= 5; attempt++ {
		for _, info := range bootstrapInfos {
			if h.Network().Connectedness(info.ID) == network.Connected {
				return // at least one connected
			}
		}
		if attempt > 1 {
			backoff := time.Duration(attempt*3) * time.Second
			fmt.Printf("p2pmobile: [bg] no bootstrap, retry %d/5 in %v\n", attempt, backoff)
			select {
			case <-n.ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		for _, info := range bootstrapInfos {
			tcpInfo := tcpOnlyPeerInfo(info)
			if len(tcpInfo.Addrs) == 0 {
				tcpInfo = info
			}
			ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
			err := h.Connect(ctx, tcpInfo)
			cancel()
			if err == nil {
				fmt.Printf("p2pmobile: [bg] bootstrap connected %s via TCP (peers=%d)\n",
					info.ID, len(h.Network().Peers()))
				return
			}
			fmt.Printf("p2pmobile: [bg] bootstrap dial failed %s: %v\n", info.ID, err)
		}
	}
	fmt.Println("p2pmobile: [bg] WARNING — all bootstrap attempts failed")
}

// keepAlive monitors bootstrap connectivity and relay reservation health.
func (n *Node) keepAlive() {
	connTicker := time.NewTicker(15 * time.Second)
	defer connTicker.Stop()
	relayTicker := time.NewTicker(25 * time.Minute) // well before 1h expiry
	defer relayTicker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-relayTicker.C:
			n.reserveRelayOnBootstrap()

		case <-connTicker.C:
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
				fmt.Printf("p2pmobile: [keepalive] %s disconnected, reconnecting via TCP...\n", info.ID)
				tcpInfo := tcpOnlyPeerInfo(info)
				if len(tcpInfo.Addrs) == 0 {
					tcpInfo = info
				}
				ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
				err := h.Connect(ctx, tcpInfo)
				cancel()
				if err != nil {
					fmt.Printf("p2pmobile: [keepalive] reconnect FAILED %s: %v\n", info.ID, err)
					continue
				}
				fmt.Printf("p2pmobile: [keepalive] reconnected %s (identify done, peers=%d)\n",
					info.ID, len(h.Network().Peers()))
				// Re-discover real addresses and re-reserve relay after reconnect
				go func() {
					n.discoverRealAddrs()
					n.reserveRelayOnBootstrap()
					n.addRelayCircuitAddrs()
				}()
			}
		}
	}
}

// addExtraAddr adds a multiaddr to the dynamic address set (thread-safe, dedup).
func (n *Node) addExtraAddr(ma multiaddr.Multiaddr) {
	maStr := ma.String()
	n.extraAddrsMu.Lock()
	defer n.extraAddrsMu.Unlock()
	for _, existing := range n.extraAddrs {
		if existing.String() == maStr {
			return // already present
		}
	}
	n.extraAddrs = append(n.extraAddrs, ma)
	fmt.Printf("p2pmobile: ✚ added extra addr: %s (total=%d)\n", maStr, len(n.extraAddrs))
}

// AddExternalAddr lets the host app (Kotlin/Swift) inject an externally
// discovered address — typically an IPv6 address from Android's ConnectivityManager.
// Format: multiaddr string, e.g. "/ip6/2405:201:xxxx/tcp/0"
// If port is 0, it is replaced with the actual TCP listen port for the matching IP version.
func (n *Node) AddExternalAddr(addrStr string) error {
	addrStr = strings.TrimSpace(addrStr)
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return fmt.Errorf("node not started")
	}

	// If port is /tcp/0, replace with actual listen port
	if strings.Contains(addrStr, "/tcp/0") {
		isIPv6 := strings.Contains(addrStr, "/ip6/")
		var listenPort string
		for _, la := range h.Network().ListenAddresses() {
			laStr := la.String()
			matchIP := (isIPv6 && strings.Contains(laStr, "/ip6/")) ||
				(!isIPv6 && strings.Contains(laStr, "/ip4/"))
			if matchIP {
				parts := strings.Split(laStr, "/")
				for i, p := range parts {
					if p == "tcp" && i+1 < len(parts) {
						listenPort = parts[i+1]
						break
					}
				}
				if listenPort != "" {
					break
				}
			}
		}
		if listenPort == "" || listenPort == "0" {
			fmt.Printf("p2pmobile: AddExternalAddr: no listen port found for %s, skipping\n", addrStr)
			return nil
		}
		addrStr = strings.Replace(addrStr, "/tcp/0", "/tcp/"+listenPort, 1)
	}

	ma, err := multiaddr.NewMultiaddr(addrStr)
	if err != nil {
		return fmt.Errorf("bad multiaddr %q: %w", addrStr, err)
	}
	n.addExtraAddr(ma)
	h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
	fmt.Printf("p2pmobile: external addr added: %s\n", ma)
	return nil
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

// IsDirectConnection returns true if the current connection to the given peer
// is direct (not via relay). Used by the UI to show hole-punched status.
func (n *Node) IsDirectConnection(peerID string) bool {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return false
	}
	pid, err := peer.Decode(strings.TrimSpace(peerID))
	if err != nil {
		return false
	}
	conns := h.Network().ConnsToPeer(pid)
	for _, c := range conns {
		if !strings.Contains(c.RemoteMultiaddr().String(), "p2p-circuit") {
			return true // at least one direct connection
		}
	}
	return false
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
// Optimized: reuses pooled streams, compresses large payloads, buffered single-write framing.
//
// Protocol v2 frame format (OptimizedDMProtocol):
//
//	[1 byte flags] [4 bytes length] [payload]
//	flags: 0x01 = compressed (zlib)
//
// Protocol v1 frame format (DirectMessageProtocol, legacy fallback):
//
//	[4 bytes length] [payload]
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

	connected := h.Network().Connectedness(pid) == network.Connected
	fmt.Printf("p2pmobile: SendToPeer to %s connected=%v size=%d\n", pid.String()[:16], connected, len(data))
	if !connected {
		if err := n.dialPeer(pid); err != nil {
			return fmt.Errorf("dial: %w", err)
		}
	}

	// ── Try pooled stream first (zero-RTT for subsequent messages) ──
	if ps := n.getPooledStream(targetPeerID); ps != nil {
		err := n.writeToPooledStream(ps, data)
		if err == nil {
			remoteAddr := ps.stream.Conn().RemoteMultiaddr().String()
			connType := "DIRECT"
			if strings.Contains(remoteAddr, "p2p-circuit") {
				connType = "RELAY"
			}
			fmt.Printf("p2pmobile: SendToPeer OK (pooled) — %d bytes to %s type=%s\n", len(data), pid.String()[:16], connType)
			return nil
		}
		// Pooled stream broken — evict and fall through to new stream
		fmt.Printf("p2pmobile: SendToPeer pooled stream broken for %s: %v — opening new\n", pid.String()[:16], err)
		n.evictPooledStream(targetPeerID)
	}

	// ── Open new stream — try v2 (optimized) first, fall back to v1 ──
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, "relay-dm")

	s, err := h.NewStream(ctx, pid, protocol.ID(OptimizedDMProtocol), protocol.ID(DirectMessageProtocol))
	if err != nil {
		fmt.Printf("p2pmobile: SendToPeer NewStream FAILED to %s: %v\n", pid.String()[:16], err)
		return fmt.Errorf("stream: %w", err)
	}

	negotiated := string(s.Protocol())
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	fmt.Printf("p2pmobile: SendToPeer new stream proto=%s type=%s addr=%s\n", negotiated, connType, remoteAddr)

	if negotiated == OptimizedDMProtocol {
		// v2: pool this stream for reuse + write with compression
		ps := &pooledStream{
			stream:  s,
			writer:  bufio.NewWriterSize(s, 64*1024), // 64KB write buffer
			lastUse: time.Now(),
		}
		n.putPooledStream(targetPeerID, ps)
		if err := n.writeToPooledStream(ps, data); err != nil {
			n.evictPooledStream(targetPeerID)
			return err
		}
		fmt.Printf("p2pmobile: SendToPeer OK (new v2, pooled) — %d bytes to %s\n", len(data), pid.String()[:16])
		return nil
	}

	// v1 legacy: single-use stream, no compression
	defer s.Close()
	length := uint32(len(data))
	// Combine header + body into single write (avoids Nagle delay between 2 packets)
	buf := make([]byte, 4+len(data))
	buf[0] = byte(length >> 24)
	buf[1] = byte(length >> 16)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)
	copy(buf[4:], data)
	if _, err := s.Write(buf); err != nil {
		fmt.Printf("p2pmobile: SendToPeer v1 write FAILED to %s: %v\n", pid.String()[:16], err)
		s.Reset()
		return err
	}
	fmt.Printf("p2pmobile: SendToPeer OK (v1) — %d bytes to %s\n", len(data), pid.String()[:16])
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
	pidShort := pid.String()[:16]

	// ── Step 1: Try direct with peerstore addrs (may include IPv6 from identify) ──
	if addrs := h.Peerstore().Addrs(pid); len(addrs) > 0 {
		var directAddrs []multiaddr.Multiaddr
		for _, a := range addrs {
			if !strings.Contains(a.String(), "p2p-circuit") {
				directAddrs = append(directAddrs, a)
			}
		}
		if len(directAddrs) > 0 {
			fmt.Printf("p2pmobile: [dial] step1 trying %d direct addrs for %s: %v\n", len(directAddrs), pidShort, directAddrs)
			ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
			err := h.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: directAddrs})
			cancel()
			if err == nil {
				conn := h.Network().ConnsToPeer(pid)
				if len(conn) > 0 {
					fmt.Printf("p2pmobile: [dial] step1 DIRECT connected to %s via %s\n", pidShort, conn[0].RemoteMultiaddr())
				}
				return nil
			}
			fmt.Printf("p2pmobile: [dial] step1 direct dial failed for %s: %v\n", pidShort, err)
		}
	}

	// ── Step 2: DHT FindPeer — discovers peer's IPv6 + all public addrs ──
	n.mu.RLock()
	d := n.dht
	n.mu.RUnlock()
	if d != nil {
		fmt.Printf("p2pmobile: [dial] step2 DHT FindPeer for %s\n", pidShort)
		ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
		peerInfo, err := d.FindPeer(ctx, pid)
		cancel()
		if err == nil && len(peerInfo.Addrs) > 0 {
			// Filter to direct (non-relay) addresses, prefer IPv6
			var directAddrs, ipv6Addrs []multiaddr.Multiaddr
			for _, a := range peerInfo.Addrs {
				aStr := a.String()
				if strings.Contains(aStr, "p2p-circuit") {
					continue
				}
				directAddrs = append(directAddrs, a)
				if strings.HasPrefix(aStr, "/ip6/") {
					ipv6Addrs = append(ipv6Addrs, a)
				}
			}
			fmt.Printf("p2pmobile: [dial] step2 DHT found %d addrs (%d IPv6) for %s: %v\n",
				len(directAddrs), len(ipv6Addrs), pidShort, directAddrs)

			// Try IPv6 first (most likely to succeed on 4G/5G)
			if len(ipv6Addrs) > 0 {
				fmt.Printf("p2pmobile: [dial] step2a trying IPv6 direct to %s: %v\n", pidShort, ipv6Addrs)
				ctx2, cancel2 := context.WithTimeout(n.ctx, 5*time.Second)
				err2 := h.Connect(ctx2, peer.AddrInfo{ID: pid, Addrs: ipv6Addrs})
				cancel2()
				if err2 == nil {
					conn := h.Network().ConnsToPeer(pid)
					if len(conn) > 0 {
						fmt.Printf("p2pmobile: [dial] step2a IPv6 DIRECT connected to %s via %s\n", pidShort, conn[0].RemoteMultiaddr())
					}
					return nil
				}
				fmt.Printf("p2pmobile: [dial] step2a IPv6 direct failed for %s: %v\n", pidShort, err2)
			}

			// Try all direct addrs (IPv4 + IPv6)
			if len(directAddrs) > 0 {
				ctx3, cancel3 := context.WithTimeout(n.ctx, 5*time.Second)
				err3 := h.Connect(ctx3, peer.AddrInfo{ID: pid, Addrs: directAddrs})
				cancel3()
				if err3 == nil {
					conn := h.Network().ConnsToPeer(pid)
					if len(conn) > 0 {
						fmt.Printf("p2pmobile: [dial] step2b DIRECT connected to %s via %s\n", pidShort, conn[0].RemoteMultiaddr())
					}
					return nil
				}
				fmt.Printf("p2pmobile: [dial] step2b direct dial failed for %s: %v\n", pidShort, err3)
			}
		} else {
			fmt.Printf("p2pmobile: [dial] step2 DHT FindPeer failed for %s: %v\n", pidShort, err)
		}
	}

	// ── Step 3: Relay fallback (last resort) ──
	fmt.Printf("p2pmobile: [dial] step3 falling back to RELAY for %s\n", pidShort)
	for _, bp := range n.bootstrapPeers {
		relayAddr := fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", bp.ID, pid)
		relayMA, err := multiaddr.NewMultiaddr(relayAddr)
		if err != nil {
			continue
		}
		relayInfo := peer.AddrInfo{ID: pid, Addrs: []multiaddr.Multiaddr{relayMA}}
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		ctx = network.WithAllowLimitedConn(ctx, "relay-dial")
		err = h.Connect(ctx, relayInfo)
		cancel()
		if err == nil {
			fmt.Printf("p2pmobile: [dial] step3 RELAY connected to %s via %s\n", pidShort, bp.ID)
			return nil
		}
		fmt.Printf("p2pmobile: [dial] step3 relay failed to %s via %s: %v\n", pidShort, bp.ID, err)
	}
	return fmt.Errorf("unreachable: %s", pid)
}

func (n *Node) handleDirectStream(s network.Stream) {
	defer s.Close()
	from := s.Conn().RemotePeer().String()
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	fmt.Printf("p2pmobile: incoming direct stream from %s type=%s addr=%s (dir=%s)\n",
		from[:16], connType, remoteAddr, s.Conn().Stat().Direction)

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

// ════════════════════════════════════════════════════════════════════════════
// Stream Pool — reuse TCP streams across messages (zero-RTT after first msg)
// ════════════════════════════════════════════════════════════════════════════

func (n *Node) getPooledStream(peerID string) *pooledStream {
	n.streamPoolMu.Lock()
	defer n.streamPoolMu.Unlock()
	return n.streamPool[peerID]
}

func (n *Node) putPooledStream(peerID string, ps *pooledStream) {
	n.streamPoolMu.Lock()
	defer n.streamPoolMu.Unlock()
	// Close old stream if replacing
	if old, ok := n.streamPool[peerID]; ok && old != ps {
		old.stream.Reset()
	}
	n.streamPool[peerID] = ps
}

func (n *Node) evictPooledStream(peerID string) {
	n.streamPoolMu.Lock()
	ps, ok := n.streamPool[peerID]
	if ok {
		delete(n.streamPool, peerID)
	}
	n.streamPoolMu.Unlock()
	if ok && ps != nil {
		ps.stream.Reset()
	}
}

// streamPoolJanitor evicts idle streams every 30s.
func (n *Node) streamPoolJanitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.streamPoolMu.Lock()
			now := time.Now()
			for pid, ps := range n.streamPool {
				if now.Sub(ps.lastUse) > streamPoolTTL {
					ps.stream.Reset()
					delete(n.streamPool, pid)
					fmt.Printf("p2pmobile: [pool] evicted idle stream for %s\n", pid[:16])
				}
			}
			n.streamPoolMu.Unlock()
		}
	}
}

// writeToPooledStream writes a v2 frame (flags + length + payload) with optional compression.
// Single buffered Flush = single TCP segment for small messages (text, acks).
func (n *Node) writeToPooledStream(ps *pooledStream, data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.lastUse = time.Now()

	var payload []byte
	var flags byte = 0x00

	// Compress payloads above threshold (images, voice notes benefit hugely)
	if len(data) > compressionThreshold {
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		w.Write(data)
		w.Close()
		compressed := buf.Bytes()
		// Only use compression if it actually shrinks the data
		if len(compressed) < len(data)-64 {
			payload = compressed
			flags = 0x01
			fmt.Printf("p2pmobile: [compress] %d → %d bytes (%.0f%% reduction)\n",
				len(data), len(compressed), 100*(1-float64(len(compressed))/float64(len(data))))
		} else {
			payload = data
		}
	} else {
		payload = data
	}

	length := uint32(len(payload))
	// Write: [flags:1] [length:4] [payload:N] — all into buffer, single Flush
	ps.writer.WriteByte(flags)
	ps.writer.WriteByte(byte(length >> 24))
	ps.writer.WriteByte(byte(length >> 16))
	ps.writer.WriteByte(byte(length >> 8))
	ps.writer.WriteByte(byte(length))
	ps.writer.Write(payload)
	return ps.writer.Flush() // single TCP write
}

// ════════════════════════════════════════════════════════════════════════════
// handleOptimizedStream — v2 protocol receiver (persistent stream, compression)
// ════════════════════════════════════════════════════════════════════════════

func (n *Node) handleOptimizedStream(s network.Stream) {
	// v2 streams are persistent — do NOT close on return. Read in a loop.
	from := s.Conn().RemotePeer().String()
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	fmt.Printf("p2pmobile: incoming v2 stream from %s type=%s addr=%s\n", from[:16], connType, remoteAddr)

	reader := bufio.NewReaderSize(s, 64*1024)
	for {
		// Read frame: [flags:1] [length:4] [payload]
		flagsByte, err := reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("p2pmobile: v2 stream closed from %s: %v\n", from[:16], err)
			}
			return
		}
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(reader, hdr); err != nil {
			fmt.Printf("p2pmobile: v2 length read error from %s: %v\n", from[:16], err)
			return
		}
		length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
		if length > maxMessageSize {
			fmt.Printf("p2pmobile: v2 message too large from %s: %d bytes\n", from[:16], length)
			return
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			fmt.Printf("p2pmobile: v2 body read error from %s: %v\n", from[:16], err)
			return
		}

		// Decompress if flagged
		data := payload
		if flagsByte&0x01 != 0 {
			r, err := zlib.NewReader(bytes.NewReader(payload))
			if err != nil {
				fmt.Printf("p2pmobile: v2 zlib init error from %s: %v\n", from[:16], err)
				continue
			}
			decompressed, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				fmt.Printf("p2pmobile: v2 zlib read error from %s: %v\n", from[:16], err)
				continue
			}
			data = decompressed
			fmt.Printf("p2pmobile: [decompress] %d → %d bytes from %s\n", len(payload), len(data), from[:16])
		}

		fmt.Printf("p2pmobile: v2 received %d bytes from %s\n", len(data), from[:16])
		if h := n.directHandler; h != nil {
			h.OnDirectMessage(from, data)
		}
		if h := n.msgHandler; h != nil {
			h.OnMessage(from, "direct", data)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// OnNetworkChanged — called by Kotlin when Android detects a network change
// ════════════════════════════════════════════════════════════════════════════

// OnNetworkChanged should be called when the device's network changes (WiFi↔cellular).
// It clears stale addresses, re-discovers real IPs, reconnects bootstrap, re-reserves
// relay, and triggers identify push so remote peers learn our new addresses immediately.
func (n *Node) OnNetworkChanged() {
	n.mu.RLock()
	h := n.host
	running := n.running
	n.mu.RUnlock()
	if !running || h == nil {
		return
	}
	fmt.Println("p2pmobile: ⚡ OnNetworkChanged — refreshing addresses and connections")

	// 1. Clear ALL stale extra addresses (old WiFi/cellular IPs are now dead)
	n.extraAddrsMu.Lock()
	oldCount := len(n.extraAddrs)
	n.extraAddrs = nil
	n.extraAddrsMu.Unlock()
	fmt.Printf("p2pmobile: [netchange] cleared %d stale extra addrs\n", oldCount)

	// 2. Evict ALL pooled streams (old TCP connections are dead after network change)
	n.streamPoolMu.Lock()
	poolCount := len(n.streamPool)
	for pid, ps := range n.streamPool {
		ps.stream.Reset()
		delete(n.streamPool, pid)
	}
	n.streamPoolMu.Unlock()
	fmt.Printf("p2pmobile: [netchange] evicted %d pooled streams\n", poolCount)

	// 3. Close stale connections to non-bootstrap peers (their addrs are now wrong for us)
	for _, p := range h.Network().Peers() {
		isBootstrap := false
		for _, bp := range n.bootstrapPeers {
			if bp.ID == p {
				isBootstrap = true
				break
			}
		}
		if !isBootstrap {
			_ = h.Network().ClosePeer(p)
		}
	}

	// 4. Reconnect to bootstrap (new network → new TCP connection → new local addr)
	n.ensureBootstrap()

	// 5. Re-discover real addresses from the new bootstrap connection
	n.discoverRealAddrs()

	// 6. Re-reserve relay slot (old reservation used old connection)
	n.reserveRelayOnBootstrap()
	n.addRelayCircuitAddrs()

	// 7. Push updated identify to bootstrap so DHT has our fresh addresses.
	// h.Connect already ran identify during ensureBootstrap, but we can also
	// trigger a push for any currently-connected peer.
	// SignalAddressChange is on BasicHost, not the Host interface — type-assert.
	if bh, ok := h.(*basichost.BasicHost); ok {
		bh.SignalAddressChange()
		fmt.Println("p2pmobile: [netchange] triggered identify address change signal")
	}

	// Log final state
	var hasIPv6, hasRelay bool
	for _, addr := range h.Addrs() {
		aStr := addr.String()
		if strings.Contains(aStr, "/ip6/") && !strings.Contains(aStr, "::1") {
			hasIPv6 = true
		}
		if strings.Contains(aStr, "p2p-circuit") {
			hasRelay = true
		}
	}
	fmt.Printf("p2pmobile: [netchange] done — addrs=%d ipv6=%v relay=%v peers=%d\n",
		len(h.Addrs()), hasIPv6, hasRelay, len(h.Network().Peers()))
}
