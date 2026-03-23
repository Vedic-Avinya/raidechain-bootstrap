// Package p2pmobile provides a gomobile-bindable libp2p node for Android/iOS.
//
// Features:
//   - Hybrid QUIC (UDP) + TCP transports: QUIC first, TCP fallback
//   - GossipSub topic pub/sub (backward-compat with existing chat)
//   - Direct peer-to-peer streams (no relay needed when NAT allows)
//   - DHT peer discovery
//   - NAT traversal: port mapping, hole punching, circuit relay v2 fallback
//
// Hybrid Transport Strategy:
//   - Listen on QUIC (UDP) + TCP for both IPv4 and IPv6
//   - QUIC preferred for lower latency and better mobile performance
//   - TCP fallback for NAT/unreachable scenarios
//   - Bootstrap always uses TCP for reliability
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
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	circuitv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

// directDialPasses is how many times we retry peerstore+DHT direct dials before circuit relay.
// Jittered backoff between passes lets addresses/identify propagate (mobile handover).
// Note: "99.99% direct" is not achievable on the open internet (CGNAT, offline peers, firewalls);
// relay remains the correctness fallback; these passes maximize direct when paths exist.
const directDialPasses = 3

// dmWriterBufferSize is the pooled v2 stream bufio size — larger = fewer syscalls for blob batches.
const dmWriterBufferSize = 512 * 1024

const (
	// DirectMessageProtocol is the libp2p stream protocol for 1-to-1 messages.
	DirectMessageProtocol = "/ridechain/dm/1.0.0"
	// OptimizedDMProtocol adds compression + framing. Peers negotiate the best version.
	OptimizedDMProtocol = "/ridechain/dm/2.0.0"
	maxMessageSize      = 20 * 1024 * 1024 // 20 MB
	identityKeyFile     = "p2p_identity.key"
	// compressionThreshold: payloads above this size may be zlib-compressed (skipped if already JPEG/PNG/WebP/etc.).
	compressionThreshold = 2560
	// streamPoolTTL: how long an idle pooled stream stays alive.
	streamPoolTTL = 60 * time.Second
	// Direct batch (JNI): packed format [4B BE count][4B BE len][payload]×N — one Go entry, many v2 frames.
	maxBatchFrames     = 512
	maxBatchTotalBytes = 24 * 1024 * 1024 // cap total plaintext per batch (below sum of maxMessageSize)
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

	ingressMu     sync.Mutex
	ingressBudget map[string]*peerIngressBudget

	// directProbeLast avoids hammering parallel direct dials while on relay (debounce per peer).
	directProbeMu   sync.Mutex
	directProbeLast map[string]time.Time
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
		topics:          make(map[string]*pubsub.Topic),
		subs:            make(map[string]*pubsub.Subscription),
		streamPool:      make(map[string]*pooledStream),
		directProbeLast: make(map[string]time.Time),
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

	// ── Phase 1: Create host with OPTIMIZED transports for mobile ──────
	// Mobile networks behind CGNAT: QUIC has high latency or fails.
	// TCP is reliable and works through CGNAT.
	//
	// Strategy:
	//   - TCP with UDP fallback (not QUIC first)
	//   - Connection-oriented: reliable for messaging
	//   - Works through CGNAT and firewalls
	h, err := libp2p.New(
		libp2p.Identity(priv),
		// TCP primary - works reliably on mobile CGNAT
		// Note: libp2p will still try UDP/QUIC internally, but TCP is preferred
		libp2p.Transport(tcp.NewTCPTransport),
		// Also listen on UDP for cases where UDP is not blocked
		libp2p.Transport(libp2pquic.NewTransport),
		// Listen on multiple ports
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",         // TCP primary (CGNAT friendly)
			"/ip6/::/tcp/0",              // IPv6 TCP
			"/ip4/0.0.0.0/udp/0/quic-v1", // UDP fallback for local/private networks
			"/ip6/::/udp/0/quic-v1",
		),
		// Enable Relay (Circuit v2) and Hole Punching for NAT traversal
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		// Custom address factory to inject externally discovered addresses
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
			isQUIC := strings.Contains(remoteAddr, "/udp/")
			transportType := "TCP"
			if isQUIC {
				transportType = "QUIC"
			}
			fmt.Printf("p2pmobile: ⚡ CONNECTED peer=%s type=%s transport=%s ipv6=%v addr=%s dir=%s\n",
				remotePeer[:16], connType, transportType, isIPv6, remoteAddr, c.Stat().Direction)

			// When we connect to a peer, inject their QUIC address into peerstore
			// This way, future dials can try QUIC directly
			if !n.isBootstrapPeerID(c.RemotePeer()) {
				n.injectPeerQUICAddr(c)
			}

			if connType == "RELAY" && !n.isBootstrapPeerID(c.RemotePeer()) {
				n.maybeScheduleDirectProbe(remotePeer)
			}
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
							// QUIC-first with TCP fallback
							quicInfo := quicOnlyPeerInfo(pi)
							if len(quicInfo.Addrs) > 0 {
								ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
								err := n.host.Connect(ctx, quicInfo)
								cancel()
								if err == nil {
									fmt.Printf("p2pmobile: [auto-reconnect] QUIC reconnected %s\n", pi.ID.String()[:16])
									n.discoverRealAddrs()
									n.reserveRelayOnBootstrap()
									n.addRelayCircuitAddrs()
									return
								}
								fmt.Printf("p2pmobile: [auto-reconnect] QUIC failed %s: %v\n", pi.ID.String()[:16], err)
							}
							// Fallback to TCP
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

	// ── Phase 2: Bootstrap in BACKGROUND ──────────────────────────────────
	// DO NOT block Start() on bootstrap connection.
	// Initialize GossipSub first so node is usable immediately.
	if err := n.initGossipSubAndReady(h); err != nil {
		return err
	}

	// Bootstrap peers are added to peerstore for connection attempts
	for _, info := range bootstrapInfos {
		h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
		h.ConnManager().Protect(info.ID, "bootstrap")
	}

	// Start bootstrap in background - returns immediately
	go n.bootstrapWithRetry()

	return nil
}

func (n *Node) bootstrapWithRetry() {
	n.mu.Lock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	ctx := n.ctx
	n.mu.Unlock()

	if h == nil || len(bootstrapInfos) == 0 {
		fmt.Printf("p2pmobile: [bootstrap] no host or no bootstrap peers, skipping\n")
		return
	}

	// Exponential backoff: start at 1s, cap at 30s
	delay := time.Second
	maxDelay := 30 * time.Second
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("p2pmobile: [bootstrap] context cancelled, stopping\n")
			return
		default:
		}

		attempt++
		fmt.Printf("p2pmobile: [bootstrap] attempt %d connecting to %d bootstrap peers...\n", attempt, len(bootstrapInfos))

		var bootstrapConnected bool
		for _, info := range bootstrapInfos {
			if !n.running {
				return
			}

			// TCP-first: CGNAT mobile networks block incoming UDP, so TCP works but QUIC doesn't
			// QUIC is tried with short timeout in case UDP happens to work

			// Try TCP first (faster on CGNAT networks)
			tcpInfo := tcpOnlyPeerInfo(info)
			if len(tcpInfo.Addrs) == 0 {
				tcpInfo = info
			}
			fmt.Printf("p2pmobile: [bootstrap] trying TCP %s addrs=%v\n", tcpInfo.ID, tcpInfo.Addrs)
			t0 := time.Now()
			ctx10, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := h.Connect(ctx10, tcpInfo)
			cancel()

			connectedInfo := tcpInfo
			if err != nil {
				fmt.Printf("p2pmobile: [bootstrap] TCP failed %s after %v: %v, trying QUIC...\n",
					tcpInfo.ID, time.Since(t0).Round(time.Millisecond), err)

				// Fall back to QUIC with short timeout (in case UDP works)
				quicInfo := quicOnlyPeerInfo(info)
				if len(quicInfo.Addrs) == 0 {
					quicInfo = info
				}
				t0 = time.Now()
				ctx3, cancel2 := context.WithTimeout(ctx, 3*time.Second)
				err = h.Connect(ctx3, quicInfo)
				cancel2()
				connectedInfo = quicInfo

				if err != nil {
					fmt.Printf("p2pmobile: [bootstrap] QUIC also failed %s after %v: %v\n",
						quicInfo.ID, time.Since(t0).Round(time.Millisecond), err)
					continue
				}
			}

			// Connected!
			fmt.Printf("p2pmobile: [bootstrap] ✓ connected to %s in %v via %s\n",
				connectedInfo.ID, time.Since(t0).Round(time.Millisecond),
				func() string {
					if strings.Contains(connectedInfo.Addrs[0].String(), "/quic") {
						return "QUIC"
					}
					return "TCP"
				}())
			bootstrapConnected = true

			// Notify connection handler
			if ch := n.connHandler; ch != nil {
				ch.OnPeerConnected(connectedInfo.ID.String())
			}

			// Trigger address discovery and relay reservation
			n.discoverRealAddrs()
			n.reserveRelayOnBootstrap()
			n.addRelayCircuitAddrs()
			break
		}

		if bootstrapConnected {
			fmt.Printf("p2pmobile: [bootstrap] ✓ SUCCESS, connected after %d attempts\n", attempt)
			return
		}

		fmt.Printf("p2pmobile: [bootstrap] ✗ all bootstrap attempts failed, retrying in %v\n", delay)
		time.Sleep(delay)
		delay = delay * 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func (n *Node) initGossipSubAndReady(h host.Host) error {
	// ── Phase 3: GossipSub ───────────────────────────────────────
	ps, err := pubsub.NewGossipSub(n.ctx, h, pubsub.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		h.Close()
		return fmt.Errorf("gossipsub: %w", err)
	}
	n.ps = ps

	n.running = true
	// Log all listen addresses with transport type
	var quicListeners, tcpListeners int
	for _, addr := range h.Addrs() {
		transport := "TCP"
		if strings.Contains(addr.String(), "/udp/") {
			transport = "QUIC"
			quicListeners++
		} else {
			tcpListeners++
		}
		fmt.Printf("p2pmobile: LISTEN addr=%s transport=%s\n", addr, transport)
	}
	fmt.Printf("p2pmobile: node READY — peerID=%s listeners={QUIC:%d TCP:%d}\n",
		h.ID(), quicListeners, tcpListeners)

	// ── Phase 4: Background upgrade ──────────────────────────────────
	go n.backgroundUpgrade()

	return nil
}

// isPrivateCGNAT returns true if this IPv4 address is in a private/CGNAT range.
// Mobile carriers commonly use 10.x.x.x ranges behind CGNAT.
func isPrivateCGNAT(ip string) bool {
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return false
	}
	first, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	// 10.0.0.0/8 - common CGNAT range
	if first == 10 {
		return true
	}
	// 172.16.0.0/12 - common in enterprise/CGNAT
	if first == 172 {
		second, err := strconv.Atoi(parts[1])
		if err == nil && second >= 16 && second <= 31 {
			return true
		}
	}
	// 192.168.0.0/16 - common private
	if first == 192 {
		second, err := strconv.Atoi(parts[1])
		if err == nil && second == 168 {
			return true
		}
	}
	return false
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
func tcpOnlyPeerInfo(info peer.AddrInfo) peer.AddrInfo {
	var tcpAddrs []multiaddr.Multiaddr
	for _, a := range info.Addrs {
		for _, p := range a.Protocols() {
			if p.Code == multiaddr.P_TCP {
				tcpAddrs = append(tcpAddrs, a)
				break
			}
		}
	}
	return peer.AddrInfo{ID: info.ID, Addrs: tcpAddrs}
}

// quicOnlyPeerInfo returns a copy of info with only QUIC (UDP) multiaddrs.
func quicOnlyPeerInfo(info peer.AddrInfo) peer.AddrInfo {
	var quicAddrs []multiaddr.Multiaddr
	for _, a := range info.Addrs {
		if strings.Contains(a.String(), "/quic") {
			quicAddrs = append(quicAddrs, a)
		}
	}
	return peer.AddrInfo{ID: info.ID, Addrs: quicAddrs}
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
	d, err := dht.New(n.ctx, h,
		dht.Mode(dht.ModeClient),
		dht.Concurrency(6),
	)
	if err != nil {
		fmt.Printf("p2pmobile: [bg] DHT failed (non-fatal): %v\n", err)
	} else {
		n.mu.Lock()
		n.dht = d
		n.mu.Unlock()
		d.Bootstrap(n.ctx)
		fmt.Printf("p2pmobile: [bg] DHT started\n")

		// Explicitly provide our addresses to DHT
		go n.provideAddrsToDHT()
	}

	// Log all addresses after full setup
	var hasIPv6, hasRelay, hasRealIP bool
	var quicAddrs, tcpAddrs int
	for _, addr := range h.Addrs() {
		addrStr := addr.String()
		if strings.Contains(addrStr, "/udp/") {
			quicAddrs++
		} else {
			tcpAddrs++
		}
		if strings.Contains(addrStr, "/ip6/") && !strings.Contains(addrStr, "/ip6/::") && !strings.Contains(addrStr, "/ip6/::1") {
			hasIPv6 = true
		}
		if strings.Contains(addrStr, "p2p-circuit") {
			hasRelay = true
		}
		if !strings.Contains(addrStr, "127.0.0.1") && !strings.Contains(addrStr, "/ip6/::1") && !strings.Contains(addrStr, "p2p-circuit") {
			hasRealIP = true
		}
		transport := "TCP"
		if strings.Contains(addrStr, "/udp/") {
			transport = "QUIC"
		}
		fmt.Printf("p2pmobile: [bg] advertised addr=%s transport=%s\n", addrStr, transport)
	}
	fmt.Printf("p2pmobile: [bg] upgrade complete — relay=%v dht=%v addrs={QUIC:%d TCP:%d} hasRealIP=%v hasIPv6=%v hasRelay=%v\n",
		n.relayReady, n.dht != nil, quicAddrs, tcpAddrs, hasRealIP, hasIPv6, hasRelay)

	// Step 4 — keep-alive (runs forever)
	n.keepAlive()
}

// discoverRealAddrs extracts real IP addresses from active connections.
// This works around Android SELinux blocking net.Interfaces().
func (n *Node) discoverRealAddrs() {
	n.mu.RLock()
	h := n.host
	bootstrapInfos := n.bootstrapPeers
	n.mu.RUnlock()
	if h == nil {
		return
	}

	// Get the listen ports from our listen addresses (TCP and QUIC)
	var tcpPort, quicPort string
	var quicListeners, tcpListeners int
	for _, la := range h.Network().ListenAddresses() {
		laStr := la.String()
		parts := strings.Split(laStr, "/")
		for i, p := range parts {
			if i+1 < len(parts) {
				if strings.Contains(laStr, "/quic") && p == "udp" {
					if quicPort == "" {
						quicPort = parts[i+1]
					}
					quicListeners++
				} else if p == "tcp" && strings.Contains(laStr, "/ip4/") {
					if tcpPort == "" {
						tcpPort = parts[i+1]
					}
					tcpListeners++
				}
			}
		}
	}
	fmt.Printf("p2pmobile: [discover] listeners: QUIC=%d (port=%s) TCP=%d (port=%s)\n",
		quicListeners, quicPort, tcpListeners, tcpPort)

	for _, info := range bootstrapInfos {
		conns := h.Network().ConnsToPeer(info.ID)
		for _, c := range conns {
			localAddr := c.LocalMultiaddr().String()
			remoteAddr := c.RemoteMultiaddr().String()
			fmt.Printf("p2pmobile: [discover] bootstrap conn local=%s remote=%s\n", localAddr, remoteAddr)

			// Extract our real IP from the local side of the connection
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

			// CGNAT/private IPs cannot receive incoming UDP, so skip them for QUIC
			isCGNAT := ipVersion == "ip4" && isPrivateCGNAT(ipAddr)
			if isCGNAT {
				fmt.Printf("p2pmobile: [discover] ⚠ CGNAT IP %s skipped for QUIC (no incoming UDP)\n", ipAddr)
			}

			// Add QUIC address only if NOT CGNAT (UDP requires public/routable IP)
			if quicPort != "" && !isCGNAT {
				realAddr := fmt.Sprintf("/%s/%s/udp/%s/quic-v1", ipVersion, ipAddr, quicPort)
				ma, err := multiaddr.NewMultiaddr(realAddr)
				if err == nil {
					n.addExtraAddr(ma)
					h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
					fmt.Printf("p2pmobile: [discover] ✚ QUIC addr discovered: %s\n", realAddr)
				}
			}

			// Add TCP address (works through CGNAT)
			if tcpPort != "" {
				realAddr := fmt.Sprintf("/%s/%s/tcp/%s", ipVersion, ipAddr, tcpPort)
				ma, err := multiaddr.NewMultiaddr(realAddr)
				if err == nil {
					n.addExtraAddr(ma)
					h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
					fmt.Printf("p2pmobile: [discover] ✚ TCP addr discovered: %s\n", realAddr)
				}
			}
		}
	}
}

// injectPeerQUICAddr extracts the peer's QUIC address from the TCP connection
// and injects it into the peerstore. This ensures future dials can try QUIC.
// Only works for non-relay, non-bootstrap connections.
func (n *Node) injectPeerQUICAddr(c network.Conn) {
	n.mu.RLock()
	h := n.host
	bootstrapIDs := make(map[peer.ID]bool)
	for _, bp := range n.bootstrapPeers {
		bootstrapIDs[bp.ID] = true
	}
	selfID := h.ID()
	n.mu.RUnlock()
	if h == nil {
		return
	}

	pid := c.RemotePeer()

	// Skip bootstrap peers and self
	if bootstrapIDs[pid] {
		fmt.Printf("p2pmobile: [peer-quic] skipping bootstrap peer %s\n", pid.String()[:16])
		return
	}
	if pid == selfID {
		fmt.Printf("p2pmobile: [peer-quic] skipping self %s\n", pid.String()[:16])
		return
	}

	remoteAddr := c.RemoteMultiaddr().String()

	// Skip relay connections
	if strings.Contains(remoteAddr, "p2p-circuit") {
		fmt.Printf("p2pmobile: [peer-quic] skipping relay conn to %s\n", pid.String()[:16])
		return
	}

	// Extract IP and port from the TCP connection
	parts := strings.Split(remoteAddr, "/")
	var ipVersion, ipAddr string
	for i, p := range parts {
		if (p == "ip4" || p == "ip6") && i+1 < len(parts) {
			ipVersion = p
			ipAddr = parts[i+1]
			break
		}
	}
	if ipAddr == "" {
		fmt.Printf("p2pmobile: [peer-quic] failed to extract IP from %s\n", remoteAddr)
		return
	}

	// Get our QUIC listen port for the same IP version
	quicPort := ""
	for _, la := range h.Network().ListenAddresses() {
		laStr := la.String()
		if !strings.Contains(laStr, "/quic") {
			continue
		}
		if (ipVersion == "ip6" && strings.Contains(laStr, "/ip6/") && !strings.Contains(laStr, "/ip6/::1")) ||
			(ipVersion == "ip4" && strings.Contains(laStr, "/ip4/") && !strings.Contains(laStr, "/ip4/127.0.0.1")) {
			laParts := strings.Split(laStr, "/")
			for j, p := range laParts {
				if p == "udp" && j+1 < len(laParts) {
					quicPort = laParts[j+1]
					break
				}
			}
			if quicPort != "" {
				break
			}
		}
	}
	if quicPort == "" {
		fmt.Printf("p2pmobile: [peer-quic] no QUIC port for %s from %s\n", ipVersion, remoteAddr)
		return
	}

	// Skip CGNAT/private IPs - they can't receive incoming UDP
	if ipVersion == "ip4" && isPrivateCGNAT(ipAddr) {
		fmt.Printf("p2pmobile: [peer-quic] skipping CGNAT IP %s for QUIC inject\n", ipAddr)
		return
	}

	// Construct QUIC address
	quicAddrStr := fmt.Sprintf("/%s/%s/udp/%s/quic-v1", ipVersion, ipAddr, quicPort)
	quicMA, err := multiaddr.NewMultiaddr(quicAddrStr)
	if err != nil {
		fmt.Printf("p2pmobile: [peer-quic] failed to construct QUIC addr %s: %v\n", quicAddrStr, err)
		return
	}

	// Check if we already have this address
	existingAddrs := h.Peerstore().Addrs(pid)
	for _, existing := range existingAddrs {
		if existing.String() == quicAddrStr {
			fmt.Printf("p2pmobile: [peer-quic] already have %s for %s\n", quicAddrStr, pid.String()[:16])
			return
		}
	}

	// Add to peerstore so future dials will try QUIC first
	h.Peerstore().AddAddr(pid, quicMA, peerstore.TempAddrTTL)
	fmt.Printf("p2pmobile: [peer-quic] ✚ injected QUIC addr for %s: %s (from TCP %s)\n",
		pid.String()[:16], quicAddrStr, remoteAddr)
}

// provideAddrsToDHT explicitly announces our addresses via the routing system.
// Addresses are exchanged through the identify protocol, but this forces a refresh.
func (n *Node) provideAddrsToDHT() {
	n.mu.RLock()
	h := n.host
	d := n.dht
	n.mu.RUnlock()
	if h == nil || d == nil {
		return
	}

	// Wait for DHT to be bootstrapped
	time.Sleep(5 * time.Second)

	selfID := h.ID()
	addrs := h.Addrs()

	var quicAddrs []string
	var tcpAddrs []string
	for _, addr := range addrs {
		addrStr := addr.String()
		if strings.Contains(addrStr, "127.0.0.1") || strings.Contains(addrStr, "/ip6/::1") {
			continue
		}
		if strings.Contains(addrStr, "p2p-circuit") {
			continue
		}
		if strings.Contains(addrStr, "/udp/") {
			quicAddrs = append(quicAddrs, addrStr)
		} else {
			tcpAddrs = append(tcpAddrs, addrStr)
		}
	}

	fmt.Printf("p2pmobile: [dht-provide] advertising %d QUIC and %d TCP addrs for self=%s\n",
		len(quicAddrs), len(tcpAddrs), selfID.String()[:16])
	fmt.Printf("p2pmobile: [dht-provide] QUIC addrs: %v\n", quicAddrs)
	fmt.Printf("p2pmobile: [dht-provide] TCP addrs: %v\n", tcpAddrs)

	// Trigger peerstore refresh to ensure addresses are advertised
	for _, pi := range h.Network().Peers() {
		if pi == h.ID() {
			continue
		}
		// Refresh routing table entry
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		_, err := d.FindPeer(ctx, h.ID())
		cancel()
		if err != nil {
			// Expected to fail for self, ignore
		}
	}

	// Log our peerstore addresses
	peerAddrs := h.Peerstore().Addrs(h.ID())
	fmt.Printf("p2pmobile: [dht-provide] peerstore has %d addrs for self\n", len(peerAddrs))

	fmt.Printf("p2pmobile: [dht-provide] completed\n")
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
// Retries with backoff using hybrid QUIC-first strategy.
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
			// Hybrid: Try QUIC first, then TCP
			quicInfo := quicOnlyPeerInfo(info)
			if len(quicInfo.Addrs) > 0 {
				ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
				err := h.Connect(ctx, quicInfo)
				cancel()
				if err == nil {
					fmt.Printf("p2pmobile: [bg] bootstrap connected %s via QUIC (peers=%d)\n",
						info.ID, len(h.Network().Peers()))
					return
				}
				fmt.Printf("p2pmobile: [bg] bootstrap QUIC dial failed %s: %v\n", info.ID, err)
			}

			// Fallback to TCP
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
			fmt.Printf("p2pmobile: [bg] bootstrap TCP dial failed %s: %v\n", info.ID, err)
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
				fmt.Printf("p2pmobile: [keepalive] %s disconnected, reconnecting (QUIC-first)...\n", info.ID)

				// Hybrid: Try QUIC first, then TCP
				quicInfo := quicOnlyPeerInfo(info)
				if len(quicInfo.Addrs) > 0 {
					ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
					err := h.Connect(ctx, quicInfo)
					cancel()
					if err == nil {
						fmt.Printf("p2pmobile: [keepalive] QUIC reconnected %s (peers=%d)\n",
							info.ID, len(h.Network().Peers()))
						go func() {
							n.discoverRealAddrs()
							n.reserveRelayOnBootstrap()
							n.addRelayCircuitAddrs()
						}()
						continue
					}
					fmt.Printf("p2pmobile: [keepalive] QUIC reconnect failed %s: %v\n", info.ID, err)
				}

				// Fallback to TCP
				tcpInfo := tcpOnlyPeerInfo(info)
				if len(tcpInfo.Addrs) == 0 {
					tcpInfo = info
				}
				ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
				err := h.Connect(ctx, tcpInfo)
				cancel()
				if err != nil {
					fmt.Printf("p2pmobile: [keepalive] TCP reconnect FAILED %s: %v\n", info.ID, err)
					continue
				}
				fmt.Printf("p2pmobile: [keepalive] TCP reconnected %s (peers=%d)\n",
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

// hasDirectConnToPeer reports whether any open connection to [pid] is not a relay circuit.
func (n *Node) hasDirectConnToPeer(pid peer.ID) bool {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return false
	}
	for _, c := range h.Network().ConnsToPeer(pid) {
		if !strings.Contains(c.RemoteMultiaddr().String(), "p2p-circuit") {
			return true
		}
	}
	return false
}

// IsDirectConnection returns true if the current connection to the given peer
// is direct (not via relay). Used by the UI to show hole-punched status.
func (n *Node) IsDirectConnection(peerID string) bool {
	pid, err := peer.Decode(strings.TrimSpace(peerID))
	if err != nil {
		return false
	}
	return n.hasDirectConnToPeer(pid)
}

func (n *Node) isBootstrapPeerID(id peer.ID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, bp := range n.bootstrapPeers {
		if bp.ID == id {
			return true
		}
	}
	return false
}

// maybeScheduleDirectProbe starts a delayed peerstore+DHT direct dial while a relay path is up.
// Relay stays connected if the probe fails (reachability, NAT). Debounced per peer.
func (n *Node) maybeScheduleDirectProbe(peerIDStr string) {
	pid, err := peer.Decode(strings.TrimSpace(peerIDStr))
	if err != nil {
		return
	}
	if n.isBootstrapPeerID(pid) {
		return
	}
	n.directProbeMu.Lock()
	now := time.Now()
	if t, ok := n.directProbeLast[peerIDStr]; ok && now.Sub(t) < 45*time.Second {
		n.directProbeMu.Unlock()
		return
	}
	if len(n.directProbeLast) > 500 {
		n.directProbeLast = make(map[string]time.Time)
	}
	n.directProbeLast[peerIDStr] = now
	n.directProbeMu.Unlock()

	go func() {
		time.Sleep(1800 * time.Millisecond)
		n.mu.RLock()
		running := n.running
		h := n.host
		n.mu.RUnlock()
		if !running || h == nil {
			return
		}
		if h.Network().Connectedness(pid) != network.Connected {
			return
		}
		if n.hasDirectConnToPeer(pid) {
			return
		}
		short := peerIDStr
		if len(short) > 16 {
			short = short[:16]
		}
		fmt.Printf("p2pmobile: [probe] relay-only to %s — parallel direct dial\n", short)
		_ = n.tryDialPeerDirect(pid)
	}()
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
	if err := topic.Publish(n.ctx, data); err != nil {
		return err
	}
	logP2pTransport("Publish PATH=GOSSIPSUB_TOPIC topic=%s bytes=%d — mesh broadcast; peers may receive duplicates",
		topicName, len(data))
	return nil
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
			from := msg.ReceivedFrom.String()
			short := from
			if len(short) > 16 {
				short = short[:16]
			}
			logP2pTransport("incoming PATH=GOSSIPSUB from=%s… topic=%s bytes=%d (mesh — same frame may arrive multiple times)",
				short, topicName, len(msg.Data))
			if h := n.msgHandler; h != nil {
				h.OnMessage(from, topicName, msg.Data)
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

// SendToPeer sends one application payload on the direct-message stream (v2 pooled when available).
func (n *Node) SendToPeer(targetPeerID string, data []byte) error {
	return n.sendDirectFrames(targetPeerID, [][]byte{data})
}

// SendBatchToPeer unpacks a JNI batch and sends each payload as its own v2 wire frame on the same pooled stream.
// Packed format (big-endian):
//
//	[uint32 count][uint32 len0][payload0][uint32 len1][payload1]...
//
// Limits: count <= maxBatchFrames, each len <= maxMessageSize, sum(len) <= maxBatchTotalBytes.
// One JNI call → one dial/pool path → N framed writes (fewer Java→Go crossings than N×SendToPeer).
func (n *Node) SendBatchToPeer(targetPeerID string, packed []byte) error {
	frames, err := unpackDirectBatch(packed)
	if err != nil {
		return err
	}
	return n.sendDirectFrames(targetPeerID, frames)
}

func unpackDirectBatch(packed []byte) ([][]byte, error) {
	if len(packed) < 4 {
		return nil, fmt.Errorf("batch: too short")
	}
	count := int(binary.BigEndian.Uint32(packed[0:4]))
	if count < 1 || count > maxBatchFrames {
		return nil, fmt.Errorf("batch: invalid count %d", count)
	}
	off := 4
	sum := 0
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		if off+4 > len(packed) {
			return nil, fmt.Errorf("batch: truncated header at frame %d", i)
		}
		ln := int(binary.BigEndian.Uint32(packed[off : off+4]))
		off += 4
		if ln < 0 || ln > maxMessageSize {
			return nil, fmt.Errorf("batch: invalid frame len %d", ln)
		}
		sum += ln
		if sum > maxBatchTotalBytes {
			return nil, fmt.Errorf("batch: total size exceeds limit")
		}
		if off+ln > len(packed) {
			return nil, fmt.Errorf("batch: truncated payload at frame %d", i)
		}
		frame := make([]byte, ln)
		copy(frame, packed[off:off+ln])
		out = append(out, frame)
		off += ln
	}
	if off != len(packed) {
		return nil, fmt.Errorf("batch: trailing bytes")
	}
	return out, nil
}

func writeV1Frame(s network.Stream, data []byte) error {
	if len(data) > maxMessageSize {
		return fmt.Errorf("message too large")
	}
	length := uint32(len(data))
	buf := make([]byte, 4+len(data))
	buf[0] = byte(length >> 24)
	buf[1] = byte(length >> 16)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)
	copy(buf[4:], data)
	_, err := s.Write(buf)
	return err
}

// sendDirectFrames sends one or more payloads; each becomes one v2 frame (or one v1 message per stream on legacy).
func (n *Node) sendDirectFrames(targetPeerID string, frames [][]byte) error {
	if len(frames) == 0 {
		return fmt.Errorf("empty batch")
	}
	single := len(frames) == 1
	var total int
	for _, f := range frames {
		if len(f) > maxMessageSize {
			return fmt.Errorf("frame too large: %d", len(f))
		}
		total += len(f)
		if total > maxBatchTotalBytes {
			return fmt.Errorf("batch total too large")
		}
	}

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

	// CRITICAL: Prevent "dial to self" - validate target is not us
	selfID := h.ID()
	if pid == selfID {
		fmt.Printf("p2pmobile: %s REJECTED — target is SELF (%s), not dialing\n",
			"SendToPeer", pid.String()[:16])
		return fmt.Errorf("reject dial-to-self: target %s is self", pid.String()[:16])
	}

	connected := h.Network().Connectedness(pid) == network.Connected
	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}
	op := "SendToPeer"
	if !single {
		op = "SendBatchToPeer"
	}
	fmt.Printf("p2pmobile: %s to %s self=%s connected=%v frames=%d totalBytes=%d\n",
		op, pidShort, selfID.String()[:16], connected, len(frames), total)
	logP2pTransport("%s START peer=%s… self=%s connected=%v frames=%d totalBytes=%d",
		op, pidShort, selfID.String()[:16], connected, len(frames), total)

	if !connected {
		if err := n.dialPeer(pid); err != nil {
			logP2pTransport("%s PATH_FAIL dial peer=%s… err=%v — Android may fall back to GossipSub/WebSocket", op, pidShort, err)
			return fmt.Errorf("dial: %w", err)
		}
	}

	// Drop a relay-only pooled stream as soon as a direct connection exists so the next
	// open uses a better path (avoids UI showing "direct" while sends stay on circuit).
	if ps := n.getPooledStream(targetPeerID); ps != nil {
		ra := ps.stream.Conn().RemoteMultiaddr().String()
		if strings.Contains(ra, "p2p-circuit") && n.hasDirectConnToPeer(pid) {
			fmt.Printf("p2pmobile: [pool] evict relay stream — direct conn available for %s\n", pidShort)
			n.evictPooledStream(targetPeerID)
		}
	}

	// ── Pooled v2 stream ─────────────────────────────────────────────
	if ps := n.getPooledStream(targetPeerID); ps != nil {
		var writeErr error
		for _, data := range frames {
			if writeErr = n.writeToPooledStream(ps, data); writeErr != nil {
				break
			}
		}
		if writeErr == nil {
			remoteAddr := ps.stream.Conn().RemoteMultiaddr().String()
			connType := "DIRECT"
			transport := "TCP"
			if strings.Contains(remoteAddr, "p2p-circuit") {
				connType = "RELAY"
			}
			if strings.Contains(remoteAddr, "/udp/") {
				transport = "QUIC"
			}
			fmt.Printf("p2pmobile: %s OK (pooled) — frames=%d totalBytes=%d to %s transport=%s type=%s\n", op, len(frames), total, pidShort, transport, connType)
			logP2pTransport("%s PATH=LIBP2P_STREAM ok=true peer=%s… frames=%d totalBytes=%d transport=%s conn=%s (pooled v2)",
				op, pidShort, len(frames), total, transport, connType)
			return nil
		}
		fmt.Printf("p2pmobile: %s pooled stream broken for %s: %v — opening new\n", op, pidShort, writeErr)
		n.evictPooledStream(targetPeerID)
		// Avoid duplicate frames if we already wrote part of a multi-frame batch.
		if !single {
			return fmt.Errorf("pooled write: %w", writeErr)
		}
	}

	// ── New stream: v2 first, else v1 ───────────────────────────────
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, "relay-dm")

	s, err := h.NewStream(ctx, pid, protocol.ID(OptimizedDMProtocol), protocol.ID(DirectMessageProtocol))
	if err != nil {
		fmt.Printf("p2pmobile: %s NewStream FAILED to %s: %v\n", op, pidShort, err)
		logP2pTransport("%s PATH_FAIL NewStream peer=%s… err=%v — Android may fall back to GossipSub/WebSocket", op, pidShort, err)
		return fmt.Errorf("stream: %w", err)
	}

	negotiated := string(s.Protocol())
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	transport := "TCP"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	if strings.Contains(remoteAddr, "/udp/") {
		transport = "QUIC"
	}
	fmt.Printf("p2pmobile: %s new stream proto=%s transport=%s type=%s addr=%s\n", op, negotiated, transport, connType, remoteAddr)
	logP2pTransport("%s PATH=LIBP2P_STREAM new_stream peer=%s… proto=%s transport=%s conn=%s addr=%s",
		op, pidShort, negotiated, transport, connType, remoteAddr)

	if negotiated == OptimizedDMProtocol {
		ps := &pooledStream{
			stream:  s,
			writer:  bufio.NewWriterSize(s, dmWriterBufferSize),
			lastUse: time.Now(),
		}
		n.putPooledStream(targetPeerID, ps)
		for _, data := range frames {
			if err := n.writeToPooledStream(ps, data); err != nil {
				n.evictPooledStream(targetPeerID)
				return err
			}
		}
		fmt.Printf("p2pmobile: %s OK (new v2, pooled) — frames=%d totalBytes=%d to %s\n", op, len(frames), total, pidShort)
		logP2pTransport("%s PATH=LIBP2P_STREAM ok=true peer=%s… frames=%d totalBytes=%d proto=v2_pooled conn=%s",
			op, pidShort, len(frames), total, connType)
		return nil
	}

	// v1: one length-prefixed message per stream (close after each frame)
	for i, data := range frames {
		var st network.Stream
		if i == 0 {
			st = s
		} else {
			s2, err2 := h.NewStream(ctx, pid, protocol.ID(DirectMessageProtocol))
			if err2 != nil {
				return fmt.Errorf("v1 stream %d: %w", i, err2)
			}
			st = s2
		}
		if err := writeV1Frame(st, data); err != nil {
			st.Reset()
			fmt.Printf("p2pmobile: %s v1 write FAILED to %s: %v\n", op, pidShort, err)
			return err
		}
		_ = st.Close()
	}
	fmt.Printf("p2pmobile: %s OK (v1) — frames=%d totalBytes=%d to %s\n", op, len(frames), total, pidShort)
	logP2pTransport("%s PATH=LIBP2P_STREAM ok=true peer=%s… frames=%d totalBytes=%d proto=v1 conn=%s",
		op, pidShort, len(frames), total, connType)
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

// tryDialPeerDirect uses peerstore + DHT only (no circuit relay).
// Hybrid strategy: QUIC (UDP) first, then TCP fallback.
// Returns true if a direct (non-relay) connection exists after.
func (n *Node) tryDialPeerDirect(pid peer.ID) bool {
	h := n.host
	if h == nil {
		return false
	}
	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}
	if n.hasDirectConnToPeer(pid) {
		return true
	}

	// Step 1: Try addresses from peerstore (QUIC first, then TCP)
	if addrs := h.Peerstore().Addrs(pid); len(addrs) > 0 {
		var quicAddrs, tcpAddrs []multiaddr.Multiaddr
		for _, a := range addrs {
			if strings.Contains(a.String(), "p2p-circuit") {
				continue
			}
			if strings.Contains(a.String(), "/quic") {
				quicAddrs = append(quicAddrs, a)
			} else if strings.Contains(a.String(), "/tcp/") {
				tcpAddrs = append(tcpAddrs, a)
			}
		}

		// Hybrid: Try QUIC first (lower latency)
		if len(quicAddrs) > 0 {
			fmt.Printf("p2pmobile: [dial] step1a QUIC (UDP) trying %d addrs for %s: %v\n", len(quicAddrs), pidShort, quicAddrs)
			ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
			err := h.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: quicAddrs})
			cancel()
			if err != nil {
				fmt.Printf("p2pmobile: [dial] step1a QUIC dial FAILED for %s: %v\n", pidShort, err)
			} else {
				conn := h.Network().ConnsToPeer(pid)[0]
				transport := "TCP"
				if strings.Contains(conn.RemoteMultiaddr().String(), "/udp/") {
					transport = "QUIC"
				}
				fmt.Printf("p2pmobile: [dial] step1a QUIC connected to %s via %s transport=%s\n", pidShort, conn.RemoteMultiaddr(), transport)
				if n.hasDirectConnToPeer(pid) {
					return true
				}
			}
		} else {
			fmt.Printf("p2pmobile: [dial] step1a NO QUIC addrs in peerstore for %s (have %d TCP)\n", pidShort, len(tcpAddrs))
		}

		// Fallback to TCP
		if len(tcpAddrs) > 0 {
			fmt.Printf("p2pmobile: [dial] step1b TCP fallback trying %d addrs for %s: %v\n", len(tcpAddrs), pidShort, tcpAddrs)
			ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
			err := h.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: tcpAddrs})
			cancel()
			if err != nil {
				fmt.Printf("p2pmobile: [dial] step1b TCP dial FAILED for %s: %v\n", pidShort, err)
			} else {
				fmt.Printf("p2pmobile: [dial] step1b TCP connected to %s via %s\n", pidShort, h.Network().ConnsToPeer(pid)[0].RemoteMultiaddr())
				if n.hasDirectConnToPeer(pid) {
					return true
				}
			}
		}
	} else {
		fmt.Printf("p2pmobile: [dial] step1 NO addrs in peerstore for %s\n", pidShort)
	}

	// Step 2: DHT lookup, then try QUIC first, then TCP
	n.mu.RLock()
	d := n.dht
	n.mu.RUnlock()

	// First, check peerstore for existing addresses
	peerstoreAddrs := h.Peerstore().Addrs(pid)
	var psQuic, psTcp int
	for _, a := range peerstoreAddrs {
		if strings.Contains(a.String(), "/quic") {
			psQuic++
		} else {
			psTcp++
		}
	}
	fmt.Printf("p2pmobile: [dial] peerstore addrs for %s: {QUIC:%d TCP:%d}\n", pidShort, psQuic, psTcp)
	fmt.Printf("p2pmobile: [dial] peerstore addrs detail: %v\n", peerstoreAddrs)

	if d != nil {
		fmt.Printf("p2pmobile: [dial] step2 DHT FindPeer for %s\n", pidShort)
		ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
		peerInfo, err := d.FindPeer(ctx, pid)
		cancel()
		if err == nil && len(peerInfo.Addrs) > 0 {
			var quicAddrs, tcpAddrs, ipv6QuicAddrs, ipv6TcpAddrs []multiaddr.Multiaddr
			for _, a := range peerInfo.Addrs {
				aStr := a.String()
				if strings.Contains(aStr, "p2p-circuit") {
					continue
				}
				isIPv6 := strings.HasPrefix(aStr, "/ip6/")
				isQUIC := strings.Contains(aStr, "/quic")

				if isIPv6 && isQUIC {
					ipv6QuicAddrs = append(ipv6QuicAddrs, a)
				} else if isIPv6 && !isQUIC {
					ipv6TcpAddrs = append(ipv6TcpAddrs, a)
				} else if isQUIC {
					quicAddrs = append(quicAddrs, a)
				} else {
					tcpAddrs = append(tcpAddrs, a)
				}
			}
			fmt.Printf("p2pmobile: [dial] step2 DHT result for %s: {QUIC:%d TCP:%d IPv6-QUIC:%d IPv6-TCP:%d}\n",
				pidShort, len(quicAddrs), len(tcpAddrs), len(ipv6QuicAddrs), len(ipv6TcpAddrs))
			fmt.Printf("p2pmobile: [dial] step2 DHT addrs: %v\n", peerInfo.Addrs)

			// Priority: IPv6-QUIC > IPv6-TCP > QUIC > TCP
			// IPv6 is preferred because many mobile carriers give public IPv6
			addrPriority := []struct {
				addrs []multiaddr.Multiaddr
				name  string
			}{
				{ipv6QuicAddrs, "IPv6-QUIC"},
				{ipv6TcpAddrs, "IPv6-TCP"},
				{quicAddrs, "QUIC"},
				{tcpAddrs, "TCP"},
			}

			// Also check peerstore for QUIC addresses we injected
			if len(ipv6QuicAddrs) == 0 && len(quicAddrs) == 0 {
				// No QUIC from DHT, but we might have injected QUIC addresses
				peerstoreAddrs := h.Peerstore().Addrs(pid)
				for _, pa := range peerstoreAddrs {
					paStr := pa.String()
					if strings.Contains(paStr, "/quic") && !strings.Contains(paStr, "p2p-circuit") {
						isIPv6 := strings.HasPrefix(paStr, "/ip6/")
						if isIPv6 {
							ipv6QuicAddrs = append(ipv6QuicAddrs, pa)
						} else {
							quicAddrs = append(quicAddrs, pa)
						}
					}
				}
				if len(ipv6QuicAddrs) > 0 || len(quicAddrs) > 0 {
					fmt.Printf("p2pmobile: [dial] step2 found QUIC in peerstore: IPv6-QUIC=%d QUIC=%d\n",
						len(ipv6QuicAddrs), len(quicAddrs))
					addrPriority = []struct {
						addrs []multiaddr.Multiaddr
						name  string
					}{
						{ipv6QuicAddrs, "IPv6-QUIC"},
						{quicAddrs, "QUIC"},
						{ipv6TcpAddrs, "IPv6-TCP"},
						{tcpAddrs, "TCP"},
					}
				}
			}

			for _, p := range addrPriority {
				if len(p.addrs) == 0 {
					continue
				}
				fmt.Printf("p2pmobile: [dial] step2 trying %s %d addrs for %s: %v\n", p.name, len(p.addrs), pidShort, p.addrs)
				ctx2, cancel2 := context.WithTimeout(n.ctx, 5*time.Second)
				err2 := h.Connect(ctx2, peer.AddrInfo{ID: pid, Addrs: p.addrs})
				cancel2()
				if err2 != nil {
					fmt.Printf("p2pmobile: [dial] step2 %s FAILED for %s: %v\n", p.name, pidShort, err2)
					continue
				}
				conn := h.Network().ConnsToPeer(pid)[0]
				actualTransport := "TCP"
				if strings.Contains(conn.RemoteMultiaddr().String(), "/udp/") {
					actualTransport = "QUIC"
				}
				fmt.Printf("p2pmobile: [dial] step2 %s connected to %s actual_transport=%s\n", p.name, pidShort, actualTransport)
				if n.hasDirectConnToPeer(pid) {
					return true
				}
			}
		} else {
			fmt.Printf("p2pmobile: [dial] step2 DHT FindPeer FAILED for %s: %v\n", pidShort, err)
		}
	} else {
		fmt.Printf("p2pmobile: [dial] step2 DHT not available for %s\n", pidShort)
	}
	return n.hasDirectConnToPeer(pid)
}

func (n *Node) dialPeerRelayFallback(pid peer.ID) error {
	h := n.host
	if h == nil {
		return fmt.Errorf("not started")
	}
	pidShort := pid.String()[:16]
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

func (n *Node) dialPeer(pid peer.ID) error {
	h := n.host
	if h == nil {
		return fmt.Errorf("not started")
	}
	pidShort := pid.String()[:16]
	if h.Network().Connectedness(pid) == network.Connected && n.hasDirectConnToPeer(pid) {
		return nil
	}
	for pass := 0; pass < directDialPasses; pass++ {
		if pass > 0 {
			j := 80 + rand.Intn(220)
			time.Sleep(time.Duration(j) * time.Millisecond)
			fmt.Printf("p2pmobile: [dial] direct pass %d/%d jitter=%dms peer=%s\n", pass+1, directDialPasses, j, pidShort)
		}
		if n.tryDialPeerDirect(pid) {
			fmt.Printf("p2pmobile: [dial] direct OK (pass %d) peer=%s\n", pass+1, pidShort)
			return nil
		}
	}
	if h.Network().Connectedness(pid) == network.Connected {
		// Already reachable (e.g. relay from earlier); no need to relay-dial again.
		return nil
	}
	return n.dialPeerRelayFallback(pid)
}

func (n *Node) handleDirectStream(s network.Stream) {
	defer s.Close()
	from := s.Conn().RemotePeer().String()
	fromShort := from
	if len(fromShort) > 16 {
		fromShort = fromShort[:16]
	}
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	p2pDebugf("p2pmobile: incoming direct stream from %s type=%s addr=%s (dir=%s)\n",
		fromShort, connType, remoteAddr, s.Conn().Stat().Direction)
	logP2pTransport("incoming PATH=LIBP2P_STREAM_V1 open from=%s… conn=%s dir=%v addr=%s",
		fromShort, connType, s.Conn().Stat().Direction, remoteAddr)

	hdr := make([]byte, 4)
	if _, err := io.ReadFull(s, hdr); err != nil {
		p2pDebugf("p2pmobile: direct stream header read error from %s: %v\n", from, err)
		return
	}
	length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	if length > maxMessageSize {
		p2pDebugf("p2pmobile: direct stream message too large from %s: %d bytes\n", from, length)
		return
	}
	rpid := s.Conn().RemotePeer()
	if !n.allowIncomingDM(rpid, int(length)) {
		fmt.Printf("p2pmobile: SECURITY dm v1 ingress rate limit peer=%s len=%d\n", fromShort, length)
		s.Reset()
		return
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(s, data); err != nil {
		p2pDebugf("p2pmobile: direct stream body read error from %s: %v\n", from, err)
		return
	}
	p2pDebugf("p2pmobile: direct stream received %d bytes from %s\n", length, from)
	logP2pTransport("incoming PATH=LIBP2P_STREAM_V1 from=%s… bytes=%d conn=%s (unicast stream, not GossipSub)",
		fromShort, length, connType)

	if h := n.directHandler; h != nil {
		h.OnDirectMessage(from, data)
	}
	// Do not call MessageHandler for direct streams — avoids duplicate JNI delivery
	// (Kotlin already ingests via DirectMessageHandler → same bytes as GossipSub path).
}

// -------- identity persistence --------

func loadOrCreateIdentity(dataDir string) (crypto.PrivKey, error) {
	if dataDir == "" {
		priv, _, err := crypto.GenerateEd25519Key(crand.Reader)
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
	if _, err := crand.Read(seed); err != nil {
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

// payloadLooksPreCompressed skips zlib for common media/container magic (saves CPU/battery).
func payloadLooksPreCompressed(b []byte) bool {
	if len(b) < 12 {
		return false
	}
	if b[0] == 0xff && b[1] == 0xd8 {
		return true // JPEG
	}
	if len(b) >= 8 && b[0] == 0x89 && b[1] == 'P' && b[2] == 'N' && b[3] == 'G' {
		return true
	}
	if len(b) >= 12 && b[0] == 'R' && b[1] == 'I' && b[2] == 'F' && b[3] == 'F' &&
		b[8] == 'W' && b[9] == 'E' && b[10] == 'B' && b[11] == 'P' {
		return true
	}
	if len(b) >= 6 && b[0] == 'G' && b[1] == 'I' && b[2] == 'F' {
		return true
	}
	if len(b) >= 4 && b[0] == 'O' && b[1] == 'g' && b[2] == 'g' && b[3] == 'S' {
		return true // Ogg (e.g. Opus)
	}
	return false
}

// jsonDmPayloadSkipsZlib returns true for payloads that should not be zlib-compressed on DM v2.
// - Large JSON (chat_msg, chat_blob_chunk, …): Base64 blobs are expensive to compress with little gain.
// - Binary blob chunks (magic "RCB1", Kotlin BlobChunkBinaryWire): ciphertext is incompressible; skip zlib CPU.
func jsonDmPayloadSkipsZlib(data []byte) bool {
	if len(data) < compressionThreshold {
		return false
	}
	if len(data) == 0 {
		return false
	}
	if data[0] == '{' {
		return true
	}
	if len(data) >= 4 && data[0] == 'R' && data[1] == 'C' && data[2] == 'B' && data[3] == '1' {
		return true
	}
	return false
}

// writeToPooledStream writes a v2 frame (flags + length + payload) with optional compression.
// Single buffered Flush = single TCP segment for small messages (text, acks).
func (n *Node) writeToPooledStream(ps *pooledStream, data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.lastUse = time.Now()

	var payload []byte
	var flags byte = 0x00

	// Compress payloads above threshold unless already compressed media (saves CPU on images).
	if len(data) > compressionThreshold && !payloadLooksPreCompressed(data) && !jsonDmPayloadSkipsZlib(data) {
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		w.Write(data)
		w.Close()
		compressed := buf.Bytes()
		// Only use compression if it actually shrinks the data
		if len(compressed) < len(data)-64 {
			payload = compressed
			flags = 0x01
			p2pDebugf("p2pmobile: [compress] %d → %d bytes (%.0f%% reduction)\n",
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
	rpid := s.Conn().RemotePeer()
	from := rpid.String()
	fromShort := from
	if len(fromShort) > 16 {
		fromShort = fromShort[:16]
	}
	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	p2pDebugf("p2pmobile: incoming v2 stream from %s type=%s addr=%s\n", fromShort, connType, remoteAddr)
	logP2pTransport("incoming PATH=LIBP2P_STREAM_V2 open from=%s… conn=%s addr=%s (persistent dm/2.0.0)",
		fromShort, connType, remoteAddr)

	reader := bufio.NewReaderSize(s, 64*1024)
	malformed := 0
	for {
		// Read frame: [flags:1] [length:4] [payload]
		flagsByte, err := reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				p2pDebugf("p2pmobile: v2 stream closed from %s: %v\n", fromShort, err)
			}
			return
		}
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(reader, hdr); err != nil {
			p2pDebugf("p2pmobile: v2 length read error from %s: %v\n", fromShort, err)
			return
		}
		length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
		if length > maxMessageSize {
			p2pDebugf("p2pmobile: v2 message too large from %s: %d bytes\n", fromShort, length)
			return
		}
		if !n.allowIncomingDM(rpid, int(length)) {
			fmt.Printf("p2pmobile: SECURITY dm v2 ingress rate limit peer=%s len=%d\n", fromShort, length)
			s.Reset()
			return
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			p2pDebugf("p2pmobile: v2 body read error from %s: %v\n", fromShort, err)
			return
		}

		// Decompress if flagged
		data := payload
		if flagsByte&0x01 != 0 {
			r, err := zlib.NewReader(bytes.NewReader(payload))
			if err != nil {
				malformed++
				p2pDebugf("p2pmobile: v2 zlib init error from %s: %v\n", fromShort, err)
				if malformed >= dmMaxMalformedFrames {
					fmt.Printf("p2pmobile: SECURITY dm v2 too many malformed frames peer=%s\n", fromShort)
					s.Reset()
					return
				}
				continue
			}
			decompressed, err := io.ReadAll(io.LimitReader(r, maxMessageSize+1))
			r.Close()
			if err != nil || len(decompressed) > maxMessageSize {
				malformed++
				p2pDebugf("p2pmobile: v2 zlib read error from %s: %v\n", fromShort, err)
				if malformed >= dmMaxMalformedFrames {
					fmt.Printf("p2pmobile: SECURITY dm v2 too many bad decompress peer=%s\n", fromShort)
					s.Reset()
					return
				}
				continue
			}
			data = decompressed
			p2pDebugf("p2pmobile: [decompress] %d → %d bytes from %s\n", len(payload), len(data), fromShort)
		}

		p2pDebugf("p2pmobile: v2 received %d bytes from %s\n", len(data), fromShort)
		logP2pTransport("incoming PATH=LIBP2P_STREAM_V2 from=%s… bytes=%d conn=%s (unicast v2 frame)",
			fromShort, len(data), connType)
		if h := n.directHandler; h != nil {
			h.OnDirectMessage(from, data)
		}
		// Direct frames are not GossipSub — only DirectMessageHandler (single JNI path).
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
	p2pDebugf("p2pmobile: ⚡ OnNetworkChanged — refreshing addresses and connections\n")

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
	p2pDebugf("p2pmobile: [netchange] evicted %d pooled streams\n", poolCount)
	n.clearIngressBudgets()

	// 3. Reconnect bootstrap first (control plane) on the new link before tearing mesh.
	n.ensureBootstrap()

	// 4. Re-discover real addresses from the new bootstrap TCP local addr
	n.discoverRealAddrs()

	// 5. Re-reserve relay + advertised circuit addrs (reservation tied to bootstrap conn)
	n.reserveRelayOnBootstrap()
	n.addRelayCircuitAddrs()

	// 6. Stagger ClosePeer for non-bootstrap peers (avoids thundering herd; sockets are stale anyway)
	var toClose []peer.ID
	for _, p := range h.Network().Peers() {
		isBootstrap := false
		for _, bp := range n.bootstrapPeers {
			if bp.ID == p {
				isBootstrap = true
				break
			}
		}
		if !isBootstrap {
			toClose = append(toClose, p)
		}
	}
	for i, p := range toClose {
		_ = h.Network().ClosePeer(p)
		if i+1 < len(toClose) {
			time.Sleep(25 * time.Millisecond)
		}
	}

	// 7. Push updated identify so remotes learn fresh reachability (DCUtR / hole-punch).
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
