// Package p2pmobile provides a gomobile-bindable libp2p node for Android/iOS.
//
// Features:
//   - QUIC-first transport: QUIC (UDP) primary, TCP fallback
//   - GossipSub topic pub/sub (backward-compat with existing chat)
//   - Direct peer-to-peer streams (no relay needed when NAT allows)
//   - DHT peer discovery
//   - NAT traversal: UPnP port mapping, DCUtR hole punching, circuit relay v2 fallback
//
// Transport Strategy:
//   - QUIC registered before TCP so the dialer prefers UDP
//   - QUIC-strict bootstrap dial: peerstore is temporarily narrowed to
//     UDP-only addrs so libp2p cannot silently fall back to TCP
//   - Private/CGNAT UDP addresses are advertised (not stripped) so that
//     DCUtR hole punching can coordinate NAT traversal via the relay
//   - TCP fallback for networks that block all UDP
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
	"runtime"
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
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/protocol"
	circuitv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

// directDialPasses is how many times we retry peerstore+DHT direct dials before circuit relay.
// Jittered backoff between passes lets addresses/identify propagate (mobile handover).
// Note: "99.99% direct" is not achievable on the open internet (CGNAT, offline peers, firewalls);
// relay remains the correctness fallback; these passes maximize direct when paths exist.
const directDialPasses = 2

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
	fileHandler   FileTransferHandler

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
		ingressBudget:   make(map[string]*peerIngressBudget),
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
	if n.running {
		n.mu.Unlock()
		return nil
	}

	n.ctx, n.cancel = context.WithCancel(context.Background())

	priv, err := loadOrCreateIdentity(dataDir)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("identity: %w", err)
	}

	bootstrapInfos := parseBootstrapAddrs(bootstrapMultiaddrs)
	n.bootstrapPeers = bootstrapInfos

	// ── Phase 1: Create host with QUIC-first transports ─────────────────
	// QUIC (UDP) is preferred: lower latency, 0-RTT resume, better mobile
	// handover.  TCP is the reliable fallback for networks that block UDP.
	// Transport registration order sets dialer priority: QUIC first.
	h, err := libp2p.New(
		libp2p.Identity(priv),
		// QUIC first — lower latency, better for mobile, enables DCUtR hole-punching
		libp2p.Transport(libp2pquic.NewTransport),
		// TCP fallback — reliable through strict CGNAT / UDP-blocking networks
		libp2p.Transport(tcp.NewTCPTransport),
		// Listen on QUIC (UDP) + TCP for both IPv4 and IPv6
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/udp/0/quic-v1", // QUIC IPv4 — primary
			"/ip6/::/udp/0/quic-v1",       // QUIC IPv6
			"/ip4/0.0.0.0/tcp/0",          // TCP IPv4 — fallback
			"/ip6/::/tcp/0",               // TCP IPv6
		),
		// NAT traversal: UPnP port mapping, relay v2, DCUtR hole punching
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		// Mobile nodes are ALWAYS behind NAT — skip AutoNAT probing delay.
		// Without this, libp2p waits for AutoNAT probes (which often fail on
		// mobile CGNAT) before activating DCUtR and relay reservation.
		libp2p.ForceReachabilityPrivate(),
		// AutoRelay with bootstrap as static relays — properly integrates relay
		// management with the identify/dcutr pipeline.  Automatically reserves
		// slots, advertises relay addrs via identify, and re-reserves on expiry.
		libp2p.EnableAutoRelayWithStaticRelays(bootstrapInfos),
		// Custom address factory to inject externally discovered addresses
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			n.extraAddrsMu.RLock()
			extra := make([]multiaddr.Multiaddr, len(n.extraAddrs))
			copy(extra, n.extraAddrs)
			n.extraAddrsMu.RUnlock()
			all := addrs
			if len(extra) > 0 {
				all = append(all, extra...)
			}
			// Filter out loopback and bind-all addresses — useless to remote
			// peers and cause wasted dial attempts + pollute peerstore.
			filtered := make([]multiaddr.Multiaddr, 0, len(all))
			for _, a := range all {
				s := a.String()
				if strings.Contains(s, "/ip4/127.0.0.1/") ||
					strings.Contains(s, "/ip6/::1/") ||
					strings.Contains(s, "/ip4/0.0.0.0/") ||
					strings.Contains(s, "/ip6/::/") {
					continue
				}
				filtered = append(filtered, a)
			}
			return filtered
		}),
	)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("host: %w", err)
	}
	n.host = h

	h.SetStreamHandler(protocol.ID(DirectMessageProtocol), n.handleDirectStream)
	h.SetStreamHandler(protocol.ID(OptimizedDMProtocol), n.handleOptimizedStream)
	h.SetStreamHandler(protocol.ID(FileTransferProtocol), n.handleFileTransferStream)
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
						fmt.Printf("p2pmobile: [auto-reconnect] bootstrap peer %s lost — reconnecting with backoff\n", remotePeer[:16])
						go func(pi peer.AddrInfo) {
							pidShort := pi.ID.String()[:16]
							backoff := 2 * time.Second
							const maxAttempts = 4

							for attempt := 1; attempt <= maxAttempts; attempt++ {
								if !n.running {
									return
								}
								time.Sleep(backoff)
								hh := n.host
								if hh == nil {
									return
								}
								// Already reconnected (e.g. keepalive beat us)?
								if hh.Network().Connectedness(pi.ID) == network.Connected {
									fmt.Printf("p2pmobile: [auto-reconnect] %s already connected (attempt %d)\n", pidShort, attempt)
									return
								}
								fmt.Printf("p2pmobile: [auto-reconnect] attempt %d/%d for %s (backoff %v)\n", attempt, maxAttempts, pidShort, backoff)

								// QUIC-strict: isolate peerstore
								quicInfo := quicOnlyPeerInfo(pi)
								if len(quicInfo.Addrs) > 0 {
									allAddrs := hh.Peerstore().Addrs(pi.ID)
									hh.Peerstore().ClearAddrs(pi.ID)
									hh.Peerstore().AddAddrs(pi.ID, quicInfo.Addrs, peerstore.TempAddrTTL)

									ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
									err := hh.Connect(ctx, quicInfo)
									cancel()

									hh.Peerstore().ClearAddrs(pi.ID)
									hh.Peerstore().AddAddrs(pi.ID, allAddrs, peerstore.PermanentAddrTTL)

									if err == nil {
										transport := actualConnTransport(hh, pi.ID)
										fmt.Printf("p2pmobile: [auto-reconnect] reconnected %s via %s (attempt %d)\n", pidShort, transport, attempt)
										n.discoverRealAddrs()
										n.reserveRelayOnBootstrap()
										n.addRelayCircuitAddrs()
										return
									}
									fmt.Printf("p2pmobile: [auto-reconnect] QUIC-strict failed %s: %v\n", pidShort, err)
								}
								// TCP fallback
								tcpInfo := tcpOnlyPeerInfo(pi)
								if len(tcpInfo.Addrs) == 0 {
									tcpInfo = pi
								}
								ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
								err := hh.Connect(ctx, tcpInfo)
								cancel()
								if err == nil {
									transport := actualConnTransport(hh, pi.ID)
									fmt.Printf("p2pmobile: [auto-reconnect] reconnected %s via %s (attempt %d)\n", pidShort, transport, attempt)
									n.discoverRealAddrs()
									n.reserveRelayOnBootstrap()
									n.addRelayCircuitAddrs()
									return
								}
								fmt.Printf("p2pmobile: [auto-reconnect] attempt %d/%d FAILED %s: %v\n", attempt, maxAttempts, pidShort, err)
								backoff *= 2 // exponential: 2s → 4s → 8s → 16s
							}
							fmt.Printf("p2pmobile: [auto-reconnect] EXHAUSTED %d attempts for %s — keepalive will retry\n", maxAttempts, pidShort)
						}(info)
						break
					}
				}
			}
		},
	})

	// ── Phase 2: GossipSub (instant) ─────────────────────────────────────
	if err := n.initGossipSubAndReady(h); err != nil {
		n.mu.Unlock()
		return err
	}

	// Bootstrap peers are added to peerstore for connection attempts
	for _, info := range bootstrapInfos {
		h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
		h.ConnManager().Protect(info.ID, "bootstrap")
	}

	// Release write lock before blocking bootstrap — ensureBootstrap()
	// needs RLock, and network callbacks also need RLock.
	n.mu.Unlock()

	// ── Phase 2b: Background bootstrap + upgrade ────────────────────────
	// Bootstrap MUST be async — synchronous blocks the JNI thread for up
	// to 90s (QUIC 8s + TCP 10s × 5 retries) causing ANR on Android.
	// Kotlin polls connectedPeerCount() with cooperative delay() instead.
	go n.backgroundUpgrade()

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

			// ── QUIC-strict dial (with retry) ────────────────────────────
			// h.Connect dials ALL addrs in the peerstore for the peer, not
			// just the ones in AddrInfo.  To enforce a genuine QUIC-only
			// attempt we must temporarily remove TCP addrs from the store.
			//
			// quic-go's internal HandshakeIdleTimeout is ~5s.  A single
			// attempt gives up too quickly on high-latency mobile networks.
			// Two attempts give ~10s of genuine QUIC dialing before TCP.
			quicInfo := quicOnlyPeerInfo(info)
			quicConnected := false
			if len(quicInfo.Addrs) > 0 {
				allAddrs := h.Peerstore().Addrs(info.ID)
				t0 := time.Now()

				for qi := 1; qi <= 2; qi++ {
					h.Peerstore().ClearAddrs(info.ID)
					h.Peerstore().AddAddrs(info.ID, quicInfo.Addrs, peerstore.TempAddrTTL)

					fmt.Printf("p2pmobile: [bootstrap] QUIC-strict attempt %d/2 %s addrs=%v\n", qi, info.ID, quicInfo.Addrs)
					ctxQ, cancelQ := context.WithTimeout(ctx, 8*time.Second)
					err := h.Connect(ctxQ, quicInfo)
					cancelQ()

					if err == nil {
						quicConnected = true
						break
					}
					fmt.Printf("p2pmobile: [bootstrap] QUIC-strict %d/2 failed %s after %v: %v\n",
						qi, info.ID, time.Since(t0).Round(time.Millisecond), err)
				}

				// Restore full address set (TCP + QUIC) regardless of outcome
				h.Peerstore().ClearAddrs(info.ID)
				h.Peerstore().AddAddrs(info.ID, allAddrs, peerstore.PermanentAddrTTL)

				if quicConnected {
					transport := actualConnTransport(h, info.ID)
					addr := actualConnAddr(h, info.ID)
					fmt.Printf("p2pmobile: [bootstrap] ✓ connected to %s in %v via %s addr=%s\n",
						info.ID, time.Since(t0).Round(time.Millisecond), transport, addr)
					bootstrapConnected = true
					n.discoverRealAddrs()
					n.reserveRelayOnBootstrap()
					n.addRelayCircuitAddrs()

					// Also open a TCP connection to bootstrap so identify learns
					// our TCP NAT-mapped address (observed addr). Without this,
					// peers only get our QUIC public addr and have no TCP fallback
					// when QUIC hole-punching fails through NAT.
					go func(bInfo peer.AddrInfo) {
						tcpBI := tcpOnlyPeerInfo(bInfo)
						if len(tcpBI.Addrs) == 0 {
							fmt.Printf("p2pmobile: [bootstrap] no TCP addrs for bootstrap — skipping secondary TCP\n")
							return
						}
						// Must use WithForceDirectDial — without it, h.Connect sees the
						// existing QUIC connection and returns nil without dialing TCP.
						// Also isolate peerstore to TCP-only addrs to prevent the swarm
						// from reusing the QUIC connection.
						allAddrs := h.Peerstore().Addrs(bInfo.ID)
						h.Peerstore().ClearAddrs(bInfo.ID)
						h.Peerstore().AddAddrs(bInfo.ID, tcpBI.Addrs, peerstore.TempAddrTTL)

						ctxTCP, cancelTCP := context.WithTimeout(n.ctx, 10*time.Second)
						ctxTCP = network.WithForceDirectDial(ctxTCP, "bootstrap-tcp-observed-addr")
						err := h.Connect(ctxTCP, tcpBI)
						cancelTCP()

						// Restore full peerstore
						h.Peerstore().ClearAddrs(bInfo.ID)
						h.Peerstore().AddAddrs(bInfo.ID, allAddrs, peerstore.PermanentAddrTTL)

						if err != nil {
							fmt.Printf("p2pmobile: [bootstrap] secondary TCP connect failed: %v\n", err)
							return
						}
						fmt.Printf("p2pmobile: [bootstrap] ✓ secondary TCP connected — identify will learn TCP observed addr\n")
						// After TCP identify completes, re-discover to pick up TCP observed addr
						time.Sleep(2 * time.Second)
						n.discoverRealAddrs()
					}(info)

					break
				}
				fmt.Printf("p2pmobile: [bootstrap] QUIC-strict exhausted for %s after %v, trying TCP...\n",
					info.ID, time.Since(t0).Round(time.Millisecond))
			}

			// ── TCP fallback ────────────────────────────────────────────
			tcpInfo := tcpOnlyPeerInfo(info)
			if len(tcpInfo.Addrs) == 0 {
				tcpInfo = info
			}
			t0 := time.Now()
			ctx10b, cancel2 := context.WithTimeout(ctx, 10*time.Second)
			err := h.Connect(ctx10b, tcpInfo)
			cancel2()

			if err != nil {
				fmt.Printf("p2pmobile: [bootstrap] TCP also failed %s after %v: %v\n",
					info.ID, time.Since(t0).Round(time.Millisecond), err)
				continue
			}

			transport := actualConnTransport(h, info.ID)
			addr := actualConnAddr(h, info.ID)
			fmt.Printf("p2pmobile: [bootstrap] ✓ connected to %s in %v via %s addr=%s\n",
				info.ID, time.Since(t0).Round(time.Millisecond), transport, addr)
			bootstrapConnected = true
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
	// Log all listen addresses with transport type and extract actual ports
	var quicListeners, tcpListeners int
	var quicPort, tcpPort string
	for _, addr := range h.Addrs() {
		aStr := addr.String()
		transport := "TCP"
		if strings.Contains(aStr, "/udp/") {
			transport = "QUIC"
			quicListeners++
			// Extract QUIC port for diagnostic logging
			parts := strings.Split(aStr, "/")
			for i, p := range parts {
				if p == "udp" && i+1 < len(parts) {
					quicPort = parts[i+1]
				}
			}
		} else {
			tcpListeners++
			parts := strings.Split(aStr, "/")
			for i, p := range parts {
				if p == "tcp" && i+1 < len(parts) {
					tcpPort = parts[i+1]
				}
			}
		}
		fmt.Printf("p2pmobile: LISTEN addr=%s transport=%s\n", addr, transport)
	}
	fmt.Printf("p2pmobile: node READY — peerID=%s listeners={QUIC:%d TCP:%d} ports={QUIC:%s TCP:%s}\n",
		h.ID(), quicListeners, tcpListeners, quicPort, tcpPort)
	fmt.Printf("p2pmobile: NAT config — ForceReachabilityPrivate=true AutoRelay=static HolePunching=true NATPortMap=true\n")

	// Phase 4 (backgroundUpgrade) is launched by Start() AFTER synchronous
	// bootstrap, to avoid racing on peerstore during QUIC-strict dials.
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

// extractIPFromMultiaddr returns the IP address from a multiaddr string.
// e.g. "/ip4/172.16.25.212/udp/34784/quic-v1" → "172.16.25.212"
func extractIPFromMultiaddr(maStr string) string {
	parts := strings.Split(maStr, "/")
	for i, p := range parts {
		if (p == "ip4" || p == "ip6") && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// actualConnTransport inspects the real connection to a peer and returns
// the transport string ("QUIC" or "TCP") based on the actual multiaddr,
// NOT the AddrInfo we passed to h.Connect (which libp2p may ignore).
func actualConnTransport(h host.Host, pid peer.ID) string {
	for _, c := range h.Network().ConnsToPeer(pid) {
		if strings.Contains(c.RemoteMultiaddr().String(), "/udp/") {
			return "QUIC"
		}
	}
	return "TCP"
}

// actualConnAddr returns the remote multiaddr string of the newest connection.
func actualConnAddr(h host.Host, pid peer.ID) string {
	conns := h.Network().ConnsToPeer(pid)
	if len(conns) == 0 {
		return "<none>"
	}
	return conns[len(conns)-1].RemoteMultiaddr().String()
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
	fmt.Printf("p2pmobile: [bg] upgrade complete — relay=%v dht=%v addrs={QUIC:%d TCP:%d} hasRealIP=%v hasIPv6=%v hasRelay=%v pool={%s}\n",
		n.relayReady, n.dht != nil, quicAddrs, tcpAddrs, hasRealIP, hasIPv6, hasRelay, PoolStats())

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
			if ipAddr == "" || ipAddr == "127.0.0.1" || ipAddr == "::1" || ipAddr == "0.0.0.0" || ipAddr == "::" {
				continue
			}

			// IMPORTANT: Do NOT skip QUIC for private/CGNAT IPs!
			// DCUtR (hole punching) requires knowing the local UDP port so the
			// relay can coordinate simultaneous UDP sends from both sides of
			// the NAT.  Stripping private UDP addrs makes hole punching
			// impossible and forces permanent relay fallback.
			isCGNAT := ipVersion == "ip4" && isPrivateCGNAT(ipAddr)
			if isCGNAT {
				fmt.Printf("p2pmobile: [discover] private/CGNAT IP %s — advertising QUIC anyway for DCUtR hole-punching\n", ipAddr)
			}

			// Add QUIC address (needed for DCUtR even behind CGNAT)
			if quicPort != "" {
				realAddr := fmt.Sprintf("/%s/%s/udp/%s/quic-v1", ipVersion, ipAddr, quicPort)
				ma, err := multiaddr.NewMultiaddr(realAddr)
				if err == nil {
					n.addExtraAddr(ma)
					h.Peerstore().AddAddr(h.ID(), ma, peerstore.PermanentAddrTTL)
					fmt.Printf("p2pmobile: [discover] ✚ QUIC addr discovered: %s (cgnat=%v)\n", realAddr, isCGNAT)
				}
			}

			// Add TCP address
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

// injectPeerQUICAddr checks if the peer already has QUIC addresses in the
// peerstore (from identify exchange). If not, it looks for QUIC addrs that
// identify may have provided with a different IP but valid port, and
// synthesizes a QUIC addr using the peer's observed TCP IP + their QUIC port.
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
		return
	}
	if pid == selfID {
		return
	}

	remoteAddr := c.RemoteMultiaddr().String()

	// Skip relay connections
	if strings.Contains(remoteAddr, "p2p-circuit") {
		return
	}

	// Extract IP from the TCP connection
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
		return
	}

	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}

	// Check peerstore (populated by identify) for the peer's QUIC addresses.
	// If the peer already has a QUIC addr with this exact IP, we're done.
	existingAddrs := h.Peerstore().Addrs(pid)
	var peerQUICPort string
	for _, a := range existingAddrs {
		aStr := a.String()
		if !strings.Contains(aStr, "/quic") {
			continue
		}
		// Already have a QUIC addr with this IP? Nothing to do.
		if strings.Contains(aStr, "/"+ipVersion+"/"+ipAddr+"/") {
			fmt.Printf("p2pmobile: [peer-quic] already have QUIC for %s: %s\n", pidShort, aStr)
			return
		}
		// Extract the peer's QUIC port from any of their QUIC addrs (from identify)
		if peerQUICPort == "" {
			aParts := strings.Split(aStr, "/")
			for j, ap := range aParts {
				if ap == "udp" && j+1 < len(aParts) {
					peerQUICPort = aParts[j+1]
					break
				}
			}
		}
	}

	if peerQUICPort == "" || peerQUICPort == "0" {
		fmt.Printf("p2pmobile: [peer-quic] no QUIC port from identify for %s (have %d addrs)\n", pidShort, len(existingAddrs))
		return
	}

	// Construct QUIC address using peer's real IP (from TCP conn) + their actual QUIC port (from identify)
	quicAddrStr := fmt.Sprintf("/%s/%s/udp/%s/quic-v1", ipVersion, ipAddr, peerQUICPort)
	quicMA, err := multiaddr.NewMultiaddr(quicAddrStr)
	if err != nil {
		return
	}

	h.Peerstore().AddAddr(pid, quicMA, peerstore.TempAddrTTL)
	fmt.Printf("p2pmobile: [peer-quic] ✚ injected QUIC addr for %s: %s (peer port from identify, IP from TCP %s)\n",
		pidShort, quicAddrStr, remoteAddr)
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
		if strings.Contains(addrStr, "127.0.0.1") || strings.Contains(addrStr, "/ip6/::1") ||
			strings.Contains(addrStr, "/ip4/0.0.0.0/") || strings.Contains(addrStr, "/ip6/::/") {
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
			// QUIC-strict: isolate peerstore so h.Connect cannot silently use TCP
			quicInfo := quicOnlyPeerInfo(info)
			if len(quicInfo.Addrs) > 0 {
				allAddrs := h.Peerstore().Addrs(info.ID)
				h.Peerstore().ClearAddrs(info.ID)
				h.Peerstore().AddAddrs(info.ID, quicInfo.Addrs, peerstore.TempAddrTTL)

				ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
				err := h.Connect(ctx, quicInfo)
				cancel()

				h.Peerstore().ClearAddrs(info.ID)
				h.Peerstore().AddAddrs(info.ID, allAddrs, peerstore.PermanentAddrTTL)

				if err == nil {
					transport := actualConnTransport(h, info.ID)
					fmt.Printf("p2pmobile: [bg] bootstrap connected %s via %s (peers=%d)\n",
						info.ID, transport, len(h.Network().Peers()))
					return
				}
				fmt.Printf("p2pmobile: [bg] bootstrap QUIC-strict failed %s: %v\n", info.ID, err)
			}

			// TCP fallback
			tcpInfo := tcpOnlyPeerInfo(info)
			if len(tcpInfo.Addrs) == 0 {
				tcpInfo = info
			}
			ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
			err := h.Connect(ctx, tcpInfo)
			cancel()
			if err == nil {
				transport := actualConnTransport(h, info.ID)
				fmt.Printf("p2pmobile: [bg] bootstrap connected %s via %s (peers=%d)\n",
					info.ID, transport, len(h.Network().Peers()))
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
				fmt.Printf("p2pmobile: [keepalive] %s disconnected, reconnecting (QUIC-strict first)...\n", info.ID)

				// QUIC-strict: isolate peerstore
				quicInfo := quicOnlyPeerInfo(info)
				if len(quicInfo.Addrs) > 0 {
					allAddrs := h.Peerstore().Addrs(info.ID)
					h.Peerstore().ClearAddrs(info.ID)
					h.Peerstore().AddAddrs(info.ID, quicInfo.Addrs, peerstore.TempAddrTTL)

					ctx, cancel := context.WithTimeout(n.ctx, 8*time.Second)
					err := h.Connect(ctx, quicInfo)
					cancel()

					h.Peerstore().ClearAddrs(info.ID)
					h.Peerstore().AddAddrs(info.ID, allAddrs, peerstore.PermanentAddrTTL)

					if err == nil {
						transport := actualConnTransport(h, info.ID)
						fmt.Printf("p2pmobile: [keepalive] reconnected %s via %s (peers=%d)\n",
							info.ID, transport, len(h.Network().Peers()))
						go func() {
							n.discoverRealAddrs()
							n.reserveRelayOnBootstrap()
							n.addRelayCircuitAddrs()
						}()
						continue
					}
					fmt.Printf("p2pmobile: [keepalive] QUIC-strict failed %s: %v\n", info.ID, err)
				}

				// TCP fallback
				tcpInfo := tcpOnlyPeerInfo(info)
				if len(tcpInfo.Addrs) == 0 {
					tcpInfo = info
				}
				ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
				err := h.Connect(ctx, tcpInfo)
				cancel()
				if err != nil {
					fmt.Printf("p2pmobile: [keepalive] TCP reconnect FAILED %s: %v\n", info.ID, err)
					continue
				}
				transport := actualConnTransport(h, info.ID)
				fmt.Printf("p2pmobile: [keepalive] reconnected %s via %s (peers=%d)\n",
					info.ID, transport, len(h.Network().Peers()))
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
		var tcpPort, quicPort string
		for _, la := range h.Network().ListenAddresses() {
			laStr := la.String()
			matchIP := (isIPv6 && strings.Contains(laStr, "/ip6/")) ||
				(!isIPv6 && strings.Contains(laStr, "/ip4/"))
			if !matchIP {
				continue
			}
			parts := strings.Split(laStr, "/")
			for i, p := range parts {
				if p == "tcp" && i+1 < len(parts) && tcpPort == "" {
					tcpPort = parts[i+1]
				}
				if p == "udp" && i+1 < len(parts) && strings.Contains(laStr, "/quic") && quicPort == "" {
					quicPort = parts[i+1]
				}
			}
		}
		if tcpPort == "" || tcpPort == "0" {
			fmt.Printf("p2pmobile: AddExternalAddr: no TCP listen port found for %s, skipping\n", addrStr)
			return nil
		}
		addrStr = strings.Replace(addrStr, "/tcp/0", "/tcp/"+tcpPort, 1)

		// Also inject the corresponding QUIC address so peers can dial us via UDP.
		// Without this, only TCP addresses have real IPs; QUIC shows 0.0.0.0.
		if quicPort != "" && quicPort != "0" {
			// Extract IP version and address from the multiaddr string
			parts := strings.Split(addrStr, "/")
			for i, p := range parts {
				if (p == "ip4" || p == "ip6") && i+1 < len(parts) {
					quicAddrStr := fmt.Sprintf("/%s/%s/udp/%s/quic-v1", p, parts[i+1], quicPort)
					quicMA, err := multiaddr.NewMultiaddr(quicAddrStr)
					if err == nil {
						n.addExtraAddr(quicMA)
						h.Peerstore().AddAddr(h.ID(), quicMA, peerstore.PermanentAddrTTL)
						fmt.Printf("p2pmobile: external QUIC addr added: %s\n", quicAddrStr)
					}
					break
				}
			}
		}
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

// Stop shuts down the node and clears all state so the next Start() is clean.
func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return nil
	}
	n.running = false

	// Cancel context first — stops all background goroutines (keepAlive,
	// backgroundUpgrade, streamPoolJanitor, bootstrapWithRetry).
	if n.cancel != nil {
		n.cancel()
	}

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
		n.dht = nil
	}
	n.ps = nil

	// Evict all pooled streams (they reference the dying host's connections).
	n.streamPoolMu.Lock()
	for pid, ps := range n.streamPool {
		ps.stream.Reset()
		delete(n.streamPool, pid)
	}
	n.streamPoolMu.Unlock()

	// Clear stale extra addresses (old Wi-Fi/cellular IPs).
	// Start() → discoverAndInjectRealAddrs() will repopulate with fresh ones.
	n.extraAddrsMu.Lock()
	n.extraAddrs = nil
	n.extraAddrsMu.Unlock()

	// Clear per-peer rate-limit and direct-probe state.
	n.ingressMu.Lock()
	n.ingressBudget = make(map[string]*peerIngressBudget)
	n.ingressMu.Unlock()

	n.directProbeMu.Lock()
	n.directProbeLast = make(map[string]time.Time)
	n.directProbeMu.Unlock()

	n.relayReady = false

	if n.host != nil {
		n.host.Close()
		n.host = nil
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

// GetPeerDirectAddr returns "ip:port" of the newest direct (non-relay) connection
// to the given peer, or "" if none exists.  Used by Kotlin to inject the known
// hole-punched address as a WebRTC ICE candidate for 0-RTT call setup.
func (n *Node) GetPeerDirectAddr(peerID string) string {
	pid, err := peer.Decode(strings.TrimSpace(peerID))
	if err != nil {
		return ""
	}
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return ""
	}
	conns := h.Network().ConnsToPeer(pid)
	// Walk newest → oldest; prefer direct.
	for i := len(conns) - 1; i >= 0; i-- {
		c := conns[i]
		ra := c.RemoteMultiaddr()
		if ra == nil {
			continue
		}
		s := ra.String()
		if strings.Contains(s, "/p2p-circuit") {
			continue
		}
		// Extract IP + port from multiaddr like /ip4/1.2.3.4/udp/4001/quic-v1
		// or /ip6/::1/tcp/9000
		ip, _ := ra.ValueForProtocol(multiaddr.P_IP4)
		if ip == "" {
			ip, _ = ra.ValueForProtocol(multiaddr.P_IP6)
		}
		if ip == "" {
			continue
		}
		port, _ := ra.ValueForProtocol(multiaddr.P_UDP)
		if port == "" {
			port, _ = ra.ValueForProtocol(multiaddr.P_TCP)
		}
		if port == "" {
			continue
		}
		addr := ip + ":" + port
		fmt.Printf("p2pmobile: GetPeerDirectAddr(%s) = %s\n", peerID[:16], addr)
		return addr
	}
	return ""
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

// maybeScheduleDirectProbe starts a fast direct upgrade when a relay connection is detected.
// Uses aggressiveDirectUpgrade for multi-attempt punching. Debounced per peer (10s).
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
	if t, ok := n.directProbeLast[peerIDStr]; ok && now.Sub(t) < 10*time.Second {
		n.directProbeMu.Unlock()
		return
	}
	if len(n.directProbeLast) > 500 {
		n.directProbeLast = make(map[string]time.Time)
	}
	n.directProbeLast[peerIDStr] = now
	n.directProbeMu.Unlock()

	go n.aggressiveDirectUpgrade(pid)
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
		// Zero-copy: slice directly into packed buffer — avoids make+copy per frame.
		// Safe because packed (from JNI) is not reused while sendDirectFrames writes.
		out = append(out, packed[off:off+ln])
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
	needed := 4 + len(data)
	buf := getFrameBuf(needed)
	buf[0] = byte(length >> 24)
	buf[1] = byte(length >> 16)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)
	copy(buf[4:], data)
	_, err := s.Write(buf[:needed])
	putFrameBuf(buf)
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

	// Upgrade TCP→QUIC: if pooled stream is TCP but a QUIC connection exists, evict
	// so the next NewStream picks the faster QUIC path.
	if ps := n.getPooledStream(targetPeerID); ps != nil {
		ra := ps.stream.Conn().RemoteMultiaddr().String()
		if !strings.Contains(ra, "/udp/") { // current pool is TCP
			for _, c := range h.Network().ConnsToPeer(pid) {
				if strings.Contains(c.RemoteMultiaddr().String(), "/udp/") {
					fmt.Printf("p2pmobile: [pool] evict TCP stream — QUIC conn available for %s\n", pidShort)
					n.evictPooledStream(targetPeerID)
					break
				}
			}
		}
	}

	// ── Pooled v2 stream ─────────────────────────────────────────────
	if ps := n.getPooledStream(targetPeerID); ps != nil {
		var writeErr error
		for i, data := range frames {
			if writeErr = n.writeToPooledStream(ps, data); writeErr != nil {
				break
			}
			// Yield between frames so QUIC ACK processing / congestion control can run.
			// Without this, a tight loop of 4×512 KB writes can fill the congestion
			// window and block, preventing ACKs from being processed on time.
			if i < len(frames)-1 {
				runtime.Gosched()
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
// Strategy: QUIC-strict first (peerstore isolated to prevent relay sneak-in),
// then TCP fallback. Returns true if a direct (non-relay) connection exists.
// If cachedDHTAddrs is non-nil, those addrs are used instead of a fresh DHT lookup
// (avoids 5s DHT timeout on repeated passes).
func (n *Node) tryDialPeerDirect(pid peer.ID, cachedDHTAddrs []multiaddr.Multiaddr) bool {
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

	// Determine which IP families we can actually reach from our listen addrs.
	hasLocalIPv4, hasLocalIPv6 := false, false
	for _, la := range h.Addrs() {
		laStr := la.String()
		if strings.Contains(laStr, "p2p-circuit") {
			continue
		}
		if strings.Contains(laStr, "/ip4/") {
			ip := extractIPFromMultiaddr(laStr)
			if ip != "127.0.0.1" && ip != "0.0.0.0" {
				hasLocalIPv4 = true
			}
		}
		if strings.Contains(laStr, "/ip6/") {
			ip := extractIPFromMultiaddr(laStr)
			if ip != "::1" && ip != "::" {
				hasLocalIPv6 = true
			}
		}
	}

	// Collect all known direct addresses (peerstore + DHT), split by transport.
	// Skip addrs whose IP family we cannot reach (e.g. IPv6-only peer when we're IPv4-only).
	var quicAddrs, tcpAddrs []multiaddr.Multiaddr
	seen := make(map[string]bool)
	var skippedFamily int

	addIfNew := func(a multiaddr.Multiaddr) {
		aStr := a.String()
		if strings.Contains(aStr, "p2p-circuit") || seen[aStr] {
			return
		}
		// Filter unroutable addresses (loopback, bind-all)
		ip := extractIPFromMultiaddr(aStr)
		if ip == "127.0.0.1" || ip == "::1" || ip == "0.0.0.0" || ip == "::" || ip == "" {
			return
		}
		// Filter unreachable IP families
		isV6 := strings.Contains(aStr, "/ip6/")
		if isV6 && !hasLocalIPv6 {
			skippedFamily++
			return
		}
		if !isV6 && strings.Contains(aStr, "/ip4/") && !hasLocalIPv4 {
			skippedFamily++
			return
		}
		seen[aStr] = true
		if strings.Contains(aStr, "/quic") {
			quicAddrs = append(quicAddrs, a)
		} else if strings.Contains(aStr, "/tcp/") {
			tcpAddrs = append(tcpAddrs, a)
		}
	}

	// Source 1: Peerstore
	for _, a := range h.Peerstore().Addrs(pid) {
		addIfNew(a)
	}

	// Source 2: cached DHT addrs (or fresh lookup if nil)
	if cachedDHTAddrs != nil {
		for _, a := range cachedDHTAddrs {
			addIfNew(a)
		}
	} else {
		n.mu.RLock()
		d := n.dht
		n.mu.RUnlock()
		if d != nil {
			ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
			peerInfo, err := d.FindPeer(ctx, pid)
			cancel()
			if err == nil {
				for _, a := range peerInfo.Addrs {
					addIfNew(a)
				}
			} else {
				fmt.Printf("p2pmobile: [dial] DHT FindPeer failed for %s: %v\n", pidShort, err)
			}
		}
	}

	fmt.Printf("p2pmobile: [dial] direct addrs for %s: QUIC=%d TCP=%d (skippedFamily=%d localIPv4=%v localIPv6=%v)\n",
		pidShort, len(quicAddrs), len(tcpAddrs), skippedFamily, hasLocalIPv4, hasLocalIPv6)

	if len(quicAddrs) == 0 && len(tcpAddrs) == 0 {
		if skippedFamily > 0 {
			fmt.Printf("p2pmobile: [dial] no reachable addrs for %s — all %d addrs in unreachable IP family\n", pidShort, skippedFamily)
		}
		return false
	}

	// Priority order: QUIC first (lower latency, better mobile), then TCP.
	// For each group, isolate peerstore to ONLY those addrs so h.Connect()
	// cannot silently fall back to relay circuit addrs in the peerstore.
	groups := []struct {
		addrs []multiaddr.Multiaddr
		name  string
	}{
		{quicAddrs, "QUIC"},
		{tcpAddrs, "TCP"},
	}

	for _, g := range groups {
		if len(g.addrs) == 0 {
			continue
		}
		for i, a := range g.addrs {
			fmt.Printf("p2pmobile: [dial] trying %s %d/%d for %s addr=%s\n", g.name, i+1, len(g.addrs), pidShort, a)
		}

		// Isolate peerstore: temporarily replace all addrs with ONLY our direct addrs.
		// This prevents h.Connect from using relay circuit addrs that may be in peerstore.
		allAddrs := h.Peerstore().Addrs(pid)
		h.Peerstore().ClearAddrs(pid)
		h.Peerstore().AddAddrs(pid, g.addrs, peerstore.TempAddrTTL)

		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		// WithForceDirectDial prevents h.Connect() from short-circuiting on an
		// existing relay connection — it forces a real dial to our direct addrs.
		ctx = network.WithForceDirectDial(ctx, "direct-upgrade")
		err := h.Connect(ctx, peer.AddrInfo{ID: pid, Addrs: g.addrs})
		cancel()

		// Restore full peerstore
		h.Peerstore().ClearAddrs(pid)
		h.Peerstore().AddAddrs(pid, allAddrs, peerstore.PermanentAddrTTL)
		// Also re-add DHT/injected addrs that were collected
		h.Peerstore().AddAddrs(pid, quicAddrs, peerstore.TempAddrTTL)
		h.Peerstore().AddAddrs(pid, tcpAddrs, peerstore.TempAddrTTL)

		if err != nil {
			fmt.Printf("p2pmobile: [dial] %s FAILED for %s: %v\n", g.name, pidShort, err)
			continue
		}

		// Verify we got a DIRECT connection (not relay that snuck in)
		if n.hasDirectConnToPeer(pid) {
			actual := actualConnTransport(h, pid)
			addr := actualConnAddr(h, pid)
			fmt.Printf("p2pmobile: [dial] ✓ DIRECT %s to %s actual=%s addr=%s\n", g.name, pidShort, actual, addr)
			return true
		}
		fmt.Printf("p2pmobile: [dial] %s connected but NOT direct for %s (relay snuck in)\n", g.name, pidShort)
	}

	return n.hasDirectConnToPeer(pid)
}

func (n *Node) dialPeerRelayFallback(pid peer.ID) error {
	h := n.host
	if h == nil {
		return fmt.Errorf("not started")
	}
	pidShort := pid.String()[:16]
	fmt.Printf("p2pmobile: [dial] falling back to RELAY for %s\n", pidShort)
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
			fmt.Printf("p2pmobile: [dial] RELAY connected to %s via %s\n", pidShort, bp.ID)

			// Push identify immediately so the remote peer learns our fresh
			// addresses. This is critical for DCUtR: the hole-punch service
			// needs both sides to know each other's real addresses.
			if emitter, eErr := h.EventBus().Emitter(new(event.EvtLocalAddressesUpdated)); eErr == nil {
				_ = emitter.Emit(event.EvtLocalAddressesUpdated{})
				_ = emitter.Close()
			}

			// NOTE: Do NOT launch aggressiveDirectUpgrade here — ConnectedF
			// already triggers maybeScheduleDirectProbe (debounced) when the
			// relay connection fires. Launching from both places causes two
			// concurrent goroutines doing peerstore manipulation for the same
			// peer, which is a race condition that destabilizes relay connections.
			return nil
		}
		fmt.Printf("p2pmobile: [dial] relay failed to %s via %s: %v\n", pidShort, bp.ID, err)
	}
	return fmt.Errorf("unreachable: %s", pid)
}

// peerHasReachableDirectAddrs returns true if the peer has at least one
// non-relay address in an IP family we can reach from our local addrs.
func (n *Node) peerHasReachableDirectAddrs(pid peer.ID) bool {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return false
	}

	// Determine local IP family capabilities
	hasLocalIPv4, hasLocalIPv6 := false, false
	for _, la := range h.Addrs() {
		laStr := la.String()
		if strings.Contains(laStr, "p2p-circuit") {
			continue
		}
		if strings.Contains(laStr, "/ip4/") {
			ip := extractIPFromMultiaddr(laStr)
			if ip != "127.0.0.1" && ip != "0.0.0.0" {
				hasLocalIPv4 = true
			}
		}
		if strings.Contains(laStr, "/ip6/") {
			ip := extractIPFromMultiaddr(laStr)
			if ip != "::1" && ip != "::" {
				hasLocalIPv6 = true
			}
		}
	}

	for _, a := range h.Peerstore().Addrs(pid) {
		aStr := a.String()
		if strings.Contains(aStr, "p2p-circuit") {
			continue
		}
		// Skip unroutable addresses
		ip := extractIPFromMultiaddr(aStr)
		if ip == "127.0.0.1" || ip == "::1" || ip == "0.0.0.0" || ip == "::" || ip == "" {
			continue
		}
		isV6 := strings.Contains(aStr, "/ip6/")
		if isV6 && hasLocalIPv6 {
			return true
		}
		if !isV6 && strings.Contains(aStr, "/ip4/") && hasLocalIPv4 {
			return true
		}
	}
	return false
}

// aggressiveDirectUpgrade tries to upgrade a relay connection to direct.
// It runs multiple fast passes with identify pushes between them, giving
// DCUtR and our custom direct dialer every chance to punch through.
func (n *Node) aggressiveDirectUpgrade(pid peer.ID) {
	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}

	// Wait briefly for identify exchange to complete on the relay connection.
	// The remote peer's fresh addresses should arrive via identify within 1-2s.
	time.Sleep(800 * time.Millisecond)

	// Pre-check: if the peer has NO reachable direct addrs (e.g. IPv6-only peer
	// vs our IPv4-only device), skip the upgrade loop entirely. Relay is the
	// only viable path and repeated upgrade attempts just destabilize it.
	if !n.peerHasReachableDirectAddrs(pid) {
		fmt.Printf("p2pmobile: [upgrade] no reachable direct addrs for %s — staying on relay\n", pidShort)
		return
	}

	for attempt := 1; attempt <= 4; attempt++ {
		n.mu.RLock()
		h := n.host
		running := n.running
		n.mu.RUnlock()
		if !running || h == nil {
			return
		}
		if h.Network().Connectedness(pid) != network.Connected {
			fmt.Printf("p2pmobile: [upgrade] peer %s disconnected, aborting\n", pidShort)
			return
		}
		if n.hasDirectConnToPeer(pid) {
			actual := actualConnTransport(h, pid)
			fmt.Printf("p2pmobile: [upgrade] ✓ DIRECT achieved for %s (attempt %d) transport=%s\n", pidShort, attempt, actual)
			return
		}

		fmt.Printf("p2pmobile: [upgrade] attempt %d/4 direct dial for %s\n", attempt, pidShort)
		if n.tryDialPeerDirect(pid, nil) {
			actual := actualConnTransport(h, pid)
			fmt.Printf("p2pmobile: [upgrade] ✓ DIRECT punched for %s (attempt %d) transport=%s\n", pidShort, attempt, actual)
			// Close relay connections now that we have direct
			for _, c := range h.Network().ConnsToPeer(pid) {
				if strings.Contains(c.RemoteMultiaddr().String(), "p2p-circuit") {
					c.Close()
				}
			}
			return
		}

		// Between attempts: push identify again (fresh addrs may have arrived
		// from Kotlin's discoverAndInjectRealAddrs) and back off briefly.
		if emitter, err := h.EventBus().Emitter(new(event.EvtLocalAddressesUpdated)); err == nil {
			_ = emitter.Emit(event.EvtLocalAddressesUpdated{})
			_ = emitter.Close()
		}
		backoff := time.Duration(500+attempt*500) * time.Millisecond
		time.Sleep(backoff)
	}
	fmt.Printf("p2pmobile: [upgrade] direct upgrade failed for %s after 4 attempts — staying on relay\n", pidShort)
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

	// Do DHT lookup ONCE before the pass loop — avoids 5s DHT timeout per pass.
	var dhtAddrs []multiaddr.Multiaddr
	n.mu.RLock()
	d := n.dht
	n.mu.RUnlock()
	if d != nil {
		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		peerInfo, err := d.FindPeer(ctx, pid)
		cancel()
		if err == nil {
			dhtAddrs = peerInfo.Addrs
			fmt.Printf("p2pmobile: [dial] DHT found %d addrs for %s\n", len(dhtAddrs), pidShort)
		} else {
			dhtAddrs = []multiaddr.Multiaddr{} // empty but non-nil = "DHT done"
			fmt.Printf("p2pmobile: [dial] DHT FindPeer failed for %s: %v\n", pidShort, err)
		}
	}

	for pass := 0; pass < directDialPasses; pass++ {
		if pass > 0 {
			j := 80 + rand.Intn(220)
			time.Sleep(time.Duration(j) * time.Millisecond)
			fmt.Printf("p2pmobile: [dial] direct pass %d/%d jitter=%dms peer=%s\n", pass+1, directDialPasses, j, pidShort)
		}
		if n.tryDialPeerDirect(pid, dhtAddrs) {
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
		zbuf := getZlibBuf()
		zw := getZlibWriter(zbuf)
		zw.Write(data)
		zw.Close()
		putZlibWriter(zw)
		compressed := zbuf.Bytes()
		// Only use compression if it actually shrinks the data
		if len(compressed) < len(data)-64 {
			payload = make([]byte, len(compressed))
			copy(payload, compressed)
			flags = 0x01
			p2pDebugf("p2pmobile: [compress] %d → %d bytes (%.0f%% reduction)\n",
				len(data), len(compressed), 100*(1-float64(len(compressed))/float64(len(data))))
		} else {
			payload = data
		}
		putZlibBuf(zbuf)
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
			// Bounded decompression using pooled copy buffer (avoids io.ReadAll heap growth).
			decompBuf := getZlibBuf()
			copyBuf := getReadBuf()
			_, cpErr := io.CopyBuffer(decompBuf, io.LimitReader(r, int64(maxMessageSize)+1), *copyBuf)
			putReadBuf(copyBuf)
			r.Close()
			if cpErr != nil || decompBuf.Len() > maxMessageSize {
				malformed++
				p2pDebugf("p2pmobile: v2 zlib read error from %s: %v (decompLen=%d)\n", fromShort, cpErr, decompBuf.Len())
				putZlibBuf(decompBuf)
				if malformed >= dmMaxMalformedFrames {
					fmt.Printf("p2pmobile: SECURITY dm v2 too many bad decompress peer=%s\n", fromShort)
					s.Reset()
					return
				}
				continue
			}
			// Copy out of pool buffer before returning it
			data = make([]byte, decompBuf.Len())
			copy(data, decompBuf.Bytes())
			p2pDebugf("p2pmobile: [decompress] %d → %d bytes from %s\n", len(payload), len(data), fromShort)
			putZlibBuf(decompBuf)
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
// It tears down ALL dead connections (including bootstrap), clears stale state, then
// rebuilds from scratch: reconnect bootstrap → discover addrs → reserve relay → identify push.
//
// Critical ordering: close ALL sockets BEFORE reconnecting. After WiFi→4G, every existing
// socket is dead but TCP keepalive takes minutes to detect. ensureBootstrap() would see
// Connectedness==Connected on the dead socket and return immediately, causing all subsequent
// steps (discoverRealAddrs, reserveRelay) to operate on the dead connection.
func (n *Node) OnNetworkChanged() {
	n.mu.RLock()
	h := n.host
	running := n.running
	n.mu.RUnlock()
	if !running || h == nil {
		return
	}
	fmt.Println("p2pmobile: ⚡ OnNetworkChanged — tearing down dead connections, rebuilding")

	// 1. Clear ALL stale extra addresses (old WiFi/cellular IPs are now dead)
	n.extraAddrsMu.Lock()
	oldCount := len(n.extraAddrs)
	n.extraAddrs = nil
	n.extraAddrsMu.Unlock()
	fmt.Printf("p2pmobile: [netchange] cleared %d stale extra addrs\n", oldCount)

	// 2. Evict ALL pooled streams (old connections are dead after network change)
	n.streamPoolMu.Lock()
	poolCount := len(n.streamPool)
	for pid, ps := range n.streamPool {
		ps.stream.Reset()
		delete(n.streamPool, pid)
	}
	n.streamPoolMu.Unlock()
	fmt.Printf("p2pmobile: [netchange] evicted %d pooled streams\n", poolCount)
	n.clearIngressBudgets()

	// 3. Close ALL connections — including bootstrap. Every socket is dead after a
	// network switch. Without this, ensureBootstrap() sees stale Connected state.
	allPeers := h.Network().Peers()
	fmt.Printf("p2pmobile: [netchange] closing %d peer connections (all dead sockets)\n", len(allPeers))
	for _, p := range allPeers {
		_ = h.Network().ClosePeer(p)
	}

	// 4. Clear stale peerstore addresses for non-bootstrap peers.
	// After a network change, the remote peer's old IPs are likely dead too
	// (especially if both sides switched networks). Keep bootstrap addrs (static).
	bootstrapIDs := make(map[peer.ID]bool)
	for _, bp := range n.bootstrapPeers {
		bootstrapIDs[bp.ID] = true
	}
	for _, p := range allPeers {
		if !bootstrapIDs[p] {
			h.Peerstore().ClearAddrs(p)
		}
	}
	fmt.Printf("p2pmobile: [netchange] cleared stale peerstore addrs for %d non-bootstrap peers\n", len(allPeers)-len(bootstrapIDs))

	// 5. Brief pause to let kernel release dead sockets
	time.Sleep(100 * time.Millisecond)

	// 6. Reconnect bootstrap fresh on the new network interface
	fmt.Println("p2pmobile: [netchange] reconnecting bootstrap on new network...")
	n.ensureBootstrap()

	// 7. Re-discover real addresses from the new bootstrap connection
	n.discoverRealAddrs()

	// 8. Re-reserve relay + advertise circuit addrs
	n.reserveRelayOnBootstrap()
	n.addRelayCircuitAddrs()

	// 9. Push updated identify so remotes learn fresh reachability (DCUtR / hole-punch).
	if emitter, err := h.EventBus().Emitter(new(event.EvtLocalAddressesUpdated)); err == nil {
		_ = emitter.Emit(event.EvtLocalAddressesUpdated{})
		_ = emitter.Close()
		fmt.Println("p2pmobile: [netchange] emitted EvtLocalAddressesUpdated → identify push")
	}

	// 10. Reset direct-probe debounce so relay connections trigger immediate probes
	n.directProbeMu.Lock()
	n.directProbeLast = make(map[string]time.Time)
	n.directProbeMu.Unlock()

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
