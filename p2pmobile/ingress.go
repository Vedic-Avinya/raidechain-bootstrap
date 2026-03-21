package p2pmobile

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Per-peer inbound limits on direct-message streams (DoS / resource abuse).
const (
	dmIngressMaxBytesPerSec  = 6 << 20 // 6 MiB/s average wire payload per peer
	dmIngressBurstBytes      = 16 << 20
	dmIngressMaxFramesPerSec = 450
	dmIngressBurstFrames     = 900
	maxIngressBudgetEntries  = 4096
	dmMaxMalformedFrames     = 24
)

type peerIngressBudget struct {
	bytes  *rate.Limiter
	frames *rate.Limiter
}

func (n *Node) getIngressBudget(pid peer.ID) *peerIngressBudget {
	key := pid.String()
	n.ingressMu.Lock()
	defer n.ingressMu.Unlock()
	if n.ingressBudget == nil {
		n.ingressBudget = make(map[string]*peerIngressBudget)
	}
	if b, ok := n.ingressBudget[key]; ok {
		return b
	}
	if len(n.ingressBudget) >= maxIngressBudgetEntries {
		// Drop one arbitrary entry to bound memory (mobile).
		for k := range n.ingressBudget {
			delete(n.ingressBudget, k)
			break
		}
	}
	b := &peerIngressBudget{
		bytes:  rate.NewLimiter(rate.Limit(dmIngressMaxBytesPerSec), dmIngressBurstBytes),
		frames: rate.NewLimiter(rate.Limit(dmIngressMaxFramesPerSec), dmIngressBurstFrames),
	}
	n.ingressBudget[key] = b
	return b
}

// allowIncomingDM returns false if this frame should be rejected (rate limit).
// wireBytes is the declared v2/v1 payload length on the wire (before allocation).
func (n *Node) allowIncomingDM(pid peer.ID, wireBytes int) bool {
	if wireBytes < 0 || wireBytes > maxMessageSize {
		return false
	}
	b := n.getIngressBudget(pid)
	now := time.Now()
	if !b.frames.AllowN(now, 1) {
		return false
	}
	if wireBytes > 0 && !b.bytes.AllowN(now, wireBytes) {
		return false
	}
	return true
}

func (n *Node) clearIngressBudgets() {
	n.ingressMu.Lock()
	n.ingressBudget = nil
	n.ingressMu.Unlock()
}
