package p2pmobile

import (
	"fmt"
	"sync/atomic"
)

// Production: call SetVerboseLogging(false) from Kotlin on release builds to silence
// p2p_transport-prefixed lines and most p2pmobile diagnostics (DoS drops still print).
var p2pVerboseLogs atomic.Bool

func init() {
	p2pVerboseLogs.Store(true)
}

// SetVerboseLogging toggles p2p_transport logs and p2pDebugf output. Security-sensitive
// events (e.g. ingress rate limit) still use fmt.Printf when needed.
func (n *Node) SetVerboseLogging(enabled bool) {
	p2pVerboseLogs.Store(enabled)
}

func logP2pTransport(format string, args ...interface{}) {
	if !p2pVerboseLogs.Load() {
		return
	}
	fmt.Printf("p2p_transport: "+format+"\n", args...)
}

func p2pDebugf(format string, args ...interface{}) {
	if !p2pVerboseLogs.Load() {
		return
	}
	fmt.Printf(format, args...)
}

// LogTransferStats prints pool hits/misses and active transfer slots to logcat.
// Call from Kotlin periodically or after large transfers for diagnostics.
func (n *Node) LogTransferStats() string {
	hits, misses := poolStats()
	hitRate := float64(0)
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) * 100.0 / float64(total)
	}
	// Count active file transfer slots
	activeSlots := len(activeTransferSemaphore)
	msg := fmt.Sprintf("pool_hits=%d pool_misses=%d hit_rate=%.1f%% active_file_transfers=%d/%d",
		hits, misses, hitRate, activeSlots, maxConcurrentFileTransfers)
	fmt.Printf("p2p_transport: transfer_stats %s\n", msg)
	return msg
}
