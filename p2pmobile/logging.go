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
