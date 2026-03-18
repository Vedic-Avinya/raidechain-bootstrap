// Package driverbridge provides the driver WebSocket bridge and optional per-peer message queue.
//
// peer_queue.go: Bounded per-peer queue for critical messages. When a peer reconnects,
// queued messages are delivered to their new connection (reduces message loss during WiFi↔5G handover).

package driverbridge

import (
	"log/slog"
	"sync"
)

const (
	maxQueuedPerPeer = 50
)

// PeerQueue holds a bounded FIFO of messages per peer ID. Thread-safe.
type PeerQueue struct {
	mu     sync.Mutex
	queues map[string][][]byte
}

// NewPeerQueue creates an empty per-peer queue.
func NewPeerQueue() *PeerQueue {
	return &PeerQueue{queues: make(map[string][] []byte)}
}

// Enqueue appends a message for the given peer. Drops oldest if at capacity.
func (q *PeerQueue) Enqueue(peerID string, data []byte) {
	if peerID == "" || len(data) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	list := q.queues[peerID]
	if len(list) >= maxQueuedPerPeer {
		list = list[1:]
	}
	q.queues[peerID] = append(list, append([]byte(nil), data...))
}

// Drain removes and returns all queued messages for the peer.
func (q *PeerQueue) Drain(peerID string) [][]byte {
	q.mu.Lock()
	list := q.queues[peerID]
	delete(q.queues, peerID)
	q.mu.Unlock()
	return list
}

// Has returns whether there are queued messages for the peer.
func (q *PeerQueue) Has(peerID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queues[peerID]) > 0
}

// FlushTo sends all queued messages for the peer to the given send function, then drains the queue.
// Logs and skips send errors; each message is sent once.
func (q *PeerQueue) FlushTo(peerID string, send func(data []byte) error) {
	for _, data := range q.Drain(peerID) {
		if err := send(data); err != nil {
			slog.Warn("driver_bridge", "msg", "failed to send queued message to peer", "peer_id", peerID, "err", err)
		}
	}
}
