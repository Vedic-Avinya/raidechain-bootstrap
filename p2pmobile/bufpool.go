package p2pmobile

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"sync"
	"sync/atomic"
)

// ════════════════════════════════════════════════════════════════════════════
// Buffer Pools — zero-allocation hot-path for high-throughput file transfer
//
// Why: A 500 MB file at 256 KB chunks = ~2000 frames. Without pooling each
// frame allocates a fresh []byte on the Go heap, causing hundreds of MB of
// GC pressure that stalls the entire process (STW pauses > 50 ms on mobile).
//
// Design:
//   - readBufPool:   64 KB slabs for stream readers (handleOptimizedStream,
//                    handleDirectStream).  Returned after the callback fires.
//   - frameBufPool:  Scratch buffers for v1 frame writes (header + payload).
//   - zlibBufPool:   *bytes.Buffer for zlib compression output.
//   - zlibWriterPool: *zlib.Writer — creating a new zlib.Writer allocates
//                    ~300 KB of hash tables; reusing avoids that per-frame.
//
// Metrics: poolHits / poolMisses counters let diagnostics verify that the
// pools are warm (hit rate should be > 95 % after the first few frames).
// ════════════════════════════════════════════════════════════════════════════

const (
	// readBufSize matches the QUIC max datagram / standard chunk boundary.
	readBufSize = 64 * 1024
	// frameBufInitSize is the starting capacity for v1 frame scratch buffers.
	frameBufInitSize = 4 + 64*1024
)

// ── Read buffer pool (64 KB slabs) ──────────────────────────────────────

var readBufPool = sync.Pool{
	New: func() interface{} {
		poolMisses.Add(1)
		b := make([]byte, readBufSize)
		return &b
	},
}

func getReadBuf() *[]byte {
	poolHits.Add(1)
	return readBufPool.Get().(*[]byte)
}

func putReadBuf(b *[]byte) {
	if b == nil {
		return
	}
	// Only return standard-sized buffers to the pool.
	if cap(*b) == readBufSize {
		readBufPool.Put(b)
	}
}

// ── Payload buffer pool (growable, for v1 frame writes / decompression) ─

var frameBufPool = sync.Pool{
	New: func() interface{} {
		poolMisses.Add(1)
		b := make([]byte, 0, frameBufInitSize)
		return b
	},
}

// getFrameBuf returns a []byte with len == needed, possibly from the pool.
// Caller must putFrameBuf when done.
func getFrameBuf(needed int) []byte {
	poolHits.Add(1)
	b := frameBufPool.Get().([]byte)
	if cap(b) >= needed {
		return b[:needed]
	}
	// Pool buf too small — allocate fresh; the old one is lost (GC'd).
	poolMisses.Add(1)
	return make([]byte, needed)
}

func putFrameBuf(b []byte) {
	// Keep buffers up to 1 MB in the pool; larger ones are one-offs.
	if cap(b) <= 1<<20 {
		frameBufPool.Put(b[:0])
	}
}

// ── Zlib compression buffer pool ────────────────────────────────────────

var zlibBufPool = sync.Pool{
	New: func() interface{} {
		poolMisses.Add(1)
		return new(bytes.Buffer)
	},
}

func getZlibBuf() *bytes.Buffer {
	poolHits.Add(1)
	buf := zlibBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putZlibBuf(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	// Don't return huge buffers (> 1 MB) — let GC reclaim them.
	if buf.Cap() <= 1<<20 {
		zlibBufPool.Put(buf)
	}
}

// ── Zlib writer pool (each writer holds ~300 KB of Huffman tables) ──────

var zlibWriterPool = sync.Pool{
	New: func() interface{} {
		poolMisses.Add(1)
		return zlib.NewWriter(nil)
	},
}

func getZlibWriter(dst *bytes.Buffer) *zlib.Writer {
	poolHits.Add(1)
	w := zlibWriterPool.Get().(*zlib.Writer)
	w.Reset(dst)
	return w
}

func putZlibWriter(w *zlib.Writer) {
	if w != nil {
		zlibWriterPool.Put(w)
	}
}

// ── Diagnostic counters ─────────────────────────────────────────────────

var (
	poolHits   atomic.Int64
	poolMisses atomic.Int64
)

// poolStats returns (hits, misses) for internal use only (not exported to gomobile).
func poolStats() (int64, int64) {
	return poolHits.Load(), poolMisses.Load()
}

// PoolStats returns a formatted string "hits=N misses=N" for gomobile (single return value).
func PoolStats() string {
	h, m := poolStats()
	return fmt.Sprintf("hits=%d misses=%d", h, m)
}
