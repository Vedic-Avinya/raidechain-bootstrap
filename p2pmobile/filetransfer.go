package p2pmobile

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ════════════════════════════════════════════════════════════════════════════
// Dedicated File Transfer Protocol — /ridechain/file/1.0.0
//
// WHY: Multiplexing 500 MB file chunks on the same dm/2.0.0 stream as real-time
// chat causes Head-of-Line (HOL) blocking — a 256 KB cipher chunk queued ahead
// of a tiny chat_msg delays the chat by the chunk's transfer time.
//
// DESIGN:
//   - Opens an ephemeral, dedicated libp2p stream per file transfer.
//   - Uses the same v2 framing (flags + length + payload) for wire compat.
//   - Flow-controlled: sender waits for per-batch ACKs from receiver so it
//     cannot overflow QUIC/TCP OS buffers.
//   - Buffers are drawn from sync.Pool (bufpool.go) — zero sustained alloc.
//   - Chat messages continue on dm/2.0.0 unblocked.
//
// WIRE FORMAT (per frame, identical to dm/2.0.0):
//   [flags:1] [length:4 BE] [payload:N]
//   flags 0x00 = raw, 0x01 = zlib, 0x02 = ACK frame
//
// ACK FRAME (receiver → sender):
//   [0x02] [4 BE: acked_seq_count]  — "I have received seq 0..N-1"
//
// TRANSFER HEADER (first frame, flags=0x00):
//   [4 BE: total_chunks] [8 BE: total_bytes] [payload: first chunk or metadata]
//
// The sender pauses after every ackBatchSize chunks and waits for an ACK.
// This provides backpressure that matches sender speed to receiver bandwidth.
// ════════════════════════════════════════════════════════════════════════════

const (
	// FileTransferProtocol is the dedicated stream protocol for large file transfers.
	FileTransferProtocol = "/ridechain/file/1.0.0"

	// FileResumeProtocol supports byte-offset resume for interrupted transfers.
	// Wire: sender opens with a RESUME_OFFER header containing a stable transferId,
	// totalChunks, and totalBytes. The receiver checks its local checkpoint store,
	// replies with a RESUME_ACK containing the count of chunks already received.
	// The sender then skips to that offset and continues normally.
	FileResumeProtocol = "/ridechain/file/2.0.0"

	// fileAckBatchSize: sender pauses after this many chunks and waits for ACK.
	// 32 chunks × 256 KB = 8 MB per batch — fits comfortably in QUIC/TCP buffers.
	fileAckBatchSize = 32

	// fileTransferTimeout: max time for the entire transfer (per-stream).
	fileTransferTimeout = 30 * time.Minute

	// fileChunkReadTimeout: max time to read a single chunk from the stream.
	fileChunkReadTimeout = 60 * time.Second

	// Maximum concurrent file transfers per node (prevent resource exhaustion).
	maxConcurrentFileTransfers = 4

	// flagRaw is the default frame flag (no compression).
	flagRaw byte = 0x00
	// flagAck is the ACK frame flag (receiver → sender).
	flagAck byte = 0x02
	// flagResumeOffer is sent by the sender to propose resuming a transfer.
	flagResumeOffer byte = 0x03
	// flagResumeAck is sent by the receiver with the last contiguous chunk index.
	flagResumeAck byte = 0x04
)

// FileTransferHandler receives completed file transfer chunks on the receiver side.
// Called per-chunk so the receiver can write to disk incrementally (zero full-file RAM).
type FileTransferHandler interface {
	// OnFileChunk is called for each received chunk.
	// seq is 0-indexed, totalChunks is the declared total, data is the raw payload.
	OnFileChunk(from string, transferId string, seq int, totalChunks int, data []byte)
	// OnFileTransferComplete is called when all chunks have been received.
	OnFileTransferComplete(from string, transferId string, totalChunks int)
	// GetResumeOffset returns the number of contiguous chunks already received
	// for the given transferId. Returns 0 if no checkpoint exists.
	// This enables pause/resume across network drops.
	GetResumeOffset(from string, transferId string) int
}

// activeTransferSemaphore bounds concurrent file transfers.
var activeTransferSemaphore = make(chan struct{}, maxConcurrentFileTransfers)

func acquireTransferSlot() bool {
	select {
	case activeTransferSemaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func releaseTransferSlot() {
	<-activeTransferSemaphore
}

// registerFileTransferHandler sets up the stream handler for incoming file transfers.
func (n *Node) registerFileTransferHandler() {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return
	}
	h.SetStreamHandler(protocol.ID(FileTransferProtocol), n.handleFileTransferStream)
	h.SetStreamHandler(protocol.ID(FileResumeProtocol), n.handleResumeFileTransferStream)
	fmt.Printf("p2pmobile: [file] registered handlers for %s + %s\n", FileTransferProtocol, FileResumeProtocol)
}

// SetFileTransferHandler sets the callback for incoming file transfer chunks.
func (n *Node) SetFileTransferHandler(handler FileTransferHandler) {
	n.mu.Lock()
	n.fileHandler = handler
	n.mu.Unlock()
}

// handleFileTransferStream processes an incoming file transfer stream.
func (n *Node) handleFileTransferStream(s network.Stream) {
	if !acquireTransferSlot() {
		fmt.Printf("p2pmobile: [file] REJECTED — max concurrent transfers reached\n")
		s.Reset()
		return
	}
	defer releaseTransferSlot()

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
	transport := "TCP"
	if strings.Contains(remoteAddr, "/udp/") {
		transport = "QUIC"
	}

	fmt.Printf("p2pmobile: [file] incoming transfer from=%s type=%s transport=%s\n",
		fromShort, connType, transport)

	reader := bufio.NewReaderSize(s, readBufSize)
	writer := bufio.NewWriterSize(s, 16) // small — only ACKs go back

	// Read transfer header: [4: totalChunks] [8: totalBytes]
	hdrBuf := make([]byte, 12)
	s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))
	if _, err := io.ReadFull(reader, hdrBuf); err != nil {
		fmt.Printf("p2pmobile: [file] header read error from %s: %v\n", fromShort, err)
		s.Reset()
		return
	}
	totalChunks := int(binary.BigEndian.Uint32(hdrBuf[0:4]))
	totalBytes := binary.BigEndian.Uint64(hdrBuf[4:12])

	if totalChunks <= 0 || totalChunks > 10_000_000 {
		fmt.Printf("p2pmobile: [file] invalid totalChunks=%d from %s\n", totalChunks, fromShort)
		s.Reset()
		return
	}

	fmt.Printf("p2pmobile: [file] transfer header: chunks=%d totalBytes=%d from=%s\n",
		totalChunks, totalBytes, fromShort)

	// Generate a transfer ID from peer + timestamp for the handler
	transferId := fmt.Sprintf("%s_%d", fromShort, time.Now().UnixMilli())

	received := 0
	var totalReceived uint64
	t0 := time.Now()

	for seq := 0; seq < totalChunks; seq++ {
		// Read frame: [flags:1] [length:4] [payload]
		s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))

		flagByte, err := reader.ReadByte()
		if err != nil {
			fmt.Printf("p2pmobile: [file] flags read error at seq=%d from %s: %v\n", seq, fromShort, err)
			s.Reset()
			return
		}

		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			fmt.Printf("p2pmobile: [file] length read error at seq=%d from %s: %v\n", seq, fromShort, err)
			s.Reset()
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length > uint32(maxMessageSize) {
			fmt.Printf("p2pmobile: [file] chunk too large=%d at seq=%d from %s\n", length, seq, fromShort)
			s.Reset()
			return
		}

		// Rate limit check
		if !n.allowIncomingDM(rpid, int(length)) {
			fmt.Printf("p2pmobile: SECURITY file ingress rate limit peer=%s seq=%d len=%d\n", fromShort, seq, length)
			s.Reset()
			return
		}

		// Read payload into pooled buffer
		payload := getFrameBuf(int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			putFrameBuf(payload)
			fmt.Printf("p2pmobile: [file] payload read error at seq=%d from %s: %v\n", seq, fromShort, err)
			s.Reset()
			return
		}

		_ = flagByte // reserved for future compression flag on file chunks

		totalReceived += uint64(length)
		received++

		// Deliver chunk to handler
		n.mu.RLock()
		fh := n.fileHandler
		n.mu.RUnlock()
		if fh != nil {
			fh.OnFileChunk(from, transferId, seq, totalChunks, payload)
		}
		putFrameBuf(payload)

		// Send ACK every ackBatchSize chunks (flow control)
		if received%fileAckBatchSize == 0 || seq == totalChunks-1 {
			ackFrame := [5]byte{flagAck, 0, 0, 0, 0}
			binary.BigEndian.PutUint32(ackFrame[1:], uint32(received))
			if _, err := writer.Write(ackFrame[:]); err != nil {
				fmt.Printf("p2pmobile: [file] ACK write error at seq=%d: %v\n", seq, err)
				s.Reset()
				return
			}
			if err := writer.Flush(); err != nil {
				fmt.Printf("p2pmobile: [file] ACK flush error at seq=%d: %v\n", seq, err)
				s.Reset()
				return
			}
		}
	}

	elapsed := time.Since(t0)
	speedMBs := float64(totalReceived) / (1024 * 1024) / elapsed.Seconds()
	hits, misses := poolStats()
	fmt.Printf("p2pmobile: [file] ✓ transfer complete from=%s chunks=%d bytes=%d elapsed=%v speed=%.1f MB/s pool_hits=%d pool_misses=%d\n",
		fromShort, totalChunks, totalReceived, elapsed.Round(time.Millisecond), speedMBs, hits, misses)

	// Notify handler
	n.mu.RLock()
	fh := n.fileHandler
	n.mu.RUnlock()
	if fh != nil {
		fh.OnFileTransferComplete(from, transferId, totalChunks)
	}

	// Graceful close (not Reset — we want the final ACK to flush)
	s.Close()
}

// SendFile sends chunked data on a dedicated file transfer stream.
// chunks is a callback that returns (chunkData, hasMore) for each seq.
// This avoids loading all chunks into memory at once.
//
// Flow control: after every fileAckBatchSize chunks, waits for receiver ACK
// before continuing. This prevents QUIC/TCP buffer overflow.
func (n *Node) SendFile(targetPeerID string, totalChunks int, totalBytes int64, getChunk func(seq int) ([]byte, error)) error {
	if totalChunks <= 0 {
		return fmt.Errorf("file: no chunks")
	}
	if !acquireTransferSlot() {
		return fmt.Errorf("file: max concurrent transfers")
	}
	defer releaseTransferSlot()

	n.mu.RLock()
	h := n.host
	running := n.running
	n.mu.RUnlock()
	if !running || h == nil {
		return fmt.Errorf("not started")
	}

	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return fmt.Errorf("bad peer id: %w", err)
	}

	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}

	// Ensure connected
	if h.Network().Connectedness(pid) != network.Connected {
		if err := n.dialPeer(pid); err != nil {
			return fmt.Errorf("dial: %w", err)
		}
	}

	// Open dedicated file transfer stream
	ctx, cancel := context.WithTimeout(n.ctx, fileTransferTimeout)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, "file-transfer")

	s, err := h.NewStream(ctx, pid, protocol.ID(FileTransferProtocol))
	if err != nil {
		return fmt.Errorf("file stream: %w", err)
	}
	defer s.Close()

	remoteAddr := s.Conn().RemoteMultiaddr().String()
	connType := "DIRECT"
	transport := "TCP"
	if strings.Contains(remoteAddr, "p2p-circuit") {
		connType = "RELAY"
	}
	if strings.Contains(remoteAddr, "/udp/") {
		transport = "QUIC"
	}

	writer := bufio.NewWriterSize(s, dmWriterBufferSize)
	reader := bufio.NewReaderSize(s, 16) // small — only ACKs come back

	fmt.Printf("p2pmobile: [file] SendFile START to=%s chunks=%d totalBytes=%d transport=%s type=%s\n",
		pidShort, totalChunks, totalBytes, transport, connType)

	t0 := time.Now()

	// Write transfer header: [4: totalChunks] [8: totalBytes]
	var hdr [12]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(totalChunks))
	binary.BigEndian.PutUint64(hdr[4:12], uint64(totalBytes))
	if _, err := writer.Write(hdr[:]); err != nil {
		return fmt.Errorf("file header write: %w", err)
	}

	var totalSent int64
	sent := 0

	for seq := 0; seq < totalChunks; seq++ {
		chunk, err := getChunk(seq)
		if err != nil {
			return fmt.Errorf("getChunk(%d): %w", seq, err)
		}

		// Write frame: [flags:1] [length:4] [payload]
		length := uint32(len(chunk))
		writer.WriteByte(flagRaw)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], length)
		writer.Write(lenBuf[:])
		writer.Write(chunk)

		totalSent += int64(len(chunk))
		sent++

		// Flush + wait for ACK every batch
		if sent%fileAckBatchSize == 0 || seq == totalChunks-1 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("file flush at seq=%d: %w", seq, err)
			}

			// Wait for ACK with timeout
			s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))
			var ackBuf [5]byte
			if _, err := io.ReadFull(reader, ackBuf[:]); err != nil {
				return fmt.Errorf("file ACK read at seq=%d: %w", seq, err)
			}
			if ackBuf[0] != flagAck {
				return fmt.Errorf("file: expected ACK flag 0x02, got 0x%02x", ackBuf[0])
			}
			ackedCount := binary.BigEndian.Uint32(ackBuf[1:])

			if seq%128 == 0 || seq == totalChunks-1 {
				elapsed := time.Since(t0)
				speedMBs := float64(totalSent) / (1024 * 1024) / elapsed.Seconds()
				fmt.Printf("p2pmobile: [file] progress seq=%d/%d acked=%d sent=%d MB speed=%.1f MB/s\n",
					seq+1, totalChunks, ackedCount, totalSent/(1024*1024), speedMBs)
			}
		}
	}

	elapsed := time.Since(t0)
	speedMBs := float64(totalSent) / (1024 * 1024) / elapsed.Seconds()
	hits, misses := poolStats()
	fmt.Printf("p2pmobile: [file] ✓ SendFile COMPLETE to=%s chunks=%d bytes=%d elapsed=%v speed=%.1f MB/s transport=%s type=%s pool_hits=%d pool_misses=%d\n",
		pidShort, totalChunks, totalSent, elapsed.Round(time.Millisecond), speedMBs, transport, connType, hits, misses)

	return nil
}

// SendFileFromBytes is a convenience wrapper for SendFile that takes pre-chunked byte slices.
// For gomobile compatibility (JNI cannot pass function callbacks).
// chunks: packed format [4B count][4B len0][data0][4B len1][data1]...
func (n *Node) SendFileFromBytes(targetPeerID string, totalBytes int64, packedChunks []byte) error {
	frames, err := unpackDirectBatch(packedChunks)
	if err != nil {
		return fmt.Errorf("file unpack: %w", err)
	}
	total := len(frames)
	var mu sync.Mutex // protect frames slice access
	return n.SendFile(targetPeerID, total, totalBytes, func(seq int) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if seq < 0 || seq >= total {
			return nil, fmt.Errorf("seq %d out of range [0,%d)", seq, total)
		}
		return frames[seq], nil
	})
}

// ════════════════════════════════════════════════════════════════════════════
// Resumable File Transfer — /ridechain/file/2.0.0
//
// Adds byte-offset resume on top of v1. When a 500 MB transfer breaks at
// chunk 293 (Wi-Fi→4G handoff), the sender reconnects and says:
//   "I want to send transferId=XYZ, totalChunks=1000, totalBytes=500MB"
// The receiver checks its checkpoint store and replies:
//   "I already have 293 contiguous chunks — start from 293"
// The sender skips to chunk 293 and resumes. Zero wasted bandwidth.
//
// RESUME WIRE:
//   Sender → Receiver:
//     [0x03] [2 BE: transferId_len] [transferId UTF-8]
//     [4 BE: totalChunks] [8 BE: totalBytes]
//
//   Receiver → Sender:
//     [0x04] [4 BE: resume_from_seq]
//
//   Then normal v1 chunk flow from resume_from_seq onwards.
// ════════════════════════════════════════════════════════════════════════════

// handleResumeFileTransferStream processes an incoming resumable file transfer.
func (n *Node) handleResumeFileTransferStream(s network.Stream) {
	if !acquireTransferSlot() {
		fmt.Printf("p2pmobile: [file/v2] REJECTED — max concurrent transfers reached\n")
		s.Reset()
		return
	}
	defer releaseTransferSlot()

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
	transport := "TCP"
	if strings.Contains(remoteAddr, "/udp/") {
		transport = "QUIC"
	}

	reader := bufio.NewReaderSize(s, readBufSize)
	writer := bufio.NewWriterSize(s, 16)

	// Read RESUME_OFFER: [0x03] [2: idLen] [id] [4: totalChunks] [8: totalBytes]
	s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))
	offerFlag, err := reader.ReadByte()
	if err != nil || offerFlag != flagResumeOffer {
		fmt.Printf("p2pmobile: [file/v2] bad resume offer flag from %s: %v\n", fromShort, err)
		s.Reset()
		return
	}

	var idLenBuf [2]byte
	if _, err := io.ReadFull(reader, idLenBuf[:]); err != nil {
		fmt.Printf("p2pmobile: [file/v2] transferId length read error: %v\n", err)
		s.Reset()
		return
	}
	idLen := binary.BigEndian.Uint16(idLenBuf[:])
	if idLen == 0 || idLen > 512 {
		fmt.Printf("p2pmobile: [file/v2] invalid transferId length=%d\n", idLen)
		s.Reset()
		return
	}
	idBuf := make([]byte, idLen)
	if _, err := io.ReadFull(reader, idBuf); err != nil {
		fmt.Printf("p2pmobile: [file/v2] transferId read error: %v\n", err)
		s.Reset()
		return
	}
	transferId := string(idBuf)

	var metaBuf [12]byte
	if _, err := io.ReadFull(reader, metaBuf[:]); err != nil {
		fmt.Printf("p2pmobile: [file/v2] meta read error: %v\n", err)
		s.Reset()
		return
	}
	totalChunks := int(binary.BigEndian.Uint32(metaBuf[0:4]))
	totalBytes := binary.BigEndian.Uint64(metaBuf[4:12])

	if totalChunks <= 0 || totalChunks > 10_000_000 {
		fmt.Printf("p2pmobile: [file/v2] invalid totalChunks=%d\n", totalChunks)
		s.Reset()
		return
	}

	// Ask the handler how many chunks we already have
	resumeFrom := 0
	n.mu.RLock()
	fh := n.fileHandler
	n.mu.RUnlock()
	if fh != nil {
		resumeFrom = fh.GetResumeOffset(from, transferId)
	}
	if resumeFrom < 0 {
		resumeFrom = 0
	}
	if resumeFrom > totalChunks {
		resumeFrom = totalChunks
	}

	fmt.Printf("p2pmobile: [file/v2] resume offer from=%s id=%s chunks=%d bytes=%d resumeFrom=%d type=%s transport=%s\n",
		fromShort, transferId, totalChunks, totalBytes, resumeFrom, connType, transport)

	// Send RESUME_ACK: [0x04] [4 BE: resume_from_seq]
	var resumeAck [5]byte
	resumeAck[0] = flagResumeAck
	binary.BigEndian.PutUint32(resumeAck[1:], uint32(resumeFrom))
	if _, err := writer.Write(resumeAck[:]); err != nil {
		fmt.Printf("p2pmobile: [file/v2] resume ack write error: %v\n", err)
		s.Reset()
		return
	}
	if err := writer.Flush(); err != nil {
		fmt.Printf("p2pmobile: [file/v2] resume ack flush error: %v\n", err)
		s.Reset()
		return
	}

	if resumeFrom >= totalChunks {
		fmt.Printf("p2pmobile: [file/v2] already complete — nothing to receive\n")
		if fh != nil {
			fh.OnFileTransferComplete(from, transferId, totalChunks)
		}
		s.Close()
		return
	}

	// Receive remaining chunks (seq = resumeFrom .. totalChunks-1)
	received := resumeFrom
	var totalReceived uint64
	t0 := time.Now()

	for seq := resumeFrom; seq < totalChunks; seq++ {
		s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))

		flagByte, err := reader.ReadByte()
		if err != nil {
			fmt.Printf("p2pmobile: [file/v2] flags read error at seq=%d: %v\n", seq, err)
			s.Reset()
			return
		}

		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			fmt.Printf("p2pmobile: [file/v2] length read error at seq=%d: %v\n", seq, err)
			s.Reset()
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length > uint32(maxMessageSize) {
			fmt.Printf("p2pmobile: [file/v2] chunk too large=%d at seq=%d\n", length, seq)
			s.Reset()
			return
		}

		if !n.allowIncomingDM(rpid, int(length)) {
			fmt.Printf("p2pmobile: SECURITY file/v2 ingress rate limit peer=%s seq=%d\n", fromShort, seq)
			s.Reset()
			return
		}

		payload := getFrameBuf(int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			putFrameBuf(payload)
			fmt.Printf("p2pmobile: [file/v2] payload read error at seq=%d: %v\n", seq, err)
			s.Reset()
			return
		}

		_ = flagByte
		totalReceived += uint64(length)
		received++

		n.mu.RLock()
		fh2 := n.fileHandler
		n.mu.RUnlock()
		if fh2 != nil {
			fh2.OnFileChunk(from, transferId, seq, totalChunks, payload)
		}
		putFrameBuf(payload)

		chunksThisBatch := received - resumeFrom
		atBatchBoundary := chunksThisBatch%fileAckBatchSize == 0
		if atBatchBoundary || seq == totalChunks-1 {
			ackFrame := [5]byte{flagAck, 0, 0, 0, 0}
			binary.BigEndian.PutUint32(ackFrame[1:], uint32(received))
			if _, err := writer.Write(ackFrame[:]); err != nil {
				fmt.Printf("p2pmobile: [file/v2] ACK write error at seq=%d: %v\n", seq, err)
				s.Reset()
				return
			}
			if err := writer.Flush(); err != nil {
				fmt.Printf("p2pmobile: [file/v2] ACK flush error at seq=%d: %v\n", seq, err)
				s.Reset()
				return
			}
		}
	}

	elapsed := time.Since(t0)
	speedMBs := float64(totalReceived) / (1024 * 1024) / elapsed.Seconds()
	resumedChunks := totalChunks - resumeFrom
	fmt.Printf("p2pmobile: [file/v2] ✓ transfer complete from=%s id=%s resumed_from=%d new_chunks=%d bytes=%d elapsed=%v speed=%.1f MB/s\n",
		fromShort, transferId, resumeFrom, resumedChunks, totalReceived, elapsed.Round(time.Millisecond), speedMBs)

	n.mu.RLock()
	fh3 := n.fileHandler
	n.mu.RUnlock()
	if fh3 != nil {
		fh3.OnFileTransferComplete(from, transferId, totalChunks)
	}
	s.Close()
}

// SendFileResumable opens a /ridechain/file/2.0.0 stream and negotiates a resume
// offset with the receiver before sending data. If the receiver already has all
// chunks, this returns immediately with no data sent.
//
// transferId must be stable across retries for the same logical file — typically
// a hash of (sender + recipient + file content hash) or a UUID generated once
// and persisted by the Kotlin layer.
func (n *Node) SendFileResumable(targetPeerID string, transferId string, totalChunks int, totalBytes int64, getChunk func(seq int) ([]byte, error)) error {
	if totalChunks <= 0 {
		return fmt.Errorf("file/v2: no chunks")
	}
	if len(transferId) == 0 || len(transferId) > 512 {
		return fmt.Errorf("file/v2: invalid transferId length")
	}
	if !acquireTransferSlot() {
		return fmt.Errorf("file/v2: max concurrent transfers")
	}
	defer releaseTransferSlot()

	n.mu.RLock()
	h := n.host
	running := n.running
	n.mu.RUnlock()
	if !running || h == nil {
		return fmt.Errorf("not started")
	}

	targetPeerID = strings.TrimSpace(targetPeerID)
	pid, err := peer.Decode(targetPeerID)
	if err != nil {
		return fmt.Errorf("bad peer id: %w", err)
	}

	pidShort := pid.String()
	if len(pidShort) > 16 {
		pidShort = pidShort[:16]
	}

	if h.Network().Connectedness(pid) != network.Connected {
		if err := n.dialPeer(pid); err != nil {
			return fmt.Errorf("dial: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(n.ctx, fileTransferTimeout)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, "file-transfer-resume")

	// Try v2 first; fall back to v1 if the remote doesn't support it.
	s, err := h.NewStream(ctx, pid, protocol.ID(FileResumeProtocol), protocol.ID(FileTransferProtocol))
	if err != nil {
		return fmt.Errorf("file stream: %w", err)
	}
	defer s.Close()

	negotiatedProto := s.Protocol()
	isResume := string(negotiatedProto) == FileResumeProtocol

	writer := bufio.NewWriterSize(s, dmWriterBufferSize)
	reader := bufio.NewReaderSize(s, 16)

	resumeFrom := 0

	if isResume {
		// Send RESUME_OFFER
		idBytes := []byte(transferId)
		writer.WriteByte(flagResumeOffer)
		var idLenBuf [2]byte
		binary.BigEndian.PutUint16(idLenBuf[:], uint16(len(idBytes)))
		writer.Write(idLenBuf[:])
		writer.Write(idBytes)

		var metaBuf [12]byte
		binary.BigEndian.PutUint32(metaBuf[0:4], uint32(totalChunks))
		binary.BigEndian.PutUint64(metaBuf[4:12], uint64(totalBytes))
		writer.Write(metaBuf[:])
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("file/v2: resume offer write: %w", err)
		}

		// Read RESUME_ACK
		s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))
		var ackBuf [5]byte
		if _, err := io.ReadFull(reader, ackBuf[:]); err != nil {
			return fmt.Errorf("file/v2: resume ack read: %w", err)
		}
		if ackBuf[0] != flagResumeAck {
			return fmt.Errorf("file/v2: expected resume ack 0x04, got 0x%02x", ackBuf[0])
		}
		resumeFrom = int(binary.BigEndian.Uint32(ackBuf[1:]))
		if resumeFrom < 0 {
			resumeFrom = 0
		}

		fmt.Printf("p2pmobile: [file/v2] SendFileResumable to=%s id=%s resumeFrom=%d/%d\n",
			pidShort, transferId, resumeFrom, totalChunks)

		if resumeFrom >= totalChunks {
			fmt.Printf("p2pmobile: [file/v2] receiver already has all chunks — nothing to send\n")
			return nil
		}
	} else {
		// Fell back to v1 — send standard header, no resume
		fmt.Printf("p2pmobile: [file/v2] peer doesn't support v2, falling back to v1 for %s\n", pidShort)
		var hdr [12]byte
		binary.BigEndian.PutUint32(hdr[0:4], uint32(totalChunks))
		binary.BigEndian.PutUint64(hdr[4:12], uint64(totalBytes))
		if _, err := writer.Write(hdr[:]); err != nil {
			return fmt.Errorf("file header write: %w", err)
		}
	}

	t0 := time.Now()
	var totalSent int64
	sentInSession := 0

	for seq := resumeFrom; seq < totalChunks; seq++ {
		chunk, err := getChunk(seq)
		if err != nil {
			return fmt.Errorf("getChunk(%d): %w", seq, err)
		}

		length := uint32(len(chunk))
		writer.WriteByte(flagRaw)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], length)
		writer.Write(lenBuf[:])
		writer.Write(chunk)

		totalSent += int64(len(chunk))
		sentInSession++

		if sentInSession%fileAckBatchSize == 0 || seq == totalChunks-1 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("file/v2 flush at seq=%d: %w", seq, err)
			}

			s.SetReadDeadline(time.Now().Add(fileChunkReadTimeout))
			var ackBuf [5]byte
			if _, err := io.ReadFull(reader, ackBuf[:]); err != nil {
				return fmt.Errorf("file/v2 ACK read at seq=%d: %w", seq, err)
			}
			if ackBuf[0] != flagAck {
				return fmt.Errorf("file/v2: expected ACK 0x02, got 0x%02x", ackBuf[0])
			}
			ackedCount := binary.BigEndian.Uint32(ackBuf[1:])

			if seq%128 == 0 || seq == totalChunks-1 {
				elapsed := time.Since(t0)
				speedMBs := float64(totalSent) / (1024 * 1024) / elapsed.Seconds()
				fmt.Printf("p2pmobile: [file/v2] progress seq=%d/%d acked=%d sent=%d MB speed=%.1f MB/s\n",
					seq+1, totalChunks, ackedCount, totalSent/(1024*1024), speedMBs)
			}
		}
	}

	elapsed := time.Since(t0)
	speedMBs := float64(totalSent) / (1024 * 1024) / elapsed.Seconds()
	fmt.Printf("p2pmobile: [file/v2] ✓ SendFileResumable COMPLETE to=%s id=%s resumed=%d chunks=%d bytes=%d elapsed=%v speed=%.1f MB/s\n",
		pidShort, transferId, resumeFrom, totalChunks-resumeFrom, totalSent, elapsed.Round(time.Millisecond), speedMBs)

	return nil
}

// SendFileResumableFromBytes is the gomobile-compatible wrapper for SendFileResumable.
// chunks: packed format [4B count][4B len0][data0][4B len1][data1]...
func (n *Node) SendFileResumableFromBytes(targetPeerID string, transferId string, totalBytes int64, packedChunks []byte) error {
	frames, err := unpackDirectBatch(packedChunks)
	if err != nil {
		return fmt.Errorf("file/v2 unpack: %w", err)
	}
	total := len(frames)
	var mu sync.Mutex
	return n.SendFileResumable(targetPeerID, transferId, total, totalBytes, func(seq int) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if seq < 0 || seq >= total {
			return nil, fmt.Errorf("seq %d out of range [0,%d)", seq, total)
		}
		return frames[seq], nil
	})
}
