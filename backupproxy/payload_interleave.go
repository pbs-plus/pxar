package backupproxy

import (
	"fmt"
	"io"
	"sync"

	"github.com/pbs-plus/pxar/buzhash"
)

// chunkBufPool reuses the payload-chunking backing slab across uploads so the
// large (~272 KiB) buffer is amortised to zero allocations after warmup. The
// pool stores *[]byte so the full-capacity slice header survives Get/Put.
var chunkBufPool = sync.Pool{
	New: func() any { bp := make([]byte, 0, 1); return &bp },
}

// payloadSink receives emitted chunks during interleaved payload upload. It is
// the seam between the shared boundary-aware interleaving logic and the
// concrete storage (PBS protocol vs. local chunk store). Each method is called
// for one chunk positioned at offset, the absolute payload-stream start offset;
// the sink must persist/upload and index the chunk accounting for exactly size
// bytes from that offset.
type payloadSink interface {
	// putRaw handles one force-split raw-data chunk (never empty) starting at
	// offset. It must compute the digest, persist/upload the chunk if not
	// already known, and index it at [offset, offset+len(raw)).
	putRaw(offset uint64, raw []byte) error
	// putInjection handles a batch of reused chunks to inject starting at offset
	// (offset == inj.Boundary). Each chunk is indexed at successive offsets.
	putInjection(offset uint64, inj InjectChunks) error
}

// interleavePayload uploads a payload stream with reused chunks injected between
// stretches of newly encoded data. It is the faithful Go port of proxmox-backup's
// payload upload pipeline (ChunkStream + InjectReusedChunksQueue + UploadCounters):
//
//   - newData supplies only the newly encoded payload bytes (reused files write no
//     bytes; their regions are "holes").
//   - injections supply reused chunks together with the absolute payload-stream
//     offset (Boundary) at which they must be spliced in.
//   - A single running offset (the returned totalSize) accounts for both raw
//     bytes and injected sizes, so the offsets handed to the server/index are
//     always contiguous — this is what prevents "strange chunk offset" errors.
//
// Raw data is force-split at each injection boundary so the injected chunks start
// exactly at offset == inj.Boundary; empty raw splits are skipped (matching
// InjectReusedChunksQueue's `if raw.is_empty() => continue`).
func interleavePayload(cfg buzhash.Config, newData io.Reader, injections <-chan InjectChunks, sink payloadSink) (totalSize uint64, err error) {
	if newData == nil {
		// No new data: drain injections only.
		for inj := range injections {
			if err := sink.putInjection(totalSize, inj); err != nil {
				return 0, err
			}
			totalSize += inj.Size
		}
		return totalSize, nil
	}

	// Reusable backing buffer for the entire stream. Raw payload bytes are
	// read directly into its free tail capacity and compacted (unconsumed data
	// slid to the front) when the consumed prefix grows, so there are zero
	// per-read allocations. This mirrors the zero-alloc Chunker design (one buf
	// + one spill slab) instead of allocating a fresh 256 KiB buffer per Read
	// and copying via append.
	const readSize = 256 * 1024
	bufCap := readSize + cfg.MaxChunkSize

	// Pool the backing slab across uploads: a backup session calls this once
	// per archive, so the pool amortises the large allocation to effectively
	// zero after warmup. Pool stores a *[]byte so the full-capacity header is
	// preserved across Get/Put (a plain []byte would lose capacity when Put as
	// a sub-slice).
	bp := chunkBufPool.Get().(*[]byte)
	if cap(*bp) < bufCap {
		*bp = make([]byte, bufCap)
	}
	backing := (*bp)[:bufCap]
	defer chunkBufPool.Put(bp)
	var bufStart, bufEnd int // valid unconsumed data = backing[bufStart:bufEnd]

	var scanner buzhash.Scanner
	scanner.Config = cfg

	var (
		scanPos    int
		pending    InjectChunks
		hasPending bool
		injClosed  bool
		rawDone    bool
	)

	// readMore reads raw bytes directly into backing's free tail, compacting
	// first to reclaim the consumed prefix. This replaces the per-read
	// make([]byte, readSize) + channel-send + append-copy of the old design.
	readMore := func() error {
		if bufStart > 0 {
			if bufEnd > bufStart {
				copy(backing, backing[bufStart:bufEnd])
			}
			bufEnd -= bufStart
			bufStart = 0
		}
		if cap(backing)-bufEnd < readSize {
			// Carryover alone exceeds capacity (only when MaxChunkSize is huge);
			// grow the slab and remember it in the pooled header so a larger
			// buffer is reused on the next call.
			newCap := cap(backing)
			for newCap < bufEnd+readSize {
				newCap *= 2
			}
			nb := make([]byte, newCap)
			copy(nb, backing[:bufEnd])
			backing = nb
			*bp = nb
		}
		n, e := newData.Read(backing[bufEnd:cap(backing)])
		bufEnd += n
		if e != nil {
			if e != io.EOF {
				return e
			}
			rawDone = true
		}
		return nil
	}

	peekInjection := func() {
		if hasPending || injClosed {
			return
		}
		select {
		case inj, ok := <-injections:
			if !ok {
				injClosed = true
			} else {
				pending = inj
				hasPending = true
			}
		default:
		}
	}

	applyInjection := func() error {
		inj := pending
		boundary := inj.Boundary
		hasPending = false
		if err := sink.putInjection(boundary, inj); err != nil {
			return err
		}
		totalSize = boundary + inj.Size
		return nil
	}

	for {
		peekInjection()

		// Snapshot the valid (unconsumed) window into the reusable backing slab.
		// All chunk slices handed to the sink reference this slab; the sink must
		// consume them synchronously (the existing contract) since the slab is
		// compacted/overwritten on the next read.
		valid := backing[bufStart:bufEnd]

		// Determine the next natural chunk boundary within the unscanned window,
		// as a full-stream offset (totalSize + position within valid).
		pos := 0
		if scanPos < len(valid) {
			pos = scanner.Scan(valid[scanPos:])
		}
		var chunkBoundary uint64
		if pos == 0 {
			chunkBoundary = totalSize + uint64(len(valid))
		} else {
			chunkBoundary = totalSize + uint64(scanPos+pos)
		}

		// Force-split at the injection boundary, mirroring ChunkStream's forced
		// boundary handling: emit the real bytes up to the boundary (skipping if
		// empty), then apply the injected chunks. This guarantees injected
		// chunks start exactly at offset == inj.Boundary.
		if hasPending {
			if pending.Boundary < totalSize {
				return 0, fmt.Errorf("invalid injection boundary %d < offset %d", pending.Boundary, totalSize)
			}
			if pending.Boundary <= chunkBoundary {
				splitLen := pending.Boundary - totalSize
				if splitLen > 0 {
					if err := sink.putRaw(totalSize, valid[:splitLen]); err != nil {
						return 0, err
					}
					totalSize += splitLen
					bufStart += int(splitLen)
				}
				scanPos = 0
				scanner.Reset()
				if err := applyInjection(); err != nil {
					return 0, err
				}
				continue
			}
			// pending.Boundary > chunkBoundary: emit up to the natural boundary
			// first, then loop to re-evaluate.
		}

		// No forced boundary applies yet — emit a natural chunk if one was found.
		if pos != 0 {
			chunkLen := scanPos + pos
			if err := sink.putRaw(totalSize, valid[:chunkLen]); err != nil {
				return 0, err
			}
			totalSize += uint64(chunkLen)
			bufStart += chunkLen
			scanPos = 0
			scanner.Reset()
			continue
		}

		// No boundary in the current window — need more data. Mark scanned range
		// and read more raw bytes (or apply pending injection at stream end).
		scanPos = len(valid)

		if hasPending && rawDone {
			// Stream ended but a forced boundary still lies ahead: emit the
			// remaining tail, then the injection.
			if len(valid) > 0 {
				if err := sink.putRaw(totalSize, valid); err != nil {
					return 0, err
				}
				totalSize += uint64(len(valid))
				bufStart = bufEnd
			}
			scanPos = 0
			if err := applyInjection(); err != nil {
				return 0, err
			}
			continue
		}

		if rawDone && injClosed {
			break
		}

		if rawDone {
			// Raw stream finished: drain remaining injections non-blockingly (each
			// must now be at totalSize, since all preceding raw bytes are emitted).
			peekInjection()
			if hasPending {
				if pending.Boundary != totalSize {
					return 0, fmt.Errorf("invalid injection boundary %d != offset %d", pending.Boundary, totalSize)
				}
				if err := applyInjection(); err != nil {
					return 0, err
				}
				continue
			}
			break
		}

		// Read raw payload bytes directly into the reusable backing slab. No
		// goroutine, no channel, no per-read allocation. Injections are received
		// non-blockingly via peekInjection at the top of the loop. The caller's
		// io.Reader (e.g. chanReader) already multiplexes data/injections, so a
		// second pump goroutine would only add allocation churn.
		if err := readMore(); err != nil {
			return 0, fmt.Errorf("read payload: %w", err)
		}
	}

	// Flush any remaining real bytes as the final tail chunk.
	if tail := backing[bufStart:bufEnd]; len(tail) > 0 {
		if err := sink.putRaw(totalSize, tail); err != nil {
			return 0, err
		}
		totalSize += uint64(len(tail))
	}

	// After the raw stream ends there must be no unconsumed injection.
	peekInjection()
	if hasPending {
		if pending.Boundary != totalSize {
			return 0, fmt.Errorf("invalid injection boundary %d != offset %d", pending.Boundary, totalSize)
		}
		if err := applyInjection(); err != nil {
			return 0, err
		}
	}

	// Drain any injections that arrive after the raw stream has ended, blocking
	// until the injections channel is closed (mirrors InjectReusedChunksQueue's
	// end-of-stream handling: leftover entries are an error).
	for !injClosed {
		inj, ok := <-injections
		if !ok {
			injClosed = true
			continue
		}
		if inj.Boundary != totalSize {
			return 0, fmt.Errorf("invalid injection boundary %d != offset %d", inj.Boundary, totalSize)
		}
		if err := sink.putInjection(totalSize, inj); err != nil {
			return 0, err
		}
		totalSize += inj.Size
	}

	return totalSize, nil
}
