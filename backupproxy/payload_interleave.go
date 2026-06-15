package backupproxy

import (
	"fmt"
	"io"

	"github.com/pbs-plus/pxar/buzhash"
)

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

	// Pump raw payload bytes from newData into a selectable channel so the
	// chunking loop can interleave injected chunks without ever blocking on a
	// read while an injection is pending. This decouples the (blocking) io.Reader
	// from the boundary-aware chunker, mirroring how the Rust async ChunkStream
	// polls its input stream and a separate boundaries channel concurrently.
	const readSize = 256 * 1024
	rawCh := make(chan []byte, 4)
	readErr := make(chan error, 1)
	go func() {
		defer close(rawCh)
		for {
			buf := make([]byte, readSize)
			n, e := newData.Read(buf)
			if n > 0 {
				rawCh <- buf[:n]
			}
			if e != nil {
				if e != io.EOF {
					readErr <- e
				}
				return
			}
		}
	}()

	scanner := buzhash.NewScanner(cfg)
	var (
		buffer    []byte
		scanPos   int
		pending   *InjectChunks
		injClosed bool
		rawDone   bool
	)

	peekInjection := func() {
		if pending != nil || injClosed {
			return
		}
		select {
		case inj, ok := <-injections:
			if !ok {
				injClosed = true
			} else {
				pending = &inj
			}
		default:
		}
	}

	applyInjection := func() error {
		inj := *pending
		boundary := pending.Boundary
		pending = nil
		if err := sink.putInjection(boundary, inj); err != nil {
			return err
		}
		totalSize = boundary + inj.Size
		return nil
	}

	for {
		peekInjection()

		// Determine the next natural chunk boundary within the unscanned buffer,
		// as a full-stream offset (totalSize + position within buffer).
		pos := 0
		if scanPos < len(buffer) {
			pos = scanner.Scan(buffer[scanPos:])
		}
		var chunkBoundary uint64
		if pos == 0 {
			chunkBoundary = totalSize + uint64(len(buffer))
		} else {
			chunkBoundary = totalSize + uint64(scanPos+pos)
		}

		// Force-split at the injection boundary, mirroring ChunkStream's forced
		// boundary handling: emit the real bytes up to the boundary (skipping if
		// empty), then apply the injected chunks. This guarantees injected
		// chunks start exactly at offset == inj.Boundary.
		if pending != nil {
			if pending.Boundary < totalSize {
				return 0, fmt.Errorf("invalid injection boundary %d < offset %d", pending.Boundary, totalSize)
			}
			if pending.Boundary <= chunkBoundary {
				splitLen := pending.Boundary - totalSize
				if splitLen > 0 {
					if err := sink.putRaw(totalSize, buffer[:splitLen]); err != nil {
						return 0, err
					}
					totalSize += splitLen
					buffer = buffer[splitLen:]
				} else if len(buffer) > 0 {
					buffer = buffer[0:0]
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
			if err := sink.putRaw(totalSize, buffer[:chunkLen]); err != nil {
				return 0, err
			}
			totalSize += uint64(chunkLen)
			buffer = buffer[chunkLen:]
			scanPos = 0
			scanner.Reset()
			continue
		}

		// No boundary in the current buffer — need more data. Mark scanned range
		// and read more raw bytes (or apply pending injection at stream end).
		scanPos = len(buffer)

		if pending != nil && rawDone {
			// Stream ended but a forced boundary still lies ahead: emit the
			// remaining tail, then the injection.
			if err := sink.putRaw(totalSize, buffer); err != nil {
				return 0, err
			}
			totalSize += uint64(len(buffer))
			buffer = buffer[0:0]
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
			if pending != nil {
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

		// Block only for more raw payload bytes. Injections are received
		// non-blockingly via peekInjection at the top of the loop — this mirrors
		// the Rust ChunkStream, which polls its byte stream (blocking) and peeks
		// the boundaries channel (try_recv, non-blocking). Receiving an injection
		// here while one is already pending would wrongly apply it before its
		// boundary, so the boundaries channel is never selected directly.
		b, ok := <-rawCh
		if !ok {
			rawDone = true
			select {
			case e := <-readErr:
				return 0, fmt.Errorf("read payload: %w", e)
			default:
			}
		} else {
			buffer = append(buffer, b...)
		}
	}

	// Flush any remaining real bytes as the final tail chunk.
	if len(buffer) > 0 {
		if err := sink.putRaw(totalSize, buffer); err != nil {
			return 0, err
		}
		totalSize += uint64(len(buffer))
	}

	// After the raw stream ends there must be no unconsumed injection.
	peekInjection()
	if pending != nil {
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
