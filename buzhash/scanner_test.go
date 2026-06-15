package buzhash

import (
	"bytes"
	"io"
	"testing"
)

// Scanner must produce the same chunk boundaries as the reader-based Chunker
// over identical data. The payload ChunkStream relies on Scan() being a
// faithful buffer-scanning port of ChunkerImpl::scan so that interleaved
// (boundary-aware) chunking yields the same content-defined cuts.
func TestScannerMatchesChunker(t *testing.T) {
	cfg, err := NewConfig(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic, semi-structured data spanning many chunks.
	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i*2654435761) ^ byte(i>>8)
	}
	for i := 0; i < len(data); i += 70000 {
		data[i] = 0xAA
	}

	// Reader-based chunker.
	var chunkerSizes []int
	c := NewChunker(bytes.NewReader(data), cfg)
	for {
		ch, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunkerSizes = append(chunkerSizes, len(ch))
	}

	// Buffer-based scanner: feed in small slices to exercise state carry-over,
	// mirroring how the ChunkStream reads in buffered increments. The Chunker
	// caps chunks at MaxChunkSize, so emulate that upper bound here too.
	sc := NewScanner(cfg)
	var scannerSizes []int
	start := 0
	for start < len(data) {
		// Feed at most 1 MiB at a time to force multi-call scanning.
		end := min(start+1<<20, len(data))
		chunkLen := 0
		for start < end {
			pos := sc.Scan(data[start:end])
			if pos == 0 {
				// No boundary in the remaining slice.
				break
			}
			chunkLen += pos
			start += pos
			scannerSizes = append(scannerSizes, pos)
			sc.Reset()
			chunkLen = 0
		}
		// Flush a max-size chunk if accumulated data exceeds MaxChunkSize,
		// matching the Chunker's hard cap.
		if end-start >= cfg.MaxChunkSize && chunkLen == 0 {
			take := cfg.MaxChunkSize
			scannerSizes = append(scannerSizes, take)
			start += take
			sc.Reset()
		}
	}
	if start < len(data) {
		scannerSizes = append(scannerSizes, len(data)-start)
	}

	if len(chunkerSizes) != len(scannerSizes) {
		t.Fatalf("boundary count mismatch: chunker=%d scanner=%d\nchunker=%v\nscanner=%v",
			len(chunkerSizes), len(scannerSizes), chunkerSizes, scannerSizes)
	}
	for i := range chunkerSizes {
		if chunkerSizes[i] != scannerSizes[i] {
			t.Fatalf("boundary %d size mismatch: chunker=%d scanner=%d\nchunker=%v\nscanner=%v",
				i, chunkerSizes[i], scannerSizes[i], chunkerSizes, scannerSizes)
		}
	}
}
