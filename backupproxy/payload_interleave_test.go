package backupproxy

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/internal/payloadpipe"
)

// appendCaptureProtocol records every dynamicIndexAppend (digest, offset) pair
// in append order, and every uploaded chunk's size, so tests can assert the
// server-side invariant that offsets are contiguous starting at 0 — the exact
// condition that produces PBS's "strange chunk offset (N != 0)" error when
// violated.
type appendCaptureProtocol struct {
	mu       sync.Mutex
	appended []mockChunkRef // digest + offset, in order
	uploaded map[string]int // digest -> raw size
	wid      uint64
}

func newAppendCaptureProtocol() *appendCaptureProtocol {
	return &appendCaptureProtocol{uploaded: map[string]int{}}
}

func (p *appendCaptureProtocol) dynamicIndexCreate(string) (uint64, error) {
	p.wid++
	return p.wid, nil
}
func (p *appendCaptureProtocol) dynamicChunkUpload(_ uint64, digest string, size, _ int, _ []byte) error {
	p.mu.Lock()
	p.uploaded[digest] = size
	p.mu.Unlock()
	return nil
}
func (p *appendCaptureProtocol) pipelineChunkUploads(_ uint64, chunks []chunkUploadReq) error {
	for _, c := range chunks {
		if err := p.dynamicChunkUpload(0, c.digest, c.size, c.encodedSize, c.data); err != nil {
			return err
		}
	}
	return nil
}
func (p *appendCaptureProtocol) dynamicChunkUploadAsync(_ uint64, digest string, size, encodedSize int, data []byte) func() error {
	err := p.dynamicChunkUpload(0, digest, size, encodedSize, data)
	return func() error { return err }
}
func (p *appendCaptureProtocol) dynamicIndexAppend(_ uint64, digests []string, offsets []uint64) error {
	p.mu.Lock()
	for i, d := range digests {
		p.appended = append(p.appended, mockChunkRef{digest: d, offset: offsets[i]})
	}
	p.mu.Unlock()
	return nil
}
func (p *appendCaptureProtocol) dynamicIndexClose(uint64, int, uint64, string) error { return nil }
func (p *appendCaptureProtocol) blobUpload(string, int, []byte) error                { return nil }
func (p *appendCaptureProtocol) downloadPrevious(string) ([]byte, error)             { return nil, nil }
func (p *appendCaptureProtocol) finish() error                                       { return nil }
func (p *appendCaptureProtocol) close()                                              {}

// TestUploadPayloadInterleavedContiguousOffsets reproduces the scenario behind
// "dynamic writer append chunk failed - got strange chunk offset (N != 0)":
// reused (injected) chunks interleaved with newly encoded data. The fix keeps a
// single running offset across both, so the offsets handed to the server must be
// strictly contiguous and monotonically increasing by each chunk's size.
func TestUploadPayloadInterleavedContiguousOffsets(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}
	proto := newAppendCaptureProtocol()
	sess := &pbsSession{
		store:       &PBSStore{chunkCfg: cfg},
		proto:       proto,
		config:      BackupConfig{BackupType: datastore.BackupHost, BackupID: "test"},
		chunkCfg:    cfg,
		knownChunks: make(map[[32]byte]bool),
		files:       make([]datastore.BackupFileInfo, 0),
	}

	// Build a realistic interleaved payload stream:
	//   [new data A][injected reused chunk][new data B][injected reused chunk][new data C]
	// The new data is the encoder payload output; injected chunks are "holes".
	newDataBytes := func(n int) []byte {
		b := make([]byte, n)
		_, _ = crand.Read(b)
		return b
	}
	dataA := newDataBytes(12000)
	dataB := newDataBytes(8000)
	dataC := newDataBytes(4000)

	injectedDigest1 := [32]byte{0x11}
	injectedDigest2 := [32]byte{0x22}
	injectedSize1 := uint64(178938529) // the value from the reported error (~170 MiB)
	injectedSize2 := uint64(4096)

	// Expected payload offsets:
	//   [0 .. lenA) new
	//   [lenA .. lenA+inj1) injected
	//   [.. + lenB) new
	//   [.. + inj2) injected
	//   [.. + lenC) new
	expectOffsetAfterA := uint64(len(dataA))
	expectOffsetAfterInj1 := expectOffsetAfterA + injectedSize1
	expectOffsetAfterB := expectOffsetAfterInj1 + uint64(len(dataB))
	expectOffsetAfterInj2 := expectOffsetAfterB + injectedSize2
	expectTotal := expectOffsetAfterInj2 + uint64(len(dataC))

	// Compose the newData reader as A||B||C and feed injections on a channel in
	// the correct boundary order. The interleaver force-splits at boundaries, so
	// A/B/C are emitted as their own chunks regardless of buzhash boundaries.
	newData := io.MultiReader(bytes.NewReader(dataA), bytes.NewReader(dataB), bytes.NewReader(dataC))
	injections := make(chan InjectChunks, 4)
	injections <- InjectChunks{Chunks: []KnownChunkRef{{Digest: injectedDigest1, Size: injectedSize1}}, Size: injectedSize1, Boundary: expectOffsetAfterA}
	injections <- InjectChunks{Chunks: []KnownChunkRef{{Digest: injectedDigest2, Size: injectedSize2}}, Size: injectedSize2, Boundary: expectOffsetAfterB}
	close(injections)

	result, err := sess.UploadPayloadInterleaved(context.Background(), "test.ppxar.didx", newData, injections)
	if err != nil {
		t.Fatalf("UploadPayloadInterleaved failed: %v", err)
	}

	if result.Size != expectTotal {
		t.Errorf("total size = %d, want %d", result.Size, expectTotal)
	}

	// Assert server-side offset contiguity: offsets strictly increasing, first
	// is 0, and each chunk's end (offset + size) equals the next chunk's offset.
	proto.mu.Lock()
	appended := append([]mockChunkRef(nil), proto.appended...)
	uploaded := proto.uploaded
	proto.mu.Unlock()

	if len(appended) == 0 {
		t.Fatal("no chunks appended")
	}
	if appended[0].offset != 0 {
		t.Errorf("first chunk offset = %d, want 0 (strange chunk offset!)", appended[0].offset)
	}

	var cur uint64
	chunkSize := func(digestHex string) uint64 {
		var d [32]byte
		if _, err := hex.Decode(d[:], []byte(digestHex)); err != nil {
			t.Fatalf("bad digest hex %q: %v", digestHex, err)
		}
		switch d {
		case injectedDigest1:
			return injectedSize1
		case injectedDigest2:
			return injectedSize2
		default:
			sz, ok := uploaded[digestHex]
			if !ok {
				t.Fatalf("unknown chunk digest %x (neither injected nor uploaded)", d)
			}
			return uint64(sz)
		}
	}

	var gotInjectedOffsets []uint64
	for i, c := range appended {
		if c.offset != cur {
			t.Errorf("chunk %d offset = %d, want contiguous %d (strange chunk offset!)", i, c.offset, cur)
		}
		cur += chunkSize(c.digest)
	}
	if cur != expectTotal {
		t.Errorf("final offset = %d, want %d", cur, expectTotal)
	}

	// Verify injected chunks landed at their boundaries.
	for _, c := range appended {
		var d [32]byte
		hex.Decode(d[:], []byte(c.digest))
		if d == injectedDigest1 {
			gotInjectedOffsets = append(gotInjectedOffsets, c.offset)
		}
		if d == injectedDigest2 {
			gotInjectedOffsets = append(gotInjectedOffsets, c.offset)
		}
	}
	if len(gotInjectedOffsets) != 2 {
		t.Fatalf("expected 2 injected chunks, got %d", len(gotInjectedOffsets))
	}
	if gotInjectedOffsets[0] != expectOffsetAfterA {
		t.Errorf("injected chunk 1 offset = %d, want %d", gotInjectedOffsets[0], expectOffsetAfterA)
	}
	if gotInjectedOffsets[1] != expectOffsetAfterB {
		t.Errorf("injected chunk 2 offset = %d, want %d", gotInjectedOffsets[1], expectOffsetAfterB)
	}
}

// TestInterleavePayloadLeadingInjection verifies the exact reported failure:
// an injection with NO new data preceding it (boundary == 0), followed by new
// data. Previously the producer started offsets at 0 while the server had
// already advanced past the injected size, yielding the "strange chunk offset"
// error.
func TestInterleavePayloadLeadingInjection(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}

	injectedSize := uint64(178938529)
	injectedDigest := [32]byte{0xAB}
	injections := make(chan InjectChunks, 1)
	injections <- InjectChunks{
		Chunks:   []KnownChunkRef{{Digest: injectedDigest, Size: injectedSize}},
		Size:     injectedSize,
		Boundary: 0,
	}
	close(injections)

	newData := bytes.NewReader(bytes.Repeat([]byte{1, 2, 3}, 100))

	type rec struct {
		offset uint64
		size   uint64
	}
	var records []rec
	sink := &captureSink{onRaw: func(off uint64, raw []byte) {
		records = append(records, rec{off, uint64(len(raw))})
	}, onInj: func(off uint64, inj InjectChunks) {
		for _, c := range inj.Chunks {
			records = append(records, rec{off, c.Size})
		}
	}}

	total, err := interleavePayload(cfg, newData, injections, sink)
	if err != nil {
		t.Fatalf("interleavePayload: %v", err)
	}

	if total != injectedSize+300 {
		t.Errorf("total = %d, want %d", total, injectedSize+300)
	}
	// The injected chunk MUST come first at offset 0.
	if len(records) < 1 || records[0].offset != 0 || records[0].size != injectedSize {
		t.Fatalf("first record = %+v, want injected chunk at offset 0 size %d", records, injectedSize)
	}
	// The first new-data chunk must start exactly at injectedSize (the server's
	// offset after the injection), NOT 0.
	for _, r := range records[1:] {
		if r.offset < injectedSize {
			t.Errorf("new-data chunk at offset %d, must be >= injected size %d (this is the strange-chunk-offset bug)", r.offset, injectedSize)
		}
	}
}

type captureSink struct {
	onRaw func(offset uint64, raw []byte)
	onInj func(offset uint64, inj InjectChunks)
}

func (s *captureSink) putRaw(offset uint64, raw []byte) error {
	if s.onRaw != nil {
		s.onRaw(offset, raw)
	}
	return nil
}
func (s *captureSink) putInjection(offset uint64, inj InjectChunks) error {
	if s.onInj != nil {
		s.onInj(offset, inj)
	}
	return nil
}

// TestInterleavePayloadBackToBackInjections verifies that two injections with
// consecutive boundary offsets (back-to-back, with real data already buffered
// before them) are handled correctly. This catches a regression where
// splitLen==0 at an injection boundary would discard the remaining buffered
// real data instead of keeping it for processing after the injections.
func TestInterleavePayloadDrainsBoundedInjectionsWithoutData(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}

	pipe := payloadpipe.New()
	injections := make(chan InjectChunks, 2)
	result := make(chan struct {
		total uint64
		err   error
	}, 1)
	go func() {
		total, err := interleavePayload(cfg, pipe, injections, &captureSink{})
		result <- struct {
			total uint64
			err   error
		}{total: total, err: err}
	}()

	const injectionCount = 100
	for i := range injectionCount {
		injections <- InjectChunks{
			Chunks:   []KnownChunkRef{{Digest: [32]byte{byte(i)}, Size: 1}},
			Size:     1,
			Boundary: uint64(i),
		}
		pipe.Wake()
	}
	close(injections)
	pipe.CloseWithError(nil)

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.total != injectionCount {
			t.Fatalf("total = %d, want %d", got.total, injectionCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bounded injection stream deadlocked")
	}
}

func TestInterleavePayloadBackToBackInjections(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}

	injSize1 := uint64(100)
	injSize2 := uint64(200)
	injDig1 := [32]byte{0x31}
	injDig2 := [32]byte{0x32}

	// Layout: [1000 bytes real][inj1 at 1000][inj2 at 1100][10000 bytes real]
	// Both injections land WITHIN data that's already buffered from a single
	// read, exercising the splitLen==0 path for inj2 while 10000 bytes of real
	// data remain in the buffer. A single bytes.Reader ensures all data is read
	// in one Read() call so the buffer spans both injection boundaries.
	realBefore := make([]byte, 1000)
	for i := range realBefore {
		realBefore[i] = byte(i)
	}
	realAfter := make([]byte, 10000)
	for i := range realAfter {
		realAfter[i] = byte(i + 128)
	}
	// Use a single reader so readMore() fills the entire buffer at once, causing
	// the second injection to see splitLen==0 with real data still pending.
	newData := bytes.NewReader(append(realBefore, realAfter...))

	injections := make(chan InjectChunks, 2)
	injections <- InjectChunks{
		Chunks:   []KnownChunkRef{{Digest: injDig1, Size: injSize1}},
		Size:     injSize1,
		Boundary: 1000,
	}
	injections <- InjectChunks{
		Chunks:   []KnownChunkRef{{Digest: injDig2, Size: injSize2}},
		Size:     injSize2,
		Boundary: 1000 + injSize1, // immediately after inj1's virtual region
	}
	close(injections)

	type rec struct {
		offset uint64
		size   uint64
		kind   string // "inj" or "raw"
	}
	var records []rec
	sink := &captureSink{onRaw: func(off uint64, raw []byte) {
		records = append(records, rec{off, uint64(len(raw)), "raw"})
	}, onInj: func(off uint64, inj InjectChunks) {
		for _, c := range inj.Chunks {
			records = append(records, rec{off, c.Size, "inj"})
		}
	}}

	total, err := interleavePayload(cfg, newData, injections, sink)
	if err != nil {
		t.Fatalf("interleavePayload: %v", err)
	}

	wantTotal := 1000 + injSize1 + injSize2 + 10000
	if total != wantTotal {
		t.Fatalf("total = %d, want %d", total, wantTotal)
	}

	// All 500 bytes of real data after the injections must be emitted.
	var realAfterBytes uint64
	sawInj2 := false
	for _, r := range records {
		if r.kind == "inj" && r.offset == 1000+injSize1 {
			sawInj2 = true
			continue
		}
		if sawInj2 && r.kind == "raw" {
			realAfterBytes += r.size
		}
	}
	if realAfterBytes != 10000 {
		t.Errorf("real bytes after injections = %d, want 10000 (back-to-back injection discarded buffered data)", realAfterBytes)
	}
}
