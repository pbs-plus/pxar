package backupproxy

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// genPayload returns n bytes of cryptographically random data (compresses
// poorly, exercising the chunker like real payload bytes).
func genPayload(n int) []byte {
	b := make([]byte, n)
	_, _ = crand.Read(b)
	return b
}

// BenchmarkScannerScan measures the innermost hot loop of payload chunking:
// the buzhash rolling-hash boundary scan. It should be zero-allocation.
func BenchmarkScannerScan(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	data := genPayload(1 << 20) // 1 MiB per scan
	scanner := buzhash.NewScanner(cfg)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner.Reset()
		off := 0
		for off < len(data) {
			p := scanner.Scan(data[off:])
			if p == 0 {
				break
			}
			off += p
			scanner.Reset()
		}
	}
}

// noopSink is a payloadSink that discards everything, isolating the chunking
// and interleaving allocation cost from storage/upload.
type noopSink struct{}

func (noopSink) putRaw(offset uint64, raw []byte) error             { return nil }
func (noopSink) putInjection(offset uint64, inj InjectChunks) error { return nil }

// BenchmarkInterleavePayloadNewData measures the interleavePayload hot path
// (read goroutine + buffer accumulation + buzhash scanning + force-splitting)
// with NO injections and a no-op sink, so it reflects pure chunking overhead.
func BenchmarkInterleavePayloadNewData(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	data := genPayload(4 << 20) // 4 MiB
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		injections := make(chan InjectChunks)
		close(injections)
		if _, err := interleavePayload(cfg, bytes.NewReader(data), injections, noopSink{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInterleavePayloadMixed measures interleavePayload with realistic
// interleaving: stretches of new data separated by injected (reused) chunks,
// exercising the force-split path that the v0.28.5 fix added.
func BenchmarkInterleavePayloadMixed(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	// 4 stretches of new data (256 KiB each) with 3 injected holes between.
	stretches := make([][]byte, 4)
	for i := range stretches {
		stretches[i] = genPayload(256 << 10)
	}
	injectedSize := uint64(64 << 10)
	boundaries := []uint64{
		256 << 10,
		(256 << 10) + injectedSize + (256 << 10),
		(256 << 10) + injectedSize + (256 << 10) + injectedSize + (256 << 10),
	}
	full := bytes.Join(stretches, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(full)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		injections := make(chan InjectChunks, 3)
		for _, bd := range boundaries {
			injections <- InjectChunks{
				Chunks:   []KnownChunkRef{{Digest: [32]byte{0xAB}, Size: injectedSize}},
				Size:     injectedSize,
				Boundary: bd,
			}
		}
		close(injections)
		if _, err := interleavePayload(cfg, bytes.NewReader(full), injections, noopSink{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPBSPayloadSinkPutRaw measures the sink hot path for NEW (unknown)
// chunks: SHA-256 digest, blob encoding, hex string conversion, and batch
// append. This is where per-chunk allocations (hex strings, blob copies) show.
func BenchmarkPBSPayloadSinkPutRaw(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	chunk := genPayload(4 << 10) // typical 4 KiB chunk
	proto := newAppendCaptureProtocol()
	sess := &pbsSession{
		store:       &PBSStore{chunkCfg: cfg},
		proto:       proto,
		config:      BackupConfig{CryptMode: datastore.CryptModeNone},
		chunkCfg:    cfg,
		knownChunks: make(map[[32]byte]bool),
	}
	sink := &pbsPayloadSink{
		session: sess,
		proto:   proto,
		wid:     1,
		idx:     datastore.NewDynamicIndexWriter(0),
	}
	// Pre-warm localKnown.
	sink.localKnown = make(map[[32]byte]bool, len(sess.knownChunks))
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mutate the chunk each iteration so it's always "unknown" (forces the
		// upload + encode path, not the dedup-hit path).
		chunk[len(chunk)-1] = byte(i)
		if err := sink.putRaw(uint64(i)*uint64(len(chunk)), chunk); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPBSPayloadSinkPutRawKnown measures the dedup-hit path: digest lookup
// hits localKnown, so no blob encoding — only digest + index append.
func BenchmarkPBSPayloadSinkPutRawKnown(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	chunk := genPayload(4 << 10)
	proto := newAppendCaptureProtocol()
	sess := &pbsSession{
		store:       &PBSStore{chunkCfg: cfg},
		proto:       proto,
		config:      BackupConfig{CryptMode: datastore.CryptModeNone},
		chunkCfg:    cfg,
		knownChunks: make(map[[32]byte]bool),
	}
	sink := &pbsPayloadSink{
		session: sess,
		proto:   proto,
		wid:     1,
		idx:     datastore.NewDynamicIndexWriter(0),
	}
	// Register the chunk as known so putRaw always hits the dedup path.
	dg := chunkDigest(chunk, nil)
	sink.localKnown = map[[32]byte]bool{dg: true}
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.putRaw(uint64(i)*uint64(len(chunk)), chunk); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUploadPayloadInterleavedEndToEnd measures the full end-to-end path
// (interleaver + pbsPayloadSink + mock protocol) with no injection, isolating
// the new-data upload allocation profile a real backup would see.
func BenchmarkUploadPayloadInterleavedEndToEnd(b *testing.B) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	data := genPayload(1 << 20) // 1 MiB
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto := newAppendCaptureProtocol()
		sess := &pbsSession{
			store:       &PBSStore{chunkCfg: cfg},
			proto:       proto,
			config:      BackupConfig{CryptMode: datastore.CryptModeNone},
			chunkCfg:    cfg,
			knownChunks: make(map[[32]byte]bool),
			files:       make([]datastore.BackupFileInfo, 0),
		}
		injections := make(chan InjectChunks)
		close(injections)
		if _, err := sess.UploadPayloadInterleaved(context.Background(), "bench.ppxar.didx", bytes.NewReader(data), injections); err != nil {
			b.Fatal(err)
		}
	}
}
