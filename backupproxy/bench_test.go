package backupproxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

func BenchmarkEncodeChunkBlob(b *testing.B) {
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := encodeChunkBlob(chunk, false, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeChunkBlobCompressed(b *testing.B) {
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := encodeChunkBlob(chunk, true, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddFileInfo(b *testing.B) {
	var digest [32]byte
	rand.Read(digest[:])

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var files []datastore.FileInfo
		addFileInfo(&files, "root.pxar.didx", 65536, digest, string(datastore.CryptModeNone))
	}
}

func BenchmarkHexEncodeDigest(b *testing.B) {
	var digest [32]byte
	rand.Read(digest[:])

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = hex.EncodeToString(digest[:])
	}
}

func BenchmarkHexEncodeDigestBuffer(b *testing.B) {
	var digest [32]byte
	rand.Read(digest[:])
	var buf [64]byte

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hex.Encode(buf[:], digest[:])
		_ = string(buf[:])
	}
}

func BenchmarkSHA256Chunk(b *testing.B) {
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = sha256.Sum256(chunk)
	}
}

func BenchmarkLocalUploadArchive(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for chunking test "), 512)

	cfg, _ := buzhash.NewConfig(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := datastore.NewDynamicIndexWriter(0)
		chunker := buzhash.NewChunker(bytes.NewReader(data), cfg)
		var totalOffset uint64
		for {
			chunk, err := chunker.Next()
			if err != nil {
				break
			}
			digest := sha256.Sum256(chunk)
			totalOffset += uint64(len(chunk))
			idx.Add(totalOffset, digest)
		}
		_, _ = idx.Finish()
	}
}
