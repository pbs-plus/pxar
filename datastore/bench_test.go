package datastore

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
)

func BenchmarkEncodeBlob(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeBlob(nil, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBlobReuse(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	dst := make([]byte, 0, BlobHeaderSize+len(data))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeBlob(dst[:0], data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeCompressedBlob(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeCompressedBlob(nil, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlobUncompressed(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	blob, _ := EncodeBlob(nil, data)
	encoded := blob

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlob(nil, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlobCompressed(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	blob, _ := EncodeCompressedBlob(nil, data)
	encoded := blob

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlob(nil, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlobCompressedInto(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	blob, _ := EncodeCompressedBlob(nil, data)
	encoded := blob
	dst := make([]byte, 0, len(data)*2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlob(dst[:0], encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDynamicIndexWriterAdd(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewDynamicIndexWriter(0)
		var offset uint64
		for j := range 256 {
			offset += 4096
			var digest [32]byte
			digest[0] = byte(j)
			w.Add(offset, digest)
		}
	}
}

func BenchmarkDynamicIndexWriterFinish(b *testing.B) {
	var entries [256][32]byte
	for i := range entries {
		entries[i][0] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := NewDynamicIndexWriter(0)
		var offset uint64
		for j := range 256 {
			offset += 4096
			w.Add(offset, entries[j])
		}
		_, _ = w.Finish()
	}
}

func BenchmarkDynamicIndexWriterCsum(b *testing.B) {
	w := NewDynamicIndexWriter(0)
	var offset uint64
	for i := range 256 {
		offset += 4096
		var digest [32]byte
		digest[0] = byte(i)
		w.Add(offset, digest)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = w.Csum()
	}
}

func BenchmarkDynamicIndexWriterCsumAfterFinish(b *testing.B) {
	w := NewDynamicIndexWriter(0)
	var offset uint64
	for i := range 256 {
		offset += 4096
		var digest [32]byte
		digest[0] = byte(i)
		w.Add(offset, digest)
	}
	_, _ = w.Finish()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = w.Csum()
	}
}

func BenchmarkDynamicIndexReadAndComputeCsum(b *testing.B) {
	w := NewDynamicIndexWriter(0)
	var offset uint64
	for i := range 256 {
		offset += 4096
		var digest [32]byte
		digest[0] = byte(i)
		w.Add(offset, digest)
	}
	raw, _ := w.Finish()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r, err := ParseDynamicIndex(raw)
		if err != nil {
			b.Fatal(err)
		}
		r.ComputeCsum()
	}
}

func BenchmarkInMemoryChunkPipeline(b *testing.B) {
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)

	config, _ := buzhash.NewConfig(64 << 10)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := NewDynamicIndexWriter(0)
		chunker := buzhash.NewChunker(bytes.NewReader(data), config)
		var offset uint64
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			digest := sha256.Sum256(chunk)
			offset += uint64(len(chunk))
			idx.Add(offset, digest)
		}
		_, _ = idx.Finish()
	}
}
