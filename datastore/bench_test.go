package datastore

import (
	"testing"
)

func BenchmarkEncodeBlob(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeBlob(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBlobTo(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeBlobTo(nil, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBlobToPooled(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	dst := make([]byte, 0, BlobHeaderSize+len(data))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := EncodeBlobTo(dst[:0], data)
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
		_, err := EncodeCompressedBlob(data)
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
	blob, _ := EncodeBlob(data)
	encoded := blob.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlob(encoded)
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
	blob, _ := EncodeCompressedBlob(data)
	encoded := blob.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlob(encoded)
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
	blob, _ := EncodeCompressedBlob(data)
	encoded := blob.Bytes()
	dst := make([]byte, 0, len(data)*2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeBlobInto(dst[:0], encoded)
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
		for j := 0; j < 256; j++ {
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
		for j := 0; j < 256; j++ {
			offset += 4096
			w.Add(offset, entries[j])
		}
		_, _ = w.Finish()
	}
}

func BenchmarkDynamicIndexWriterCsum(b *testing.B) {
	w := NewDynamicIndexWriter(0)
	var offset uint64
	for i := 0; i < 256; i++ {
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
	for i := 0; i < 256; i++ {
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
	for i := 0; i < 256; i++ {
		offset += 4096
		var digest [32]byte
		digest[0] = byte(i)
		w.Add(offset, digest)
	}
	raw, _ := w.Finish()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r, err := ReadDynamicIndex(raw)
		if err != nil {
			b.Fatal(err)
		}
		r.ComputeCsum()
	}
}
