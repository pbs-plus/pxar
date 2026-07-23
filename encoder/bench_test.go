package encoder

import (
	"bytes"
	"fmt"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

func encDirMeta(mode uint64) *pxar.Metadata {
	ts := format.NewStatxTimestamp(1430487000, 0)
	return &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | mode, Mtime: ts}}
}

func encFileMeta(mode uint64) *pxar.Metadata {
	ts := format.NewStatxTimestamp(1430487000, 0)
	return &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | mode, Mtime: ts}}
}

func BenchmarkEncodeFlat100Files(b *testing.B) {
	content := bytes.Repeat([]byte("x"), 64)
	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		for f := range 100 {
			if _, err := enc.AddFile(encFileMeta(0o644), fmt.Sprintf("file_%04d.txt", f), content); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFlat1000Files(b *testing.B) {
	content := bytes.Repeat([]byte("x"), 64)
	names := make([]string, 1000)
	metas := make([]*pxar.Metadata, 1000)
	for f := range 1000 {
		names[f] = fmt.Sprintf("file_%04d.txt", f)
		metas[f] = encFileMeta(0o644)
	}
	var buf bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		for f := range 1000 {
			if _, err := enc.AddFile(metas[f], names[f], content); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeNested10x50(b *testing.B) {
	content := bytes.Repeat([]byte("x"), 128)
	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		for d := range 10 {
			if err := enc.CreateDirectory(fmt.Sprintf("dir_%03d", d), encDirMeta(0o755)); err != nil {
				b.Fatal(err)
			}
			for f := range 50 {
				if _, err := enc.AddFile(encFileMeta(0o644), fmt.Sprintf("file_%04d.txt", f), content); err != nil {
					b.Fatal(err)
				}
			}
			if err := enc.Finish(); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeLargeFile_4M(b *testing.B) {
	content := bytes.Repeat([]byte("A"), 4*1024*1024)
	var buf bytes.Buffer

	b.SetBytes(int64(len(content)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		if _, err := enc.AddFile(encFileMeta(0o644), "large.bin", content); err != nil {
			b.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeStreamFile_4M(b *testing.B) {
	size := uint64(4 * 1024 * 1024)
	chunk := bytes.Repeat([]byte("B"), 64*1024)
	var buf bytes.Buffer

	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		fw, err := enc.CreateFile(encFileMeta(0o644), "stream.bin", size)
		if err != nil {
			b.Fatal(err)
		}
		written := uint64(0)
		for written < size {
			n := min(uint64(len(chunk)), size-written)
			if _, err := fw.Write(chunk[:n]); err != nil {
				b.Fatal(err)
			}
			written += n
		}
		if err := fw.Close(); err != nil {
			b.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeWithXAttrs(b *testing.B) {
	content := []byte("small")
	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		enc := NewEncoder(&buf, nil, encDirMeta(0o755), nil)
		meta := encFileMeta(0o644)
		meta.XAttrs = []format.XAttr{
			format.NewXAttr([]byte("user.comment"), []byte("test value")),
			format.NewXAttr([]byte("security.label"), []byte("system_u:object_r:test_t")),
		}
		if _, err := enc.AddFile(meta, "xattr_file.txt", content); err != nil {
			b.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeSplitArchive(b *testing.B) {
	content := bytes.Repeat([]byte("C"), 4096)
	var metaBuf, payloadBuf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metaBuf.Reset()
		payloadBuf.Reset()
		enc := NewEncoder(&metaBuf, &payloadBuf, encDirMeta(0o755), nil)
		for f := range 100 {
			if _, err := enc.AddFile(encFileMeta(0o644), fmt.Sprintf("file_%04d.txt", f), content); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
