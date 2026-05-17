package decoder

import (
	"bytes"
	"fmt"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func buildDecodeArchive(b *testing.B, nFiles int, fileSize int) []byte {
	b.Helper()
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1430487000, 0)
	enc := encoder.NewEncoder(&buf, nil, &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}, nil)
	content := bytes.Repeat([]byte("D"), fileSize)
	for i := range nFiles {
		if _, err := enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}}, fmt.Sprintf("file_%04d.txt", i), content); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func BenchmarkDecode100Files_64B(b *testing.B) {
	benchmarkDecode(b, 100, 64)
}

func BenchmarkDecode1000Files_64B(b *testing.B) {
	benchmarkDecode(b, 1000, 64)
}

func BenchmarkDecode100Files_4K(b *testing.B) {
	benchmarkDecode(b, 100, 4096)
}

func BenchmarkDecode100Files_64K(b *testing.B) {
	benchmarkDecode(b, 100, 64*1024)
}

func benchmarkDecode(b *testing.B, nFiles, fileSize int) {
	archive := buildDecodeArchive(b, nFiles, fileSize)
	b.SetBytes(int64(len(archive)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := NewDecoder(bytes.NewReader(archive), nil)
		count := 0
		for {
			_, err := dec.Next()
			if err != nil {
				break
			}
			count++
		}
		_ = count
	}
}

func BenchmarkDecodeSkipPayload(b *testing.B) {
	// Measure overhead of decoding entries but skipping payload
	archive := buildDecodeArchive(b, 100, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := NewDecoder(bytes.NewReader(archive), nil)
		for {
			entry, err := dec.Next()
			if err != nil {
				break
			}
			if entry.IsRegularFile() && dec.Contents() != nil {
				// Drain payload
				_, _ = bytes.NewBuffer(nil).ReadFrom(dec.Contents())
			}
		}
	}
}
