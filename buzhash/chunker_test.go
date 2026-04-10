package buzhash

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

// copyChunk copies chunk data out of the chunker's internal buffers.
func copyChunk(chunk []byte) []byte {
	cp := make([]byte, len(chunk))
	copy(cp, chunk)
	return cp
}

func TestChunkerBasic(t *testing.T) {
	config, err := NewConfig(4096)
	if err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 100<<10)
	rand.Read(data)

	chunker := NewChunker(bytes.NewReader(data), config)
	var chunks [][]byte
	total := 0

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, copyChunk(chunk))
		total += len(chunk)
	}

	if total != len(data) {
		t.Errorf("total chunk bytes = %d, want %d", total, len(data))
	}
	if len(chunks) == 0 {
		t.Error("expected at least one chunk")
	}

	for i, chunk := range chunks {
		if len(chunk) > config.MaxChunkSize {
			t.Errorf("chunk %d: size %d > max %d", i, len(chunk), config.MaxChunkSize)
		}
	}

	var reconstructed bytes.Buffer
	for _, chunk := range chunks {
		reconstructed.Write(chunk)
	}
	if !bytes.Equal(reconstructed.Bytes(), data) {
		t.Error("reconstructed data doesn't match original")
	}
}

func TestChunkerEmptyInput(t *testing.T) {
	config, _ := NewConfig(4096)
	chunker := NewChunker(bytes.NewReader(nil), config)

	_, err := chunker.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestChunkerSmallInput(t *testing.T) {
	config, _ := NewConfig(4096)
	data := []byte("hello")

	chunker := NewChunker(bytes.NewReader(data), config)
	chunk, err := chunker.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(chunk) != "hello" {
		t.Errorf("chunk = %q, want %q", chunk, "hello")
	}

	_, err = chunker.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestChunkerDeterminism(t *testing.T) {
	config, _ := NewConfig(4096)
	data := make([]byte, 50<<10)
	rand.Read(data)

	chunker1 := NewChunker(bytes.NewReader(data), config)
	var sizes1 []int
	for {
		chunk, err := chunker1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sizes1 = append(sizes1, len(copyChunk(chunk)))
	}

	chunker2 := NewChunker(bytes.NewReader(data), config)
	var sizes2 []int
	for {
		chunk, err := chunker2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sizes2 = append(sizes2, len(copyChunk(chunk)))
	}

	if len(sizes1) != len(sizes2) {
		t.Errorf("different number of chunks: %d vs %d", len(sizes1), len(sizes2))
	}
	for i := range sizes1 {
		if sizes1[i] != sizes2[i] {
			t.Errorf("chunk %d: size %d vs %d", i, sizes1[i], sizes2[i])
		}
	}
}

func TestChunkerSizeBounds(t *testing.T) {
	config, _ := NewConfig(4096)
	data := make([]byte, 200<<10)
	rand.Read(data)

	chunker := NewChunker(bytes.NewReader(data), config)
	for i := 0; ; i++ {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		size := len(chunk)
		if size == 0 {
			t.Errorf("chunk %d is empty", i)
		}
		if size > config.MaxChunkSize {
			t.Errorf("chunk %d: %d > max %d", i, size, config.MaxChunkSize)
		}
	}
}

func TestChunkerReset(t *testing.T) {
	config, _ := NewConfig(4096)
	data1 := make([]byte, 10<<10)
	data2 := make([]byte, 10<<10)
	rand.Read(data1)
	rand.Read(data2)

	chunker := NewChunker(bytes.NewReader(data1), config)
	var total1 int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total1 += len(copyChunk(chunk))
	}

	chunker.Reset(bytes.NewReader(data2))
	var total2 int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total2 += len(copyChunk(chunk))
	}

	if total1 != len(data1) {
		t.Errorf("first pass: %d != %d", total1, len(data1))
	}
	if total2 != len(data2) {
		t.Errorf("second pass: %d != %d", total2, len(data2))
	}
}

func TestChunkerMaxSize(t *testing.T) {
	config, _ := NewConfig(64)
	data := make([]byte, 1<<20)
	rand.Read(data)

	chunker := NewChunker(bytes.NewReader(data), config)
	for i := 0; ; i++ {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(chunk) > config.MaxChunkSize {
			t.Errorf("chunk %d: size %d exceeds max %d", i, len(chunk), config.MaxChunkSize)
		}
	}
}

func TestChunkerZeroAllocs(t *testing.T) {
	config, _ := NewConfig(4 << 20)
	data := make([]byte, 1<<20)
	rand.Read(data)

	r := bytes.NewReader(data)
	chunker := NewChunker(r, config)

	allocs := testing.AllocsPerRun(10, func() {
		r.Reset(data)
		chunker.Reset(r)
		for {
			_, err := chunker.Next()
			if err == io.EOF {
				break
			}
		}
	})

	if allocs > 0 {
		t.Errorf("Next() allocated %.1f times, expected 0", allocs)
	}
}

func BenchmarkHashUpdate(b *testing.B) {
	h := NewHasher()
	data := make([]byte, b.N)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Update(data[i])
	}
}

func BenchmarkInitWindowScalar(b *testing.B) {
	data := make([]byte, WindowSize)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		initWindowScalar(data)
	}
}

func BenchmarkChunkerThroughput(b *testing.B) {
	config, _ := NewConfig(4 << 20)
	data := make([]byte, 1<<20)
	rand.Read(data)

	r := bytes.NewReader(data)
	chunker := NewChunker(r, config)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(data)
		chunker.Reset(r)
		for {
			_, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkChunkerLargeData(b *testing.B) {
	config, _ := NewConfig(4 << 20)
	data := make([]byte, 10<<20)
	rand.Read(data)

	r := bytes.NewReader(data)
	chunker := NewChunker(r, config)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(data)
		chunker.Reset(r)
		for {
			_, err := chunker.Next()
			if err == io.EOF {
				break
			}
		}
	}
}

var sink uint32

func BenchmarkBuzhashTableLookup(b *testing.B) {
	data := make([]byte, b.N)
	for i := range data {
		data[i] = byte(i)
	}
	b.ResetTimer()
	var h uint32
	for i := 0; i < b.N; i++ {
		h ^= buzhashTable[data[i]]
	}
	sink = h
}
