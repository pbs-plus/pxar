package buzhash

import "errors"

const WindowSize = 64

// Config holds chunking parameters derived from an average chunk size.
type Config struct {
	AvgChunkSize int
	MinChunkSize int
	MaxChunkSize int
	Mask         uint32
	Threshold    uint32
}

// NewConfig creates a Config from an average chunk size, which must be a power of two.
func NewConfig(avgChunkSize int) (Config, error) {
	if avgChunkSize < 1 || avgChunkSize&(avgChunkSize-1) != 0 {
		return Config{}, errors.New("avg chunk size must be a power of two")
	}

	mask := uint32(avgChunkSize*2 - 1)
	threshold := mask - 2

	return Config{
		AvgChunkSize: avgChunkSize,
		MinChunkSize: avgChunkSize >> 2,
		MaxChunkSize: avgChunkSize << 2,
		Mask:         mask,
		Threshold:    threshold,
	}, nil
}

// DefaultConfig returns the standard 4MB average chunk size configuration.
func DefaultConfig() Config {
	c, _ := NewConfig(4 << 20)
	return c
}

// Hasher implements the buzhash rolling hash.
type Hasher struct {
	h      uint32
	window [WindowSize]byte
	wpos   int
	count  int
}

// NewHasher creates a new Hasher.
func NewHasher() *Hasher {
	return &Hasher{}
}

// Reset returns the hasher to its initial state.
func (h *Hasher) Reset() {
	h.h = 0
	h.window = [WindowSize]byte{}
	h.wpos = 0
	h.count = 0
}

// Update slides the window by one byte.
func (h *Hasher) Update(in byte) {
	out := h.window[h.wpos]
	h.window[h.wpos] = in
	h.wpos = (h.wpos + 1) & (WindowSize - 1)
	h.h = rotl32(h.h, 1) ^ buzhashTable[out] ^ buzhashTable[in]
	h.count++
}

// Sum returns the current hash value.
func (h *Hasher) Sum() uint32 {
	return h.h
}

// BytesProcessed returns the total number of bytes fed to the hasher.
func (h *Hasher) BytesProcessed() int {
	return h.count
}

// InitFromData initializes the hash from the first WindowSize bytes of data.
// If data has fewer than WindowSize bytes, all bytes are consumed.
func (h *Hasher) InitFromData(data []byte) {
	h.Reset()

	n := len(data)
	if n > WindowSize {
		n = WindowSize
	}

	// Use batch initialization for the first n bytes
	if n > 0 {
		h.initWindow(data[:n])
		// Copy bytes into window
		copy(h.window[:], data[:n])
		h.wpos = n & (WindowSize - 1)
		h.count = n
	}
}

func rotl32(x uint32, n uint32) uint32 {
	n &= 31
	return (x << n) | (x >> (32 - n))
}

// initWindowScalar computes the initial hash by iterating byte-by-byte:
//
//	h = rotl(T[b0], 63) ^ rotl(T[b1], 62) ^ ... ^ T[b_{n-1}]
func initWindowScalar(data []byte) uint32 {
	var h uint32
	n := uint32(len(data))
	for i := uint32(0); i < n; i++ {
		h ^= rotl32(buzhashTable[data[i]], n-1-i)
	}
	return h
}
