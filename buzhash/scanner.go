package buzhash

// Scanner scans a byte buffer for content-defined chunk boundaries using the
// same buzhash rolling hash as Chunker, but without owning a reader. This
// mirrors the Rust pbs-datastore `Chunker` trait / `ChunkerImpl` used by the
// payload ChunkStream: the caller controls buffering and feeds data slices via
// Scan, which returns the offset of the next boundary (or 0 when none is found
// in the provided data, so more data should be supplied later). State persists
// across calls until Reset.
type Scanner struct {
	h          uint32
	window     [WindowSize]byte
	windowSize int
	chunkSize  int
	config     Config
}

// NewScanner creates a Scanner for the given config.
func NewScanner(config Config) *Scanner {
	return &Scanner{config: config}
}

// shallBreak mirrors Rust ChunkerImpl::shall_break (fast, non-modulo form):
//
//	(h & break_test_mask) >= break_test_minimum
//
// where mask = avg*2-1 and minimum = mask-2. It also enforces the absolute
// min/max chunk sizes. This matches Chunker's boundary test.
func (s *Scanner) shallBreak() bool {
	if s.chunkSize >= s.config.MaxChunkSize {
		return true
	}
	if s.chunkSize < s.config.MinChunkSize {
		return false
	}
	return (s.h & s.config.Mask) >= s.config.Threshold
}

// Scan scans data for a chunk boundary, mirroring ChunkerImpl::scan exactly.
// Returns 0 if no boundary was found within data (call again with more data),
// or a value > 0 indicating the position of the boundary relative to the start
// of data. State persists across calls so consecutive slices are treated as a
// contiguous stream until Reset is called.
func (s *Scanner) Scan(data []byte) int {
	windowLen := WindowSize
	dataLen := len(data)

	pos := 0

	// Phase 1: fill the rolling-hash window (init formula, no outgoing XOR),
	// identical to ChunkerImpl::scan's window-fill loop.
	if s.windowSize < windowLen {
		need := windowLen - s.windowSize
		copyLen := min(dataLen, need)

		for range copyLen {
			b := data[pos]
			s.window[s.windowSize] = b
			s.h = rotl32(s.h, 1) ^ buzhashTable[b]
			pos++
			s.windowSize++
		}

		s.chunkSize += copyLen

		if s.windowSize < windowLen {
			return 0
		}
	}

	// Phase 2: roll the window and test for a boundary.
	// idx = chunk_size & 0x3f  (window size 64 -> & 63), matching Rust's & 0x3f.
	idx := s.chunkSize & 0x3f

	for pos < dataLen {
		enter := data[pos]
		leave := s.window[idx]
		s.h = rotl32(s.h, 1) ^ buzhashTable[leave] ^ buzhashTable[enter]

		s.chunkSize++
		pos++
		s.window[idx] = enter

		if s.shallBreak() {
			s.h = 0
			s.chunkSize = 0
			s.windowSize = 0
			return pos
		}

		idx = s.chunkSize & 0x3f
	}

	return 0
}

// Reset returns the scanner to its initial state, mirroring ChunkerImpl::reset
// (h=0, chunk_size=0, window_size=0). Called after a chunk boundary (natural or
// forced) so the next chunk starts hashing from scratch.
func (s *Scanner) Reset() {
	s.h = 0
	s.chunkSize = 0
	s.windowSize = 0
}
