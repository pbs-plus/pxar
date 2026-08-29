package buzhash

// SuggestedScanner wraps Scanner with encoder-suggested chunk boundaries,
// mirroring the Rust pbs-datastore PayloadChunker. Suggestions are absolute
// stream offsets (typically file ends in the payload stream) supplied in
// ascending order via Suggest. A suggestion is honored only when the resulting
// chunk size falls within [MinChunkSize, MaxChunkSize]; otherwise it is
// dropped and boundaries come from the rolling hash alone.
//
// Unlike Rust, which receives suggestions over an mpsc channel because the
// encoder runs on a separate tokio task, the Go encoder and chunker run on the
// same call stack, so Suggest is a direct call and Scan consults the queue
// synchronously.
//
// Scan mirrors PayloadChunker::scan's Context-based contract: base is the
// absolute stream offset where the current in-progress chunk starts and pos is
// the absolute stream offset of data[0]. Keeping the context caller-owned (as
// Rust does) means forced cuts (for example injected chunk boundaries) need
// only a Reset plus the new base on the next Scan; no wrapper state can go
// stale.
type SuggestedScanner struct {
	sc    Scanner
	queue []uint64
}

// NewSuggestedScanner creates a SuggestedScanner for the given config.
func NewSuggestedScanner(config Config) *SuggestedScanner {
	s := &SuggestedScanner{}
	s.sc.Config = config
	return s
}

// Suggest registers a suggested boundary at the given absolute stream offset.
// Suggestions must be supplied in ascending stream order, matching the
// emission order of payload writers (file ends occur in write order).
func (s *SuggestedScanner) Suggest(boundary uint64) {
	s.queue = append(s.queue, boundary)
}

// Scan scans data (starting at absolute stream offset pos, with the current
// chunk having started at absolute offset base) for a chunk boundary,
// returning the boundary's position relative to the start of data, or 0 when
// none is found in data. Mirrors PayloadChunker::scan exactly:
//
//   - a suggestion before pos is stale and dropped;
//   - a suggestion past pos+len(data) is undecidable and the rolling hash
//     decides;
//   - a suggestion yielding a chunk smaller than MinChunkSize is dropped;
//   - a suggestion yielding a chunk up to MaxChunkSize cuts exactly there and
//     resets the rolling hash;
//   - a suggestion yielding a chunk larger than MaxChunkSize defers to the
//     rolling hash.
func (s *SuggestedScanner) Scan(base, pos uint64, data []byte) int {
	for len(s.queue) > 0 {
		boundary := s.queue[0]
		switch {
		case boundary < pos:
			s.queue = s.queue[1:]
		case boundary > pos+uint64(len(data)):
			return s.sc.Scan(data)
		default:
			chunkSize := boundary - base
			if chunkSize < uint64(s.sc.Config.MinChunkSize) {
				s.queue = s.queue[1:]
				continue
			}
			if chunkSize <= uint64(s.sc.Config.MaxChunkSize) {
				s.queue = s.queue[1:]
				length := boundary - pos
				if length == 0 {
					return s.sc.Scan(data)
				}
				s.sc.Reset()
				return int(length)
			}
			return s.sc.Scan(data)
		}
	}
	return s.sc.Scan(data)
}

// Reset resets the rolling hash only. Pending suggestions survive, matching
// PayloadChunker::reset, which clears chunker state but not the suggestion
// queue: after a forced cut, a still-future suggestion is re-evaluated against
// the new chunk.
func (s *SuggestedScanner) Reset() {
	s.sc.Reset()
}
