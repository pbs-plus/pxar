package datastore

const chunkReusePaddingThreshold = 0.1

// ChunkReusePlan describes chunks to inject before referencing a previous payload range.
type ChunkReusePlan struct {
	Chunks       []ChunkInfo
	PrefixSize   uint64
	StartPadding uint64
	Reusable     bool
}

// ChunkReusePlanner applies Proxmox's padding threshold and shared boundary-chunk reuse.
type ChunkReusePlanner struct {
	index   *DynamicIndexReader
	held    reusableChunk
	hasHeld bool
}

type reusableChunk struct {
	info    ChunkInfo
	padding uint64
}

// NewChunkReusePlanner creates a planner for a previous dynamic payload index.
func NewChunkReusePlanner(index *DynamicIndexReader) *ChunkReusePlanner {
	return &ChunkReusePlanner{index: index}
}

// Plan selects chunks covering [rangeStart, rangeEnd). A held final chunk must be flushed before any payload write.
func (p *ChunkReusePlanner) Plan(rangeStart, rangeEnd uint64, keepLast bool) ChunkReusePlan {
	chunks, startPadding, endPadding := p.lookup(rangeStart, rangeEnd)
	previous, hasPrevious := p.held, p.hasHeld
	p.hasHeld = false

	plan := ChunkReusePlan{StartPadding: startPadding}
	if len(chunks) == 0 {
		if hasPrevious {
			plan.Chunks = append(plan.Chunks, previous.info)
		}
		return plan
	}

	padding := startPadding + endPadding
	totalSize := rangeEnd - rangeStart + padding
	shared := false
	if hasPrevious && previous.sameIndexedChunkAs(chunks[0]) {
		used := previous.size() - previous.padding
		shared = used <= padding && used <= chunks[0].padding
		if shared {
			padding -= used
		}
	}
	if totalSize == 0 || float64(padding)/float64(totalSize) > chunkReusePaddingThreshold {
		if hasPrevious {
			plan.Chunks = append(plan.Chunks, previous.info)
		}
		return plan
	}

	if hasPrevious {
		if shared {
			chunks[0].padding -= previous.size() - previous.padding
		} else {
			plan.Chunks = append(plan.Chunks, previous.info)
			plan.PrefixSize = previous.size()
		}
	}

	plan.Reusable = true
	if keepLast {
		p.held = chunks[len(chunks)-1]
		p.hasHeld = true
		chunks = chunks[:len(chunks)-1]
	}
	for _, chunk := range chunks {
		plan.Chunks = append(plan.Chunks, chunk.info)
	}
	return plan
}

// Flush returns and clears the held boundary chunk.
func (p *ChunkReusePlanner) Flush() []ChunkInfo {
	if !p.hasHeld {
		return nil
	}
	chunk := p.held.info
	p.hasHeld = false
	return []ChunkInfo{chunk}
}

func (p *ChunkReusePlanner) lookup(rangeStart, rangeEnd uint64) ([]reusableChunk, uint64, uint64) {
	if p.index == nil || rangeStart >= rangeEnd {
		return nil, 0, 0
	}
	startIndex, ok := p.index.ChunkFromOffset(rangeStart)
	if !ok {
		return nil, 0, 0
	}
	first, ok := p.index.ChunkInfo(startIndex)
	if !ok {
		return nil, 0, 0
	}
	startPadding := rangeStart - first.Start
	chunks := make([]reusableChunk, 0, 1)
	for i := startIndex; i < p.index.Count(); i++ {
		info, ok := p.index.ChunkInfo(i)
		if !ok {
			break
		}
		chunks = append(chunks, reusableChunk{info: info})
		if rangeEnd <= info.End {
			endPadding := info.End - rangeEnd
			chunks[0].padding += startPadding
			chunks[len(chunks)-1].padding += endPadding
			return chunks, startPadding, endPadding
		}
	}
	return nil, 0, 0
}

func (c reusableChunk) size() uint64 {
	return c.info.End - c.info.Start
}

func (c reusableChunk) sameIndexedChunkAs(other reusableChunk) bool {
	return c.info.Digest == other.info.Digest && c.info.End == other.info.End
}
