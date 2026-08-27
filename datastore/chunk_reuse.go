package datastore

const chunkReusePaddingThreshold = 0.1

// ChunkReusePlan describes chunks to inject before referencing a previous payload range.
type ChunkReusePlan struct {
	Chunks       []ChunkInfo
	PrefixSize   uint64
	StartPadding uint64
	Reusable     bool
}

// ChunkReuseRange is an allocation-free view of chunks selected for reuse.
type ChunkReuseRange struct {
	PrefixSize   uint64
	StartPadding uint64
	Reusable     bool

	index     *DynamicIndexReader
	prefix    ChunkInfo
	start     int
	end       int
	hasPrefix bool
}

// ChunkCount returns the number of chunks selected for injection.
func (r ChunkReuseRange) ChunkCount() int {
	count := r.end - r.start
	if r.hasPrefix {
		count++
	}
	return count
}

// Chunk returns the selected chunk at pos.
func (r ChunkReuseRange) Chunk(pos int) (ChunkInfo, bool) {
	if pos < 0 {
		return ChunkInfo{}, false
	}
	if r.hasPrefix {
		if pos == 0 {
			return r.prefix, true
		}
		pos--
	}
	if pos >= r.end-r.start || r.index == nil {
		return ChunkInfo{}, false
	}
	return r.index.ChunkInfo(r.start + pos)
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
	rangePlan := p.PlanRange(rangeStart, rangeEnd, keepLast)
	plan := ChunkReusePlan{
		PrefixSize:   rangePlan.PrefixSize,
		StartPadding: rangePlan.StartPadding,
		Reusable:     rangePlan.Reusable,
	}
	if count := rangePlan.ChunkCount(); count > 0 {
		plan.Chunks = make([]ChunkInfo, count)
		for i := range count {
			plan.Chunks[i], _ = rangePlan.Chunk(i)
		}
	}
	return plan
}

// PlanRange selects chunks without allocating a chunk slice.
func (p *ChunkReusePlanner) PlanRange(rangeStart, rangeEnd uint64, keepLast bool) ChunkReuseRange {
	start, end, startPadding, endPadding, ok := p.lookupRange(rangeStart, rangeEnd)
	previous, hasPrevious := p.held, p.hasHeld
	p.hasHeld = false

	plan := ChunkReuseRange{StartPadding: startPadding}
	if !ok {
		if hasPrevious {
			plan.prefix = previous.info
			plan.hasPrefix = true
		}
		return plan
	}

	firstInfo, _ := p.index.ChunkInfo(start)
	first := reusableChunk{info: firstInfo, padding: startPadding}
	if start+1 == end {
		first.padding += endPadding
	}
	padding := startPadding + endPadding
	totalSize := rangeEnd - rangeStart + padding
	shared := false
	if hasPrevious && previous.sameIndexedChunkAs(first) {
		used := previous.size() - previous.padding
		shared = used <= padding && used <= first.padding
		if shared {
			padding -= used
			first.padding -= used
		}
	}
	if totalSize == 0 || float64(padding)/float64(totalSize) > chunkReusePaddingThreshold {
		if hasPrevious {
			plan.prefix = previous.info
			plan.hasPrefix = true
		}
		return plan
	}

	if hasPrevious && !shared {
		plan.prefix = previous.info
		plan.hasPrefix = true
		plan.PrefixSize = previous.size()
	}

	plan.Reusable = true
	if keepLast {
		lastInfo, _ := p.index.ChunkInfo(end - 1)
		lastPadding := endPadding
		if start+1 == end {
			lastPadding = first.padding
		}
		p.held = reusableChunk{info: lastInfo, padding: lastPadding}
		p.hasHeld = true
		end--
	}
	plan.index = p.index
	plan.start = start
	plan.end = end
	return plan
}

// Flush returns and clears the held boundary chunk.
func (p *ChunkReusePlanner) Flush() []ChunkInfo {
	rangePlan := p.FlushRange()
	if rangePlan.ChunkCount() == 0 {
		return nil
	}
	chunk, _ := rangePlan.Chunk(0)
	return []ChunkInfo{chunk}
}

// FlushRange returns an allocation-free view containing the held boundary chunk.
func (p *ChunkReusePlanner) FlushRange() ChunkReuseRange {
	if !p.hasHeld {
		return ChunkReuseRange{}
	}
	plan := ChunkReuseRange{prefix: p.held.info, hasPrefix: true}
	p.hasHeld = false
	return plan
}

func (p *ChunkReusePlanner) lookupRange(rangeStart, rangeEnd uint64) (int, int, uint64, uint64, bool) {
	if p.index == nil || rangeStart >= rangeEnd {
		return 0, 0, 0, 0, false
	}
	start, ok := p.index.ChunkFromOffset(rangeStart)
	if !ok {
		return 0, 0, 0, 0, false
	}
	last, ok := p.index.ChunkFromOffset(rangeEnd - 1)
	if !ok {
		return 0, 0, 0, 0, false
	}
	firstInfo, ok := p.index.ChunkInfo(start)
	if !ok {
		return 0, 0, 0, 0, false
	}
	lastInfo, ok := p.index.ChunkInfo(last)
	if !ok {
		return 0, 0, 0, 0, false
	}
	return start, last + 1, rangeStart - firstInfo.Start, lastInfo.End - rangeEnd, true
}

func (c reusableChunk) size() uint64 {
	return c.info.End - c.info.Start
}

func (c reusableChunk) sameIndexedChunkAs(other reusableChunk) bool {
	return c.info.Digest == other.info.Digest && c.info.End == other.info.End
}
