package datastore

import "testing"

func TestChunkReusePlannerPaddingThreshold(t *testing.T) {
	idx := testChunkReuseIndex(100, 200)

	accepted := NewChunkReusePlanner(idx).Plan(5, 95, false)
	if !accepted.Reusable || accepted.StartPadding != 5 || len(accepted.Chunks) != 1 {
		t.Fatalf("accepted plan = %+v", accepted)
	}

	rejected := NewChunkReusePlanner(idx).Plan(6, 95, false)
	if rejected.Reusable || len(rejected.Chunks) != 0 {
		t.Fatalf("rejected plan = %+v", rejected)
	}
}

func TestChunkReusePlannerSharesBoundaryChunk(t *testing.T) {
	idx := testChunkReuseIndex(100, 200, 300)
	planner := NewChunkReusePlanner(idx)

	first := planner.Plan(10, 190, true)
	if !first.Reusable || len(first.Chunks) != 1 || first.Chunks[0].End != 100 {
		t.Fatalf("first plan = %+v", first)
	}

	second := planner.Plan(190, 290, true)
	if !second.Reusable || second.PrefixSize != 0 || len(second.Chunks) != 1 || second.Chunks[0].End != 200 {
		t.Fatalf("second plan = %+v", second)
	}

	last := planner.Flush()
	if len(last) != 1 || last[0].End != 300 {
		t.Fatalf("flush = %+v", last)
	}
}

func TestChunkReusePlannerFlushesDistinctHeldChunk(t *testing.T) {
	idx := testChunkReuseIndex(100, 200, 300)
	planner := NewChunkReusePlanner(idx)
	first := planner.Plan(10, 190, true)
	if !first.Reusable {
		t.Fatal("first range was not reusable")
	}

	second := planner.Plan(200, 290, false)
	if !second.Reusable || second.PrefixSize != 100 || len(second.Chunks) != 2 {
		t.Fatalf("second plan = %+v", second)
	}
	if second.Chunks[0].End != 200 || second.Chunks[1].End != 300 {
		t.Fatalf("chunk order = %+v", second.Chunks)
	}
}

func TestChunkReusePlannerUsesIndexPositionForIdentity(t *testing.T) {
	idx := testChunkReuseIndex(100, 200, 300)
	idx.entries[2].Digest = idx.entries[1].Digest
	planner := NewChunkReusePlanner(idx)
	first := planner.Plan(10, 190, true)
	if !first.Reusable {
		t.Fatal("first range was not reusable")
	}

	second := planner.Plan(200, 290, false)
	if second.PrefixSize != 100 || len(second.Chunks) != 2 {
		t.Fatalf("dedup collision absorbed distinct chunk: %+v", second)
	}
}

func testChunkReuseIndex(ends ...uint64) *DynamicIndexReader {
	entries := make([]DynamicEntry, len(ends))
	for i, end := range ends {
		entries[i].EndOffset = end
		entries[i].Digest[0] = byte(i + 1)
	}
	return &DynamicIndexReader{entries: entries}
}
