package datastore

import (
	"fmt"
	"testing"
)

var chunkReuseRangeSink ChunkReuseRange

func BenchmarkChunkReusePlannerPlanRange(b *testing.B) {
	for _, chunks := range []int{1, 128, 512, 4096} {
		b.Run(fmt.Sprintf("chunks=%d", chunks), func(b *testing.B) {
			ends := make([]uint64, chunks)
			for i := range ends {
				ends[i] = uint64(i+1) * 4096
			}
			index := testChunkReuseIndex(ends...)
			planner := NewChunkReusePlanner(index)

			b.ReportAllocs()
			for b.Loop() {
				chunkReuseRangeSink = planner.PlanRange(0, uint64(chunks)*4096, false)
			}
		})
	}
}
