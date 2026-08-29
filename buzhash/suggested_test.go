package buzhash

import (
	"math/rand"
	"testing"
)

// scanAll scans data in one shot, returning cumulative cut offsets including
// the final tail chunk.
func scanAll(t *testing.T, cfg Config, data []byte) []int {
	t.Helper()
	sc := Scanner{Config: cfg}
	var cuts []int
	off := 0
	for off < len(data) {
		n := sc.Scan(data[off:])
		if n == 0 {
			return append(cuts, len(data))
		}
		off += n
		cuts = append(cuts, off)
	}
	return cuts
}

// runSuggested feeds data to a SuggestedScanner in fixed-size windows the way
// the payload chunk-stream loop does, tracking base (chunk start) and pos
// (window start) as absolute stream offsets.
func runSuggested(cfg Config, data []byte, suggestions []uint64, feed int) []int {
	ss := NewSuggestedScanner(cfg)
	for _, s := range suggestions {
		ss.Suggest(s)
	}
	var cuts []int
	var buf []byte
	consumed := 0
	scanPos := 0
	for consumed < len(data) {
		if scanPos == len(buf) {
			if consumed+len(buf) >= len(data) {
				if len(buf) > 0 {
					cuts = append(cuts, len(data))
				}
				break
			}
			end := min(consumed+len(buf)+feed, len(data))
			buf = append(buf, data[consumed+len(buf):end]...)
		}
		n := ss.Scan(uint64(consumed), uint64(consumed+scanPos), buf[scanPos:])
		if n == 0 {
			scanPos = len(buf)
			continue
		}
		cut := consumed + scanPos + n
		cuts = append(cuts, cut)
		c := scanPos + n
		copy(buf, buf[c:])
		buf = buf[:len(buf)-c]
		consumed = cut
		scanPos = 0
	}
	return cuts
}

func testData(n int) []byte {
	r := rand.New(rand.NewSource(42))
	b := make([]byte, n)
	r.Read(b)
	return b
}

// TestSuggestedMatchesPlainWithoutSuggestions checks the wrapper is
// transparent without suggestions across feed sizes, including byte-at-a-time
// windows that exercise the past/future bookkeeping.
func TestSuggestedMatchesPlainWithoutSuggestions(t *testing.T) {
	cfg, err := NewConfig(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	data := testData(1 << 20)
	want := scanAll(t, cfg, data)
	for _, feed := range []int{1, 999, 64 << 10, 1 << 20} {
		got := runSuggested(cfg, data, nil, feed)
		if len(got) != len(want) {
			t.Fatalf("feed %d: got %d cuts, want %d", feed, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("feed %d: cut %d = %d, want %d", feed, i, got[i], want[i])
			}
		}
	}
}

// TestSuggestedDecisionTable walks every branch of PayloadChunker::scan.
func TestSuggestedDecisionTable(t *testing.T) {
	cfg, err := NewConfig(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	data := testData(1 << 20)
	plain := scanAll(t, cfg, data)
	if len(plain) < 4 {
		t.Fatal("test data produced too few natural cuts")
	}

	fitting := cfg.MinChunkSize + 1
	got := runSuggested(cfg, data, []uint64{uint64(fitting)}, 1<<20)
	if got[0] != fitting {
		t.Fatalf("fitting: first cut = %d, want %d", got[0], fitting)
	}
	wantTail := scanAll(t, cfg, data[fitting:])
	for i, w := range wantTail[:len(wantTail)-1] {
		if got[i+1] != fitting+w {
			t.Fatalf("tail cut %d = %d, want %d", i, got[i+1], fitting+w)
		}
	}

	priority := plain[0] + 1
	if priority > cfg.MaxChunkSize {
		t.Fatalf("seed produced plain[0]=%d too close to max", plain[0])
	}
	got = runSuggested(cfg, data, []uint64{uint64(priority)}, 1<<20)
	if got[0] != priority {
		t.Fatalf("priority: first cut = %d, want %d", got[0], priority)
	}

	got = runSuggested(cfg, data, []uint64{5000}, 1<<20)
	for i, w := range plain {
		if got[i] != w {
			t.Fatalf("too small: cut %d = %d, want %d", i, got[i], w)
		}
	}

	got = runSuggested(cfg, data, []uint64{500}, 1000)
	for i, w := range plain {
		if got[i] != w {
			t.Fatalf("stale: cut %d = %d, want %d", i, got[i], w)
		}
	}

	got = runSuggested(cfg, data, []uint64{uint64(len(data)) + 5}, 1<<20)
	for i, w := range plain {
		if got[i] != w {
			t.Fatalf("future: cut %d = %d, want %d", i, got[i], w)
		}
	}
}

// TestSuggestedMultipleQueued honors several in-bounds suggestions in order.
// With a small config every pairwise gap stays inside [min, max].
func TestSuggestedMultipleQueued(t *testing.T) {
	cfg, err := NewConfig(1 << 12)
	if err != nil {
		t.Fatal(err)
	}
	data := testData(100_000)
	got := runSuggested(cfg, data, []uint64{2000, 5000, 9000}, 100_000)
	want := []int{2000, 5000, 9000}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("cut %d = %d, want %d", i, got[i], w)
		}
	}
}

// TestSuggestedForcedReset covers Reset semantics directly: hash state clears
// and a still-future suggestion survives to cut against the new chunk.
func TestSuggestedForcedReset(t *testing.T) {
	cfg, err := NewConfig(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	data := testData(600_000)

	ss := NewSuggestedScanner(cfg)
	ss.Suggest(300_000)
	n := ss.Scan(0, 0, data[:100_000])
	if n == 0 {
		t.Fatal("no natural boundary before forced cut")
	}
	ss.Reset()
	n = ss.Scan(100_000, 100_000, data[100_000:])
	if n != 300_000-100_000 {
		t.Fatalf("after forced reset: cut length %d, want %d", n, 300_000-100_000)
	}
}
