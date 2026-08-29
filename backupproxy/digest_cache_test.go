package backupproxy

import "testing"

func TestDigestCacheBoundsEntries(t *testing.T) {
	cache := newDigestCache(2)
	one := [32]byte{1}
	two := [32]byte{2}
	three := [32]byte{3}
	cache.add(one)
	cache.add(two)
	cache.add(two)
	if !cache.contains(one) || !cache.contains(two) || len(cache.items) != 2 {
		t.Fatalf("unexpected initial cache: %+v", cache.items)
	}
	cache.add(three)
	if cache.contains(one) || !cache.contains(two) || !cache.contains(three) || len(cache.items) != 2 {
		t.Fatalf("unexpected bounded cache: %+v", cache.items)
	}
}
