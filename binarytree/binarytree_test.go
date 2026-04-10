package binarytree

import (
	"cmp"
	"testing"
)

func TestCopy(t *testing.T) {
	runTest := func(len int) []int {
		const marker = 0xfffffff
		output := make([]int, len)
		for i := range output {
			output[i] = marker
		}
		Copy(len, func(s, d int) {
			if output[d] != marker {
				t.Fatalf("Copy(%d): destination %d already written", len, d)
			}
			output[d] = s
		})
		for i, v := range output {
			if v == marker {
				t.Fatalf("Copy(%d): output[%d] not written", len, i)
			}
		}
		return output
	}

	expected := map[int][]int{
		0:  {},
		1:  {0},
		2:  {1, 0},
		3:  {1, 0, 2},
		4:  {2, 1, 3, 0},
		5:  {3, 1, 4, 0, 2},
		6:  {3, 1, 5, 0, 2, 4},
		7:  {3, 1, 5, 0, 2, 4, 6},
		8:  {4, 2, 6, 1, 3, 5, 7, 0},
		9:  {5, 3, 7, 1, 4, 6, 8, 0, 2},
		10: {6, 3, 8, 1, 5, 7, 9, 0, 2, 4},
		11: {7, 3, 9, 1, 5, 8, 10, 0, 2, 4, 6},
		12: {7, 3, 10, 1, 5, 9, 11, 0, 2, 4, 6, 8},
		13: {7, 3, 11, 1, 5, 9, 12, 0, 2, 4, 6, 8, 10},
		14: {7, 3, 11, 1, 5, 9, 13, 0, 2, 4, 6, 8, 10, 12},
		15: {7, 3, 11, 1, 5, 9, 13, 0, 2, 4, 6, 8, 10, 12, 14},
		16: {8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13, 15, 0},
		17: {9, 5, 13, 3, 7, 11, 15, 1, 4, 6, 8, 10, 12, 14, 16, 0, 2},
	}

	for n, want := range expected {
		got := runTest(n)
		if len(got) != len(want) {
			t.Errorf("Copy(%d): length %d, want %d", n, len(got), len(want))
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("Copy(%d): got %v, want %v", n, got, want)
				break
			}
		}
	}

	// Test larger sizes just don't panic
	for n := 18; n < 1000; n++ {
		runTest(n)
	}
}

func TestSearchBy(t *testing.T) {
	// Build a BST from sorted values
	vals := []int{0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 8, 8, 8}
	clone := make([]int, len(vals))
	copy(clone, vals)
	Copy(len(vals), func(s, d int) {
		vals[d] = clone[s]
	})

	expected := []int{5, 2, 8, 1, 3, 6, 8, 0, 2, 2, 4, 6, 7, 8}
	for i, v := range vals {
		if v != expected[i] {
			t.Fatalf("BST construction: vals=%v, expected=%v", vals, expected)
		}
	}

	// Search for value 8, skip=0
	idx, found := SearchBy(vals, 0, 0, func(el int) int { return cmp.Compare(8, el) })
	if !found || idx != 2 {
		t.Errorf("search(8, skip=0): idx=%d, found=%v, want idx=2, found=true", idx, found)
	}

	// Search for value 8, skip=1 from index 2
	idx, found = SearchBy(vals, 2, 1, func(el int) int { return cmp.Compare(8, el) })
	if !found || idx != 6 {
		t.Errorf("search(8, skip=1, start=2): idx=%d, found=%v, want idx=6, found=true", idx, found)
	}

	// Search for value 8, skip=1 from index 6
	idx, found = SearchBy(vals, 6, 1, func(el int) int { return cmp.Compare(8, el) })
	if !found || idx != 13 {
		t.Errorf("search(8, skip=1, start=6): idx=%d, found=%v, want idx=13, found=true", idx, found)
	}

	// Search for value 5, skip=1
	idx, found = SearchBy(vals, 0, 1, func(el int) int { return cmp.Compare(5, el) })
	if found {
		t.Errorf("search(5, skip=1): idx=%d, found=%v, want found=false", idx, found)
	}

	// Search starting at length
	idx, found = SearchBy(vals, len(vals), 0, func(el int) int { return cmp.Compare(5, el) })
	if found {
		t.Errorf("search(start=len): should return not found")
	}

	// Search starting beyond length
	idx, found = SearchBy(vals, len(vals)+1, 0, func(el int) int { return cmp.Compare(5, el) })
	if found {
		t.Errorf("search(start=len+1): should return not found")
	}
}

func TestCopyZero(t *testing.T) {
	called := false
	Copy(0, func(s, d int) {
		called = true
	})
	if called {
		t.Error("Copy(0) should not call copyFunc")
	}
}
