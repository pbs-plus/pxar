// Package binarytree implements binary search tree operations on arrays.
//
// For any given sorted array, Copy permutes the array so that for each item
// with index i, the item at 2i+1 is smaller and the item at 2i+2 is larger.
// This permits O(log n) binary searches with strictly monotonically increasing indexes.
//
// Algorithm from casync (camakebst.c), originally by L. Bressel, 2017.
package binarytree

// Copy rearranges a sorted array of size n into a binary search tree array.
// For each item at index i, items at 2i+1 are smaller and 2i+2 are larger.
// copyFunc is called with (sourceIndex, destinationIndex) for each element.
func Copy(n int, copyFunc func(src, dest int)) {
	if n == 0 {
		return
	}
	e := uint(0)
	for v := n; v > 1; v >>= 1 {
		e++
	}
	copyInner(copyFunc, n, 0, e, 0)
}

func copyInner(copyFunc func(src, dest int), n int, o int, e uint, i int) {
	p := 1 << e
	t := p + (p >> 1) - 1

	var m int
	if n > t {
		m = p - 1
	} else {
		m = p - 1 - (t - n)
	}

	copyFunc(o+m, i)

	if m > 0 {
		copyInner(copyFunc, m, o, e-1, i*2+1)
	}

	if m+1 < n {
		copyInner(copyFunc, n-m-1, o+m+1, e-1, i*2+2)
	}
}

// SearchBy searches a BST array starting at the given index.
// The compare function should compare the search value to the element.
// skip defines how many matches to ignore (for duplicate handling).
// Returns the index where compare returns 0, or -1, false if not found.
func SearchBy[T any](tree []T, start int, skip int, compare func(T) int) (int, bool) {
	i := start
	for i < len(tree) {
		c := compare(tree[i])
		if c < 0 {
			i = 2*i + 1
		} else if c > 0 {
			i = 2*i + 2
		} else if skip == 0 {
			return i, true
		} else {
			nextI := 2*i + 1
			if idx, found := SearchBy(tree, nextI, skip-1, compare); found {
				return idx, true
			}
			if idx, found := SearchBy(tree, nextI+1, skip-1, compare); found {
				return idx, true
			}
			return 0, false
		}
	}
	return 0, false
}
