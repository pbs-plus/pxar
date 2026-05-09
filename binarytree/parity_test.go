package binarytree_test

import (
	"slices"
	"testing"

	"github.com/pbs-plus/pxar/binarytree"
)

// TestCopyParityWithPBS validates binary tree BST permutation against
// expected outputs from Proxmox's Rust reference implementation
// (pxar/src/binary_tree_array.rs test_binary_search_tree).
func TestCopyParityWithPBS(t *testing.T) {
	expected := map[int][]int{
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
		t.Run("n="+itoa(n), func(t *testing.T) {
			got := make([]int, n)
			for i := range got {
				got[i] = -1
			}
			binarytree.Copy(n, func(src, dest int) {
				if got[dest] != -1 {
					t.Errorf("Copy(%d): destination %d already set to %d, trying to set to %d", n, dest, got[dest], src)
				}
				got[dest] = src
			})
			for i := range got {
				if got[i] == -1 {
					t.Errorf("Copy(%d): position %d was never set", n, i)
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("Copy(%d) = %v, want %v", n, got, want)
			}
		})
	}

	// Verify for sizes 18-1000: every position filled exactly once
	for n := 18; n <= 1000; n++ {
		seen := make([]bool, n)
		binarytree.Copy(n, func(src, dest int) {
			if dest < 0 || dest >= n || src < 0 || src >= n {
				t.Errorf("Copy(%d): invalid indices src=%d dest=%d", n, src, dest)
			}
			if seen[dest] {
				t.Errorf("Copy(%d): duplicate dest %d", n, dest)
			}
			seen[dest] = true
		})
		for i, s := range seen {
			if !s {
				t.Errorf("Copy(%d): position %d never filled", n, i)
				break
			}
		}
	}
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
