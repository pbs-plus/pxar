// Package binarytree implements binary search tree operations on arrays.
//
// For any given sorted array, Copy permutes the array so that for each item
// with index i, the item at 2i+1 is smaller and the item at 2i+2 is larger.
// This permits O(log n) binary searches with strictly monotonically increasing
// indexes.
//
// Algorithm from casync (camakebst.c), originally by L. Bressel, 2017.
package binarytree
