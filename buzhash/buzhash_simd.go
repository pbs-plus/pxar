//go:build goexperiment.simd && amd64

package buzhash

import (
	"simd/archsimd"
)

var hasAVX2 bool

func init() {
	hasAVX2 = archsimd.X86.AVX2()
}

func (h *Hasher) initWindow(data []byte) {
	if hasAVX2 && len(data) == WindowSize {
		h.h = initWindowAVX2(data)
		return
	}
	h.h = initWindowScalar(data)
}

// initWindowAVX2 computes the initial hash using AVX2 per-element shifts.
//
// For a full 64-byte window:
//
//	h = rotl(T[b0], 63) ^ rotl(T[b1], 62) ^ ... ^ T[b63]
//
// Since rotl(x, n) = rotl(x, n & 31), shift amounts are masked to 5 bits.
// Rotation is implemented as (x << n) | (x >> (32-n)) using VPSLLVD + VPSRLVD.
func initWindowAVX2(data []byte) uint32 {
	var acc archsimd.Uint32x4

	for i := 0; i < WindowSize; i += 4 {
		// Raw shift amounts: 63-i, 62-i, 61-i, 60-i
		// Masked to 31 to handle rotation in 32-bit space
		s0 := uint32(WindowSize-1-i) & 31
		s1 := uint32(WindowSize-2-i) & 31
		s2 := uint32(WindowSize-3-i) & 31
		s3 := uint32(WindowSize-4-i) & 31

		vals := [4]uint32{
			buzhashTable[data[i]],
			buzhashTable[data[i+1]],
			buzhashTable[data[i+2]],
			buzhashTable[data[i+3]],
		}
		vec := archsimd.LoadUint32x4(&vals)

		leftShifts := [4]uint32{s0, s1, s2, s3}
		leftVec := archsimd.LoadUint32x4(&leftShifts)

		rightShifts := [4]uint32{32 - s0, 32 - s1, 32 - s2, 32 - s3}
		rightVec := archsimd.LoadUint32x4(&rightShifts)

		// rotl(x, n) = (x << n) | (x >> (32-n))
		rotated := vec.ShiftLeft(leftVec).Or(vec.ShiftRight(rightVec))

		acc = acc.Xor(rotated)
	}

	return acc.GetElem(0) ^ acc.GetElem(1) ^ acc.GetElem(2) ^ acc.GetElem(3)
}
