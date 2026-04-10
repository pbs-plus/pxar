package format

// SipHash24 implementation for filename hashing.
// This implements SipHash-2-4 as specified by Jean-Philippe Aumasson and Daniel J. Bernstein.

func sipRound(v0, v1, v2, v3 *uint64) {
	*v0 += *v1
	*v1 = (*v1 << 13) | (*v1 >> (64 - 13))
	*v1 ^= *v0
	*v0 = (*v0 << 32) | (*v0 >> (64 - 32))

	*v2 += *v3
	*v3 = (*v3 << 16) | (*v3 >> (64 - 16))
	*v3 ^= *v2

	*v0 += *v3
	*v3 = (*v3 << 21) | (*v3 >> (64 - 21))
	*v3 ^= *v0

	*v2 += *v1
	*v1 = (*v1 << 17) | (*v1 >> (64 - 17))
	*v1 ^= *v2
	*v2 = (*v2 << 32) | (*v2 >> (64 - 32))
}

// siphash24 computes SipHash-2-4 with the given key (k0, k1) and message.
func siphash24(k0, k1 uint64, msg []byte) uint64 {
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573

	// Process full 8-byte blocks
	mlen := uint64(len(msg))
	w := mlen / 8

	for i := range w {
		m := le64(msg[i*8:])
		v3 ^= m
		sipRound(&v0, &v1, &v2, &v3)
		sipRound(&v0, &v1, &v2, &v3)
		v0 ^= m
	}

	// Process last partial block
	var last uint64
	remaining := msg[w*8:]
	for i := range remaining {
		last |= uint64(remaining[i]) << (8 * i)
	}
	last |= (mlen & 0xff) << 56

	v3 ^= last
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	v0 ^= last

	// Finalization
	v2 ^= 0xff
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)
	sipRound(&v0, &v1, &v2, &v3)

	return v0 ^ v1 ^ v2 ^ v3
}

func le64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// HashFilename computes the SipHash24 of a filename for use in goodbye tables.
func HashFilename(name []byte) uint64 {
	return siphash24(HashKey1, HashKey2, name)
}
