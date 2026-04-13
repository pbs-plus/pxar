package datastore

// ChunkInfo describes a single chunk's position and digest.
type ChunkInfo struct {
	Start  uint64
	End    uint64
	Digest [32]byte
}
