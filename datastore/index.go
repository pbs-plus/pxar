package datastore

// ChunkInfo describes a single chunk's position and digest.
type ChunkInfo struct {
	Start  uint64
	End    uint64
	Digest [32]byte
}

// IndexFile is the common interface for chunk index types.
type IndexFile interface {
	Count() int
	IndexBytes() uint64
	CTime() int64
	ChunkInfo(pos int) (ChunkInfo, bool)
	ChunkFromOffset(offset uint64) (int, bool)
	IndexDigest(pos int) ([32]byte, bool)
	ComputeCsum() ([32]byte, uint64)
}
