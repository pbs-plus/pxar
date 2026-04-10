//go:build !goexperiment.simd || !amd64

package buzhash

func (h *Hasher) initWindow(data []byte) {
	h.h = initWindowScalar(data)
}
