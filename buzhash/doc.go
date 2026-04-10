// Package buzhash implements the buzhash rolling hash algorithm for
// content-defined chunking.
//
// Content-defined chunking splits a byte stream into variable-size chunks
// based on the data content rather than fixed boundaries. This enables
// deduplication: identical data produces identical chunk boundaries, so
// unchanged regions between backups yield the same chunks.
//
// # Configuration
//
// Use NewConfig to create a Config from a desired average chunk size. The
// config determines minimum, maximum, and average chunk sizes as well as
// the hash mask and threshold used for boundary detection:
//
//	cfg, err := buzhash.NewConfig(4096) // ~4 KiB average chunks
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Chunking a Stream
//
// Create a Chunker from an io.Reader and call Next repeatedly:
//
//	chunker := buzhash.NewChunker(reader, cfg)
//	for {
//	    chunk, err := chunker.Next()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // process chunk
//	}
//
// # Low-level Hasher
//
// For custom chunking logic, use Hasher directly. It provides a 32-bit
// rolling hash over a 64-byte sliding window:
//
//	h := buzhash.NewHasher()
//	h.Update(byte)
//	sum := h.Sum()
package buzhash
