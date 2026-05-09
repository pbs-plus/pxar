// Package decoder reads pxar archives sequentially.
//
// Decoder provides a streaming iterator (Next) over pxar archive entries. It
// handles both v1 and v2 formats, including optional FORMAT_VERSION headers
// and PRELUDE data.
//
// # Usage
//
//	dec := decoder.NewDecoder(reader, nil)
//	for {
//	    entry, err := dec.Next()
//	    if err == io.EOF {
//	        break
//	    }
//	    // process entry
//	}
//
// For files that carry content, Contents returns an io.Reader to stream the
// payload. The decoder buffers minimal state; paths are tracked via length
// markers for efficient reset.
package decoder
