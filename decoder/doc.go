// Package decoder reads pxar archives sequentially.
//
// Decoder provides a streaming iterator (Next) over pxar archive entries. It
// handles both v1 and v2 formats, including FORMAT_VERSION headers and
// PRELUDE data.
//
// # Usage
//
//	dec := decoder.NewDecoder(reader, nil)
//	for {
//	    entry, err := dec.Next()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // process entry
//	}
//
// For files that carry content, Contents returns an io.Reader to stream the
// payload. The reader is valid until the next call to Next.
//
// For v2 split archives, provide a payload reader as the second argument
// to NewDecoder.
package decoder
