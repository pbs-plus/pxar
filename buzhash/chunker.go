package buzhash

import (
	"io"
)

const readBufSize = 256 << 10 // 256KB read buffer

// Chunker splits a data stream into variable-size chunks using buzhash
// content-defined chunking.
type Chunker struct {
	config  Config
	reader  io.Reader
	hasher  *Hasher
	buf     []byte
	bufPos  int
	bufLen  int
	eof     bool
	inited  bool
}

// NewChunker creates a chunker that reads from r with the given config.
func NewChunker(r io.Reader, config Config) *Chunker {
	return &Chunker{
		config: config,
		reader: r,
		hasher: NewHasher(),
		buf:    make([]byte, readBufSize),
	}
}

// Next returns the next chunk of data. The returned slice is valid only until
// the next call to Next. Returns io.EOF when there is no more data.
func (c *Chunker) Next() ([]byte, error) {
	var result []byte

	for {
		// Fill buffer if needed
		if c.bufPos >= c.bufLen && !c.eof {
			n, err := io.ReadFull(c.reader, c.buf)
			if err == io.EOF {
				if c.bufPos >= c.bufLen {
					// No data at all or all consumed
					if result != nil {
						return result, nil
					}
					return nil, io.EOF
				}
				c.eof = true
			} else if err != nil && err != io.ErrUnexpectedEOF {
				return nil, err
			}
			c.bufLen = n
			c.bufPos = 0
		}

		// If we've consumed all buffered data and hit EOF
		if c.bufPos >= c.bufLen {
			if result != nil {
				return result, nil
			}
			return nil, io.EOF
		}

		// Initialize hasher window if not yet done
		if !c.inited {
			// Accumulate data until we have WindowSize bytes or hit EOF
			avail := c.bufLen - c.bufPos
			needed := WindowSize - c.hasher.count
			if avail < needed && !c.eof {
				// Not enough data yet and more may come; accumulate what we have
				// and read more on the next iteration
				// Actually, just feed what we have and loop to read more
			}

			if c.hasher.count < WindowSize {
				toInit := min(avail, needed)
				if toInit > 0 {
					// Feed bytes one at a time using the initial formula (no outgoing)
					for i := 0; i < toInit; i++ {
						b := c.buf[c.bufPos+i]
						c.hasher.window[c.hasher.wpos] = b
						c.hasher.h = rotl32(c.hasher.h, 1) ^ buzhashTable[b]
						c.hasher.wpos = (c.hasher.wpos + 1) & (WindowSize - 1)
						c.hasher.count++
					}

					chunkData := c.buf[c.bufPos : c.bufPos+toInit]
					result = append(result, chunkData...)
					c.bufPos += toInit
				}

				if c.hasher.count < WindowSize {
					if c.eof {
						// Less than a full window of data total
						if len(result) > 0 {
							return result, nil
						}
						return nil, io.EOF
					}
					continue // read more data
				}

				c.inited = true
				continue // check for boundary with the initial hash
			}
			c.inited = true
		}

		// Scan for chunk boundary
		chunkStart := c.bufPos
		for c.bufPos < c.bufLen {
			b := c.buf[c.bufPos]
			out := c.hasher.window[c.hasher.wpos]
			c.hasher.window[c.hasher.wpos] = b
			c.hasher.wpos = (c.hasher.wpos + 1) & (WindowSize - 1)
			c.hasher.h = rotl32(c.hasher.h, 1) ^ buzhashTable[out] ^ buzhashTable[b]
			c.hasher.count++
			c.bufPos++

			chunkSize := len(result) + (c.bufPos - chunkStart)

			if chunkSize >= c.config.MaxChunkSize {
				result = append(result, c.buf[chunkStart:c.bufPos]...)
				return result, nil
			}

			if chunkSize >= c.config.MinChunkSize {
				if (c.hasher.h & c.config.Mask) >= c.config.Threshold {
					result = append(result, c.buf[chunkStart:c.bufPos]...)
					return result, nil
				}
			}
		}

		// Consumed all buffered data, accumulate into result
		result = append(result, c.buf[chunkStart:c.bufPos]...)
	}
}

// Reset resets the chunker to process a new stream.
func (c *Chunker) Reset(r io.Reader) {
	c.reader = r
	c.hasher.Reset()
	c.bufPos = 0
	c.bufLen = 0
	c.eof = false
	c.inited = false
}
