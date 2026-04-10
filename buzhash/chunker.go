package buzhash

import (
	"io"
)

const readBufSize = 256 << 10 // 256KB read buffer

// Chunker splits a data stream into variable-size chunks using buzhash
// content-defined chunking. It performs zero heap allocations during scanning.
//
// The returned slice from Next references internal buffers and is valid only
// until the next call to Next. Callers must copy the data if they need to
// retain it.
type Chunker struct {
	config Config
	reader io.Reader
	hasher Hasher
	buf    []byte // read buffer
	out    []byte // pre-allocated spill buffer (size = MaxChunkSize)
	outLen int    // bytes in spill buffer
	bufPos int
	bufLen int
	eof    bool
	inited bool
}

// NewChunker creates a chunker that reads from r with the given config.
func NewChunker(r io.Reader, config Config) *Chunker {
	return &Chunker{
		config: config,
		reader: r,
		buf:    make([]byte, readBufSize),
		out:    make([]byte, config.MaxChunkSize),
	}
}

// Next returns the next chunk of data. The returned slice references internal
// buffers and is valid only until the next call to Next.
// Returns io.EOF when there is no more data.
func (c *Chunker) Next() ([]byte, error) {
	c.outLen = 0

	for {
		// Fill read buffer if needed
		if c.bufPos >= c.bufLen && !c.eof {
			n, err := io.ReadFull(c.reader, c.buf)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				c.eof = true
				if n == 0 {
					if c.outLen > 0 {
						return c.out[:c.outLen], nil
					}
					return nil, io.EOF
				}
			} else if err != nil {
				return nil, err
			}
			c.bufLen = n
			c.bufPos = 0
		}

		if c.bufPos >= c.bufLen {
			// No more buffered data
			if c.outLen > 0 {
				return c.out[:c.outLen], nil
			}
			return nil, io.EOF
		}

		// Phase 1: Initialize hasher window (no outgoing XOR for first WindowSize bytes)
		if !c.inited {
			avail := c.bufLen - c.bufPos
			needed := WindowSize - c.hasher.count
			toInit := min(avail, needed)

			c.feedInit(c.buf[c.bufPos : c.bufPos+toInit])

			// Spill init bytes into output buffer
			c.outLen += copy(c.out[c.outLen:], c.buf[c.bufPos:c.bufPos+toInit])
			c.bufPos += toInit

			if c.hasher.count < WindowSize {
				if c.eof {
					// Total data < WindowSize — it's one chunk
					if c.outLen > 0 {
						return c.out[:c.outLen], nil
					}
					return nil, io.EOF
				}
				continue // read more
			}
			c.inited = true
			// Don't continue — fall through to check boundary on init hash
		}

		// Phase 2: Scan for chunk boundary
		chunkStart := c.bufPos
		for c.bufPos < c.bufLen {
			b := c.buf[c.bufPos]
			out := c.hasher.window[c.hasher.wpos]
			c.hasher.window[c.hasher.wpos] = b
			c.hasher.wpos = (c.hasher.wpos + 1) & (WindowSize - 1)
			c.hasher.h = rotl32(c.hasher.h, 1) ^ buzhashTable[out] ^ buzhashTable[b]
			c.hasher.count++
			c.bufPos++

			chunkSize := c.outLen + (c.bufPos - chunkStart)

			if chunkSize >= c.config.MaxChunkSize {
				return c.emitChunk(chunkStart, c.bufPos-chunkStart)
			}

			if chunkSize >= c.config.MinChunkSize {
				if (c.hasher.h & c.config.Mask) >= c.config.Threshold {
					return c.emitChunk(chunkStart, c.bufPos-chunkStart)
				}
			}
		}

		// Buffer exhausted — spill and read more
		if c.bufPos > chunkStart {
			c.outLen += copy(c.out[c.outLen:], c.buf[chunkStart:c.bufPos])
		}
	}
}

// emitChunk returns chunk data. If no spill (outLen == 0), returns a direct
// view into the read buffer. Otherwise copies remaining data into the spill
// buffer and returns that.
func (c *Chunker) emitChunk(start, length int) ([]byte, error) {
	if c.outLen == 0 {
		return c.buf[start : start+length], nil
	}
	c.outLen += copy(c.out[c.outLen:], c.buf[start:start+length])
	return c.out[:c.outLen], nil
}

// feedInit feeds bytes into the hasher using the initial formula (no outgoing byte).
func (c *Chunker) feedInit(data []byte) {
	for _, b := range data {
		c.hasher.window[c.hasher.wpos] = b
		c.hasher.h = rotl32(c.hasher.h, 1) ^ buzhashTable[b]
		c.hasher.wpos = (c.hasher.wpos + 1) & (WindowSize - 1)
		c.hasher.count++
	}
}

// Reset resets the chunker to process a new stream.
func (c *Chunker) Reset(r io.Reader) {
	c.reader = r
	c.hasher.Reset()
	c.outLen = 0
	c.bufPos = 0
	c.bufLen = 0
	c.eof = false
	c.inited = false
}
