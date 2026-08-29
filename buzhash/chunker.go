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
	reader  io.Reader
	buf     []byte
	out     []byte
	scanner Scanner
	outLen  int
	bufPos  int
	bufLen  int
	eof     bool
}

// NewChunker creates a chunker that reads from r with the given config.
func NewChunker(r io.Reader, config Config) *Chunker {
	return &Chunker{
		reader:  r,
		buf:     make([]byte, readBufSize),
		out:     make([]byte, config.MaxChunkSize),
		scanner: Scanner{Config: config},
	}
}

// Next returns the next chunk of data. The returned slice references internal
// buffers and is valid only until the next call to Next.
// Returns io.EOF when there is no more data.
func (c *Chunker) Next() ([]byte, error) {
	c.outLen = 0

	for {
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
			if c.outLen > 0 {
				return c.out[:c.outLen], nil
			}
			return nil, io.EOF
		}

		start := c.bufPos
		if boundary := c.scanner.Scan(c.buf[start:c.bufLen]); boundary > 0 {
			c.bufPos += boundary
			if c.outLen == 0 {
				return c.buf[start:c.bufPos], nil
			}
			c.outLen += copy(c.out[c.outLen:], c.buf[start:c.bufPos])
			return c.out[:c.outLen], nil
		}

		c.bufPos = c.bufLen
		c.outLen += copy(c.out[c.outLen:], c.buf[start:c.bufPos])
	}
}

// Reset resets the chunker to process a new stream.
func (c *Chunker) Reset(r io.Reader) {
	c.reader = r
	c.scanner.Reset()
	c.outLen = 0
	c.bufPos = 0
	c.bufLen = 0
	c.eof = false
}
