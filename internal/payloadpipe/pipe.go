package payloadpipe

import (
	"io"
	"sync"
)

// Pipe is a synchronous byte pipe whose reader can be woken without adding data.
type Pipe struct {
	data chan []byte
	ack  chan struct{}
	wake chan struct{}
	done chan struct{}
	once sync.Once

	mu  sync.Mutex
	err error

	current []byte
}

// New creates a Pipe.
func New() *Pipe {
	return &Pipe{
		data: make(chan []byte),
		ack:  make(chan struct{}),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// Write blocks until the reader consumes p or the pipe closes.
func (p *Pipe) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	select {
	case p.data <- data:
	case <-p.done:
		return 0, p.writeError()
	}
	select {
	case <-p.ack:
		return len(data), nil
	case <-p.done:
		return 0, p.writeError()
	}
}

// Read returns zero bytes after Wake so callers can process out-of-band work.
func (p *Pipe) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for len(p.current) == 0 {
		select {
		case p.current = <-p.data:
		case <-p.wake:
			return 0, nil
		case <-p.done:
			return 0, p.readError()
		}
	}

	n := copy(dst, p.current)
	p.current = p.current[n:]
	if len(p.current) == 0 {
		select {
		case p.ack <- struct{}{}:
		case <-p.done:
		}
	}
	return n, nil
}

// Wake interrupts a blocked Read without transferring bytes.
func (p *Pipe) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// CloseWithError closes the pipe and reports err to its reader.
func (p *Pipe) CloseWithError(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	})
}

// Done is closed when the pipe closes.
func (p *Pipe) Done() <-chan struct{} {
	return p.done
}

// Err returns the error supplied to CloseWithError.
func (p *Pipe) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Pipe) readError() error {
	if err := p.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (p *Pipe) writeError() error {
	if err := p.Err(); err != nil {
		return err
	}
	return io.ErrClosedPipe
}
