package backupproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// pbsH2Conn is a raw HTTP/2 client for the PBS backup protocol. Unlike a
// synchronous send-then-read client, it runs a central frame-reader goroutine
// and tracks send-side flow control (the peer's per-stream and connection
// receive windows), so request bodies larger than the peer's initial window can
// be streamed without deadlocking and many uploads can be in flight
// concurrently over H2 multiplexing. This mirrors what proxmox-backup's Rust
// client gets for free from tokio's async h2 implementation.
type pbsH2Conn struct {
	conn      net.Conn
	framer    *http2.Framer
	enc       *hpack.Encoder
	dec       *hpack.Decoder
	hdrBuf    *bytes.Buffer
	authority string

	// writeMu serializes all framer writes (HEADERS, DATA, WINDOW_UPDATE,
	// SETTINGS, PING) and protects nextID, enc and hdrBuf. The framer and hpack
	// encoder are not goroutine-safe; concurrent senders acquire this lock per
	// frame while waiting for flow-control credit between frames.
	writeMu      sync.Mutex
	nextID       uint32
	maxFrameSize uint32

	// Send-side flow control: the peer's receive windows, i.e. how many bytes we
	// may still send on the connection (peerConnWin) and per stream
	// (peerStreamWin). Updated by the reader on WINDOW_UPDATE; awaited by
	// senders in awaitSendWindow. peerInitWin is the per-stream window granted
	// by the server's SETTINGS (default 65535); new streams start with it.
	sendMu        sync.Mutex
	sendCond      *sync.Cond
	peerConnWin   int64
	peerStreamWin map[uint32]int64
	peerInitWin   int64
	closed        atomic.Bool
	closeErr      atomic.Pointer[error]

	// Per-stream response state. Owned by the reader goroutine; streams are
	// registered under writeMu before their HEADERS frame is written, so the
	// reader never sees a response for an unregistered stream.
	streamsMu sync.Mutex
	streams   map[uint32]*stream

	// streamSlots bounds the number of concurrently open streams to the peer's
	// SETTINGS_MAX_CONCURRENT_STREAMS (faithful to the h2 crate's
	// SendRequest::ready(), which waits for a stream slot). nil if the peer did
	// not advertise a limit. Acquired in sendRequest before opening a stream,
	// released in finishStream.
	streamSlots chan struct{}

	// streamPool reuses stream structs (and their response buffers) across
	// requests, the Go-flavored counterpart of the Rust client's bytes::Bytes
	// buffer reuse. The done channel is recreated per use (a closed channel
	// cannot be reused); the hdrBuf/dataBuf retain capacity across uses.
	streamPool sync.Pool

	// Our receive-side flow control for response data (e.g. previous-index
	// downloads). The reader replenishes by sending WINDOW_UPDATE when the
	// window drops below half.
	recvConnWin     int64
	recvConnInitial int64
}

// stream is the handle for one H2 request/response. The body is sent
// asynchronously by a goroutine spawned in sendRequest; Wait blocks until the
// reader has the full response.
type stream struct {
	id      uint32
	status  int
	hdrBuf  bytes.Buffer // accumulated response HPACK
	dataBuf bytes.Buffer // response body

	// Our per-stream receive window for response data on this stream.
	recvWin int64

	done chan struct{} // closed when the response is complete
	err  error

	pool *pbsH2Conn // owning pool for release (nil if not pooled)
}

// newStream returns a stream for a new request, reusing a pooled struct and
// its response buffers (capacity retained) when available. The done channel is
// always fresh (a closed channel cannot be reused).
func (c *pbsH2Conn) newStream(id uint32, recvInit int64) *stream {
	s, _ := c.streamPool.Get().(*stream)
	s.id = id
	s.status = 0
	s.recvWin = recvInit
	s.err = nil
	s.pool = c
	s.hdrBuf.Reset()
	s.dataBuf.Reset()
	s.done = make(chan struct{})
	return s
}

// release returns the stream to the pool after the caller has consumed the
// response (Wait/WaitRaw). It must not be called twice or before the response
// is read.
func (s *stream) release() {
	if s.pool != nil {
		s.pool.streamPool.Put(s)
	}
}

// dialPBSH2 establishes an H2 connection to PBS via HTTP/1.1 upgrade and starts
// the central frame-reader goroutine.
func dialPBSH2(ctx context.Context, rawURL, datastore, authToken string, cfg BackupConfig, skipTLS bool) (*pbsH2Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse PBS URL: %w", err)
	}

	host := u.Host
	if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
		host = host + ":8007"
	}

	params := url.Values{}
	params.Set("store", datastore)
	params.Set("backup-type", cfg.BackupType.String())
	params.Set("backup-id", cfg.BackupID)
	params.Set("backup-time", strconv.FormatInt(cfg.BackupTime, 10))
	if cfg.Namespace != "" {
		params.Set("ns", cfg.Namespace)
	}
	if cfg.Debug {
		params.Set("debug", "1")
	}
	upgradePath := u.Path + "/backup?" + params.Encode()

	tlsCfg := &tls.Config{
		InsecureSkipVerify: skipTLS,
		NextProtos:         []string{"http/1.1"},
	}
	var d tls.Dialer
	d.Config = tlsCfg
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("TLS dial %s: %w", host, err)
	}

	hostHeader := u.Host
	if _, _, splitErr := net.SplitHostPort(hostHeader); splitErr != nil {
		hostHeader = host
	}
	upgradeReq := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: proxmox-backup-protocol-v1\r\n"+
			"Authorization: PBSAPIToken %s\r\n"+
			"\r\n",
		upgradePath, hostHeader, authToken,
	)
	if _, err := conn.Write([]byte(upgradeReq)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}

	br := bufio.NewReaderSize(conn, 1<<20)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read upgrade response: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("upgrade failed: HTTP %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write H2 preface: %w", err)
	}

	framer := http2.NewFramer(conn, br)
	framer.SetMaxReadFrameSize(1 << 24) // 16 MiB

	const (
		targetWindow   = (1 << 31) - 2 // ~2 GiB, matches proxmox-backup's h2 builder (initial_connection_window_size = initial_window_size)
		targetMaxFrame = 1 << 22       // 4 MiB, matches proxmox-backup's max_frame_size
	)

	if err := framer.WriteSettings(
		http2.Setting{ID: http2.SettingInitialWindowSize, Val: targetWindow},
		http2.Setting{ID: http2.SettingMaxFrameSize, Val: targetMaxFrame},
	); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write SETTINGS: %w", err)
	}
	if err := framer.WriteWindowUpdate(0, uint32(targetWindow-65535)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write connection WINDOW_UPDATE: %w", err)
	}

	// Read server SETTINGS and our SETTINGS ACK. Capture the peer's initial
	// stream window (SettingInitialWindowSize), max frame size, and max
	// concurrent streams so send-side flow control and stream concurrency are
	// correct (matching the h2 crate's ready()/capacity handling).
	maxFrame := uint32(1 << 14)  // default 16384
	peerInitWin := int64(65535)  // RFC 7540 default initial window
	var peerMaxConcurrent uint32 // 0 = unlimited (RFC default)
	gotSettings := false
	gotAck := false
	for !gotSettings || !gotAck {
		frame, err := framer.ReadFrame()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("handshake: %w", err)
		}
		sf, ok := frame.(*http2.SettingsFrame)
		if !ok {
			continue
		}
		if sf.IsAck() {
			gotAck = true
			continue
		}
		if v, ok := sf.Value(http2.SettingMaxFrameSize); ok {
			maxFrame = v
		}
		if v, ok := sf.Value(http2.SettingInitialWindowSize); ok {
			peerInitWin = int64(v)
		}
		if v, ok := sf.Value(http2.SettingMaxConcurrentStreams); ok {
			peerMaxConcurrent = v
		}
		if err := framer.WriteSettingsAck(); err != nil {
			conn.Close()
			return nil, fmt.Errorf("SETTINGS ACK: %w", err)
		}
		gotSettings = true
	}

	hdrBuf := new(bytes.Buffer)
	c := &pbsH2Conn{
		conn:            conn,
		framer:          framer,
		enc:             hpack.NewEncoder(hdrBuf),
		dec:             hpack.NewDecoder(4096, nil),
		hdrBuf:          hdrBuf,
		authority:       u.Host,
		nextID:          1,
		maxFrameSize:    maxFrame,
		peerConnWin:     65535, // connection send window starts at the H2 default; grows via server WINDOW_UPDATE(0)
		peerStreamWin:   make(map[uint32]int64),
		peerInitWin:     peerInitWin,
		streams:         make(map[uint32]*stream),
		recvConnWin:     targetWindow,
		recvConnInitial: targetWindow,
	}
	c.streamPool = sync.Pool{New: func() any { return &stream{} }}
	if peerMaxConcurrent > 0 {
		c.streamSlots = make(chan struct{}, peerMaxConcurrent)
	}
	c.sendCond = sync.NewCond(&c.sendMu)
	go c.readLoop()
	return c, nil
}

// readLoop is the central frame reader. It runs for the connection's lifetime,
// dispatching frames to per-stream state and processing WINDOW_UPDATEs so that
// senders blocked on flow control can proceed. All framer reads happen here;
// no other goroutine reads frames.
func (c *pbsH2Conn) readLoop() {
	for {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			c.fail(fmt.Errorf("read frame: %w", err))
			return
		}
		c.handleFrame(frame)
	}
}

func (c *pbsH2Conn) handleFrame(frame http2.Frame) {
	switch f := frame.(type) {
	case *http2.WindowUpdateFrame:
		c.sendMu.Lock()
		if f.StreamID == 0 {
			c.peerConnWin += int64(f.Increment)
		} else {
			c.peerStreamWin[f.StreamID] += int64(f.Increment)
		}
		c.sendCond.Broadcast()
		c.sendMu.Unlock()

	case *http2.HeadersFrame:
		st := c.streamLookup(f.StreamID)
		if st == nil {
			if f.Flags.Has(http2.FlagHeadersEndHeaders) {
				_, _ = c.dec.DecodeFull(f.HeaderBlockFragment())
			}
			break
		}
		st.hdrBuf.Write(f.HeaderBlockFragment())
		if f.Flags.Has(http2.FlagHeadersEndHeaders) {
			st.status = c.decodeStatus(&st.hdrBuf)
		}
		if f.StreamEnded() {
			c.finishStream(st)
		}

	case *http2.ContinuationFrame:
		st := c.streamLookup(f.StreamID)
		if st == nil {
			break
		}
		st.hdrBuf.Write(f.HeaderBlockFragment())
		if f.Flags.Has(http2.FlagHeadersEndHeaders) {
			st.status = c.decodeStatus(&st.hdrBuf)
		}

	case *http2.DataFrame:
		dataLen := int64(len(f.Data()))
		// Connection-level receive flow control.
		c.recvConnWin -= dataLen
		if c.recvConnWin < c.recvConnInitial/2 {
			incr := uint32(c.recvConnInitial - c.recvConnWin)
			c.writeWindowUpdate(0, incr)
			c.recvConnWin += int64(incr)
		}
		st := c.streamLookup(f.StreamID)
		if st == nil {
			break
		}
		st.dataBuf.Write(f.Data())
		// Stream-level receive flow control.
		st.recvWin -= dataLen
		if st.recvWin < streamRecvInitial/2 {
			incr := uint32(streamRecvInitial - st.recvWin)
			c.writeWindowUpdate(f.StreamID, incr)
			st.recvWin += int64(incr)
		}
		if f.StreamEnded() {
			c.finishStream(st)
		}

	case *http2.SettingsFrame:
		if f.IsAck() {
			break
		}
		// Apply SETTINGS: a change to InitialWindowSize adjusts every live
		// stream's send window by the delta (RFC 7540 §6.9.2). The connection-
		// level window is unaffected.
		if v, ok := f.Value(http2.SettingInitialWindowSize); ok {
			newInit := int64(v)
			delta := newInit - c.peerInitWin
			c.peerInitWin = newInit
			c.sendMu.Lock()
			for sid := range c.peerStreamWin {
				c.peerStreamWin[sid] += delta
			}
			c.sendCond.Broadcast()
			c.sendMu.Unlock()
		}
		if v, ok := f.Value(http2.SettingMaxFrameSize); ok {
			c.writeMu.Lock()
			c.maxFrameSize = v
			c.writeMu.Unlock()
		}
		c.writeSettingsAck()

	case *http2.PingFrame:
		if !f.IsAck() {
			c.writeMu.Lock()
			_ = c.framer.WritePing(true, f.Data)
			c.writeMu.Unlock()
		}

	case *http2.RSTStreamFrame:
		st := c.streamLookup(f.StreamID)
		if st != nil {
			st.err = fmt.Errorf("stream reset: error code %d", f.ErrCode)
			c.finishStream(st)
		}

	case *http2.GoAwayFrame:
		c.fail(fmt.Errorf("server GOAWAY: error code %d", f.ErrCode))
	}
}

// finishStream marks a stream's response complete and wakes Waiters.
func (c *pbsH2Conn) finishStream(st *stream) {
	c.streamsMu.Lock()
	if _, ok := c.streams[st.id]; !ok {
		c.streamsMu.Unlock()
		return
	}
	delete(c.streams, st.id)
	c.streamsMu.Unlock()
	c.releaseSlot()
	close(st.done)
}

// releaseSlot returns a MAX_CONCURRENT_STREAMS slot to the pool. nil when the
// peer did not advertise a limit.
func (c *pbsH2Conn) releaseSlot() {
	if c.streamSlots != nil {
		<-c.streamSlots
	}
}

func (c *pbsH2Conn) streamLookup(id uint32) *stream {
	c.streamsMu.Lock()
	st := c.streams[id]
	c.streamsMu.Unlock()
	return st
}

// fail closes the connection and errors out every live stream and every sender
// blocked on flow control.
func (c *pbsH2Conn) fail(err error) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	e := err
	c.closeErr.Store(&e)
	_ = c.conn.Close()
	c.sendMu.Lock()
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
	c.streamsMu.Lock()
	nStreams := len(c.streams)
	for _, st := range c.streams {
		if st.err == nil {
			st.err = err
		}
		select {
		case <-st.done:
		default:
			close(st.done)
		}
	}
	c.streams = make(map[uint32]*stream)
	c.streamsMu.Unlock()
	// Release every stream's MAX_CONCURRENT_STREAMS slot so senders blocked in
	// sendRequest's slot acquire unblock and observe the closed connection.
	for range nStreams {
		c.releaseSlot()
	}
}

// sendRequest writes an H2 request on a new stream and returns its handle. The
// HEADERS frame is written synchronously; the body (if any) is streamed
// asynchronously by a goroutine under flow control, so the caller is not
// blocked by a slow peer window and many requests can be in flight at once.
func (c *pbsH2Conn) sendRequest(method, path string, params url.Values, body []byte, contentType string) (*stream, error) {
	if c.closed.Load() {
		if e := c.closeErr.Load(); e != nil {
			return nil, *e
		}
		return nil, errConnClosed
	}

	// Wait for a stream slot if the peer advertised MAX_CONCURRENT_STREAMS,
	// faithful to the h2 crate's SendRequest::ready() which yields until a
	// stream can be opened. The slot is held until the stream closes
	// (finishStream), matching the server's concurrent-stream accounting.
	if c.streamSlots != nil {
		c.streamSlots <- struct{}{}
		if c.closed.Load() {
			c.releaseSlot()
			if e := c.closeErr.Load(); e != nil {
				return nil, *e
			}
			return nil, errConnClosed
		}
	}

	fullPath := path
	if len(params) > 0 {
		fullPath += "?" + params.Encode()
	}

	c.writeMu.Lock()
	id := c.allocID()
	st := c.newStream(id, streamRecvInitial)
	c.streamsMu.Lock()
	c.streams[id] = st
	c.streamsMu.Unlock()
	c.sendMu.Lock()
	c.peerStreamWin[id] = c.peerInitWin
	c.sendMu.Unlock()
	c.hdrBuf.Reset()
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":method", Value: method})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":path", Value: fullPath})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":authority", Value: c.authority})
	if contentType != "" {
		_ = c.enc.WriteField(hpack.HeaderField{Name: "content-type", Value: contentType})
	}
	if body != nil {
		_ = c.enc.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(body))})
	}
	err := c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      id,
		BlockFragment: c.hdrBuf.Bytes(),
		EndHeaders:    true,
		EndStream:     body == nil,
	})
	c.writeMu.Unlock()
	if err != nil {
		c.streamsMu.Lock()
		delete(c.streams, id)
		c.streamsMu.Unlock()
		c.sendMu.Lock()
		delete(c.peerStreamWin, id)
		c.sendMu.Unlock()
		c.releaseSlot()
		return nil, fmt.Errorf("write HEADERS: %w", err)
	}

	if body != nil {
		go func() {
			if err := c.writeBody(st, body); err != nil && !c.closed.Load() {
				st.err = err
				c.finishStream(st)
			}
		}()
	}
	return st, nil
}

// writeBody streams data as DATA frames, waiting for send-side flow-control
// credit (connection + stream window) before each frame. This is what lets
// bodies larger than the peer's initial window proceed without deadlocking:
// the reader delivers WINDOW_UPDATEs while we are blocked here.
func (c *pbsH2Conn) writeBody(st *stream, body []byte) error {
	max := int(c.maxFrameSize)
	for len(body) > 0 {
		n := min(len(body), max)
		end := len(body) == n
		if err := c.awaitSendWindow(st, int64(n)); err != nil {
			return err
		}
		c.writeMu.Lock()
		err := c.framer.WriteData(st.id, end, body[:n])
		c.writeMu.Unlock()
		if err != nil {
			return fmt.Errorf("write DATA: %w", err)
		}
		body = body[n:]
	}
	return nil
}

// awaitSendWindow blocks until the peer allows sending n more bytes on both the
// connection and the stream window, then deducts n from both. Returns on
// connection close or if the stream is reset (st.done closed) -- faithful to
// the Rust PipeToSendStream's poll_reset check.
func (c *pbsH2Conn) awaitSendWindow(st *stream, n int64) error {
	if n == 0 {
		return nil
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	for {
		if c.closed.Load() {
			if e := c.closeErr.Load(); e != nil {
				return *e
			}
			return errConnClosed
		}
		// Per-stream reset check: if the reader closed st.done (RST_STREAM or
		// connection failure), abort the send with the stream's error.
		select {
		case <-st.done:
			if st.err != nil {
				return fmt.Errorf("send aborted: %w", st.err)
			}
			return fmt.Errorf("send aborted: stream closed")
		default:
		}
		if c.peerConnWin >= n && c.peerStreamWin[st.id] >= n {
			c.peerConnWin -= n
			c.peerStreamWin[st.id] -= n
			return nil
		}
		c.sendCond.Wait()
	}
}

// allocID returns the next client stream ID (odd). Caller holds writeMu.
func (c *pbsH2Conn) allocID() uint32 {
	id := c.nextID
	c.nextID += 2
	return id
}

func (c *pbsH2Conn) writeWindowUpdate(streamID uint32, incr uint32) {
	c.writeMu.Lock()
	_ = c.framer.WriteWindowUpdate(streamID, incr)
	c.writeMu.Unlock()
}

func (c *pbsH2Conn) writeSettingsAck() {
	c.writeMu.Lock()
	_ = c.framer.WriteSettingsAck()
	c.writeMu.Unlock()
}

// decodeStatus extracts the :status value from accumulated HPACK data. Called
// only by the reader goroutine.
func (c *pbsH2Conn) decodeStatus(buf *bytes.Buffer) int {
	headers, _ := c.dec.DecodeFull(buf.Bytes())
	buf.Reset()
	for _, hf := range headers {
		if hf.Name == ":status" {
			s, _ := strconv.Atoi(hf.Value)
			return s
		}
	}
	return 0
}

// do sends a request and returns the parsed JSON "data" field. It is a
// synchronous wrapper over sendRequest+Wait for control RPCs (small bodies).
func (c *pbsH2Conn) do(method, path string, params url.Values, body []byte, contentType string) (json.RawMessage, error) {
	st, err := c.sendRequest(method, path, params, body, contentType)
	if err != nil {
		return nil, err
	}
	data, err := st.Wait()
	st.release()
	return data, err
}

// doRaw sends a body-less request and returns the raw response body (for binary
// endpoints like "previous" that return raw index data).
func (c *pbsH2Conn) doRaw(method, path string, params url.Values) ([]byte, error) {
	st, err := c.sendRequest(method, path, params, nil, "")
	if err != nil {
		return nil, err
	}
	return st.WaitRaw()
}

// Wait blocks until the response is complete and returns the parsed JSON
// "data" field (or an error).
func (s *stream) Wait() (json.RawMessage, error) {
	<-s.done
	if s.err != nil {
		return nil, s.err
	}
	if s.status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", s.status, s.dataBuf.String())
	}
	if s.dataBuf.Len() == 0 {
		return nil, nil
	}
	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(s.dataBuf.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("parse response JSON: %w", err)
	}
	return result.Data, nil
}

// WaitRaw blocks until the response is complete and returns the raw body.
func (s *stream) WaitRaw() ([]byte, error) {
	<-s.done
	if s.err != nil {
		return nil, s.err
	}
	if s.status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", s.status, s.dataBuf.String())
	}
	return s.dataBuf.Bytes(), nil
}

func (c *pbsH2Conn) close() error {
	c.fail(errConnClosed)
	return c.conn.Close()
}

// streamRecvInitial is the per-stream receive window we grant the peer for
// response data. Response bodies are small (JSON) except for previous-index
// downloads, which the reader replenishes via WINDOW_UPDATE.
const streamRecvInitial = 1 << 20 // 1 MiB

var errConnClosed = fmt.Errorf("h2: connection closed")
