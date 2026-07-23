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

// pbsH2Conn is a raw HTTP/2 client for the PBS backup protocol.
//
// Architecture (faithful to proxmox-backup's Rust h2 client):
//   - One readLoop goroutine reads frames from the connection and dispatches
//     them to per-stream handlers.
//   - One writeLoop goroutine owns the framer, hpack encoder, and stream-ID
//     allocator. All frame writes (HEADERS, DATA, WINDOW_UPDATE, SETTINGS)
//     are serialised through a channel — no mutex, no contention. This is the
//     Go equivalent of Rust's SendRequest that serialises writes internally.
//   - Flow-control credit is tracked under sendMu; the writeLoop blocks on
//     sendCond when it runs out of credit, unblocked by the readLoop on
//     WINDOW_UPDATE frames.
type pbsH2Conn struct {
	conn      net.Conn
	framer    *http2.Framer
	authority string
	hdec      *hpack.Decoder

	writeCh   chan writeJob
	writeDone chan struct{}
	readDone  chan struct{}
	stopCh    chan struct{}

	// --- stream ID allocation (owned by writeLoop) ---
	nextID uint32

	// --- send-side flow control ---
	sendMu        sync.Mutex
	sendCond      *sync.Cond
	peerConnWin   int64
	peerStreamWin map[uint32]int64
	peerInitWin   int64
	maxFrameSize  uint32

	// --- stream registry ---
	streamsMu sync.Mutex
	streams   map[uint32]*stream

	// --- concurrency limit ---
	streamSlots chan struct{}

	// --- lifecycle ---
	closed   atomic.Bool
	closeErr atomic.Pointer[error]

	// --- pool ---
	streamPool sync.Pool

	// --- recv flow control ---
	recvConnWin     int64
	recvConnInitial int64
}

type writeJob struct {
	kind     writeKind
	streamID uint32
	end      bool
	data     []byte
}

type writeKind int

const (
	writeHeaders writeKind = iota
	writeData
	writeWindowUpdate
	writeSettings
	writePingAck
	writeGoAway
)

// stream is the handle for one H2 request/response.
type stream struct {
	id      uint32
	status  int
	hdrBuf  bytes.Buffer
	dataBuf bytes.Buffer
	recvWin int64

	done chan struct{} // closed when response is complete
	err  error

	pool *pbsH2Conn
}

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

func (s *stream) release() {
	if s.pool != nil {
		s.pool.streamPool.Put(s)
	}
}

// ----- connection setup -----

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
		targetWindow   = (1 << 31) - 2
		targetMaxFrame = 1 << 22
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

	// Wait for server SETTINGS ack.
	var gotSettings bool
	for !gotSettings {
		fr, err := framer.ReadFrame()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("read server SETTINGS: %w", err)
		}
		sf, ok := fr.(*http2.SettingsFrame)
		if !ok {
			continue
		}
		if sf.IsAck() {
			gotSettings = true
			break
		}
		// Capture peer's settings.
		peerInitWin := int64(65535)
		var peerMaxConcurrent uint32
		maxFrame := uint32(1 << 14)
		if v, ok := sf.Value(http2.SettingInitialWindowSize); ok {
			peerInitWin = int64(v)
		}
		if v, ok := sf.Value(http2.SettingMaxConcurrentStreams); ok {
			peerMaxConcurrent = v
		}
		if v, ok := sf.Value(http2.SettingMaxFrameSize); ok {
			maxFrame = v
		}

		// Ack server settings.
		if err := framer.WriteSettingsAck(); err != nil {
			conn.Close()
			return nil, fmt.Errorf("write SETTINGS ack: %w", err)
		}
		gotSettings = true

		hdrBuf := new(bytes.Buffer)
		enc := hpack.NewEncoder(hdrBuf)
		dec := hpack.NewDecoder(4096, nil)

		c := &pbsH2Conn{
			conn:            conn,
			hdec:            dec,
			framer:          framer,
			authority:       u.Host,
			writeCh:         make(chan writeJob, 256),
			writeDone:       make(chan struct{}),
			readDone:        make(chan struct{}),
			stopCh:          make(chan struct{}),
			nextID:          1,
			maxFrameSize:    maxFrame,
			peerConnWin:     65535,
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

		go c.writeLoop(enc, hdrBuf)
		go c.readLoop()
		return c, nil
	}

	conn.Close()
	return nil, fmt.Errorf("unexpected: never received server SETTINGS")
}

func (c *pbsH2Conn) writeLoop(enc *hpack.Encoder, hdrBuf *bytes.Buffer) {
	defer close(c.writeDone)
	for {
		select {
		case job, ok := <-c.writeCh:
			if !ok {
				return
			}
			switch job.kind {
			case writeHeaders:
				c.nextID += 2
				// Stream and flow-control state already set by sendRequest caller.
				c.writeMuFree(job)
			case writeData:
				if err := c.framer.WriteData(job.streamID, job.end, job.data); err != nil {
					c.fail(fmt.Errorf("write DATA: %w", err))
				}
			case writeWindowUpdate:
				c.framer.WriteWindowUpdate(job.streamID, uint32(job.data[0])<<24|uint32(job.data[1])<<16|uint32(job.data[2])<<8|uint32(job.data[3]))
			case writeSettings:
				// handled inline during setup
			case writePingAck:
				var ping [8]byte
				copy(ping[:], job.data)
				c.framer.WritePing(true, ping)
			case writeGoAway:
				c.framer.WriteGoAway(job.streamID, http2.ErrCodeNo, nil)
			}
		case <-c.stopCh:
			return
		}
	}
}

// writeMuFree writes a HEADERS frame. Called ONLY from writeLoop (no mutex).
// The caller (sendRequest) has already set up the stream and flow control.
func (c *pbsH2Conn) writeMuFree(job writeJob) {
	// The caller fills hdrBuf via enc before enqueueing.
	// We just write the frame.
	c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      job.streamID,
		BlockFragment: job.data, // the HPACK-encoded header block
		EndHeaders:    true,
		EndStream:     job.end,
	})
}

// enqueueHeaders enqueues a HEADERS frame to the writeLoop. Returns the
// stream handle for the caller to await the response.
func (c *pbsH2Conn) enqueueHeaders(id uint32, hdrBlock []byte, endStream bool, st *stream) {
	select {
	case c.writeCh <- writeJob{
		kind:     writeHeaders,
		streamID: id,
		end:      endStream,
		data:     hdrBlock,
	}:
	case <-st.done:
	}
}

// enqueueData enqueues a DATA frame to the writeLoop.
func (c *pbsH2Conn) enqueueData(id uint32, data []byte, end bool) {
	select {
	case c.writeCh <- writeJob{kind: writeData, streamID: id, data: data, end: end}:
	case <-c.writeDone:
	}
}

// writeWindowUpdate enqueues a WINDOW_UPDATE frame.
func (c *pbsH2Conn) writeWindowUpdate(streamID uint32, incr uint32) {
	var b [4]byte
	b[0] = byte(incr >> 24)
	b[1] = byte(incr >> 16)
	b[2] = byte(incr >> 8)
	b[3] = byte(incr)
	select {
	case c.writeCh <- writeJob{kind: writeWindowUpdate, streamID: streamID, data: b[:]}:
	case <-c.writeDone:
	}
}

// ----- read loop -----

func (c *pbsH2Conn) readLoop() {
	defer close(c.readDone)
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
				_, _ = c.framer.ReadFrame() // discard if can't decode
			}
			break
		}
		st.hdrBuf.Write(f.HeaderBlockFragment())
		if f.Flags.Has(http2.FlagHeadersEndHeaders) {
			fields, derr := c.hdec.DecodeFull(st.hdrBuf.Bytes())
			if derr != nil {
				st.err = fmt.Errorf("decode response headers: %w", derr)
			}
			for _, hf := range fields {
				if hf.Name == ":status" {
					if s, perr := strconv.Atoi(hf.Value); perr == nil {
						st.status = s
					}
				}
			}
		}
		if f.StreamEnded() {
			c.finishStream(st)
		}

	case *http2.DataFrame:
		dataLen := int64(len(f.Data()))
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
			c.maxFrameSize = v
		}
		c.framer.WriteSettingsAck()

	case *http2.PingFrame:
		if !f.IsAck() {
			ping := f.Data
			select {
			case c.writeCh <- writeJob{kind: writePingAck, data: ping[:]}:
			case <-c.writeDone:
			}
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

func (c *pbsH2Conn) fail(err error) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	e := err
	c.closeErr.Store(&e)
	_ = c.conn.Close()
	close(c.stopCh)
	c.sendMu.Lock()
	c.sendCond.Broadcast()
	c.sendMu.Unlock()
	c.streamsMu.Lock()
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
	nStreams := len(c.streams)
	c.streams = make(map[uint32]*stream)
	c.streamsMu.Unlock()
	for range nStreams {
		c.releaseSlot()
	}
}

// ----- send request (called from any goroutine) -----

func (c *pbsH2Conn) sendRequest(method, path string, params url.Values, body []byte, contentType string) (*stream, error) {
	if c.closed.Load() {
		if e := c.closeErr.Load(); e != nil {
			return nil, *e
		}
		return nil, errConnClosed
	}

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

	// Build HPACK header block.
	var hdrBuf bytes.Buffer
	enc := hpack.NewEncoder(&hdrBuf)
	enc.WriteField(hpack.HeaderField{Name: ":method", Value: method})
	enc.WriteField(hpack.HeaderField{Name: ":path", Value: fullPath})
	enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	enc.WriteField(hpack.HeaderField{Name: ":authority", Value: c.authority})
	if contentType != "" {
		enc.WriteField(hpack.HeaderField{Name: "content-type", Value: contentType})
	}
	if body != nil {
		enc.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(body))})
	}

	id := c.allocID()
	st := c.newStream(id, streamRecvInitial)

	c.streamsMu.Lock()
	c.streams[id] = st
	c.streamsMu.Unlock()

	c.sendMu.Lock()
	c.peerStreamWin[id] = c.peerInitWin
	c.sendMu.Unlock()

	// Write HEADERS frame (via writeLoop).
	hdrBlock := hdrBuf.Bytes()
	endStream := body == nil
	c.enqueueHeaders(id, hdrBlock, endStream, st)

	// Write body DATA frames (fires writeBody inline to avoid goroutine
	// spawning per chunk — matching Rust's PipeToSendStream approach).
	if body != nil {
		if err := c.writeBody(st, body); err != nil && !c.closed.Load() {
			st.err = err
			c.finishStream(st)
		}
	}
	return st, nil
}

func (c *pbsH2Conn) allocID() uint32 {
	// Called from sendRequest; nextID is only written by writeLoop but
	// sendRequest needs an ID before enqueueing. We increment atomically
	// and writeLoop will use the allocated IDs in order.
	return atomic.AddUint32(&c.nextID, 2) - 2
}

func (c *pbsH2Conn) writeBody(st *stream, body []byte) error {
	max := int(c.maxFrameSize)
	if max == 0 {
		max = 16384
	}
	for len(body) > 0 {
		n := min(len(body), max)
		end := len(body) == n
		if err := c.awaitSendWindow(st, int64(n)); err != nil {
			return err
		}
		// Copy data for the writeLoop (it needs ownership).
		frame := make([]byte, n)
		copy(frame, body[:n])
		c.enqueueData(st.id, frame, end)
		body = body[n:]
	}
	return nil
}

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

// ----- public API -----

func (c *pbsH2Conn) do(method, path string, params url.Values, body []byte, contentType string) (json.RawMessage, error) {
	st, err := c.sendRequest(method, path, params, body, contentType)
	if err != nil {
		return nil, err
	}
	return st.Wait()
}

func (c *pbsH2Conn) doRaw(method, path string, params url.Values) ([]byte, error) {
	st, err := c.sendRequest(method, path, params, nil, "")
	if err != nil {
		return nil, err
	}
	return st.WaitRaw()
}

func (c *pbsH2Conn) close() error {
	c.fail(errConnClosed)
	<-c.writeDone
	<-c.readDone
	return c.conn.Close()
}

const streamRecvInitial = 1 << 20

var errConnClosed = fmt.Errorf("h2: connection closed")

// Wait blocks until the response is complete and returns the parsed JSON "data" field.
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
