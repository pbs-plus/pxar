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

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// pbsH2Conn is a raw HTTP/2 client for the PBS backup protocol.
type pbsH2Conn struct {
	conn              net.Conn
	framer            *http2.Framer
	enc               *hpack.Encoder
	dec               *hpack.Decoder
	hdrBuf            *bytes.Buffer
	authority         string
	nextID            uint32
	maxFrameSize      uint32
	connWindow        uint32
	connInitialWindow uint32
}

// dialPBSH2 establishes an H2 connection to PBS via HTTP/1.1 upgrade.
func dialPBSH2(ctx context.Context, rawURL, datastore, authToken string, cfg BackupConfig, skipTLS bool) (*pbsH2Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse PBS URL: %w", err)
	}

	host := u.Host
	if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
		host = host + ":8007"
	}

	// Build upgrade request path
	params := url.Values{}
	params.Set("store", datastore)
	params.Set("backup-type", cfg.BackupType.String())
	params.Set("backup-id", cfg.BackupID)
	params.Set("backup-time", strconv.FormatInt(cfg.BackupTime, 10))
	if cfg.Namespace != "" {
		params.Set("ns", cfg.Namespace)
	}
	upgradePath := u.Path + "/backup?" + params.Encode()

	// TLS dial (use http/1.1 ALPN for upgrade)
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

	// Send HTTP/1.1 upgrade request
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

	// Read 101 response
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

	// Write H2 client connection preface
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write H2 preface: %w", err)
	}

	framer := http2.NewFramer(conn, br)
	framer.SetMaxReadFrameSize(1 << 24) // 16MB

	const (
		targetWindow   = 1 << 30 // 1 GiB
		targetMaxFrame = 1 << 22 // 4 MiB
	)

	// Send client SETTINGS
	if err := framer.WriteSettings(
		http2.Setting{ID: http2.SettingInitialWindowSize, Val: targetWindow},
		http2.Setting{ID: http2.SettingMaxFrameSize, Val: targetMaxFrame},
	); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write SETTINGS: %w", err)
	}

	// Increase connection-level window (stream 0)
	if err := framer.WriteWindowUpdate(0, uint32(targetWindow-65535)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write connection WINDOW_UPDATE: %w", err)
	}

	// Read server SETTINGS and wait for our SETTINGS to be ACKed
	maxFrame := uint32(1 << 14) // default 16384
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
		if err := framer.WriteSettingsAck(); err != nil {
			conn.Close()
			return nil, fmt.Errorf("SETTINGS ACK: %w", err)
		}
		gotSettings = true
	}

	hdrBuf := new(bytes.Buffer)
	dec := hpack.NewDecoder(4096, nil)
	return &pbsH2Conn{
		conn:              conn,
		framer:            framer,
		enc:               hpack.NewEncoder(hdrBuf),
		dec:               dec,
		hdrBuf:            hdrBuf,
		authority:         u.Host,
		nextID:            1,
		maxFrameSize:      maxFrame,
		connWindow:        uint32(targetWindow),
		connInitialWindow: uint32(targetWindow),
	}, nil
}

// allocID returns the next available stream ID (client-initiated, odd).
func (c *pbsH2Conn) allocID() uint32 {
	id := c.nextID
	c.nextID += 2
	return id
}

// do sends an H2 request and reads the JSON response.
// The response is expected to be {"data": ...} and the inner "data" value is returned.
func (c *pbsH2Conn) do(method, path string, params url.Values, body []byte, contentType string) (json.RawMessage, error) {
	streamID := c.allocID()

	// Build full path with query params
	fullPath := path
	if len(params) > 0 {
		fullPath += "?" + params.Encode()
	}

	// Encode HPACK headers
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

	// Write HEADERS frame
	if err := c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: c.hdrBuf.Bytes(),
		EndHeaders:    true,
		EndStream:     body == nil,
	}); err != nil {
		return nil, fmt.Errorf("write HEADERS: %w", err)
	}

	// Write DATA frames if there's a body
	if body != nil {
		if err := c.writeDataFrames(streamID, body); err != nil {
			return nil, fmt.Errorf("write DATA: %w", err)
		}
	}

	return c.readResponse(streamID)
}

// writeDataFrames splits data into frames respecting maxFrameSize.
func (c *pbsH2Conn) writeDataFrames(streamID uint32, data []byte) error {
	max := int(c.maxFrameSize)
	for len(data) > 0 {
		n := min(len(data), max)
		end := len(data) == n
		if err := c.framer.WriteData(streamID, end, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// decodeStatus extracts the :status value from accumulated HPACK header data.
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

// readResponse reads H2 frames until the response for streamID is complete.
// Manages connection-level HTTP/2 flow control by sending WINDOW_UPDATE frames
// when the receive window drops below half, preventing deadlocks on large responses.
func (c *pbsH2Conn) readResponse(streamID uint32) (json.RawMessage, error) {
	var (
		status      int
		dataBuf     bytes.Buffer
		gotEnd      bool
		hdrBuf      bytes.Buffer
		otherHdrBuf bytes.Buffer // To keep HPACK state for other streams
	)

	const initialStreamWindow = 65535
	streamWin := int32(initialStreamWindow)
	streamThreshold := int32(initialStreamWindow / 2)
	connThreshold := int32(c.connInitialWindow / 2)

	for !gotEnd {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			return nil, fmt.Errorf("read frame: %w", err)
		}

		switch f := frame.(type) {
		case *http2.HeadersFrame:
			if f.StreamID != streamID {
				if f.Flags.Has(http2.FlagHeadersEndHeaders) {
					_, _ = c.dec.DecodeFull(f.HeaderBlockFragment())
				} else {
					otherHdrBuf.Reset()
					otherHdrBuf.Write(f.HeaderBlockFragment())
				}
				continue
			}
			hdrBuf.Write(f.HeaderBlockFragment())
			if f.Flags.Has(http2.FlagHeadersEndHeaders) {
				status = c.decodeStatus(&hdrBuf)
			}
			if f.StreamEnded() {
				gotEnd = true
			}

		case *http2.ContinuationFrame:
			if f.StreamID != streamID {
				otherHdrBuf.Write(f.HeaderBlockFragment())
				if f.Flags.Has(http2.FlagHeadersEndHeaders) {
					_, _ = c.dec.DecodeFull(otherHdrBuf.Bytes())
					otherHdrBuf.Reset()
				}
				continue
			}
			hdrBuf.Write(f.HeaderBlockFragment())
			if f.Flags.Has(http2.FlagHeadersEndHeaders) {
				status = c.decodeStatus(&hdrBuf)
			}

		case *http2.DataFrame:
			dataLen := int32(len(f.Data()))

			// Connection-level flow control
			c.connWindow -= uint32(dataLen)
			if c.connWindow < uint32(connThreshold) {
				incr := c.connInitialWindow - c.connWindow
				if incr > 0 {
					_ = c.framer.WriteWindowUpdate(0, incr)
					c.connWindow += incr
				}
			}

			if f.StreamID != streamID {
				continue
			}

			// Stream-level flow control
			streamWin -= dataLen
			if streamWin < streamThreshold {
				incr := uint32(initialStreamWindow - streamWin)
				_ = c.framer.WriteWindowUpdate(streamID, incr)
				streamWin += int32(incr)
			}

			dataBuf.Write(f.Data())
			if f.StreamEnded() {
				gotEnd = true
			}

		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = c.framer.WriteSettingsAck()
			}

		case *http2.PingFrame:
			if !f.IsAck() {
				_ = c.framer.WritePing(true, f.Data)
			}

		case *http2.RSTStreamFrame:
			if f.StreamID == streamID {
				return nil, fmt.Errorf("stream reset: error code %d", f.ErrCode)
			}

		case *http2.GoAwayFrame:
			return nil, fmt.Errorf("server GOAWAY: error code %d", f.ErrCode)
		}
	}

	if status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", status, dataBuf.String())
	}

	if dataBuf.Len() == 0 {
		return nil, nil
	}

	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(dataBuf.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("parse response JSON: %w", err)
	}

	return result.Data, nil
}

// doRaw sends a request and returns the raw response body without JSON parsing.
// Used for binary endpoints like "previous" that return raw index data.
func (c *pbsH2Conn) doRaw(method, path string, params url.Values) ([]byte, error) {
	streamID := c.allocID()

	fullPath := path
	if len(params) > 0 {
		fullPath += "?" + params.Encode()
	}

	c.hdrBuf.Reset()
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":method", Value: method})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":path", Value: fullPath})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	_ = c.enc.WriteField(hpack.HeaderField{Name: ":authority", Value: c.authority})

	if err := c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: c.hdrBuf.Bytes(),
		EndHeaders:    true,
		EndStream:     true,
	}); err != nil {
		return nil, fmt.Errorf("write HEADERS: %w", err)
	}

	return c.readRawResponse(streamID)
}

// readRawResponse reads the response and returns raw bytes without JSON parsing.
// Manages per-stream flow control for large responses (e.g. previous DIDX downloads).
func (c *pbsH2Conn) readRawResponse(streamID uint32) ([]byte, error) {
	var (
		status  int
		dataBuf bytes.Buffer
		gotEnd  bool
		hdrBuf  bytes.Buffer
	)

	// Default H2 initial stream window is 65535. Track consumption and replenish
	// below half to avoid stalling on large responses.
	const initialStreamWindow = 65535
	streamWin := int32(initialStreamWindow)
	streamThreshold := int32(initialStreamWindow / 2)

	for !gotEnd {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			return nil, fmt.Errorf("read frame: %w", err)
		}

		switch f := frame.(type) {
		case *http2.HeadersFrame:
			if f.StreamID != streamID {
				continue
			}
			hdrBuf.Write(f.HeaderBlockFragment())
			if f.Flags.Has(http2.FlagHeadersEndHeaders) {
				status = c.decodeStatus(&hdrBuf)
			}
			if f.StreamEnded() {
				gotEnd = true
			}

		case *http2.ContinuationFrame:
			if f.StreamID != streamID {
				continue
			}
			hdrBuf.Write(f.HeaderBlockFragment())
			if f.Flags.Has(http2.FlagHeadersEndHeaders) {
				status = c.decodeStatus(&hdrBuf)
			}

		case *http2.DataFrame:
			if f.StreamID != streamID {
				continue
			}
			dataBuf.Write(f.Data())
			streamWin -= int32(len(f.Data()))
			if streamWin < streamThreshold {
				incr := uint32(initialStreamWindow - streamWin)
				_ = c.framer.WriteWindowUpdate(streamID, incr)
				streamWin += int32(incr)
			}
			if f.StreamEnded() {
				gotEnd = true
			}

		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = c.framer.WriteSettingsAck()
			}

		case *http2.PingFrame:
			if !f.IsAck() {
				_ = c.framer.WritePing(true, f.Data)
			}

		case *http2.RSTStreamFrame:
			if f.StreamID == streamID {
				return nil, fmt.Errorf("stream reset: error code %d", f.ErrCode)
			}

		case *http2.GoAwayFrame:
			return nil, fmt.Errorf("server GOAWAY: error code %d", f.ErrCode)
		}
	}

	if status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", status, dataBuf.String())
	}

	return dataBuf.Bytes(), nil
}

func (c *pbsH2Conn) close() error {
	return c.conn.Close()
}
