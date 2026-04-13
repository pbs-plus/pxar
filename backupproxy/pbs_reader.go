package backupproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pxar/datastore"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// PBSReader provides read access to a PBS datastore via the reader protocol.
type PBSReader struct {
	config     PBSConfig
	conn       *pbsReaderConn
	backupType string
	backupID   string
	backupTime int64
}

// NewPBSReader creates a new PBS reader for the given backup snapshot.
func NewPBSReader(config PBSConfig, backupType, backupID string, backupTime int64) *PBSReader {
	return &PBSReader{
		config:     config,
		backupType: backupType,
		backupID:   backupID,
		backupTime: backupTime,
	}
}

// Connect establishes the H2 reader connection to PBS.
func (r *PBSReader) Connect(ctx context.Context) error {
	conn, err := dialPBSReaderH2(ctx, r.config, r.backupType, r.backupID, r.backupTime)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}

// Close closes the reader connection.
func (r *PBSReader) Close() error {
	if r.conn != nil {
		return r.conn.close()
	}
	return nil
}

// DownloadFile downloads an index file (.didx, .fidx, .blob) from PBS.
func (r *PBSReader) DownloadFile(fileName string) ([]byte, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	params := url.Values{}
	params.Set("file-name", fileName)

	return r.conn.doBinary("GET", "download", params, nil, "")
}

// DownloadChunk downloads a chunk by its digest.
func (r *PBSReader) DownloadChunk(digest [32]byte) ([]byte, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	params := url.Values{}
	params.Set("digest", hex.EncodeToString(digest[:]))

	return r.conn.doBinary("GET", "chunk", params, nil, "")
}

// AsChunkSource returns a ChunkSource interface for the restorer.
func (r *PBSReader) AsChunkSource() datastore.ChunkSource {
	return &pbsChunkSource{reader: r}
}

// RestoreFile restores a complete file from the given didx index.
// This downloads all chunks and reconstructs the file content.
func (r *PBSReader) RestoreFile(idx *datastore.DynamicIndexReader, w io.Writer) error {
	restorer := datastore.NewRestorer(r.AsChunkSource())
	return restorer.RestoreFile(idx, w)
}

// RestoreFileRange restores a specific byte range from a file.
// Useful for partial reads without downloading the entire file.
func (r *PBSReader) RestoreFileRange(idx *datastore.DynamicIndexReader, offset, length uint64, w io.Writer) error {
	restorer := datastore.NewRestorer(r.AsChunkSource())
	return restorer.RestoreRange(idx, offset, length, w)
}

// pbsChunkSource implements datastore.ChunkSource for PBS.
type pbsChunkSource struct {
	reader *PBSReader
}

func (s *pbsChunkSource) GetChunk(digest [32]byte) ([]byte, error) {
	return s.reader.DownloadChunk(digest)
}

// pbsReaderConn is a raw HTTP/2 client for the PBS reader protocol.
type pbsReaderConn struct {
	conn         net.Conn
	framer       *http2.Framer
	enc          *hpack.Encoder
	dec          *hpack.Decoder
	hdrBuf       *bytes.Buffer
	nextID       uint32
	maxFrameSize uint32
	authority    string
}

// dialPBSReaderH2 establishes an H2 reader connection to PBS.
func dialPBSReaderH2(ctx context.Context, cfg PBSConfig, backupType, backupID string, backupTime int64) (*pbsReaderConn, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse PBS URL: %w", err)
	}

	host := u.Host
	if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
		host = host + ":8007"
	}

	// Build reader upgrade path
	params := url.Values{}
	params.Set("store", cfg.Datastore)
	params.Set("backup-type", backupType)
	params.Set("backup-id", backupID)
	params.Set("backup-time", strconv.FormatInt(backupTime, 10))
	if cfg.Namespace != "" {
		params.Set("ns", cfg.Namespace)
	}

	upgradePath := u.Path + "/reader?" + params.Encode()

	// TLS dial
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.SkipTLSVerify,
		NextProtos:         []string{"http/1.1"},
	}
	var d tls.Dialer
	d.Config = tlsCfg
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, fmt.Errorf("TLS dial %s: %w", host, err)
	}

	// Send HTTP/1.1 upgrade request for reader protocol
	hostHeader := u.Host
	if _, _, splitErr := net.SplitHostPort(hostHeader); splitErr != nil {
		hostHeader = host
	}
	upgradeReq := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: proxmox-backup-reader-protocol-v1\r\n"+
			"Authorization: PBSAPIToken %s\r\n"+
			"\r\n",
		upgradePath, hostHeader, cfg.AuthToken,
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

	// Send client SETTINGS
	if err := framer.WriteSettings(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write SETTINGS: %w", err)
	}

	// Read server SETTINGS
	maxFrame := uint32(1 << 14) // default 16384
	gotSettings := false
	for !gotSettings {
		frame, err := framer.ReadFrame()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("read server SETTINGS: %w", err)
		}
		if sf, ok := frame.(*http2.SettingsFrame); ok && !sf.IsAck() {
			if v, ok := sf.Value(http2.SettingMaxFrameSize); ok {
				maxFrame = v
			}
			if err := framer.WriteSettingsAck(); err != nil {
				conn.Close()
				return nil, fmt.Errorf("SETTINGS ACK: %w", err)
			}
			gotSettings = true
		}
	}

	hdrBuf := new(bytes.Buffer)
	dec := hpack.NewDecoder(4096, nil)
	return &pbsReaderConn{
		conn:         conn,
		framer:       framer,
		enc:          hpack.NewEncoder(hdrBuf),
		dec:          dec,
		hdrBuf:       hdrBuf,
		nextID:       1,
		maxFrameSize: maxFrame,
		authority:    u.Host,
	}, nil
}

// allocID returns the next available stream ID.
func (c *pbsReaderConn) allocID() uint32 {
	id := c.nextID
	c.nextID += 2
	return id
}

// doBinary sends an H2 request and returns the raw binary response (for chunks/files).
func (c *pbsReaderConn) doBinary(method, path string, params url.Values, body []byte, contentType string) ([]byte, error) {
	streamID := c.allocID()

	// Ensure path starts with "/" as required by HTTP/2
	fullPath := path
	if !strings.HasPrefix(fullPath, "/") {
		fullPath = "/" + fullPath
	}
	if len(params) > 0 {
		fullPath += "?" + params.Encode()
	}

	// Encode HPACK headers
	c.hdrBuf.Reset()
	c.enc.WriteField(hpack.HeaderField{Name: ":method", Value: method})
	c.enc.WriteField(hpack.HeaderField{Name: ":path", Value: fullPath})
	c.enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	c.enc.WriteField(hpack.HeaderField{Name: ":authority", Value: c.authority})
	if contentType != "" {
		c.enc.WriteField(hpack.HeaderField{Name: "content-type", Value: contentType})
	}
	if body != nil {
		c.enc.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(body))})
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

	return c.readBinaryResponse(streamID)
}

// writeDataFrames splits data into frames respecting maxFrameSize.
func (c *pbsReaderConn) writeDataFrames(streamID uint32, data []byte) error {
	max := int(c.maxFrameSize)
	for len(data) > 0 {
		n := len(data)
		if n > max {
			n = max
		}
		end := len(data) == n
		if err := c.framer.WriteData(streamID, end, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// decodeStatus extracts the :status value from accumulated HPACK header data.
func (c *pbsReaderConn) decodeStatus(buf *bytes.Buffer) int {
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

// readBinaryResponse reads H2 frames until the response is complete, returning raw bytes.
func (c *pbsReaderConn) readBinaryResponse(streamID uint32) ([]byte, error) {
	var (
		status     int
		dataBuf    bytes.Buffer
		gotEnd     bool
		hdrBuf     bytes.Buffer
		gotHeaders bool
	)

	// Set a deadline to prevent indefinite hanging
	if err := c.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	// Clear deadline when done
	defer c.conn.SetReadDeadline(time.Time{})

	for !gotEnd {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return nil, fmt.Errorf("timeout reading response for stream %d: %w", streamID, err)
			}
			return nil, fmt.Errorf("read frame: %w", err)
		}

		switch f := frame.(type) {
		case *http2.HeadersFrame:
			if f.StreamID != streamID {
				continue
			}
			gotHeaders = true
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
			if f.StreamEnded() {
				gotEnd = true
			}

		case *http2.SettingsFrame:
			if !f.IsAck() {
				c.framer.WriteSettingsAck()
			}

		case *http2.PingFrame:
			if !f.IsAck() {
				c.framer.WritePing(true, f.Data)
			}

		case *http2.WindowUpdateFrame:
			// Handle window updates globally

		case *http2.RSTStreamFrame:
			if f.StreamID == streamID {
				return nil, fmt.Errorf("stream reset: error code %d", f.ErrCode)
			}

		case *http2.GoAwayFrame:
			return nil, fmt.Errorf("server GOAWAY: error code %d", f.ErrCode)
		}
	}

	if !gotHeaders {
		return nil, fmt.Errorf("no headers received for stream %d", streamID)
	}

	if status >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", status, dataBuf.String())
	}

	return dataBuf.Bytes(), nil
}

// readJSONResponse reads H2 frames and parses JSON response.
func (c *pbsReaderConn) readJSONResponse(streamID uint32) (json.RawMessage, error) {
	data, err := c.readBinaryResponse(streamID)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("parse response JSON: %w", err)
	}

	return result.Data, nil
}

func (c *pbsReaderConn) close() error {
	return c.conn.Close()
}
