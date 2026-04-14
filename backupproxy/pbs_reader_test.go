package backupproxy

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// TestFlowControlReplenishment verifies that the client sends WINDOW_UPDATE
// frames when the flow control window drops below half the initial size.
func TestFlowControlReplenishment(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	const (
		initialWinSize = 1000
		threshold      = initialWinSize / 2
		sendSize       = 600 // > threshold
	)

	// Setup pbsReaderConn manually to bypass dialPBSReaderH2 complexity
	framer := http2.NewFramer(clientConn, clientConn)
	c := &pbsReaderConn{
		conn:              clientConn,
		framer:            framer,
		enc:               hpack.NewEncoder(new(bytes.Buffer)),
		dec:               hpack.NewDecoder(4096, nil),
		hdrBuf:            new(bytes.Buffer),
		nextID:            1,
		maxFrameSize:      16384,
		authority:         "localhost",
		connWindow:        initialWinSize,
		connInitialWindow: initialWinSize,
		streamWindow:      initialWinSize,
	}

	errChan := make(chan error, 1)
	go func() {
		serverFramer := http2.NewFramer(serverConn, serverConn)

		// 1. Send HEADERS for stream 1
		var hb bytes.Buffer
		henc := hpack.NewEncoder(&hb)
		henc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		if err := serverFramer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      1,
			BlockFragment: hb.Bytes(),
			EndHeaders:    true,
		}); err != nil {
			errChan <- fmt.Errorf("server write headers: %v", err)
			return
		}

		// 2. Send DATA for stream 1 (600 bytes)
		data := make([]byte, sendSize)
		if err := serverFramer.WriteData(1, true, data); err != nil {
			errChan <- fmt.Errorf("server write data: %v", err)
			return
		}

		// 3. Expect WINDOW_UPDATE for connection (stream 0) and stream 1
		gotConnUpdate := false
		gotStreamUpdate := false
		for i := 0; i < 2; i++ {
			frame, err := serverFramer.ReadFrame()
			if err != nil {
				errChan <- fmt.Errorf("server read frame: %v", err)
				return
			}
			wu, ok := frame.(*http2.WindowUpdateFrame)
			if !ok {
				errChan <- fmt.Errorf("expected WINDOW_UPDATE, got %T", frame)
				return
			}
			if wu.StreamID == 0 {
				gotConnUpdate = true
			} else if wu.StreamID == 1 {
				gotStreamUpdate = true
			}
			if wu.Increment != sendSize {
				errChan <- fmt.Errorf("expected increment %d, got %d", sendSize, wu.Increment)
				return
			}
		}

		if !gotConnUpdate || !gotStreamUpdate {
			errChan <- fmt.Errorf("did not get both conn and stream updates")
			return
		}

		errChan <- nil
	}()

	// The client call that triggers reading frames
	_, err := c.readBinaryResponse(1)
	if err != nil {
		t.Fatalf("readBinaryResponse: %v", err)
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

// TestHPACKConsistency verifies that headers for other streams are correctly
// decoded so the HPACK dynamic table stays consistent.
func TestHPACKConsistency(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Setup pbsReaderConn
	framer := http2.NewFramer(clientConn, clientConn)
	c := &pbsReaderConn{
		conn:              clientConn,
		framer:            framer,
		enc:               hpack.NewEncoder(new(bytes.Buffer)),
		dec:               hpack.NewDecoder(4096, nil),
		hdrBuf:            new(bytes.Buffer),
		nextID:            1,
		maxFrameSize:      16384,
		authority:         "localhost",
		connWindow:        65535,
		connInitialWindow: 65535,
		streamWindow:      65535,
	}

	errChan := make(chan error, 1)
	go func() {
		serverFramer := http2.NewFramer(serverConn, serverConn)
		var hb bytes.Buffer
		henc := hpack.NewEncoder(&hb)

		// 1. Send HEADERS for a DIFFERENT stream (ID 3)
		// We add a field to the dynamic table.
		hb.Reset()
		henc.WriteField(hpack.HeaderField{Name: "x-custom", Value: "value-for-stream-3"})
		if err := serverFramer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      3,
			BlockFragment: hb.Bytes(),
			EndHeaders:    true,
		}); err != nil {
			errChan <- err
			return
		}

		// 2. Send HEADERS for the TARGET stream (ID 1)
		hb.Reset()
		henc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		if err := serverFramer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      1,
			BlockFragment: hb.Bytes(),
			EndHeaders:    true,
			EndStream:     true,
		}); err != nil {
			errChan <- err
			return
		}

		errChan <- nil
	}()

	_, err := c.readBinaryResponse(1)
	if err != nil {
		t.Fatalf("first readBinaryResponse: %v", err)
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}

	// Now check if HPACK state is consistent by performing another request.
	go func() {
		serverFramer := http2.NewFramer(serverConn, serverConn)
		var hb bytes.Buffer
		henc := hpack.NewEncoder(&hb)

		hb.Reset()
		henc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		serverFramer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      5,
			BlockFragment: hb.Bytes(),
			EndHeaders:    true,
			EndStream:     true,
		})
	}()

	_, err = c.readBinaryResponse(5)
	if err != nil {
		t.Fatalf("second readBinaryResponse: %v", err)
	}
}
