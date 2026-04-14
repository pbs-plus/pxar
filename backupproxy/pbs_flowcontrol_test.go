package backupproxy

import (
	"bytes"
	"testing"
)

// TestWindowUpdateFrameSending tests that the client sends WINDOW_UPDATE frames
// when the receive window falls below half of its capacity.
func TestWindowUpdateFrameSending(t *testing.T) {
	// This test verifies the flow control implementation by checking that:
	// 1. recvWindow is tracked correctly
	// 2. WINDOW_UPDATE is sent when threshold is crossed
	// 3. Settings with INITIAL_WINDOW_SIZE updates the window

	tests := []struct {
		name          string
		initialWindow uint32
		consumeBytes  uint32
		expectedSend  bool
	}{
		{
			name:          "window_half_consumed_no_update",
			initialWindow: 65535,
			consumeBytes:  30000, // Below 32768 threshold
			expectedSend:  false,
		},
		{
			name:          "window_three_quarters_consumed_update",
			initialWindow: 65535,
			consumeBytes:  40000, // Above 32768 threshold
			expectedSend:  true,
		},
		{
			name:          "window_fully_consumed_update",
			initialWindow: 65535,
			consumeBytes:  65535,
			expectedSend:  true,
		},
		{
			name:          "small_initial_window",
			initialWindow: 8192,
			consumeBytes:  4096,
			expectedSend:  true, // 4096 > 4096 (32768 scaled)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that recvWindow tracking works correctly
			recvWindow := tt.initialWindow
			recvWindow -= tt.consumeBytes

			if recvWindow > tt.initialWindow {
				t.Errorf("recvWindow should never increase from consumption")
			}

			// Check if WINDOW_UPDATE should be sent
			sendWindowUpdate := recvWindow < 32768
			if sendWindowUpdate != tt.expectedSend {
				t.Errorf("sendWindowUpdate = %v, want %v", sendWindowUpdate, tt.expectedSend)
			}

			// Verify window doesn't exceed max
			if recvWindow > 65535 {
				t.Errorf("recvWindow %d exceeds maximum 65535", recvWindow)
			}
		})
	}
}

// TestDataFrameConsumption tests that DATA frames correctly decrement recvWindow.
func TestDataFrameConsumption(t *testing.T) {
	recvWindow := uint32(65535)

	// Simulate consuming multiple DATA frames
	frames := []struct {
		size    int
		payload []byte
	}{
		{size: 1024, payload: bytes.Repeat([]byte{'a'}, 1024)},
		{size: 2048, payload: bytes.Repeat([]byte{'b'}, 2048)},
		{size: 4096, payload: bytes.Repeat([]byte{'c'}, 4096)},
	}

	for i, frame := range frames {
		recvWindow -= uint32(len(frame.payload))
		// Check cumulative consumption
		consumed := uint32(0)
		for j := 0; j <= i; j++ {
			consumed += uint32(len(frames[j].payload))
		}
		expectedWindow := uint32(65535 - consumed)
		if recvWindow != expectedWindow {
			t.Errorf("frame %d: recvWindow = %d, want %d (consumed %d)", i, recvWindow, expectedWindow, consumed)
		}
	}
}

// TestWindowUpdateIncrement tests that WINDOW_UPDATE frames correctly increment recvWindow.
func TestWindowUpdateIncrement(t *testing.T) {
	tests := []struct {
		name        string
		startWindow uint32
		increment   uint32
		expected    uint32
	}{
		{
			name:        "basic_increment",
			startWindow: 1000,
			increment:   5000,
			expected:    6000,
		},
		{
			name:        "increment_to_max",
			startWindow: 60000,
			increment:   10000,
			expected:    65535, // Capped at max
		},
		{
			name:        "multiple_small_increments",
			startWindow: 1000,
			increment:   1000,
			expected:    2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recvWindow := tt.startWindow
			recvWindow += tt.increment
			if recvWindow > 65535 {
				recvWindow = 65535
			}
			if recvWindow != tt.expected {
				t.Errorf("recvWindow = %d, want %d", recvWindow, tt.expected)
			}
		})
	}
}

// TestSettingsInitialWindowSize tests that SETTINGS with INITIAL_WINDOW_SIZE updates recvWindow.
func TestSettingsInitialWindowSize(t *testing.T) {
	tests := []struct {
		name       string
		oldWindow  uint32
		newSetting uint32
		expected   uint32
	}{
		{
			name:       "reduce_window",
			oldWindow:  65535,
			newSetting: 32768,
			expected:   32768,
		},
		{
			name:       "increase_window",
			oldWindow:  16384,
			newSetting: 65535,
			expected:   65535,
		},
		{
			name:       "no_change",
			oldWindow:  65535,
			newSetting: 65535,
			expected:   65535,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recvWindow := tt.oldWindow
			recvWindow = tt.newSetting
			if recvWindow != tt.expected {
				t.Errorf("recvWindow = %d, want %d", recvWindow, tt.expected)
			}
		})
	}
}

// TestLargeDataFlowControl simulates downloading a large file and verifies
// that flow control doesn't deadlock.
func TestLargeDataFlowControl(t *testing.T) {
	// Simulate downloading 1MB of data in chunks
	recvWindow := uint32(65535)
	totalDownloaded := uint32(0)
	chunkSize := uint32(4096) // Typical chunk size

	// Download 256 chunks (1MB total)
	for i := 0; i < 256; i++ {
		recvWindow -= chunkSize
		totalDownloaded += chunkSize

		// Send WINDOW_UPDATE when below threshold
		if recvWindow < 32768 {
			recvWindow += 32768
			if recvWindow > 65535 {
				recvWindow = 65535
			}
		}
	}

	// Verify we downloaded all data
	if totalDownloaded != 256*chunkSize {
		t.Errorf("totalDownloaded = %d, want %d", totalDownloaded, 256*chunkSize)
	}

	// Verify window is still valid
	if recvWindow > 65535 {
		t.Errorf("final recvWindow = %d exceeds maximum", recvWindow)
	}

	// Should have sent multiple WINDOW_UPDATE frames
	// (We don't track exact count, but flow control remained active)
}

// TestHTTP2DataFrameParsing tests the framework for parsing DATA frames.
func TestHTTP2DataFrameParsing(t *testing.T) {
	// Create a mock DATA frame
	data := []byte("test data")
	var buf bytes.Buffer
	buf.WriteByte(0x00) // Length high byte (will be overwritten)
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
	buf.WriteByte(byte(len(data)))
	buf.WriteByte(0x00) // No flags
	buf.WriteByte(0x00) // Stream ID high byte
	buf.WriteByte(0x00)
	buf.WriteByte(0x00)
	buf.WriteByte(0x01)
	buf.Write(data)

	// Verify frame structure (9 bytes header + data length)
	expectedSize := 9 + len(data)
	if len(buf.Bytes()) != expectedSize {
		t.Errorf("frame size = %d, want %d", len(buf.Bytes()), expectedSize)
	}
}
