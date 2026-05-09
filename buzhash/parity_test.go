package buzhash_test

import (
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
)

// TestChunkerConfigParityWithPBS validates that our chunker configuration
// matches PBS's ChunkerImpl::new() parameters exactly.
func TestChunkerConfigParityWithPBS(t *testing.T) {
	tests := []struct {
		name          string
		avgChunkSize  int
		wantMin       int
		wantMax       int
		wantMask      uint32
		wantThreshold uint32
	}{
		{
			// PBS test uses: ChunkerImpl::new(64 * 1024)
			name:          "64KB (PBS test default)",
			avgChunkSize:  64 * 1024,
			wantMin:       16 * 1024,
			wantMax:       256 * 1024,
			wantMask:      uint32(64*1024*2 - 1),
			wantThreshold: uint32(64*1024*2 - 3),
		},
		{
			// PBS default: 4MB
			name:          "4MB (PBS default)",
			avgChunkSize:  4 * 1024 * 1024,
			wantMin:       1 * 1024 * 1024,
			wantMax:       16 * 1024 * 1024,
			wantMask:      uint32(4*1024*1024*2 - 1),
			wantThreshold: uint32(4*1024*1024*2 - 3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buzhash.NewConfig(tt.avgChunkSize)
			if err != nil {
				t.Fatalf("NewConfig(%d): %v", tt.avgChunkSize, err)
			}

			if cfg.MinChunkSize != tt.wantMin {
				t.Errorf("MinChunkSize = %d, want %d", cfg.MinChunkSize, tt.wantMin)
			}
			if cfg.MaxChunkSize != tt.wantMax {
				t.Errorf("MaxChunkSize = %d, want %d", cfg.MaxChunkSize, tt.wantMax)
			}
			if cfg.Mask != tt.wantMask {
				t.Errorf("Mask = %d, want %d (PBS break_test_mask)", cfg.Mask, tt.wantMask)
			}
			if cfg.Threshold != tt.wantThreshold {
				t.Errorf("Threshold = %d, want %d (PBS break_test_minimum)", cfg.Threshold, tt.wantThreshold)
			}
		})
	}
}
