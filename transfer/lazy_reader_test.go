package transfer_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
)

func TestReadSeekerReadAtConcurrent(t *testing.T) {
	// Build a chunked archive to get a ReadSeeker with real data.
	store, idxData := createChunkedArchive(t, map[string]string{
		"a.txt": string(make([]byte, 4096)), // large enough for multiple chunks
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	seeker := transfer.NewReadSeeker(idx, source, 4)

	// Read full content via sequential Read to get ground truth.
	seeker.Seek(0, io.SeekStart)
	groundTruth, err := io.ReadAll(seeker)
	if err != nil {
		t.Fatal(err)
	}

	// ReadAt from multiple goroutines at different offsets concurrently.
	const numGoroutines = 32
	errCh := make(chan error, numGoroutines)
	var wg sync.WaitGroup

	for i := range numGoroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			offset := int64(i * 64)
			if offset >= int64(len(groundTruth)) {
				return
			}
			buf := make([]byte, 128)
			n, err := seeker.ReadAt(buf, offset)
			if err != nil && err != io.EOF {
				errCh <- err
				return
			}
			if n == 0 && len(groundTruth) > int(offset) {
				errCh <- io.ErrUnexpectedEOF
				return
			}
			if n > 0 {
				end := min(offset+int64(n), int64(len(groundTruth)))
				if !bytes.Equal(buf[:n], groundTruth[offset:end]) {
					errCh <- io.ErrUnexpectedEOF
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent ReadAt failed: %v", err)
	}
}

func TestReadSeekerReadAtSeekIndependent(t *testing.T) {
	store, idxData := createChunkedArchive(t, map[string]string{
		"a.txt": string(make([]byte, 4096)),
	})
	source := datastore.NewChunkStoreSource(store)

	idx, err := datastore.ParseDynamicIndex(idxData)
	if err != nil {
		t.Fatal(err)
	}

	seeker := transfer.NewReadSeeker(idx, source, 0)

	// Read full content for ground truth.
	seeker.Seek(0, io.SeekStart)
	groundTruth, err := io.ReadAll(seeker)
	if err != nil {
		t.Fatal(err)
	}

	// Set position via Seek.
	seeker.Seek(100, io.SeekStart)

	// ReadAt at a different offset should not move the cursor.
	if len(groundTruth) < 600 {
		t.Fatal("ground truth too short")
	}
	var buf [10]byte
	n, err := seeker.ReadAt(buf[:], 500)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("ReadAt returned %d bytes, want 10", n)
	}
	if !bytes.Equal(buf[:], groundTruth[500:510]) {
		t.Fatal("ReadAt returned wrong data")
	}

	// Read should continue from offset 100, not 510.
	var buf2 [10]byte
	n, err = seeker.Read(buf2[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("Read returned %d bytes, want 10", n)
	}
	if !bytes.Equal(buf2[:], groundTruth[100:110]) {
		t.Fatal("Read returned wrong data — ReadAt corrupted the seek position")
	}
}
