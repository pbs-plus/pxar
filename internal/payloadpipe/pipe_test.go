package payloadpipe

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestWakeInterruptsRead(t *testing.T) {
	pipe := New()
	result := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		n, err := pipe.Read(buf)
		if err == nil && n != 0 {
			err = errors.New("wake returned payload bytes")
		}
		result <- err
	}()

	pipe.Wake()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not wake")
	}
}

func TestPipeTransfersWithoutCopyingOwnershipEarly(t *testing.T) {
	pipe := New()
	writeDone := make(chan error, 1)
	data := []byte("payload")
	go func() {
		_, err := pipe.Write(data)
		writeDone <- err
	}()

	got, err := io.ReadAll(io.LimitReader(pipe, int64(len(data))))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("payload = %q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	pipe.CloseWithError(nil)
}
