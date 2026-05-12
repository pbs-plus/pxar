// Package testutil provides test helpers for pxar packages.
package testutil

import (
	"io"
	"testing"
)

// ReadAll is a test helper that streams file content from a reader
// returned by ArchiveReader.ReadFileContentReader and fails the test
// on error. Usage:
//
//	r, err := reader.ReadFileContentReader(entry)
//	content := testutil.ReadAll(t, r, err)
func ReadAll(tb testing.TB, r io.ReadCloser, err error) []byte {
	if err != nil {
		tb.Fatal(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}
