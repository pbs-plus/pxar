//go:build !linux

package backupproxy

import (
	"io"
	"os"
)

func cloneFile(destination, source *os.File) error {
	_, err := io.Copy(destination, source)
	return err
}
