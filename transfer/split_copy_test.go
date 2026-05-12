package transfer_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// TestSplitArchiveCopyRoundTrip mimics what pxar-mount commit does:
// 1. Create a source split archive
// 2. Read entries from it
// 3. Re-write them to a new split archive via SplitSessionArchiveWriter
// 4. Read back the new archive and verify
func TestSplitArchiveCopyRoundTrip(t *testing.T) {
	// --- Step 1: Create source archive in memory ---
	var srcMeta, srcPayload bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&srcMeta, &srcPayload, &rootMeta, nil)

	fileContent1 := []byte("hello world from source")
	fileMeta := pxar.FileMetadata(0o644).Build()
	_, _ = enc.AddFile(&fileMeta, "file1.txt", fileContent1)

	dirMetaVal := pxar.DirMetadata(0o755).Build()
	_ = enc.CreateDirectory("subdir", &dirMetaVal)

	nestedContent := []byte("nested file content")
	_, _ = enc.AddFile(&fileMeta, "nested.txt", nestedContent)

	_ = enc.Finish()

	symlinkMeta := pxar.SymlinkMetadata(0o777).Build()
	_ = enc.AddSymlink(&symlinkMeta, "link.txt", "file1.txt")

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Source meta: %d bytes, payload: %d bytes", srcMeta.Len(), srcPayload.Len())

	// --- Step 2: Read source archive ---
	srcReader := transfer.NewSplitFileArchiveReader(bytes.NewReader(srcMeta.Bytes()), bytes.NewReader(srcPayload.Bytes()))
	defer srcReader.Close()

	// --- Step 3: Re-write to new split archive via local store ---
	dir := t.TempDir()
	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}

	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "copy-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	dstWriter := transfer.NewSplitSessionArchiveWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")

	dstRootMeta := pxar.DirMetadata(0o755).Build()
	if err := dstWriter.Begin(&dstRootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	// Copy entries by walking the source - mimics walkOverlay
	root, err := srcReader.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}

	err = srcReader.ListDirectory(int64(root.ContentOffset), accessor.ListOption{}, func(entry *pxar.Entry) error {
		t.Logf("Copying entry: %s kind=%v size=%d", entry.Path, entry.Kind, entry.FileSize)

		if entry.IsDir() {
			dirMeta := pxar.Metadata{Stat: entry.Metadata.Stat}
			if err := dstWriter.BeginDirectory(entry.Path, &dirMeta); err != nil {
				return err
			}
			err := srcReader.ListDirectory(int64(entry.ContentOffset), accessor.ListOption{}, func(child *pxar.Entry) error {
				t.Logf("  Child: %s kind=%v size=%d", child.Path, child.Kind, child.FileSize)
				if child.IsRegularFile() {
					r, err := srcReader.ReadFileContentReader(child)
					if err != nil {
						return err
					}
					content, err := io.ReadAll(r)
					r.Close()
					if err != nil {
						return err
					}
					clone := *child
					return dstWriter.WriteEntry(&clone, content)
				}
				if child.IsSymlink() {
					clone := *child
					return dstWriter.WriteEntry(&clone, nil)
				}
				return nil
			})
			if err != nil {
				return err
			}
			return dstWriter.EndDirectory()
		}

		if entry.IsRegularFile() {
			r2, err := srcReader.ReadFileContentReader(entry)
			if err != nil {
				return err
			}
			content, err := io.ReadAll(r2)
			r2.Close()
			if err != nil {
				return err
			}
			clone := *entry
			return dstWriter.WriteEntry(&clone, content)
		}

		if entry.IsSymlink() {
			clone := *entry
			return dstWriter.WriteEntry(&clone, nil)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := dstWriter.Finish(); err != nil {
		t.Fatal(err)
	}

	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}

	// --- Step 4: Read back and verify ---
	metaData, err := os.ReadFile(filepath.Join(dir, "root.mpxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	payloadData, err := os.ReadFile(filepath.Join(dir, "root.ppxar.didx"))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Dest meta didx: %d bytes, payload didx: %d bytes", len(metaData), len(payloadData))

	store, _ := datastore.NewChunkStore(dir)
	source := datastore.NewChunkStoreSource(store)

	reader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		t.Fatalf("NewSplitArchiveReader: %v", err)
	}
	defer reader.Close()

	e1, err := reader.Lookup("/file1.txt")
	if err != nil {
		t.Fatalf("Lookup /file1.txt: %v", err)
	}
	c1R, err := reader.ReadFileContentReader(e1)
	if err != nil {
		t.Fatalf("ReadFileContent file1: %v", err)
	}
	defer c1R.Close()
	c1, err := io.ReadAll(c1R)
	if err != nil {
		t.Fatalf("read file1: %v", err)
	}
	if string(c1) != string(fileContent1) {
		t.Errorf("file1 content = %q, want %q", string(c1), string(fileContent1))
	}

	e2, err := reader.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatalf("Lookup /subdir/nested.txt: %v", err)
	}
	c2R, err := reader.ReadFileContentReader(e2)
	if err != nil {
		t.Fatalf("ReadFileContent nested: %v", err)
	}
	defer c2R.Close()
	c2, err := io.ReadAll(c2R)
	if err != nil {
		t.Fatalf("read nested: %v", err)
	}
	if string(c2) != string(nestedContent) {
		t.Errorf("nested content = %q, want %q", string(c2), string(nestedContent))
	}

	e3, err := reader.Lookup("/link.txt")
	if err != nil {
		t.Fatalf("Lookup /link.txt: %v", err)
	}
	if !e3.IsSymlink() {
		t.Errorf("expected symlink, got %v", e3.Kind)
	}
	if e3.LinkTarget != "file1.txt" {
		t.Errorf("link target = %q, want %q", e3.LinkTarget, "file1.txt")
	}

	t.Log("Full copy roundtrip succeeded!")
}
