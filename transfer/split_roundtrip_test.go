package transfer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

func TestSplitArchiveFullRoundTrip(t *testing.T) {
	dir := t.TempDir()

	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}

	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "300",
	})
	if err != nil {
		t.Fatal(err)
	}

	writer := transfer.NewSplitSessionArchiveWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")

	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	fileMeta := pxar.FileMetadata(0o644).Build()
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "hello.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 11,
	}, []byte("hello world")); err != nil {
		t.Fatal(err)
	}

	dirMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.BeginDirectory("subdir", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "nested.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 14,
	}, []byte("nested content")); err != nil {
		t.Fatal(err)
	}
	if err := writer.EndDirectory(); err != nil {
		t.Fatal(err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}

	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}

	// Read back
	metaData, err := os.ReadFile(filepath.Join(dir, "root.mpxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	payloadData, err := os.ReadFile(filepath.Join(dir, "root.ppxar.didx"))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("meta didx: %d bytes, payload didx: %d bytes", len(metaData), len(payloadData))

	store, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	source := datastore.NewChunkStoreSource(store)

	reader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		t.Fatalf("NewSplitArchiveReader: %v", err)
	}
	defer reader.Close()

	entry, err := reader.Lookup("/hello.txt")
	if err != nil {
		t.Fatalf("Lookup /hello.txt: %v", err)
	}
	if !entry.IsRegularFile() {
		t.Errorf("expected regular file, got %v", entry.Kind)
	}
	if entry.FileSize != 11 {
		t.Errorf("file size = %d, want 11", entry.FileSize)
	}

	content, err := reader.ReadFileContent(entry)
	if err != nil {
		t.Fatalf("ReadFileContent: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", string(content), "hello world")
	}

	nested, err := reader.Lookup("/subdir/nested.txt")
	if err != nil {
		t.Fatalf("Lookup /subdir/nested.txt: %v", err)
	}
	nestedContent, err := reader.ReadFileContent(nested)
	if err != nil {
		t.Fatalf("ReadFileContent nested: %v", err)
	}
	if string(nestedContent) != "nested content" {
		t.Errorf("nested content = %q, want %q", string(nestedContent), "nested content")
	}
}
