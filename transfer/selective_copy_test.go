package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func TestCopyPlansOverlappingSelectionsInSourceOrder(t *testing.T) {
	data := createNestedArchiveForSelection(t)
	source := NewFileReader(bytes.NewReader(data))
	defer source.Close()

	var targetData bytes.Buffer
	target := NewStreamWriter(&targetData)
	root := pxar.DirMetadata(0o755).Build()
	if err := target.Begin(&root, Options{Format: format.FormatVersion1}); err != nil {
		t.Fatal(err)
	}
	mappings := []PathMapping{
		{Src: "/a/b/deep.txt", Dst: "/a/b/deep.txt"},
		{Src: "/top.txt", Dst: "/top.txt"},
		{Src: "/a", Dst: "/a"},
	}
	if err := Copy(source, target, mappings, CopyOption{}); err != nil {
		t.Fatal(err)
	}
	if err := target.Finish(); err != nil {
		t.Fatal(err)
	}

	reader := NewFileReader(bytes.NewReader(targetData.Bytes()))
	defer reader.Close()
	for path, want := range map[string]string{
		"/top.txt":      "top level",
		"/a/b/deep.txt": "deep",
		"/a/mid.txt":    "mid",
	} {
		entry, err := reader.Lookup(path)
		if err != nil {
			t.Fatalf("lookup %s: %v", path, err)
		}
		content, err := reader.ReadFileContentReader(entry)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(content)
		_ = content.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", path, got, want)
		}
	}
}

func TestCopyPreservesOrMaterializesHardlinks(t *testing.T) {
	var sourceData bytes.Buffer
	meta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&sourceData, nil, &meta, nil)
	fileMeta := pxar.FileMetadata(0o644).Build()
	offset, err := enc.AddFile(&fileMeta, "original.txt", []byte("shared content"))
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.AddHardlink("linked.txt", "original.txt", offset); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	t.Run("target selected", func(t *testing.T) {
		source := NewFileReader(bytes.NewReader(sourceData.Bytes()))
		defer source.Close()
		var targetData bytes.Buffer
		target := NewStreamWriter(&targetData)
		if err := target.Begin(&meta, Options{Format: format.FormatVersion1}); err != nil {
			t.Fatal(err)
		}
		if err := Copy(source, target, []PathMapping{
			{Src: "/linked.txt", Dst: "/linked.txt"},
			{Src: "/original.txt", Dst: "/original.txt"},
		}, CopyOption{}); err != nil {
			t.Fatal(err)
		}
		if err := target.Finish(); err != nil {
			t.Fatal(err)
		}
		reader := NewFileReader(bytes.NewReader(targetData.Bytes()))
		defer reader.Close()
		link, err := reader.Lookup("/linked.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !link.IsHardlink() {
			t.Fatalf("linked.txt kind = %v, want hardlink", link.Kind)
		}
		targetOffset := link.FileOffset - link.LinkOffset
		resolved, err := reader.ReadEntryAt(int64(targetOffset))
		if err != nil {
			t.Fatal(err)
		}
		if resolved.FileName() != "original.txt" {
			t.Fatalf("hardlink target = %q", resolved.FileName())
		}
	})

	t.Run("target excluded", func(t *testing.T) {
		source := NewFileReader(bytes.NewReader(sourceData.Bytes()))
		defer source.Close()
		var targetData bytes.Buffer
		target := NewStreamWriter(&targetData)
		if err := target.Begin(&meta, Options{Format: format.FormatVersion1}); err != nil {
			t.Fatal(err)
		}
		if err := Copy(source, target, []PathMapping{{Src: "/linked.txt", Dst: "/linked.txt"}}, CopyOption{}); err != nil {
			t.Fatal(err)
		}
		if err := target.Finish(); err != nil {
			t.Fatal(err)
		}
		reader := NewFileReader(bytes.NewReader(targetData.Bytes()))
		defer reader.Close()
		entry, err := reader.Lookup("/linked.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !entry.IsRegularFile() {
			t.Fatalf("linked.txt kind = %v, want regular file", entry.Kind)
		}
		content, err := reader.ReadFileContentReader(entry)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(content)
		_ = content.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "shared content" {
			t.Fatalf("content = %q", got)
		}
	})
}

func TestCopyReplaysInteriorChunksWithoutLeakingBoundaries(t *testing.T) {
	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	targetDir := t.TempDir()
	secret := []byte("SECRET-BOUNDARY-MARKER-DO-NOT-COPY")
	selected := bytes.Repeat([]byte("selected-public-payload-"), 32<<10)

	createChunkedSelectionSource(t, sourceDir, cfg, secret, selected)
	sourceMeta := readFile(t, filepath.Join(sourceDir, "root.mpxar.didx"))
	sourcePayload := readFile(t, filepath.Join(sourceDir, "root.ppxar.didx"))
	sourceStore, err := datastore.NewChunkStore(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	sourceChunks := &closableChunkSource{inner: datastore.NewChunkStoreSource(sourceStore)}
	sourceReader, err := NewSplitReader(sourceMeta, sourcePayload, sourceChunks)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceReader.Close()

	targetStore, err := backupproxy.NewLocalStore(targetDir, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	targetSession, err := targetStore.StartSession(context.Background(), backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "selected-target",
		BackupTime: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	targetWriter, err := NewRemoteDedupWriter(context.Background(), targetSession, "root.mpxar.didx", "root.ppxar.didx")
	if err != nil {
		t.Fatal(err)
	}
	root := pxar.DirMetadata(0o755).Build()
	if err := targetWriter.Begin(&root, Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}
	if err := Copy(sourceReader, targetWriter, []PathMapping{{Src: "/selected.bin", Dst: "/selected.bin"}}, CopyOption{}); err != nil {
		t.Fatal(err)
	}
	sourceChunks.closed = true
	if err := targetWriter.Finish(); err != nil {
		t.Fatal(err)
	}

	targetMeta := readFile(t, filepath.Join(targetDir, "root.mpxar.didx"))
	targetPayload := readFile(t, filepath.Join(targetDir, "root.ppxar.didx"))
	targetChunkStore, err := datastore.NewChunkStore(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	targetChunks := datastore.NewChunkStoreSource(targetChunkStore)
	targetReader, err := NewSplitReader(targetMeta, targetPayload, targetChunks)
	if err != nil {
		t.Fatal(err)
	}
	defer targetReader.Close()
	entry, err := targetReader.Lookup("/selected.bin")
	if err != nil {
		t.Fatal(err)
	}
	content, err := targetReader.ReadFileContentReader(entry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(content)
	_ = content.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, selected) {
		t.Fatalf("selected content digest = %x, want %x", sha256.Sum256(got), sha256.Sum256(selected))
	}

	sourceIndex, err := datastore.ParseDynamicIndex(sourcePayload)
	if err != nil {
		t.Fatal(err)
	}
	targetIndex, err := datastore.ParseDynamicIndex(targetPayload)
	if err != nil {
		t.Fatal(err)
	}
	sourceEntry, err := sourceReader.Lookup("/selected.bin")
	if err != nil {
		t.Fatal(err)
	}
	spanEnd := sourceEntry.PayloadOffset + format.HeaderSize + sourceEntry.FileSize
	reusable := make(map[[32]byte]bool)
	for i := range sourceIndex.Count() {
		info, _ := sourceIndex.ChunkInfo(i)
		if info.Start >= sourceEntry.PayloadOffset && info.End <= spanEnd {
			reusable[info.Digest] = true
		}
	}
	reused := false
	for i := range targetIndex.Count() {
		info, _ := targetIndex.ChunkInfo(i)
		if reusable[info.Digest] {
			reused = true
		}
		blob, err := targetChunks.GetChunk(info.Digest)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := datastore.DecodeBlob(blob)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(decoded, secret) {
			t.Fatalf("target payload chunk %x contains unselected boundary data", info.Digest[:8])
		}
	}
	if !reused {
		t.Fatal("target payload did not reuse any complete source chunk")
	}
}

type closableChunkSource struct {
	inner  datastore.ChunkSource
	closed bool
}

func (s *closableChunkSource) GetChunk(digest [32]byte) ([]byte, error) {
	if s.closed {
		return nil, errors.New("source closed")
	}
	return s.inner.GetChunk(digest)
}

func createNestedArchiveForSelection(t *testing.T) []byte {
	t.Helper()
	var data bytes.Buffer
	root := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&data, nil, &root, nil)
	fileMeta := pxar.FileMetadata(0o644).Build()
	if _, err := enc.AddFile(&fileMeta, "top.txt", []byte("top level")); err != nil {
		t.Fatal(err)
	}
	dirMeta := pxar.DirMetadata(0o755).Build()
	if err := enc.CreateDirectory("a", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := enc.CreateDirectory("b", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if _, err := enc.AddFile(&fileMeta, "deep.txt", []byte("deep")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := enc.AddFile(&fileMeta, "mid.txt", []byte("mid")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func createChunkedSelectionSource(t *testing.T, dir string, cfg buzhash.Config, secret, selected []byte) {
	t.Helper()
	store, err := backupproxy.NewLocalStore(dir, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.StartSession(context.Background(), backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   "selection-source",
		BackupTime: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := NewSessionWriter(context.Background(), session, "root.mpxar.didx", "root.ppxar.didx")
	root := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&root, Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}
	meta := pxar.FileMetadata(0o600).Build()
	before := bytes.Repeat(secret, 256)
	files := []struct {
		name    string
		content []byte
	}{
		{name: "secret-before.bin", content: before},
		{name: "selected.bin", content: selected},
		{name: "secret-after.bin", content: before},
	}
	for _, file := range files {
		if err := writer.WriteEntry(&pxar.Entry{Path: file.name, Kind: pxar.KindFile, Metadata: meta, FileSize: uint64(len(file.content))}, file.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
