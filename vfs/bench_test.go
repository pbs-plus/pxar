package vfs_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// ---------- Archive builders ----------

func buildVFSArchiveBench(b *testing.B, nFiles int) *transfer.FileArchiveReader {
	b.Helper()
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1430487000, 0)
	enc := encoder.NewEncoder(&buf, nil, &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}, nil)
	content := bytes.Repeat([]byte("V"), 64)
	for i := range nFiles {
		if _, err := enc.AddFile(&pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}}, fmt.Sprintf("file_%04d.txt", i), content); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}
	return transfer.NewFileArchiveReader(bytes.NewReader(buf.Bytes()))
}

func newBenchRemoteFS(b *testing.B, nFiles int) *vfs.RemoteFileSystem {
	b.Helper()
	r := buildVFSArchiveBench(b, nFiles)
	localFS := vfs.NewLocalFS(r)
	server := vfs.NewRemoteServer(localFS)
	tp := &directTransport{server: server}
	return vfs.NewRemoteFS(tp)
}

// ---------- LocalFileSystem benchmarks ----------

func BenchmarkLocalFS_Stat_CacheHit(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	if _, err := fs.Stat("/file_0050.txt"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fs.Stat("/file_0050.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalFS_Stat_CacheMiss(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fs.Stat(fmt.Sprintf("/file_%04d.txt", i%100)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalFS_ReadDir100(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := fs.ReadDir("/")
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 100 {
			b.Fatalf("expected 100, got %d", len(entries))
		}
	}
}

func BenchmarkLocalFS_ReadDir100_Cached(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	_, _ = fs.ReadDir("/")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := fs.ReadDir("/")
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 100 {
			b.Fatalf("expected 100, got %d", len(entries))
		}
	}
}

func BenchmarkLocalFS_ReadFile(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := fs.ReadFile(fmt.Sprintf("/file_%04d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
		if len(data) != 64 {
			b.Fatalf("expected 64 bytes, got %d", len(data))
		}
	}
}

func BenchmarkLocalFS_ReadFile_Cached(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	fs := vfs.NewLocalFS(r)
	_, _ = fs.ReadDir("/")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := fs.ReadFile(fmt.Sprintf("/file_%04d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
		if len(data) != 64 {
			b.Fatalf("expected 64 bytes, got %d", len(data))
		}
	}
}

// ---------- RemoteFileSystem benchmarks ----------

func BenchmarkRemoteFS_Stat_CacheHit(b *testing.B) {
	remoteFS := newBenchRemoteFS(b, 100)
	if _, err := remoteFS.Stat("/file_0050.txt"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := remoteFS.Stat("/file_0050.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteFS_ReadDir100(b *testing.B) {
	remoteFS := newBenchRemoteFS(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := remoteFS.ReadDir("/")
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 100 {
			b.Fatalf("expected 100, got %d", len(entries))
		}
	}
}

func BenchmarkRemoteFS_ReadFile(b *testing.B) {
	remoteFS := newBenchRemoteFS(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := remoteFS.ReadFile(fmt.Sprintf("/file_%04d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
		if len(data) != 64 {
			b.Fatalf("expected 64 bytes, got %d", len(data))
		}
	}
}

func BenchmarkRemoteFS_ReadFile_Cached(b *testing.B) {
	remoteFS := newBenchRemoteFS(b, 100)
	_, _ = remoteFS.ReadDir("/")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := remoteFS.ReadFile(fmt.Sprintf("/file_%04d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
		if len(data) != 64 {
			b.Fatalf("expected 64 bytes, got %d", len(data))
		}
	}
}

// ---------- Server handler benchmarks ----------

func BenchmarkServerHandleStat(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	localFS := vfs.NewLocalFS(r)
	server := vfs.NewRemoteServer(localFS)
	entries, _ := localFS.ReadDir("/")
	name := entries[0].Info.Name()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := server.HandleStat(&vfs.StatRequest{Path: "/" + name}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerHandleReadDir(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	localFS := vfs.NewLocalFS(r)
	server := vfs.NewRemoteServer(localFS)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := server.HandleReadDir(&vfs.ReadDirRequest{Path: "/"})
		if err != nil {
			b.Fatal(err)
		}
		if len(resp.Entries) != 100 {
			b.Fatalf("expected 100, got %d", len(resp.Entries))
		}
	}
}

func BenchmarkServerHandleReadFile(b *testing.B) {
	r := buildVFSArchiveBench(b, 100)
	localFS := vfs.NewLocalFS(r)
	server := vfs.NewRemoteServer(localFS)
	entries, _ := localFS.ReadDir("/")
	name := entries[0].Info.Name()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := server.HandleReadFile(&vfs.ReadFileRequest{Path: "/" + name}); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------- WriteTree benchmarks ----------

func BenchmarkWriteTree_Flat100(b *testing.B) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1430487000, 0)
	content := bytes.Repeat([]byte("W"), 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w := transfer.NewStreamArchiveWriter(&buf)
		rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
		if err := vfs.WriteTree(w, rootMeta, transfer.WriterOptions{}, func(dir string) ([]vfs.ChildEntry, error) {
			children := make([]vfs.ChildEntry, 100)
			for f := range 100 {
				children[f] = vfs.ChildEntry{
					Name:    fmt.Sprintf("file_%04d.txt", f),
					Kind:    pxar.KindFile,
					Meta:    &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
					Content: content,
				}
			}
			return children, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteTree_Nested10x50(b *testing.B) {
	var buf bytes.Buffer
	ts := format.NewStatxTimestamp(1430487000, 0)
	content := bytes.Repeat([]byte("W"), 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w := transfer.NewStreamArchiveWriter(&buf)
		rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}}
		if err := vfs.WriteTree(w, rootMeta, transfer.WriterOptions{}, func(dir string) ([]vfs.ChildEntry, error) {
			if dir != "/" {
				return nil, nil
			}
			children := make([]vfs.ChildEntry, 10)
			for d := range 10 {
				children[d] = vfs.ChildEntry{
					Name: fmt.Sprintf("dir_%03d", d),
					Kind: pxar.KindDirectory,
					Meta: &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755, Mtime: ts}},
					Children: func(_ string) ([]vfs.ChildEntry, error) {
						files := make([]vfs.ChildEntry, 50)
						for f := range 50 {
							files[f] = vfs.ChildEntry{
								Name:    fmt.Sprintf("file_%04d.txt", f),
								Kind:    pxar.KindFile,
								Meta:    &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644, Mtime: ts}},
								Content: content,
							}
						}
						return files, nil
					},
				}
			}
			return children, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// Compile-time check
var _ vfs.RPCTransport = (*directTransport)(nil)
var _ context.Context
