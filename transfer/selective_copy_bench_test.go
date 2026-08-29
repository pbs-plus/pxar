package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

type benchmarkDrainSession struct{}

func (benchmarkDrainSession) UploadPayloadInterleaved(_ context.Context, _ string, newData io.Reader, injections <-chan backupproxy.InjectChunks) (*backupproxy.UploadResult, error) {
	type copyResult struct {
		n   int64
		err error
	}
	copied := make(chan copyResult, 1)
	go func() {
		n, err := io.Copy(io.Discard, newData)
		copied <- copyResult{n: n, err: err}
	}()
	var size uint64
	for injection := range injections {
		size += injection.Size
		if injection.Processed != nil {
			injection.Processed <- nil
		}
	}
	result := <-copied
	if result.err != nil {
		return nil, result.err
	}
	return &backupproxy.UploadResult{Size: size + uint64(result.n)}, nil
}

func (benchmarkDrainSession) UploadArchive(_ context.Context, _ string, data io.Reader) (*backupproxy.UploadResult, error) {
	n, err := io.Copy(io.Discard, data)
	return &backupproxy.UploadResult{Size: uint64(n)}, err
}

func (benchmarkDrainSession) UploadSplitArchive(context.Context, string, io.Reader, string, io.Reader) (*backupproxy.SplitArchiveResult, error) {
	return nil, nil
}

func (benchmarkDrainSession) UploadBlob(context.Context, string, []byte) error { return nil }

func (benchmarkDrainSession) Finish(context.Context) (*datastore.Manifest, error) {
	return &datastore.Manifest{}, nil
}

func (benchmarkDrainSession) Close() error { return nil }

func BenchmarkSelectiveCopyDirectory(b *testing.B) {
	metaIndex, payloadIndex, chunks := buildSelectiveCopyBenchmarkSource(b, 10_000)
	rootMeta := pxar.DirMetadata(0o755).Build()
	mapping := []PathMapping{{Src: "/selected", Dst: "/selected"}}

	b.ReportAllocs()
	for b.Loop() {
		source, err := NewSplitReader(metaIndex, payloadIndex, chunks)
		if err != nil {
			b.Fatal(err)
		}
		writer, err := NewRemoteDedupWriter(context.Background(), benchmarkDrainSession{}, "root.mpxar.didx", "root.ppxar.didx")
		if err != nil {
			b.Fatal(err)
		}
		if err := writer.Begin(&rootMeta, Options{Format: format.FormatVersion2}); err != nil {
			b.Fatal(err)
		}
		if err := Copy(source, writer, mapping, CopyOption{}); err != nil {
			b.Fatal(err)
		}
		if err := writer.Finish(); err != nil {
			b.Fatal(err)
		}
		_ = source.Close()
	}
}

func buildSelectiveCopyBenchmarkSource(b *testing.B, files int) ([]byte, []byte, staticChunkSource) {
	b.Helper()
	var metadata, payload bytes.Buffer
	rootMeta := pxar.DirMetadata(0o755).Build()
	enc := encoder.NewEncoder(&metadata, &payload, &rootMeta, nil)
	dirMeta := pxar.DirMetadata(0o755).Build()
	if err := enc.CreateDirectory("selected", &dirMeta); err != nil {
		b.Fatal(err)
	}
	fileMeta := pxar.FileMetadata(0o644).Build()
	for i := range files {
		if _, err := enc.AddFile(&fileMeta, fmt.Sprintf("file-%08d", i), nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := enc.Finish(); err != nil {
		b.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		b.Fatal(err)
	}

	cfg, err := buzhash.NewConfig(4 << 10)
	if err != nil {
		b.Fatal(err)
	}
	chunks := make(staticChunkSource)
	metaIndex := chunkBenchmarkStream(b, metadata.Bytes(), cfg, chunks)
	payloadIndex := chunkBenchmarkStream(b, payload.Bytes(), cfg, chunks)
	return metaIndex, payloadIndex, chunks
}

func chunkBenchmarkStream(b *testing.B, data []byte, cfg buzhash.Config, chunks staticChunkSource) []byte {
	b.Helper()
	chunker := buzhash.NewChunker(bytes.NewReader(data), cfg)
	index := datastore.NewDynamicIndexWriter(0)
	var offset uint64
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		digest := sha256.Sum256(chunk)
		blob, err := datastore.EncodeBlob(nil, chunk)
		if err != nil {
			b.Fatal(err)
		}
		chunks[digest] = blob
		offset += uint64(len(chunk))
		index.Add(offset, digest)
	}
	raw, err := index.Finish()
	if err != nil {
		b.Fatal(err)
	}
	return raw
}
