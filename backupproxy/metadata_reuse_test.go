package backupproxy

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func TestMetadataWalkerInjectsUnchangedPayloadWithoutRestoring(t *testing.T) {
	var digest [32]byte
	digest[0] = 1
	idxWriter := datastore.NewDynamicIndexWriter(0)
	idxWriter.Add(200, digest)
	raw, err := idxWriter.Finish()
	if err != nil {
		t.Fatal(err)
	}
	idx, err := datastore.ParseDynamicIndex(raw)
	if err != nil {
		t.Fatal(err)
	}

	fileMeta := pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFREG | 0o644}}
	var injections []InjectChunks
	walker := metadataWalker{
		planner: datastore.NewChunkReusePlanner(idx),
		emitInjection: func(injection InjectChunks) error {
			injections = append(injections, injection)
			return nil
		},
		pending: []pendingReuse{
			{name: "a", fullPath: "/a", previous: &SnapshotEntry{Metadata: fileMeta, IsRegularFile: true, PayloadOffset: 16, FileSize: 74}},
			{name: "b", fullPath: "/b", previous: &SnapshotEntry{Metadata: fileMeta, IsRegularFile: true, PayloadOffset: 106, FileSize: 78}},
		},
	}
	var metadata, payload bytes.Buffer
	rootMeta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
	enc := encoder.NewEncoder(&metadata, &payload, rootMeta, nil)
	initialPayloadSize := payload.Len()

	if err := walker.flushPending(enc, false); err != nil {
		t.Fatal(err)
	}
	if payload.Len() != initialPayloadSize {
		t.Fatalf("encoded %d unchanged payload bytes", payload.Len()-initialPayloadSize)
	}
	if len(injections) != 1 || injections[0].Size != 200 {
		t.Fatalf("injections = %+v", injections)
	}
	if enc.PayloadPosition() != 216 {
		t.Fatalf("payload position = %d, want 216", enc.PayloadPosition())
	}
}
