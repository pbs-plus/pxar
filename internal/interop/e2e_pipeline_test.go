package interop

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

func chunkPipeline(t *testing.T, dir string, stream []byte, cc *datastore.CryptConfig, compress bool) ([]byte, *datastore.ChunkStore) {
	t.Helper()
	store, err := datastore.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := buzhash.NewConfig(4 * 1024 * 1024)
	chunker := buzhash.NewChunker(bytes.NewReader(stream), cfg)
	idx := datastore.NewDynamicIndexWriter(1_700_000_000)

	var total uint64
	for {
		chunk, err := chunker.Next()
		if err != nil {
			break
		}
		var digest [32]byte
		if cc != nil {
			digest = cc.ComputeDigest(chunk)
		} else {
			digest = sha256.Sum256(chunk)
		}
		blob := encodeChunkBlob(t, chunk, cc, compress)
		if _, _, err := store.InsertChunk(digest, blob); err != nil {
			t.Fatalf("insert chunk: %v", err)
		}
		total += uint64(len(chunk))
		idx.Add(total, digest)
	}
	raw, err := idx.Finish()
	if err != nil {
		t.Fatalf("finish didx: %v", err)
	}
	return raw, store
}

func encodeChunkBlob(t *testing.T, chunk []byte, cc *datastore.CryptConfig, compress bool) []byte {
	t.Helper()
	switch {
	case cc == nil && !compress:
		b, err := datastore.EncodeBlob(chunk)
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	case cc == nil:
		b, err := datastore.EncodeCompressedBlob(chunk)
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	case !compress:
		b, err := datastore.EncodeEncryptedBlob(chunk, cc, false)
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	default:
		b, err := datastore.EncodeEncryptedBlob(chunk, cc, true)
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
}

func decodeChunkBlob(t *testing.T, raw []byte, cc *datastore.CryptConfig) []byte {
	t.Helper()
	if cc != nil {
		dec, err := datastore.DecodeEncryptedBlob(raw, cc)
		if err != nil {
			t.Fatalf("decrypt chunk: %v", err)
		}
		return dec
	}
	dec, err := datastore.DecodeBlob(raw)
	if err != nil {
		t.Fatalf("decode chunk: %v", err)
	}
	return dec
}

func reassembleFromDidx(t *testing.T, raw []byte, store *datastore.ChunkStore, cc *datastore.CryptConfig) []byte {
	t.Helper()
	r, err := datastore.ParseDynamicIndex(raw)
	if err != nil {
		t.Fatalf("parse didx: %v", err)
	}
	wantCsum, _ := r.ComputeCsum()
	if wantCsum != r.IndexCsum() {
		t.Fatalf("didx index_csum mismatch: header %x, recomputed %x", r.IndexCsum(), wantCsum)
	}
	var out bytes.Buffer
	for i := range r.Count() {
		ci, ok := r.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d", i)
		}
		blob, err := store.LoadChunk(ci.Digest)
		if err != nil {
			t.Fatalf("load chunk %d: %v", i, err)
		}
		chunk := decodeChunkBlob(t, blob, cc)
		var got [32]byte
		if cc != nil {
			got = cc.ComputeDigest(chunk)
		} else {
			got = sha256.Sum256(chunk)
		}
		if got != ci.Digest {
			t.Fatalf("chunk %d digest mismatch", i)
		}
		out.Write(chunk)
	}
	return out.Bytes()
}

func runDynamicMode(t *testing.T, name string, stream []byte, cc *datastore.CryptConfig, compress bool) {
	t.Helper()
	dir := t.TempDir()
	raw, store := chunkPipeline(t, dir, stream, cc, compress)
	got := reassembleFromDidx(t, raw, store, cc)
	if !bytes.Equal(got, stream) {
		t.Fatalf("%s: reassembled stream (%d bytes) != input (%d bytes)", name, len(got), len(stream))
	}
	r, _ := datastore.ParseDynamicIndex(raw)
	if r.UUID() == ([16]byte{}) {
		t.Fatalf("%s: didx UUID is zero (PBS generates a random v4 UUID)", name)
	}
}

func runImageMode(t *testing.T, image []byte) {
	t.Helper()
	const chunkSize = 4 * 1024 * 1024
	w, err := datastore.NewFixedIndexWriter(1_700_000_000, uint64(len(image)), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(image); off += chunkSize {
		end := min(off+chunkSize, len(image))
		d := sha256.Sum256(image[off:end])
		w.Set(off/chunkSize, d)
	}
	raw, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	r, err := datastore.ParseFixedIndex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.UUID() == ([16]byte{}) {
		t.Fatalf("fidx UUID is zero")
	}
	wantCsum, _ := r.ComputeCsum()
	if wantCsum != r.IndexCsum() {
		t.Fatalf("fidx index_csum mismatch")
	}
	var out bytes.Buffer
	for i := range r.Count() {
		ci, ok := r.ChunkInfo(i)
		if !ok {
			t.Fatalf("chunk info %d", i)
		}
		block := image[ci.Start:ci.End]
		if d := sha256.Sum256(block); d != ci.Digest {
			t.Fatalf("image chunk %d digest mismatch", i)
		}
		out.Write(block)
	}
	if !bytes.Equal(out.Bytes(), image) {
		t.Fatalf("image reassembly mismatch")
	}
}

func TestE2EPipelineAllModes(t *testing.T) {
	legacy := encodeV1(t)
	meta, payload := encodeV2(t)

	var key [32]byte
	for i := range key {
		key[i] = byte('k')
	}
	cc, err := datastore.NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}

	modes := []struct {
		name     string
		encrypt  bool
		compress bool
	}{
		{"plain", false, false},
		{"compress", false, true},
		{"encrypt", true, false},
		{"encrypt_compress", true, true},
	}

	for _, m := range modes {
		var cryptCfg *datastore.CryptConfig
		if m.encrypt {
			cryptCfg = cc
		}
		t.Run(m.name+"_legacy_dynamic", func(t *testing.T) {
			runDynamicMode(t, m.name+"/legacy", legacy, cryptCfg, m.compress)
		})
		t.Run(m.name+"_split_metadata", func(t *testing.T) {
			runDynamicMode(t, m.name+"/split-meta", meta, cryptCfg, m.compress)
		})
		t.Run(m.name+"_split_payload", func(t *testing.T) {
			runDynamicMode(t, m.name+"/split-payload", payload, cryptCfg, m.compress)
		})
	}

	img := make([]byte, 10*1024*1024)
	for i := range img {
		img[i] = byte((i*31 + 7) & 0xff)
	}
	t.Run("image_fidx", func(t *testing.T) { runImageMode(t, img) })

	t.Run("catalog_pcat1", func(t *testing.T) {
		catBytes := buildReferenceCatalog(t)
		dir := t.TempDir()
		raw, store := chunkPipeline(t, dir, catBytes, nil, false)
		got := reassembleFromDidx(t, raw, store, nil)
		if !bytes.Equal(got, catBytes) {
			t.Fatalf("catalog reassembly mismatch")
		}
		if dir := os.Getenv("PXAR_INTEROP_DIR"); dir != "" {
			if want, err := os.ReadFile(filepath.Join(dir, "rust_catalog.hex")); err == nil {
				if hex.EncodeToString(catBytes) != string(bytes.TrimSpace(want)) {
					t.Fatalf("catalog bytes != PBS reference")
				}
			}
		}
	})

	t.Run("manifest_sign_verify", func(t *testing.T) {
		didxRaw, store := chunkPipeline(t, t.TempDir(), legacy, nil, false)
		r, _ := datastore.ParseDynamicIndex(didxRaw)
		csum, _ := r.ComputeCsum()
		size := r.LastEndOffset()
		manifest := &datastore.Manifest{
			BackupType: "vm",
			BackupID:   "100",
			Files: []datastore.BackupFileInfo{{
				Filename:  "root.pxar.didx",
				Size:      size,
				CSum:      hex.EncodeToString(csum[:]),
				CryptMode: "none",
			}},
			BackupTime: 1_700_000_000,
		}
		if err := datastore.SignManifest(manifest, cc); err != nil {
			t.Fatalf("sign: %v", err)
		}
		if err := datastore.VerifyManifestSignature(manifest, cc); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if len(manifest.Signature) != 64 {
			t.Fatalf("signature not lowercase hex (len %d)", len(manifest.Signature))
		}
		good := manifest.Signature
		manifest.Files[0].Size++
		manifest.Signature = good
		if err := datastore.VerifyManifestSignature(manifest, cc); err == nil {
			t.Fatalf("tampered manifest verified (should fail)")
		}
		_ = store
	})
}

func buildReferenceCatalog(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	cw := datastore.NewCatalogWriter(&buf)
	cw.StartDirectory("")
	cw.AddFile("file1.txt", 100, 1700000000)
	cw.StartDirectory("subdir")
	cw.AddFile("nested.txt", 75, 2000)
	cw.EndDirectory()
	cw.AddSymlink("link")
	cw.AddFile("big", 1<<32, -1)
	if err := cw.Finish(); err != nil {
		t.Fatalf("finish catalog: %v", err)
	}
	return buf.Bytes()
}

func init() { _ = fmt.Sprintf }
