package transfer

import (
	"bytes"
	"testing"

	"github.com/pbs-plus/pxar/datastore"
)

type staticChunkSource map[[32]byte][]byte

func (s staticChunkSource) GetChunk(digest [32]byte) ([]byte, error) {
	return s[digest], nil
}

func TestCryptIdentityMatches(t *testing.T) {
	var key1, key2 [32]byte
	key1[0], key2[0] = 1, 2
	config1, err := datastore.NewCryptConfig(key1)
	if err != nil {
		t.Fatal(err)
	}
	config1Again, err := datastore.NewCryptConfig(key1)
	if err != nil {
		t.Fatal(err)
	}
	config2, err := datastore.NewCryptConfig(key2)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		opts CopyOption
		want bool
	}{
		{name: "plain", want: true},
		{name: "same encrypted key", opts: CopyOption{SourceCryptMode: datastore.CryptModeEncrypt, TargetCryptMode: datastore.CryptModeEncrypt, SourceCryptConfig: config1, TargetCryptConfig: config1Again}, want: true},
		{name: "different encrypted key", opts: CopyOption{SourceCryptMode: datastore.CryptModeEncrypt, TargetCryptMode: datastore.CryptModeEncrypt, SourceCryptConfig: config1, TargetCryptConfig: config2}},
		{name: "different mode", opts: CopyOption{SourceCryptMode: datastore.CryptModeSign, TargetCryptMode: datastore.CryptModeEncrypt, SourceCryptConfig: config1, TargetCryptConfig: config1Again}},
		{name: "missing target key", opts: CopyOption{SourceCryptMode: datastore.CryptModeEncrypt, TargetCryptMode: datastore.CryptModeEncrypt, SourceCryptConfig: config1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cryptIdentityMatches(test.opts); got != test.want {
				t.Fatalf("cryptIdentityMatches() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSplitPayloadSourceRetainsEncryptedChunkConfig(t *testing.T) {
	var key [32]byte
	key[0] = 3
	config, err := datastore.NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}
	indexWriter := datastore.NewDynamicIndexWriter(0)
	indexData, err := indexWriter.Finish()
	if err != nil {
		t.Fatal(err)
	}
	index, err := datastore.ParseDynamicIndex(indexData)
	if err != nil {
		t.Fatal(err)
	}
	inner := &staticChunkSource{}
	reader := &SplitReader{payloadIdx: index, source: NewDecryptSource(inner, config)}
	gotIndex, gotSource, gotConfig, ok := splitPayloadSource(reader)
	if !ok || gotIndex != index || gotSource != inner || gotConfig != config {
		t.Fatal("encrypted split payload source was not preserved")
	}
}

func TestDecryptSourceReturnsPlainBlob(t *testing.T) {
	var key [32]byte
	key[0] = 1
	config, err := datastore.NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("encrypted payload chunk")
	digest := config.ComputeDigest(content)
	encrypted, err := datastore.EncodeEncryptedBlob(nil, content, config, false)
	if err != nil {
		t.Fatal(err)
	}
	source := NewDecryptSource(staticChunkSource{digest: encrypted}, config)
	blob, err := source.GetChunk(digest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := datastore.DecodeBlob(nil, blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, content) {
		t.Fatalf("decoded content = %q", decoded)
	}
}
