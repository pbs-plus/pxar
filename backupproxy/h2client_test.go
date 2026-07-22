package backupproxy

import (
	"bytes"
	"strconv"
	"testing"

	"golang.org/x/net/http2/hpack"
)

func TestHPACKStatusDecoding(t *testing.T) {
	cases := []int{200, 204, 206, 304, 400, 404, 500, 503}
	dec := hpack.NewDecoder(4096, nil)

	for _, want := range cases {
		var buf bytes.Buffer
		enc := hpack.NewEncoder(&buf)
		enc.WriteField(hpack.HeaderField{Name: ":status", Value: strconv.Itoa(want)})

		fields, err := dec.DecodeFull(buf.Bytes())
		if err != nil {
			t.Fatalf("status %d: decode: %v", want, err)
		}
		var got int
		for _, hf := range fields {
			if hf.Name == ":status" {
				got, _ = strconv.Atoi(hf.Value)
			}
		}
		if got != want {
			t.Errorf("status %d: decoded %d (indexed/huffman representation not handled)", want, got)
		}
	}
}
