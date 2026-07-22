package interop

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pxar/buzhash"
)

func lcgData(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = byte(x >> 33)
	}
	return out
}

func chunkCases() map[string][]byte {
	mod251 := make([]byte, 1<<21)
	for i := range mod251 {
		mod251[i] = byte(i % 251)
	}
	return map[string][]byte{
		"lcg-4m-seed1":  lcgData(1<<22, 1),
		"lcg-3m-seed42": lcgData(3<<20, 42),
		"zeros-2m":      make([]byte, 1<<21),
		"mod251-2m":     mod251,
		"lcg-70k-seed7": lcgData(70000, 7),
	}
}

func scanBoundaries(data []byte, avg, feed int) []int {
	cfg, err := buzhash.NewConfig(avg)
	if err != nil {
		panic(err)
	}
	s := buzhash.NewScanner(cfg)
	var out []int
	for start := 0; start < len(data); {
		end := min(start+feed, len(data))
		off := start
		for {
			pos := s.Scan(data[off:end])
			if pos == 0 {
				break
			}
			off += pos
			out = append(out, off)
			if off >= end {
				break
			}
		}
		start = end
	}
	return out
}

func TestScannerBoundariesMatchRust(t *testing.T) {
	dir := interopDir(t)
	f, err := os.Open(filepath.Join(dir, "rust_chunks.txt"))
	if err != nil {
		t.Skipf("rust_chunks.txt not found: %v", err)
	}
	defer f.Close()

	cases := chunkCases()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	checked := 0
	for sc.Scan() {
		line := sc.Text()
		var name string
		var avg, feed, n, sum int
		fields := strings.SplitN(line, " ", 6)
		if len(fields) < 6 {
			t.Fatalf("bad line: %q", line)
		}
		name = fields[0]
		if _, err := fmt.Sscanf(strings.Join(fields[1:5], " "), "avg=%d feed=%d n=%d sum=%d", &avg, &feed, &n, &sum); err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		data, ok := cases[name]
		if !ok {
			t.Fatalf("unknown case %q", name)
		}
		got := scanBoundaries(data, avg, feed)
		gotSum := 0
		for _, b := range got {
			gotSum += b
		}
		if len(got) != n || gotSum != sum {
			t.Fatalf("%s avg=%d feed=%d: go n=%d sum=%d, rust n=%d sum=%d\nfirst go: %v",
				name, avg, feed, len(got), gotSum, n, sum, got[:min(10, len(got))])
		}
		checked++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("no cases checked")
	}
	t.Logf("verified %d chunker cases against rust reference", checked)
}

func TestChunkerMatchesScanner(t *testing.T) {
	for name, data := range chunkCases() {
		for _, avg := range []int{64 * 1024, 128 * 1024} {
			cfg, err := buzhash.NewConfig(avg)
			if err != nil {
				t.Fatal(err)
			}
			want := scanBoundaries(data, avg, len(data))
			c := buzhash.NewChunker(bytes.NewReader(data), cfg)
			var got []int
			pos := 0
			for {
				chunk, err := c.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				pos += len(chunk)
				got = append(got, pos)
			}
			if len(got) > 0 && got[len(got)-1] == len(data) {
				if len(want) == 0 || want[len(want)-1] != len(data) {
					got = got[:len(got)-1]
				}
			}
			if len(got) != len(want) {
				t.Fatalf("%s avg=%d: chunker %d boundaries, scanner %d", name, avg, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s avg=%d: boundary %d differs: chunker %d, scanner %d", name, avg, i, got[i], want[i])
				}
			}
		}
	}
}
