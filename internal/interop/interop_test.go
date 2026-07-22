package interop

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/decoder"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func interopDir(t *testing.T) string {
	dir := os.Getenv("PXAR_INTEROP_DIR")
	if dir == "" {
		dir = "/tmp/pxar-interop"
	}
	if _, err := os.Stat(filepath.Join(dir, "rust_v1.pxar")); err != nil {
		t.Skipf("rust reference data not found in %s; run scripts/gen-interop.sh", dir)
	}
	return dir
}

func fnv1a(data []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, b := range data {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return h
}

func ts(secs int64, nanos uint32) format.StatxTimestamp {
	return format.StatxTimestamp{Secs: secs, Nanos: nanos}
}

func rootMeta() pxar.Metadata {
	return pxar.DirMetadata(format.ModeIFDIR|0o755).
		Owner(1000, 1000).
		Mtime(ts(0x11223344, 0x22334455)).
		XAttr([]byte("user.root"), []byte("rootval")).
		Build()
}

func bigContent() []byte {
	out := make([]byte, 70000)
	for i := range out {
		out[i] = byte((i*7 + 13) & 0xff)
	}
	return out
}

func buildTree(t *testing.T, enc *encoder.Encoder) {
	t.Helper()

	fileMeta := pxar.FileMetadata(format.ModeIFREG|0o644).
		Owner(0, 0).
		Mtime(ts(1234567890, 42)).
		XAttr([]byte("user.zeta"), []byte("z")).
		XAttr([]byte("user.alpha"), []byte{0, 1, 2, 255}).
		FCaps([]byte{1, 0, 0, 2, 0x20, 0, 0, 0, 0x20, 0, 0, 0, 0, 0, 0, 0}).
		Build()
	aOff, err := enc.AddFile(&fileMeta, "a.txt", []byte("hello pxar"))
	if err != nil {
		t.Fatal(err)
	}

	emptyMeta := pxar.FileMetadata(format.ModeIFREG|0o600).
		Owner(1, 2).Mtime(ts(3, 4)).Build()
	if _, err := enc.AddFile(&emptyMeta, "empty", nil); err != nil {
		t.Fatal(err)
	}

	linkMeta := pxar.SymlinkMetadata(format.ModeIFLNK|0o777).
		Owner(1000, 1000).Mtime(ts(5, 6)).Build()
	if err := enc.AddSymlink(&linkMeta, "link", "a.txt"); err != nil {
		t.Fatal(err)
	}

	if err := enc.AddHardlink("hl", "a.txt", aOff); err != nil {
		t.Fatal(err)
	}

	cdevMeta := pxar.DeviceMetadata(format.ModeIFCHR|0o666).
		Owner(0, 0).Mtime(ts(7, 8)).Build()
	if err := enc.AddDevice(&cdevMeta, "cdev", format.Device{Major: 1, Minor: 3}); err != nil {
		t.Fatal(err)
	}

	bdevMeta := pxar.NewMetadataBuilder(format.ModeIFBLK|0o660).
		Owner(0, 6).Mtime(ts(9, 10)).Build()
	if err := enc.AddDevice(&bdevMeta, "bdev", format.Device{Major: 7, Minor: 0}); err != nil {
		t.Fatal(err)
	}

	fifoMeta := pxar.FIFOMetadata(format.ModeIFIFO|0o644).
		Owner(11, 12).Mtime(ts(13, 14)).Build()
	if err := enc.AddFIFO(&fifoMeta, "fifo"); err != nil {
		t.Fatal(err)
	}

	sockMeta := pxar.SocketMetadata(format.ModeIFSOCK|0o600).
		Owner(15, 16).Mtime(ts(17, 18)).Build()
	if err := enc.AddSocket(&sockMeta, "sock"); err != nil {
		t.Fatal(err)
	}

	dir1Meta := pxar.DirMetadata(format.ModeIFDIR|0o750).
		Owner(1000, 1000).Mtime(ts(19, 20)).
		QuotaProjectID(4711).
		Build()
	dir1Meta.ACL.Users = []format.ACLUser{
		{UID: 1000, Permissions: 7},
		{UID: 1001, Permissions: 5},
	}
	dir1Meta.ACL.Groups = []format.ACLGroup{{GID: 500, Permissions: 4}}
	dir1Meta.ACL.GroupObj = &format.ACLGroupObject{Permissions: 6}
	dir1Meta.ACL.Default = &format.ACLDefault{
		UserObjPermissions:  7,
		GroupObjPermissions: 5,
		OtherPermissions:    0,
		MaskPermissions:     format.ACLPermissions(format.ACLNoMask),
	}
	dir1Meta.ACL.DefaultUsers = []format.ACLUser{{UID: 2000, Permissions: 6}}
	dir1Meta.ACL.DefaultGroups = []format.ACLGroup{{GID: 3000, Permissions: 4}}
	if err := enc.CreateDirectory("dir1", &dir1Meta); err != nil {
		t.Fatal(err)
	}
	nestedMeta := pxar.FileMetadata(format.ModeIFREG|0o444).
		Owner(21, 22).Mtime(ts(23, 24)).Build()
	if _, err := enc.AddFile(&nestedMeta, "nested.txt", []byte("nested content")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}

	manyMeta := pxar.DirMetadata(format.ModeIFDIR|0o755).
		Owner(0, 0).Mtime(ts(25, 26)).Build()
	if err := enc.CreateDirectory("many", &manyMeta); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		name := fmt.Sprintf("f%02d", i)
		meta := pxar.FileMetadata(format.ModeIFREG|0o644).
			Owner(0, 0).Mtime(ts(int64(100+i), 0)).Build()
		if _, err := enc.AddFile(&meta, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	uniMeta := pxar.FileMetadata(format.ModeIFREG|0o644).
		Owner(0, 0).Mtime(ts(200, 0)).Build()
	if _, err := enc.AddFile(&uniMeta, "übêr-ño", []byte("unic")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(); err != nil {
		t.Fatal(err)
	}

	bigMeta := pxar.FileMetadata(format.ModeIFREG|0o644).
		Owner(0, 0).Mtime(ts(300, 999999999)).Build()
	if _, err := enc.AddFile(&bigMeta, "big.bin", bigContent()); err != nil {
		t.Fatal(err)
	}
}

func encodeV1(t *testing.T) []byte {
	buf := &bytes.Buffer{}
	meta := rootMeta()
	enc := encoder.NewEncoder(buf, nil, &meta, nil)
	buildTree(t, enc)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func encodeV2(t *testing.T) ([]byte, []byte) {
	buf := &bytes.Buffer{}
	payloadBuf := &bytes.Buffer{}
	meta := rootMeta()
	enc := encoder.NewEncoder(buf, payloadBuf, &meta, []byte("prelude-blob-data"))
	buildTree(t, enc)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), payloadBuf.Bytes()
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

func TestEncodeV1MatchesRust(t *testing.T) {
	dir := interopDir(t)
	want, err := os.ReadFile(filepath.Join(dir, "rust_v1.pxar"))
	if err != nil {
		t.Fatal(err)
	}
	got := encodeV1(t)
	if d := firstDiff(got, want); d >= 0 {
		lo := max(d-32, 0)
		t.Fatalf("archive differs at byte %d (go len %d, rust len %d)\ngo:   %x\nrust: %x",
			d, len(got), len(want), got[lo:min(d+32, len(got))], want[lo:min(d+32, len(want))])
	}
}

func TestEncodeV2MatchesRust(t *testing.T) {
	dir := interopDir(t)
	wantMeta, err := os.ReadFile(filepath.Join(dir, "rust_v2.mpxar"))
	if err != nil {
		t.Fatal(err)
	}
	wantPayload, err := os.ReadFile(filepath.Join(dir, "rust_v2.ppxar"))
	if err != nil {
		t.Fatal(err)
	}
	gotMeta, gotPayload := encodeV2(t)
	if d := firstDiff(gotMeta, wantMeta); d >= 0 {
		lo := max(d-32, 0)
		t.Fatalf("metadata stream differs at byte %d (go len %d, rust len %d)\ngo:   %x\nrust: %x",
			d, len(gotMeta), len(wantMeta), gotMeta[lo:min(d+32, len(gotMeta))], wantMeta[lo:min(d+32, len(wantMeta))])
	}
	if d := firstDiff(gotPayload, wantPayload); d >= 0 {
		t.Fatalf("payload stream differs at byte %d (go len %d, rust len %d)",
			d, len(gotPayload), len(wantPayload))
	}
}

func dumpMetadata(sb *strings.Builder, meta *pxar.Metadata) {
	fmt.Fprintf(sb, "mode=%o uid=%d gid=%d mtime=%d.%09d",
		meta.Stat.Mode, meta.Stat.UID, meta.Stat.GID, meta.Stat.Mtime.Secs, meta.Stat.Mtime.Nanos)
	for _, x := range meta.XAttrs {
		fmt.Fprintf(sb, " xattr[%s]=%x", x.Name(), fnv1a(x.Value()))
	}
	for _, u := range meta.ACL.Users {
		fmt.Fprintf(sb, " acl_user[%d]=%d", u.UID, u.Permissions)
	}
	for _, g := range meta.ACL.Groups {
		fmt.Fprintf(sb, " acl_group[%d]=%d", g.GID, g.Permissions)
	}
	if meta.ACL.GroupObj != nil {
		fmt.Fprintf(sb, " acl_group_obj=%d", meta.ACL.GroupObj.Permissions)
	}
	if d := meta.ACL.Default; d != nil {
		fmt.Fprintf(sb, " acl_default=%d,%d,%d,%d",
			d.UserObjPermissions, d.GroupObjPermissions, d.OtherPermissions, d.MaskPermissions)
	}
	for _, u := range meta.ACL.DefaultUsers {
		fmt.Fprintf(sb, " acl_default_user[%d]=%d", u.UID, u.Permissions)
	}
	for _, g := range meta.ACL.DefaultGroups {
		fmt.Fprintf(sb, " acl_default_group[%d]=%d", g.GID, g.Permissions)
	}
	if meta.FCaps != nil {
		fmt.Fprintf(sb, " fcaps=%x", fnv1a(meta.FCaps))
	}
	if meta.QuotaProjectID != nil {
		fmt.Fprintf(sb, " quota=%d", *meta.QuotaProjectID)
	}
}

func dumpArchive(t *testing.T, archive, payload []byte) string {
	t.Helper()
	var payloadReader *bytes.Reader
	var dec *decoder.Decoder
	if payload != nil {
		payloadReader = bytes.NewReader(payload)
		dec = decoder.NewDecoder(bytes.NewReader(archive), payloadReader)
	} else {
		dec = decoder.NewDecoder(bytes.NewReader(archive), nil)
	}

	sb := &strings.Builder{}
	for {
		entry, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if entry == nil {
			break
		}
		switch entry.Kind {
		case pxar.KindVersion:
			fmt.Fprintf(sb, "version Version%d\n", entry.FileSize)
			continue
		case pxar.KindPrelude:
			fmt.Fprintf(sb, "prelude %x\n", fnv1a([]byte(entry.LinkTarget)))
			continue
		}

		var kind string
		switch entry.Kind {
		case pxar.KindSymlink:
			kind = fmt.Sprintf("symlink %s", entry.LinkTarget)
		case pxar.KindHardlink:
			kind = fmt.Sprintf("hardlink %s off=%d", entry.LinkTarget, entry.LinkOffset)
		case pxar.KindDevice:
			kind = fmt.Sprintf("device %d:%d", entry.DeviceInfo.Major, entry.DeviceInfo.Minor)
		case pxar.KindSocket:
			kind = "socket"
		case pxar.KindFIFO:
			kind = "fifo"
		case pxar.KindFile:
			buf := &bytes.Buffer{}
			r := dec.Contents()
			if r != nil {
				if _, err := buf.ReadFrom(r); err != nil {
					t.Fatalf("contents: %v", err)
				}
			}
			kind = fmt.Sprintf("file size=%d content=%x", entry.FileSize, fnv1a(buf.Bytes()))
		case pxar.KindDirectory:
			kind = "directory"
		case pxar.KindGoodbyeTable:
			kind = "goodbye"
		default:
			t.Fatalf("unexpected entry kind %v", entry.Kind)
		}
		fmt.Fprintf(sb, "%s :: %s :: ", entry.Path, kind)
		dumpMetadata(sb, &entry.Metadata)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func compareDump(t *testing.T, got, wantFile string) {
	t.Helper()
	want, err := os.ReadFile(wantFile)
	if err != nil {
		t.Fatal(err)
	}
	gotLines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	wantLines := strings.Split(strings.TrimRight(string(want), "\n"), "\n")
	n := min(len(gotLines), len(wantLines))
	for i := range n {
		if gotLines[i] != wantLines[i] {
			t.Fatalf("dump differs at line %d:\ngo:   %s\nrust: %s", i+1, gotLines[i], wantLines[i])
		}
	}
	if len(gotLines) != len(wantLines) {
		t.Fatalf("dump line count differs: go %d, rust %d", len(gotLines), len(wantLines))
	}
}

func TestDecodeRustV1(t *testing.T) {
	dir := interopDir(t)
	archive, err := os.ReadFile(filepath.Join(dir, "rust_v1.pxar"))
	if err != nil {
		t.Fatal(err)
	}
	compareDump(t, dumpArchive(t, archive, nil), filepath.Join(dir, "rust_v1.dump"))
}

func TestDecodeRustV2(t *testing.T) {
	dir := interopDir(t)
	archive, err := os.ReadFile(filepath.Join(dir, "rust_v2.mpxar"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(dir, "rust_v2.ppxar"))
	if err != nil {
		t.Fatal(err)
	}
	compareDump(t, dumpArchive(t, archive, payload), filepath.Join(dir, "rust_v2.dump"))
}

func TestWriteGoArchivesForRust(t *testing.T) {
	dir := interopDir(t)
	if err := os.WriteFile(filepath.Join(dir, "go_v1.pxar"), encodeV1(t), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, payload := encodeV2(t)
	if err := os.WriteFile(filepath.Join(dir, "go_v2.mpxar"), meta, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go_v2.ppxar"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
}
