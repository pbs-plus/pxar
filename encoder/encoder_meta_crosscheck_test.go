package encoder

import (
	"bytes"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// TestProbeEncodeMetadataCrosscheck encodes a directory with full metadata
// (ACL users/groups/group_obj/default/default_users/default_groups, FCaps,
// QuotaProjectID) and asserts byte-for-byte equality with the Rust reference
// (probe_encode_meta). This covers every encode_metadata code path.
func TestProbeEncodeMetadataCrosscheck(t *testing.T) {
	uid := uint64(1001)
	gid := uint64(2002)
	rootMeta := &pxar.Metadata{
		Stat: mkStat(format.ModeIFDIR | 0o755),
		ACL: pxar.ACL{
			Users:    []format.ACLUser{{UID: uid, Permissions: 0o6}},
			Groups:   []format.ACLGroup{{GID: gid, Permissions: 0o4}},
			GroupObj: &format.ACLGroupObject{Permissions: 0o7},
			Default: &format.ACLDefault{
				UserObjPermissions:  0o7,
				GroupObjPermissions: 0o4,
				OtherPermissions:    0o5,
				MaskPermissions:     0o6,
			},
			DefaultUsers:  []format.ACLUser{{UID: 3003, Permissions: 0o6}},
			DefaultGroups: []format.ACLGroup{{GID: 4004, Permissions: 0o4}},
		},
		FCaps: []byte{0x01, 0x02, 0x03, 0x04, 'd', 'e', 'a', 'd', 'b', 'e', 'e', 'f'},
		QuotaProjectID: func() *uint64 {
			v := uint64(0xdeadbeefcafe)
			return &v
		}(),
	}

	buf := &bytes.Buffer{}
	enc := NewEncoder(buf, nil, rootMeta, nil)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	goHex := hex(buf.Bytes())
	const rustLen = 348
	const rustHex = "efac88e5746495d53800000000000000ed410000000000000000000000000000e8030000e803000044332211000000008877665500000000b8557d450a54e82c2000000000000000e9030000000000000600000000000000ab034cb0ce3e6e132000000000000000d2070000000000000400000000000000762858e93180861018000000000000000700000000000000f596685a4113bbbb300000000000000007000000000000000400000000000000050000000000000006000000000000001fcd3205b45793c82000000000000000bb0b0000000000000600000000000000fe8f0316588a0af92000000000000000a40f000000000000040000000000000067fbf7b59ddda92d1c00000000000000010203046465616462656566bb1c7d2fe84075e01800000000000000fecaefbeadde00001d73d542a64fec2f280000000000000055153e755bed5eef34010000000000002800000000000000"

	if len(buf.Bytes()) != rustLen {
		t.Errorf("length: go=%d rust=%d", len(buf.Bytes()), rustLen)
	}
	if goHex != rustHex {
		minLen := min(len(rustHex), len(goHex))
		for i := range minLen {
			if goHex[i] != rustHex[i] {
				t.Errorf("first hex diff at byte %d: go=...%s rust=...%s",
					i/2, goHex[max(0, i-8):minLen], rustHex[max(0, i-8):minLen])
				break
			}
		}
		t.Logf("GO_META_HEX=%s", goHex)
	}
}
