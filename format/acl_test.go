package format

import (
	"encoding/binary"
	"testing"
)

func TestACLUserSize(t *testing.T) {
	want := 16
	got := binary.Size(ACLUser{})
	if got != want {
		t.Errorf("ACLUser size = %d, want %d", got, want)
	}
}

func TestACLGroupSize(t *testing.T) {
	want := 16
	got := binary.Size(ACLGroup{})
	if got != want {
		t.Errorf("ACLGroup size = %d, want %d", got, want)
	}
}

func TestACLGroupObjectSize(t *testing.T) {
	want := 8
	got := binary.Size(ACLGroupObject{})
	if got != want {
		t.Errorf("ACLGroupObject size = %d, want %d", got, want)
	}
}

func TestACLDefaultSize(t *testing.T) {
	want := 32
	got := binary.Size(ACLDefault{})
	if got != want {
		t.Errorf("ACLDefault size = %d, want %d", got, want)
	}
}
