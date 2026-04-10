package format

// ACL permission bit constants.
const (
	ACLRead    uint64 = 4
	ACLWrite   uint64 = 2
	ACLExecute uint64 = 1
	ACLNoMask  uint64 = ^uint64(0)
)

// ACLPermissions represents ACL permission bits. 8 bytes.
type ACLPermissions uint64

// ACLUser represents a user ACL entry. 24 bytes.
type ACLUser struct {
	UID         uint64
	Permissions ACLPermissions
}

// ACLGroup represents a group ACL entry. 24 bytes.
type ACLGroup struct {
	GID         uint64
	Permissions ACLPermissions
}

// ACLGroupObject represents the group object ACL entry. 8 bytes.
type ACLGroupObject struct {
	Permissions ACLPermissions
}

// ACLDefault represents the default ACL entry for a directory. 32 bytes.
type ACLDefault struct {
	UserObjPermissions  ACLPermissions
	GroupObjPermissions ACLPermissions
	OtherPermissions    ACLPermissions
	MaskPermissions     ACLPermissions
}
