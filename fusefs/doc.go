// Package fusefs provides a read-only filesystem interface over pxar archives,
// compatible with hanwen/go-fuse's InodeEmbedder and Inode interfaces.
//
// This package does NOT import go-fuse. It defines compatible interfaces
// (FileSystem, Attr, DirEntry) so callers can wrap them into a go-fuse
// FileSystem without the library requiring the dependency.
//
// # Basic Usage
//
// Open a pxar archive and create a Session:
//
//	f, err := os.Open("backup.pxar")
//	defer f.Close()
//	fi, _ := f.Stat()
//
//	sess, err := fusefs.NewSession(f, fi.Size())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sess.Close()
//
// # Filesystem Operations
//
// Session implements the FileSystem interface:
//
//	// Look up a file in a directory
//	inode, attr, err := sess.Lookup(fusefs.RootInode, "example.txt")
//
//	// Read file data
//	sess.Open(inode, 0)
//	buf := make([]byte, attr.Size)
//	n, err := sess.Read(inode, buf, 0)
//
//	// List directory
//	entries, err := sess.Readdir(fusefs.RootInode, 0)
//
//	// Read symlink target
//	target, err := sess.Readlink(symlinkInode)
//
// # Inode Model
//
// The root directory uses inode 1 (RootInode). Non-directory inodes have the
// NonDirBit (bit 63) set. Use IsDirInode to distinguish:
//
//	fusefs.IsDirInode(inode) // true if directory
//
// Nodes track entry ranges and optional content ranges for file data access.
// Reference counting is handled automatically via Ref/Unref/Forget.
package fusefs
