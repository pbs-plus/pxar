package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

type osFS struct{}

func isSkippedXAttr(name string) bool {
	for _, prefix := range []string{"system.posix_acl_", "security.capability"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (fs *osFS) GetXAttrs(path string) ([]format.XAttr, error) {
	size, err := unix.Llistxattr(path, nil)
	if err != nil {
		return nil, nil
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	n, err := unix.Llistxattr(path, buf)
	if err != nil {
		return nil, nil
	}
	buf = buf[:n]

	var result []format.XAttr
	for _, name := range bytesToStrings(buf) {
		if isSkippedXAttr(name) {
			continue
		}
		valSize, err := unix.Lgetxattr(path, name, nil)
		if err != nil {
			continue
		}
		val := make([]byte, valSize)
		if _, err := unix.Lgetxattr(path, name, val); err != nil {
			continue
		}
		result = append(result, format.NewXAttr([]byte(name), val))
	}
	return result, nil
}

func (fs *osFS) GetACL(path string) (pxar.ACL, error) {
	var acl pxar.ACL

	accessACL, err := fs.parsePOSIXACL(path, "system.posix_acl_access")
	if err == nil && accessACL != nil {
		acl = *accessACL
	}

	defaultACL, err := fs.parsePOSIXACL(path, "system.posix_acl_default")
	if err == nil && defaultACL != nil {
		if len(defaultACL.Users) > 0 {
			acl.DefaultUsers = defaultACL.Users
		}
		if len(defaultACL.Groups) > 0 {
			acl.DefaultGroups = defaultACL.Groups
		}
		if defaultACL.GroupObj != nil {
			acl.Default = &format.ACLDefault{
				UserObjPermissions:  defaultACL.GroupObj.Permissions,
				GroupObjPermissions: defaultACL.GroupObj.Permissions,
				OtherPermissions:    0,
				MaskPermissions:     defaultACL.GroupObj.Permissions,
			}
		}
	}

	return acl, nil
}

func (fs *osFS) parsePOSIXACL(path, xattrName string) (*pxar.ACL, error) {
	valSize, err := unix.Lgetxattr(path, xattrName, nil)
	if err != nil {
		return nil, err
	}
	if valSize < 4 {
		return nil, fmt.Errorf("ACL xattr too small")
	}
	val := make([]byte, valSize)
	if _, err := unix.Lgetxattr(path, xattrName, val); err != nil {
		return nil, err
	}

	version := binary.LittleEndian.Uint32(val[:4])
	if version != 2 && version != 4 {
		return nil, fmt.Errorf("unsupported ACL version %d", version)
	}

	var acl pxar.ACL
	offset := uint32(4)
	entrySize := uint32(12)
	if version == 4 {
		entrySize = 16
	}

	for offset+entrySize <= uint32(len(val)) {
		tag := binary.LittleEndian.Uint16(val[offset:])
		perm := binary.LittleEndian.Uint16(val[offset+2:])
		id := binary.LittleEndian.Uint32(val[offset+4:])

		permissions := format.ACLPermissions(perm)

		switch tag {
		case 1:
			acl.GroupObj = &format.ACLGroupObject{Permissions: permissions}
		case 2:
			acl.Users = append(acl.Users, format.ACLUser{UID: uint64(id), Permissions: permissions})
		case 4:
			acl.Groups = append(acl.Groups, format.ACLGroup{GID: uint64(id), Permissions: permissions})
		case 8:
		case 0x20:
		default:
		}

		offset += entrySize
	}

	return &acl, nil
}

func (fs *osFS) GetFCaps(path string) ([]byte, error) {
	valSize, err := unix.Lgetxattr(path, "security.capability", nil)
	if err != nil {
		return nil, nil
	}
	if valSize == 0 {
		return nil, nil
	}
	val := make([]byte, valSize)
	if _, err := unix.Lgetxattr(path, "security.capability", val); err != nil {
		return nil, nil
	}
	return val, nil
}

func bytesToStrings(buf []byte) []string {
	if len(buf) == 0 {
		return nil
	}
	var result []string
	start := 0
	for i, b := range buf {
		if b == 0 {
			result = append(result, string(buf[start:i]))
			start = i + 1
		}
	}
	return result
}

func (fs *osFS) Stat(path string) (format.Stat, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return format.Stat{}, err
	}
	return format.Stat{
		Mode:  uint64(st.Mode),
		Flags: 0,
		UID:   st.Uid,
		GID:   st.Gid,
		Mtime: format.StatxTimestampNew(st.Mtim.Sec, uint32(st.Mtim.Nsec)),
	}, nil
}

func (fs *osFS) ReadDir(path string) ([]backupproxy.DirEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var result []backupproxy.DirEntry
	for _, e := range entries {
		var st syscall.Stat_t
		fullPath := filepath.Join(path, e.Name())
		if err := syscall.Lstat(fullPath, &st); err != nil {
			continue
		}
		result = append(result, backupproxy.DirEntry{
			Name: e.Name(),
			Stat: format.Stat{
				Mode:  uint64(st.Mode),
				Flags: 0,
				UID:   st.Uid,
				GID:   st.Gid,
				Mtime: format.StatxTimestampNew(st.Mtim.Sec, uint32(st.Mtim.Nsec)),
			},
			Size: uint64(st.Size),
		})
	}
	return result, nil
}

func (fs *osFS) ReadFile(path string, offset, length int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if offset > 0 {
		if _, err := f.Seek(offset, 0); err != nil {
			return nil, err
		}
	}
	if length < 0 {
		return io.ReadAll(f)
	}
	buf := make([]byte, length)
	n, err := f.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (fs *osFS) ReadLink(path string) (string, error) {
	target, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return target, nil
}

func parseMode(s string) backupproxy.DetectionMode {
	switch strings.ToLower(s) {
	case "legacy":
		return backupproxy.DetectionLegacy
	case "data":
		return backupproxy.DetectionData
	case "metadata":
		return backupproxy.DetectionMetadata
	default:
		return backupproxy.DetectionLegacy
	}
}

func runBackup() error {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	repo := fs.String("repository", "", "PBS repository URL (root@pam@host:port:datastore)")
	password := fs.String("password", "", "PBS password (or set PBS_PASSWORD)")
	fingerprint := fs.String("fingerprint", "", "PBS certificate fingerprint (or set PBS_FINGERPRINT)")
	backupID := fs.String("backup-id", "cli", "backup ID")
	mode := fs.String("mode", "legacy", "detection mode: legacy, data, metadata")
	prevID := fs.String("previous-backup-id", "", "previous backup ID for metadata mode")
	prevTime := fs.Int64("previous-backup-time", 0, "previous backup timestamp for metadata mode")
	cryptMode := fs.String("crypt-mode", "none", "encryption mode: none, encrypt, sign-only")
	compressFlag := fs.Bool("compress", false, "compress chunks with zstd")
	keyfile := fs.String("keyfile", "", "path to encryption key file (JSON)")
	keyPassword := fs.String("key-password", "", "password for encrypted key file (or set PBS_ENCRYPTION_PASSWORD)")
	fs.Parse(os.Args[2:])

	if *repo == "" {
		*repo = os.Getenv("PBS_REPOSITORY")
	}
	if *repo == "" {
		return fmt.Errorf("--repository or PBS_REPOSITORY required")
	}
	if *password == "" {
		*password = os.Getenv("PBS_PASSWORD")
	}
	if *fingerprint == "" {
		*fingerprint = os.Getenv("PBS_FINGERPRINT")
	}

	pathArg := fs.Arg(0)
	if pathArg == "" {
		return fmt.Errorf("backup path required")
	}

	parsed := parseRepo(*repo)
	if parsed == nil {
		return fmt.Errorf("invalid repository format: %s (expected user@host:port:datastore)", *repo)
	}

	chunkCfg, err := buzhash.NewConfig(4 * 1024 * 1024)
	if err != nil {
		return err
	}

	pbsCfg := backupproxy.PBSConfig{
		BaseURL:       fmt.Sprintf("https://%s:%d/api2/json", parsed.host, parsed.port),
		Datastore:     parsed.datastore,
		AuthToken:     parsed.token,
		SkipTLSVerify: *fingerprint != "",
	}

	store := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, *compressFlag)
	client := backupproxy.NewLocalClient(&osFS{})
	srv := backupproxy.NewServer(client, store)

	detMode := parseMode(*mode)
	backupTime := time.Now().Unix()

	parsedCryptMode := datastore.CryptModeNone
	switch *cryptMode {
	case "encrypt":
		parsedCryptMode = datastore.CryptModeEncrypt
	case "sign-only":
		parsedCryptMode = datastore.CryptModeSign
	}

	cfg := backupproxy.BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      *backupID,
		BackupTime:    backupTime,
		DetectionMode: detMode,
		Compress:      *compressFlag,
		CryptMode:     parsedCryptMode,
	}

	if parsedCryptMode != datastore.CryptModeNone {
		if *keyfile == "" {
			return fmt.Errorf("--crypt-mode %s requires --keyfile", *cryptMode)
		}
		keyData, err := os.ReadFile(*keyfile)
		if err != nil {
			return fmt.Errorf("read keyfile: %w", err)
		}
		kp := *keyPassword
		if kp == "" {
			kp = os.Getenv("PBS_ENCRYPTION_PASSWORD")
		}
		if kp != "" {
			cc, err := datastore.LoadKeyFile(keyData, kp)
			if err != nil {
				return fmt.Errorf("load key file: %w", err)
			}
			cfg.CryptConfig = cc
		} else {
			cc, err := datastore.LoadKeyFileNoPassword(keyData)
			if err != nil {
				return fmt.Errorf("load key file (no password): %w", err)
			}
			cfg.CryptConfig = cc
		}
	}

	if detMode == backupproxy.DetectionMetadata {
		if *prevID == "" || *prevTime == 0 {
			return fmt.Errorf("metadata mode requires --previous-backup-id and --previous-backup-time")
		}
		cfg.PreviousBackup = &backupproxy.PreviousBackupRef{
			BackupType: datastore.BackupHost,
			BackupID:   *prevID,
			BackupTime: *prevTime,
		}
	}

	totalStart := time.Now()
	result, err := srv.RunBackupWithMode(context.Background(), pathArg, cfg)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}
	totalWall := time.Since(totalStart)

	fmt.Printf("Duration: %.3fs\n", result.Duration.Seconds())
	fmt.Printf("WallTime: %.3fs\n", totalWall.Seconds())
	fmt.Printf("Files: %d, Dirs: %d, Bytes: %d\n", result.FileCount, result.DirCount, result.TotalBytes)
	fmt.Printf("BackupID: %s\n", *backupID)
	fmt.Printf("BackupTime: %d\n", backupTime)
	fmt.Printf("Mode: %s\n", detMode)

	return nil
}

type repoInfo struct {
	host      string
	port      int
	datastore string
	token     string
}

func parseRepo(s string) *repoInfo {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return nil
	}
	userHost := parts[0]
	portStr := parts[1]
	ds := parts[2]

	hostPart := userHost
	if at := strings.LastIndex(userHost, "@"); at >= 0 {
		hostPart = userHost[at+1:]
	}

	port := 8007
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		port = 8007
	}

	token := os.Getenv("PBS_TOKEN")
	if token == "" {
		token = os.Getenv("PBS_AUTH_TOKEN")
	}

	return &repoInfo{
		host:      hostPart,
		port:      port,
		datastore: ds,
		token:     token,
	}
}

func runKeygen() error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	keyPassword := fs.String("password", "", "password to protect the key file")
	fs.Parse(os.Args[2:])

	var keyData []byte
	var err error
	if *keyPassword != "" {
		keyData, err = datastore.GenerateKeyFile(*keyPassword)
	} else {
		key, err2 := datastore.CreateRandomKey()
		if err2 != nil {
			return err2
		}
		cc, err2 := datastore.NewCryptConfig(key)
		if err2 != nil {
			return err2
		}
		fp := cc.Fingerprint()
		kc := &datastore.KeyConfig{
			Kdf:         datastore.KeyDerivationConfig{Type: "none"},
			Data:        key[:],
			Fingerprint: datastore.FormatFingerprint(fp),
		}
		keyData, err = json.MarshalIndent(kc, "", "  ")
	}
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	fmt.Print(string(keyData))
	return nil
}

func openArchiveReader(path string) (transfer.ArchiveReader, error) {
	// Determine format from extension
	switch {
	case strings.HasSuffix(path, ".mpxar.didx"):
		// Split archive metadata — need payload too
		return nil, fmt.Errorf("split archive requires both .mpxar.didx and .ppxar.didx; use the .mpxar.didx path and provide --payload")
	case strings.HasSuffix(path, ".ppxar.didx"):
		// Split archive payload — need metadata too
		return nil, fmt.Errorf("split archive requires both .mpxar.didx and .ppxar.didx; use the .mpxar.didx path and provide --payload")
	case strings.HasSuffix(path, ".pxar.didx"):
		// Chunked v1 archive
		idxData, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read index: %w", err)
		}
		// Use the chunk store relative to the index file
		baseDir := filepath.Dir(path)
		chunkStore, err := datastore.NewChunkStore(baseDir)
		if err != nil {
			return nil, fmt.Errorf("create chunk store: %w", err)
		}
		chunkSource := datastore.NewChunkStoreSource(chunkStore)
		return transfer.NewChunkedArchiveReader(idxData, chunkSource)
	case strings.HasSuffix(path, ".pxar"):
		// Standalone v1 archive
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open archive: %w", err)
		}
		return transfer.NewFileArchiveReader(f), nil
	default:
		// Try as standalone .pxar
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open archive: %w", err)
		}
		return transfer.NewFileArchiveReader(f), nil
	}
}

func runLs() error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	fs.Parse(os.Args[2:])

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: pxar-cli ls <archive> [path]")
	}
	archivePath := fs.Arg(0)
	listPath := ""
	if fs.NArg() > 1 {
		listPath = fs.Arg(1)
	}

	reader, err := openArchiveReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	if listPath == "" || listPath == "/" {
		root, err := reader.ReadRoot()
		if err != nil {
			return fmt.Errorf("read root: %w", err)
		}
		entries, err := reader.ListDirectory(int64(root.ContentOffset))
		if err != nil {
			return fmt.Errorf("list root: %w", err)
		}
		for _, e := range entries {
			fmt.Printf("%s\n", e.Path)
		}
	} else {
		entry, err := reader.Lookup(listPath)
		if err != nil {
			return fmt.Errorf("lookup %q: %w", listPath, err)
		}
		if entry.IsDir() {
			entries, err := reader.ListDirectory(int64(entry.ContentOffset))
			if err != nil {
				return fmt.Errorf("list directory: %w", err)
			}
			for _, e := range entries {
				fmt.Printf("%s\n", e.Path)
			}
		} else {
			fmt.Printf("%s (size=%d)\n", entry.Path, entry.FileSize)
		}
	}
	return nil
}

func runExtract() error {
	fs := flag.NewFlagSet("extract", flag.ExitOnError)
	output := fs.String("o", "", "output file (default: stdout)")
	fs.Parse(os.Args[2:])

	if fs.NArg() < 2 {
		return fmt.Errorf("usage: pxar-cli extract <archive> <path> [-o output]")
	}
	archivePath := fs.Arg(0)
	filePath := fs.Arg(1)

	reader, err := openArchiveReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	entry, err := reader.Lookup(filePath)
	if err != nil {
		return fmt.Errorf("lookup %q: %w", filePath, err)
	}

	if !entry.IsRegularFile() {
		return fmt.Errorf("%q is not a regular file (kind: %v)", filePath, entry.Kind)
	}

	content, err := reader.ReadFileContent(entry)
	if err != nil {
		return fmt.Errorf("read file content: %w", err)
	}

	if *output != "" {
		if err := os.WriteFile(*output, content, 0o644); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Extracted %d bytes to %s\n", len(content), *output)
	} else {
		os.Stdout.Write(content)
	}
	return nil
}

func runCp() error {
	fs := flag.NewFlagSet("cp", flag.ExitOnError)
	formatFlag := fs.String("format", "v1", "output format: v1 or v2")
	outputPath := fs.String("o", "", "output file (required)")
	fs.Parse(os.Args[2:])

	if fs.NArg() < 2 || *outputPath == "" {
		return fmt.Errorf("usage: pxar-cli cp <source_archive> <src_path> [dst_path] -o <output_archive> [-format v1|v2]")
	}
	srcPath := fs.Arg(0)
	srcFilePath := fs.Arg(1)
	dstFilePath := srcFilePath
	if fs.NArg() >= 3 {
		dstFilePath = fs.Arg(2)
	}

	reader, err := openArchiveReader(srcPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	var targetFormat format.FormatVersion = format.FormatVersion1
	if *formatFlag == "v2" {
		targetFormat = format.FormatVersion2
	}

	outFile, err := os.Create(*outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer outFile.Close()

	writer := transfer.NewStreamArchiveWriter(outFile)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: targetFormat}); err != nil {
		return fmt.Errorf("begin writer: %w", err)
	}

	err = transfer.Copy(reader, writer, []transfer.PathMapping{{Src: srcFilePath, Dst: dstFilePath}}, transfer.TransferOption{
		TargetFormat: targetFormat,
	})
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}
	return nil
}

func runMerge() error {
	fs := flag.NewFlagSet("merge", flag.ExitOnError)
	formatFlag := fs.String("format", "v1", "output format: v1 or v2")
	outputPath := fs.String("o", "", "output file (required)")
	fs.Parse(os.Args[2:])

	if fs.NArg() < 1 || *outputPath == "" {
		return fmt.Errorf("usage: pxar-cli merge <source_archive> -o <output_archive> [-format v1|v2]")
	}
	srcPath := fs.Arg(0)

	reader, err := openArchiveReader(srcPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	var targetFormat format.FormatVersion = format.FormatVersion1
	if *formatFlag == "v2" {
		targetFormat = format.FormatVersion2
	}

	outFile, err := os.Create(*outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer outFile.Close()

	writer := transfer.NewStreamArchiveWriter(outFile)
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: targetFormat}); err != nil {
		return fmt.Errorf("begin writer: %w", err)
	}

	if err := transfer.CopyTree(reader, writer, "/", "/", transfer.TransferOption{
		TargetFormat: targetFormat,
	}); err != nil {
		return fmt.Errorf("merge: %w", err)
	}

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: pxar-cli <command> [options]\n")
		fmt.Fprintf(os.Stderr, "Commands: backup, keygen, ls, extract, cp, merge\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "backup":
		if err := runBackup(); err != nil {
			log.Fatal(err)
		}
	case "keygen":
		if err := runKeygen(); err != nil {
			log.Fatal(err)
		}
	case "ls":
		if err := runLs(); err != nil {
			log.Fatal(err)
		}
	case "extract":
		if err := runExtract(); err != nil {
			log.Fatal(err)
		}
	case "cp":
		if err := runCp(); err != nil {
			log.Fatal(err)
		}
	case "merge":
		if err := runMerge(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
