package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
)

type osFS struct{}

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
		return ioReadAll(f)
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

func ioReadAll(f *os.File) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 64*1024)
	for {
		n, err := f.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
	}
	return buf, nil
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

	chunkCfg, err := buzhash.NewConfig(4096)
	if err != nil {
		return err
	}

	pbsCfg := backupproxy.PBSConfig{
		BaseURL:       fmt.Sprintf("https://%s:%d/api2/json", parsed.host, parsed.port),
		Datastore:     parsed.datastore,
		AuthToken:     parsed.token,
		SkipTLSVerify: *fingerprint != "",
	}

	store := backupproxy.NewPBSRemoteStore(pbsCfg, chunkCfg, false)
	client := backupproxy.NewLocalClient(&osFS{})
	srv := backupproxy.NewServer(client, store)

	detMode := parseMode(*mode)
	backupTime := time.Now().Unix()

	cfg := backupproxy.BackupConfig{
		BackupType:    datastore.BackupHost,
		BackupID:      *backupID,
		BackupTime:    backupTime,
		DetectionMode: detMode,
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

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: pxar-cli <command> [options]\n")
		fmt.Fprintf(os.Stderr, "Commands: backup\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "backup":
		if err := runBackup(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}
