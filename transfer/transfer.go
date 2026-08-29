package transfer

import (
	"fmt"
	"math"
	"path"
	"sort"
	"strings"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

func Copy(src ArchiveReader, dst ArchiveWriter, mappings []PathMapping, opts CopyOption) error {
	payloadIdx, chunkSource, sourceCryptConfig, hasChunks := splitPayloadSource(src)
	if opts.SourceCryptConfig == nil && sourceCryptConfig != nil {
		opts.SourceCryptConfig = sourceCryptConfig
		if opts.SourceCryptMode == "" || opts.SourceCryptMode == datastore.CryptModeNone {
			opts.SourceCryptMode = datastore.CryptModeEncrypt
		}
	}
	payloadDst, canWritePayload := dst.(interface{ WritePayload([]byte) error })
	optimized := hasChunks && canWritePayload && dst.Encoder() != nil
	nextPayload := uint64(0)
	if optimized {
		nextPayload = dst.Encoder().PayloadPosition()
	}
	emit := copyEmitter{
		source:             src,
		destination:        dst,
		optimized:          optimized,
		nextPayload:        nextPayload,
		targetOffsets:      make(map[uint64]copiedTarget),
		payloadIndex:       payloadIdx,
		chunkSource:        chunkSource,
		payloadDestination: payloadDst,
		sourceCryptConfig:  sourceCryptConfig,
		opts:               opts,
	}
	if len(mappings) == 1 {
		if err := emit.mapping(mappings[0]); err != nil {
			return err
		}
		return emit.flushPayload()
	}
	groups, grouped, err := groupMappings(src, mappings)
	if err != nil {
		return err
	}
	if !grouped {
		groups = []mappingGroup{{mappings: mappings}}
	}
	for _, group := range groups {
		if len(group.mappings) == 1 {
			if err := emit.mapping(group.mappings[0]); err != nil {
				return err
			}
			continue
		}
		plan := copyPlan{
			source:    src,
			overwrite: opts.Overwrite,
			root: &copyNode{
				children: make(map[string]*copyNode),
				order:    math.MaxUint64,
			},
		}
		for _, mapping := range group.mappings {
			if err := plan.addMapping(mapping); err != nil {
				return err
			}
		}
		if err := emit.children(plan.root); err != nil {
			return err
		}
	}
	return emit.flushPayload()
}

func CopyTree(src ArchiveReader, dst ArchiveWriter, srcPath, dstPath string, opts CopyOption) error {
	return Copy(src, dst, []PathMapping{{Src: srcPath, Dst: dstPath}}, opts)
}

type mappingGroup struct {
	mappings []PathMapping
	name     string
	order    uint64
}

func groupMappings(source ArchiveReader, mappings []PathMapping) ([]mappingGroup, bool, error) {
	byName := make(map[string]*mappingGroup)
	for _, mapping := range mappings {
		sourcePath, err := cleanArchivePath(mapping.Src)
		if err != nil {
			return nil, false, fmt.Errorf("invalid source path %q: %w", mapping.Src, err)
		}
		destinationPath := mapping.Dst
		if destinationPath == "" {
			destinationPath = sourcePath
		}
		destinationPath, err = cleanArchivePath(destinationPath)
		if err != nil {
			return nil, false, fmt.Errorf("invalid destination path %q: %w", mapping.Dst, err)
		}
		parts := pxar.SplitPath(destinationPath)
		if len(parts) == 0 {
			return nil, false, nil
		}
		entry, err := source.Lookup(sourcePath)
		if err != nil {
			return nil, false, fmt.Errorf("lookup %q in source: %w", sourcePath, err)
		}
		group := byName[parts[0]]
		if group == nil {
			group = &mappingGroup{name: parts[0], order: entry.FileOffset}
			byName[parts[0]] = group
		}
		group.order = min(group.order, entry.FileOffset)
		group.mappings = append(group.mappings, mapping)
	}
	groups := make([]mappingGroup, 0, len(byName))
	for _, group := range byName {
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].order != groups[j].order {
			return groups[i].order < groups[j].order
		}
		return groups[i].name < groups[j].name
	})
	return groups, true, nil
}

type copyPlan struct {
	source    ArchiveReader
	root      *copyNode
	overwrite bool
}

type copyNode struct {
	entry    *pxar.Entry
	source   *pxar.Entry
	children map[string]*copyNode
	order    uint64
}

func (p *copyPlan) addMapping(mapping PathMapping) error {
	srcPath, err := cleanArchivePath(mapping.Src)
	if err != nil {
		return fmt.Errorf("invalid source path %q: %w", mapping.Src, err)
	}
	dstPath := mapping.Dst
	if dstPath == "" {
		dstPath = srcPath
	}
	dstPath, err = cleanArchivePath(dstPath)
	if err != nil {
		return fmt.Errorf("invalid destination path %q: %w", mapping.Dst, err)
	}

	entry, err := p.source.Lookup(srcPath)
	if err != nil {
		return fmt.Errorf("lookup %q in source: %w", srcPath, err)
	}
	if !entry.IsDir() && dstPath == "/" {
		return fmt.Errorf("cannot map non-directory %q to archive root", srcPath)
	}
	return p.collect(entry, dstPath, entry.IsDir() && dstPath == "/")
}

func (p *copyPlan) collect(source *pxar.Entry, destination string, contentsOnly bool) error {
	if source.IsDir() && contentsOnly {
		return p.source.ListDirectory(int64(source.ContentOffset), accessor.ListOption{}, func(child *pxar.Entry) error {
			copy := cloneEntry(child, path.Join(destination, child.FileName()))
			return p.collect(copy, copy.Path, false)
		})
	}

	entry := cloneEntry(source, destination)
	node, err := p.insert(entry, source)
	if err != nil {
		return err
	}
	if !source.IsDir() {
		return nil
	}
	if node.children == nil {
		node.children = make(map[string]*copyNode)
	}
	return p.source.ListDirectory(int64(source.ContentOffset), accessor.ListOption{}, func(child *pxar.Entry) error {
		copy := cloneEntry(child, path.Join(destination, child.FileName()))
		return p.collect(copy, copy.Path, false)
	})
}

func (p *copyPlan) insert(entry, source *pxar.Entry) (*copyNode, error) {
	parts := pxar.SplitPath(entry.Path)
	if len(parts) == 0 {
		return p.root, nil
	}

	current := p.root
	currentPath := ""
	for _, name := range parts[:len(parts)-1] {
		currentPath = path.Join(currentPath, name)
		next := current.children[name]
		if next == nil {
			meta := pxar.DirMetadata(0o755).Build()
			synthetic := &pxar.Entry{Path: "/" + currentPath, Kind: pxar.KindDirectory, Metadata: meta}
			synthetic.SetFileName(name)
			next = &copyNode{entry: synthetic, children: make(map[string]*copyNode), order: source.FileOffset}
			current.children[name] = next
		} else if !next.entry.IsDir() {
			return nil, fmt.Errorf("destination parent %q is not a directory", next.entry.Path)
		}
		next.order = min(next.order, source.FileOffset)
		current.order = min(current.order, source.FileOffset)
		current = next
	}

	name := parts[len(parts)-1]
	existing := current.children[name]
	if existing != nil {
		if existing.source != nil && existing.source.FileOffset == source.FileOffset && existing.entry.Kind == entry.Kind {
			return existing, nil
		}
		if existing.entry.IsDir() && entry.IsDir() {
			if existing.source == nil || p.overwrite {
				existing.entry = entry
				existing.source = source
			}
			existing.order = min(existing.order, source.FileOffset)
			return existing, nil
		}
		if !p.overwrite {
			return nil, fmt.Errorf("destination path %q selected more than once", entry.Path)
		}
	}

	node := &copyNode{entry: entry, source: source, order: source.FileOffset}
	if entry.IsDir() {
		node.children = make(map[string]*copyNode)
	}
	current.children[name] = node
	current.order = min(current.order, source.FileOffset)
	return node, nil
}

func cleanArchivePath(value string) (string, error) {
	if value == "" {
		return "/", nil
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return "", fmt.Errorf("parent traversal is not allowed")
		}
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	return path.Clean(value), nil
}

func cloneEntry(source *pxar.Entry, destination string) *pxar.Entry {
	copy := *source
	copy.Path = destination
	copy.SetFileName(path.Base(destination))
	return &copy
}

func sortedChildren(node *copyNode) []*copyNode {
	children := make([]*copyNode, 0, len(node.children))
	for _, child := range node.children {
		children = append(children, child)
	}
	sort.Slice(children, func(i, j int) bool {
		if children[i].order != children[j].order {
			return children[i].order < children[j].order
		}
		return children[i].entry.Path < children[j].entry.Path
	})
	return children
}

type copiedTarget struct {
	path   string
	offset encoder.LinkOffset
}

type copyEmitter struct {
	source             ArchiveReader
	destination        ArchiveWriter
	optimized          bool
	nextPayload        uint64
	targetOffsets      map[uint64]copiedTarget
	payloadIndex       *datastore.DynamicIndexReader
	chunkSource        datastore.ChunkSource
	payloadDestination rawPayloadWriter
	sourceCryptConfig  *datastore.CryptConfig
	pendingPayload     bool
	pendingSourceStart uint64
	pendingSourceEnd   uint64
	pendingTargetStart uint64
	opts               CopyOption
}

func (e *copyEmitter) mapping(mapping PathMapping) error {
	sourcePath, err := cleanArchivePath(mapping.Src)
	if err != nil {
		return fmt.Errorf("invalid source path %q: %w", mapping.Src, err)
	}
	destinationPath := mapping.Dst
	if destinationPath == "" {
		destinationPath = sourcePath
	}
	destinationPath, err = cleanArchivePath(destinationPath)
	if err != nil {
		return fmt.Errorf("invalid destination path %q: %w", mapping.Dst, err)
	}
	source, err := e.source.Lookup(sourcePath)
	if err != nil {
		return fmt.Errorf("lookup %q in source: %w", sourcePath, err)
	}
	if !source.IsDir() && destinationPath == "/" {
		return fmt.Errorf("cannot map non-directory %q to archive root", sourcePath)
	}
	if source.IsDir() && destinationPath == "/" {
		return e.source.ListDirectory(int64(source.ContentOffset), accessor.ListOption{}, func(child *pxar.Entry) error {
			return e.stream(cloneEntry(child, path.Join(destinationPath, child.FileName())), child)
		})
	}

	parts := pxar.SplitPath(destinationPath)
	parentDepth := max(len(parts)-1, 0)
	for _, name := range parts[:parentDepth] {
		meta := pxar.DirMetadata(0o755).Build()
		if err := e.destination.BeginDirectory(name, &meta); err != nil {
			return fmt.Errorf("begin destination parent %q: %w", name, err)
		}
	}
	if err := e.stream(cloneEntry(source, destinationPath), source); err != nil {
		return err
	}
	for range parentDepth {
		if err := e.destination.EndDirectory(); err != nil {
			return fmt.Errorf("end destination parent: %w", err)
		}
	}
	return nil
}

func (e *copyEmitter) stream(entry, source *pxar.Entry) error {
	if entry.IsDir() {
		if err := e.destination.BeginDirectory(entry.FileName(), &entry.Metadata); err != nil {
			return fmt.Errorf("begin directory %q: %w", entry.Path, err)
		}
		if err := e.source.ListDirectory(int64(source.ContentOffset), accessor.ListOption{}, func(child *pxar.Entry) error {
			return e.stream(cloneEntry(child, path.Join(entry.Path, child.FileName())), child)
		}); err != nil {
			return err
		}
		if err := e.destination.EndDirectory(); err != nil {
			return fmt.Errorf("end directory %q: %w", entry.Path, err)
		}
		return nil
	}
	node := copyNode{entry: entry, source: source}
	if entry.IsHardlink() {
		return e.hardlink(&node)
	}
	if entry.IsRegularFile() {
		return e.regular(entry, source)
	}
	if err := e.destination.WriteEntry(entry, nil); err != nil {
		return fmt.Errorf("write %q: %w", entry.Path, err)
	}
	return nil
}

func (e *copyEmitter) children(parent *copyNode) error {
	for _, node := range sortedChildren(parent) {
		if err := e.node(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *copyEmitter) node(node *copyNode) error {
	if node.entry.IsDir() {
		if err := e.destination.BeginDirectory(node.entry.FileName(), &node.entry.Metadata); err != nil {
			return fmt.Errorf("begin directory %q: %w", node.entry.Path, err)
		}
		if err := e.children(node); err != nil {
			return err
		}
		if err := e.destination.EndDirectory(); err != nil {
			return fmt.Errorf("end directory %q: %w", node.entry.Path, err)
		}
		return nil
	}

	if node.entry.IsHardlink() {
		return e.hardlink(node)
	}
	if node.entry.IsRegularFile() {
		return e.regular(node.entry, node.source)
	}
	if err := e.destination.WriteEntry(node.entry, nil); err != nil {
		return fmt.Errorf("write %q: %w", node.entry.Path, err)
	}
	return nil
}

func (e *copyEmitter) hardlink(node *copyNode) error {
	if node.source.FileOffset >= node.source.LinkOffset {
		targetSourceOffset := node.source.FileOffset - node.source.LinkOffset
		if target, ok := e.targetOffsets[targetSourceOffset]; ok {
			writer, ok := e.destination.(interface {
				WriteHardlink(string, string, encoder.LinkOffset) error
			})
			if ok {
				if err := writer.WriteHardlink(node.entry.FileName(), strings.TrimPrefix(target.path, "/"), target.offset); err != nil {
					return fmt.Errorf("write hardlink %q: %w", node.entry.Path, err)
				}
				return nil
			}
		}
	}

	if node.source.FileOffset < node.source.LinkOffset {
		return fmt.Errorf("hardlink %q has invalid target offset", node.source.Path)
	}
	targetOffset := node.source.FileOffset - node.source.LinkOffset
	target, err := e.source.ReadEntryAt(int64(targetOffset))
	if err != nil {
		return fmt.Errorf("resolve hardlink %q: %w", node.source.Path, err)
	}
	if !target.IsRegularFile() {
		return fmt.Errorf("hardlink %q target is not a regular file", node.source.Path)
	}
	materialized := cloneEntry(target, node.entry.Path)
	return e.regular(materialized, target)
}

func (e *copyEmitter) regular(entry, source *pxar.Entry) error {
	if !e.optimized {
		reader, err := e.source.ReadFileContentReader(source)
		if err != nil {
			return fmt.Errorf("open file content for %q: %w", source.Path, err)
		}
		defer reader.Close()
		if err := e.destination.WriteEntryReader(entry, reader, source.FileSize); err != nil {
			return fmt.Errorf("write file %q: %w", entry.Path, err)
		}
	} else {
		span, err := payloadSpanSize(source.FileSize)
		if err != nil {
			return fmt.Errorf("file %q: %w", source.Path, err)
		}
		if e.nextPayload > math.MaxUint64-span || source.PayloadOffset > math.MaxUint64-span {
			return fmt.Errorf("payload range for %q overflows", source.Path)
		}
		sourceEnd := source.PayloadOffset + span
		contiguous := e.pendingPayload && source.PayloadOffset == e.pendingSourceEnd && e.nextPayload == e.pendingTargetStart+(e.pendingSourceEnd-e.pendingSourceStart)
		if e.pendingPayload && !contiguous {
			if err := e.flushPayload(); err != nil {
				return err
			}
		}
		if err := e.destination.WriteEntryRef(entry, e.nextPayload); err != nil {
			return fmt.Errorf("write payload reference %q: %w", entry.Path, err)
		}
		if contiguous {
			e.pendingSourceEnd = sourceEnd
		} else {
			e.pendingPayload = true
			e.pendingSourceStart = source.PayloadOffset
			e.pendingSourceEnd = sourceEnd
			e.pendingTargetStart = e.nextPayload
		}
		e.nextPayload += span
	}

	if _, exists := e.targetOffsets[source.FileOffset]; !exists {
		if offsetWriter, ok := e.destination.(interface {
			LastEntryOffset() (encoder.LinkOffset, bool)
		}); ok {
			if offset, ok := offsetWriter.LastEntryOffset(); ok {
				e.targetOffsets[source.FileOffset] = copiedTarget{path: entry.Path, offset: offset}
			}
		}
	}
	if e.opts.OnProgress != nil {
		e.opts.OnProgress(source.Path, source.FileSize)
	}
	return nil
}

func payloadSpanSize(fileSize uint64) (uint64, error) {
	if fileSize > math.MaxUint64-format.HeaderSize {
		return 0, fmt.Errorf("payload size overflows")
	}
	return format.HeaderSize + fileSize, nil
}

type rawPayloadWriter interface {
	WritePayload([]byte) error
}

func splitPayloadSource(reader ArchiveReader) (*datastore.DynamicIndexReader, datastore.ChunkSource, *datastore.CryptConfig, bool) {
	switch reader := reader.(type) {
	case *SplitReader:
		index := reader.PayloadIndex()
		if index == nil || reader.source == nil {
			return nil, nil, nil, false
		}
		source := reader.source
		var cryptConfig *datastore.CryptConfig
		if decrypt, ok := source.(*DecryptSource); ok {
			source = decrypt.inner
			cryptConfig = decrypt.cc
		}
		return index, source, cryptConfig, true
	case *PBSReader:
		return splitPayloadSource(reader.inner)
	case *DecryptingReader:
		return splitPayloadSource(reader.inner)
	default:
		return nil, nil, nil, false
	}
}

func (e *copyEmitter) flushPayload() error {
	if !e.pendingPayload {
		return nil
	}
	if e.destination.Encoder().PayloadPosition() != e.pendingTargetStart {
		return fmt.Errorf("target payload position %d does not match planned offset %d", e.destination.Encoder().PayloadPosition(), e.pendingTargetStart)
	}
	if err := writePayloadRange(
		e.payloadIndex,
		e.chunkSource,
		e.payloadDestination,
		e.destination,
		e.pendingSourceStart,
		e.pendingSourceEnd,
		cryptIdentityMatches(e.opts),
		e.sourceCryptConfig,
	); err != nil {
		return err
	}
	e.pendingPayload = false
	return nil
}

func cryptIdentityMatches(opts CopyOption) bool {
	sourceMode := opts.SourceCryptMode
	if sourceMode == "" {
		sourceMode = datastore.CryptModeNone
	}
	targetMode := opts.TargetCryptMode
	if targetMode == "" {
		targetMode = datastore.CryptModeNone
	}
	if sourceMode != targetMode {
		return false
	}
	if sourceMode == datastore.CryptModeNone {
		return opts.SourceCryptConfig == nil && opts.TargetCryptConfig == nil
	}
	if opts.SourceCryptConfig == nil || opts.TargetCryptConfig == nil {
		return false
	}
	return opts.SourceCryptConfig.Fingerprint() == opts.TargetCryptConfig.Fingerprint()
}

const replayChunkBatchSize = 64

func writePayloadRange(
	index *datastore.DynamicIndexReader,
	source datastore.ChunkSource,
	destination rawPayloadWriter,
	archiveWriter ArchiveWriter,
	start, end uint64,
	replay bool,
	cryptConfig *datastore.CryptConfig,
) error {
	if start >= end || end > index.IndexBytes() {
		return fmt.Errorf("payload range [%d,%d) exceeds source payload size %d", start, end, index.IndexBytes())
	}
	first, ok := index.ChunkFromOffset(start)
	if !ok {
		return fmt.Errorf("payload offset %d has no source chunk", start)
	}

	written := uint64(0)
	var replayBatch []backupproxy.KnownChunkRef
	flushReplay := func() error {
		if len(replayBatch) == 0 {
			return nil
		}
		if err := archiveWriter.InjectChunks(replayBatch); err != nil {
			return err
		}
		replayBatch = replayBatch[:0]
		return nil
	}
	for chunkIndex := first; chunkIndex < index.Count(); chunkIndex++ {
		info, ok := index.ChunkInfo(chunkIndex)
		if !ok || info.Start >= end {
			break
		}
		overlapStart := max(start, info.Start)
		overlapEnd := min(end, info.End)
		if overlapStart == info.Start && overlapEnd == info.End && replay {
			digest := info.Digest
			replayBatch = append(replayBatch, backupproxy.KnownChunkRef{
				Digest: digest,
				Size:   info.End - info.Start,
				LoadEncodedBlob: func() ([]byte, error) {
					blob, err := source.GetChunk(digest)
					if err != nil {
						return nil, fmt.Errorf("load source chunk %x: %w", digest[:8], err)
					}
					return blob, nil
				},
			})
			if len(replayBatch) == replayChunkBatchSize {
				if err := flushReplay(); err != nil {
					return fmt.Errorf("replay source chunks: %w", err)
				}
			}
		} else {
			if err := flushReplay(); err != nil {
				return fmt.Errorf("replay source chunks: %w", err)
			}
			blob, err := source.GetChunk(info.Digest)
			if err != nil {
				return fmt.Errorf("load source chunk %x: %w", info.Digest[:8], err)
			}
			decoded, err := decodeChunk(blob, cryptConfig)
			if err != nil {
				return fmt.Errorf("decode source chunk %x: %w", info.Digest[:8], err)
			}
			if uint64(len(decoded)) != info.End-info.Start {
				return fmt.Errorf("source chunk %x decoded to %d bytes, want %d", info.Digest[:8], len(decoded), info.End-info.Start)
			}
			from := overlapStart - info.Start
			to := overlapEnd - info.Start
			if err := destination.WritePayload(decoded[from:to]); err != nil {
				return fmt.Errorf("write isolated payload boundary: %w", err)
			}
		}
		written += overlapEnd - overlapStart
	}
	if err := flushReplay(); err != nil {
		return fmt.Errorf("replay source chunks: %w", err)
	}
	if written != end-start {
		return fmt.Errorf("wrote %d payload bytes, want %d", written, end-start)
	}
	return nil
}

func decodeChunk(blob []byte, cryptConfig *datastore.CryptConfig) ([]byte, error) {
	if len(blob) >= 8 {
		var magic [8]byte
		copy(magic[:], blob[:8])
		if datastore.IsEncryptedMagic(magic) {
			if cryptConfig == nil {
				return nil, fmt.Errorf("encrypted source chunk requires SourceCryptConfig")
			}
			return datastore.DecodeEncryptedBlob(nil, blob, cryptConfig)
		}
	}
	return datastore.DecodeBlob(nil, blob)
}

var _ ArchiveReader = (*FileReader)(nil)
var _ ArchiveReader = (*ChunkedReader)(nil)
var _ ArchiveReader = (*SplitReader)(nil)
var _ ArchiveReader = (*DecryptingReader)(nil)
var _ ArchiveReader = (*PBSReader)(nil)
var _ ArchiveWriter = (*StreamWriter)(nil)
var _ ArchiveWriter = (*RemoteDedupWriter)(nil)
var _ ArchiveWriter = (*SessionWriter)(nil)
