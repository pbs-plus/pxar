package transfer

import (
	"fmt"

	pxar "github.com/pbs-plus/pxar"
)

// walkFrame holds the iteration state for one directory level.
type walkFrame struct {
	entries []pxar.Entry
	idx     int
}

// TreeWalker provides a pull-based iterator for walking a pxar archive tree.
// It reuses a single Entry across all Next() calls, producing zero heap
// allocations per iteration.
//
// Example:
//
//	walker := transfer.NewTreeWalker(reader, transfer.WalkOption{
//	    MetaOnly: true,
//	    Filter:   transfer.WalkFiles | transfer.WalkDirs,
//	})
//	if err := walker.Init("/"); err != nil { ... }
//	for walker.Next() {
//	    entry := walker.Entry()
//	    // entry is reused each iteration — copy values you need to keep
//	}
//	if err := walker.Err(); err != nil { ... }
type TreeWalker struct {
	reader ArchiveReader
	opts   WalkOption
	stack  []walkFrame
	entry  pxar.Entry
	err    error
}

// NewTreeWalker creates a pull-based walker for the archive.
// Call Init to set the root path before calling Next.
func NewTreeWalker(reader ArchiveReader, opts WalkOption) *TreeWalker {
	return &TreeWalker{
		reader: reader,
		opts:   opts,
	}
}

// Init resolves the root entry and prepares the walker for iteration.
// Must be called before Next.
func (w *TreeWalker) Init(rootPath string) error {
	var root *pxar.Entry
	var err error
	if rootPath == "" || rootPath == "/" {
		root, err = w.reader.ReadRoot()
	} else {
		root, err = w.reader.Lookup(rootPath)
	}
	if err != nil {
		return fmt.Errorf("lookup root path %q: %w", rootPath, err)
	}

	// Apply filter to root — if root doesn't match and isn't a dir, nothing to do.
	// If root is a dir that doesn't match the filter, we still push it so its
	// children can be visited.
	if w.opts.Filter != 0 && !w.opts.Filter.matches(root.Kind) && root.Kind != pxar.KindDirectory {
		return nil
	}

	w.stack = []walkFrame{{entries: []pxar.Entry{*root}, idx: -1}}
	return nil
}

// Next advances to the next entry matching the walk filter.
// Returns false when there are no more entries or on error (check Err).
func (w *TreeWalker) Next() bool {
	for {
		if len(w.stack) == 0 {
			return false
		}

		frame := &w.stack[len(w.stack)-1]
		frame.idx++

		if frame.idx >= len(frame.entries) {
			// Exhausted this level, pop stack
			w.stack = w.stack[:len(w.stack)-1]
			continue
		}

		src := &frame.entries[frame.idx]

		// Directories are always descended into to find matching children.
		// Non-directory entries that don't match the filter are skipped.
		if src.Kind != pxar.KindDirectory {
			if w.opts.Filter != 0 && !w.opts.Filter.matches(src.Kind) {
				continue
			}
		}

		// Copy into reusable entry (struct copy — no heap allocation)
		w.entry = *src

		// If it's a directory, push its children for subsequent iterations.
		if src.Kind == pxar.KindDirectory {
			children, err := w.reader.ListDirectory(int64(src.ContentOffset))
			if err != nil {
				w.err = fmt.Errorf("list directory %q: %w", src.Path, err)
				return false
			}
			if len(children) > 0 {
				w.stack = append(w.stack, walkFrame{entries: children, idx: -1})
			}

			// If the directory itself doesn't match the filter, don't yield it.
			// We already pushed its children above so they'll still be visited.
			if w.opts.Filter != 0 && !w.opts.Filter.matches(src.Kind) {
				continue
			}
		}

		return true
	}
}

// Entry returns the current entry. The returned pointer is valid only until
// the next call to Next. The same Entry memory is reused each iteration.
func (w *TreeWalker) Entry() *pxar.Entry {
	return &w.entry
}

// Err returns the error that stopped iteration, if any.
func (w *TreeWalker) Err() error {
	return w.err
}
