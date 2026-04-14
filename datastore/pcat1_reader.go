package datastore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type CatalogEntryType int

const (
	CatalogEntryTypeFile CatalogEntryType = iota
	CatalogEntryTypeDir
	CatalogEntryTypeSymlink
	CatalogEntryTypeHardlink
	CatalogEntryTypeBlockDev
	CatalogEntryTypeCharDev
	CatalogEntryTypeFifo
	CatalogEntryTypeSocket
)

type CatalogTreeEntry struct {
	EntryType CatalogEntryType
	Name      string
	Size      uint64
	Mtime     int64
	Children  []CatalogTreeEntry
}

type CatalogReader struct {
	r   io.Reader
	pos int64
}

func NewCatalogReader(data []byte) *CatalogReader {
	return &CatalogReader{r: bytes.NewReader(data), pos: 0}
}

func ReadCatalogTree(data []byte) (*CatalogTreeEntry, error) {
	cr := NewCatalogReader(data)

	var magic [8]byte
	if _, err := io.ReadFull(cr.r, magic[:]); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	cr.pos += 8

	if magic != CatalogMagic {
		return nil, fmt.Errorf("invalid catalog magic: %x", magic)
	}

	rootPosData := data[len(data)-8:]
	rootPos := int64(binary.LittleEndian.Uint64(rootPosData))

	return cr.readDir(data, rootPos)
}

func (cr *CatalogReader) readDir(data []byte, dirStartPos int64) (*CatalogTreeEntry, error) {
	if dirStartPos < 8 || dirStartPos >= int64(len(data))-8 {
		return nil, fmt.Errorf("invalid dir position: %d", dirStartPos)
	}

	r := bytes.NewReader(data[dirStartPos:])

	entryCount, err := catalogDecodeU64FromReader(r)
	if err != nil {
		return nil, fmt.Errorf("decode entry count: %w", err)
	}

	dirNameLen, err := catalogDecodeU64FromReader(r)
	if err != nil {
		return nil, fmt.Errorf("decode dir name len: %w", err)
	}

	dirName := make([]byte, dirNameLen)
	if dirNameLen > 0 {
		if _, err := io.ReadFull(r, dirName); err != nil {
			return nil, fmt.Errorf("read dir name: %w", err)
		}
	}

	root := &CatalogTreeEntry{
		EntryType: CatalogEntryTypeDir,
		Name:      string(dirName),
	}

	for i := uint64(0); i < entryCount; i++ {
		nameLen, err := catalogDecodeU64FromReader(r)
		if err != nil {
			return nil, fmt.Errorf("decode entry name len [%d]: %w", i, err)
		}

		var entryTypeByte [1]byte
		if _, err := io.ReadFull(r, entryTypeByte[:]); err != nil {
			return nil, fmt.Errorf("read entry type [%d]: %w", i, err)
		}

		name := make([]byte, nameLen)
		if nameLen > 0 {
			if _, err := io.ReadFull(r, name); err != nil {
				return nil, fmt.Errorf("read entry name [%d]: %w", i, err)
			}
		}

		switch entryTypeByte[0] {
		case CatalogEntryFile:
			size, err := catalogDecodeU64FromReader(r)
			if err != nil {
				return nil, fmt.Errorf("decode file size: %w", err)
			}
			mtime, err := catalogDecodeI64FromReader(r)
			if err != nil {
				return nil, fmt.Errorf("decode file mtime: %w", err)
			}
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeFile,
				Name:      string(name),
				Size:      size,
				Mtime:     mtime,
			})

		case CatalogEntryDir:
			offset, err := catalogDecodeU64FromReader(r)
			if err != nil {
				return nil, fmt.Errorf("decode dir offset: %w", err)
			}
			childDir, err := cr.readDir(data, int64(offset))
			if err != nil {
				return nil, fmt.Errorf("decode child dir %q: %w", string(name), err)
			}
			root.Children = append(root.Children, *childDir)

		case CatalogEntrySymlink:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeSymlink,
				Name:      string(name),
			})

		case CatalogEntryHardlink:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeHardlink,
				Name:      string(name),
			})

		case CatalogEntryBlockDev:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeBlockDev,
				Name:      string(name),
			})

		case CatalogEntryCharDev:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeCharDev,
				Name:      string(name),
			})

		case CatalogEntryFifo:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeFifo,
				Name:      string(name),
			})

		case CatalogEntrySocket:
			root.Children = append(root.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeSocket,
				Name:      string(name),
			})

		default:
			return nil, fmt.Errorf("unknown entry type: 0x%02x", entryTypeByte[0])
		}
	}

	return root, nil
}

func catalogDecodeU64FromReader(r io.Reader) (uint64, error) {
	var val uint64
	for i := 0; i < 10; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		val |= uint64(b[0]&0x7f) << (i * 7)
		if b[0] < 128 {
			return val, nil
		}
	}
	return 0, fmt.Errorf("varint overflow")
}

func catalogDecodeI64FromReader(r io.Reader) (int64, error) {
	var val uint64
	negative := false
	for i := 0; i < 10; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		if b[0] == 0x00 {
			negative = true
			break
		}
		if b[0]&0x80 != 0 {
			val |= uint64(b[0]&0x7f) << (i * 7)
		} else {
			val |= uint64(b[0]) << (i * 7)
			return int64(val), nil
		}
	}
	if !negative {
		return int64(val), nil
	}
	return -int64(val), nil
}
