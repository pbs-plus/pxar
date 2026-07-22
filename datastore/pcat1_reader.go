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
	Name      string
	Children  []CatalogTreeEntry
	EntryType CatalogEntryType
	Size      uint64
	Mtime     int64
}

type CatalogReader struct {
	r io.Reader
}

func NewCatalogReader(data []byte) *CatalogReader {
	return &CatalogReader{r: bytes.NewReader(data)}
}

func ReadCatalogTree(data []byte) (*CatalogTreeEntry, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("catalog too short: %d bytes", len(data))
	}

	var magic [8]byte
	copy(magic[:], data[0:8])
	if magic != CatalogMagic {
		return nil, fmt.Errorf("invalid catalog magic: %x", magic)
	}

	rootStart := int64(binary.LittleEndian.Uint64(data[len(data)-8:]))
	cr := &CatalogReader{r: bytes.NewReader(data)}
	return cr.readDir(data, rootStart, "")
}

func (cr *CatalogReader) readDir(data []byte, dirStart int64, name string) (*CatalogTreeEntry, error) {
	if dirStart < 8 || dirStart >= int64(len(data))-8 {
		return nil, fmt.Errorf("invalid dir position: %d", dirStart)
	}

	r := bytes.NewReader(data[dirStart:])

	tableLen, err := catalogDecodeU64FromReader(r)
	if err != nil {
		return nil, fmt.Errorf("decode table length: %w", err)
	}
	table := make([]byte, tableLen)
	if _, err := io.ReadFull(r, table); err != nil {
		return nil, fmt.Errorf("read dir block: %w", err)
	}

	tr := bytes.NewReader(table)
	count, err := catalogDecodeU64FromReader(tr)
	if err != nil {
		return nil, fmt.Errorf("decode entry count: %w", err)
	}

	node := &CatalogTreeEntry{EntryType: CatalogEntryTypeDir, Name: name}

	for i := range count {
		var entryTypeByte [1]byte
		if _, err := io.ReadFull(tr, entryTypeByte[:]); err != nil {
			return nil, fmt.Errorf("read entry type [%d]: %w", i, err)
		}

		nameLen, err := catalogDecodeU64FromReader(tr)
		if err != nil {
			return nil, fmt.Errorf("decode entry name len [%d]: %w", i, err)
		}
		if nameLen > 1<<20 {
			return nil, fmt.Errorf("entry name too long: %d", nameLen)
		}
		entryName := make([]byte, nameLen)
		if _, err := io.ReadFull(tr, entryName); err != nil {
			return nil, fmt.Errorf("read entry name [%d]: %w", i, err)
		}

		switch entryTypeByte[0] {
		case CatalogEntryFile:
			size, err := catalogDecodeU64FromReader(tr)
			if err != nil {
				return nil, fmt.Errorf("decode file size [%d]: %w", i, err)
			}
			mtime, err := catalogDecodeI64FromReader(tr)
			if err != nil {
				return nil, fmt.Errorf("decode file mtime [%d]: %w", i, err)
			}
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeFile,
				Name:      string(entryName),
				Size:      size,
				Mtime:     mtime,
			})

		case CatalogEntryDir:
			relOff, err := catalogDecodeU64FromReader(tr)
			if err != nil {
				return nil, fmt.Errorf("decode dir offset [%d]: %w", i, err)
			}
			childDir, err := cr.readDir(data, dirStart-int64(relOff), string(entryName))
			if err != nil {
				return nil, fmt.Errorf("decode child dir %q: %w", string(entryName), err)
			}
			node.Children = append(node.Children, *childDir)

		case CatalogEntrySymlink:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeSymlink,
				Name:      string(entryName),
			})

		case CatalogEntryHardlink:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeHardlink,
				Name:      string(entryName),
			})

		case CatalogEntryBlockDev:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeBlockDev,
				Name:      string(entryName),
			})

		case CatalogEntryCharDev:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeCharDev,
				Name:      string(entryName),
			})

		case CatalogEntryFifo:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeFifo,
				Name:      string(entryName),
			})

		case CatalogEntrySocket:
			node.Children = append(node.Children, CatalogTreeEntry{
				EntryType: CatalogEntryTypeSocket,
				Name:      string(entryName),
			})

		default:
			return nil, fmt.Errorf("unknown entry type: 0x%02x", entryTypeByte[0])
		}
	}

	if tr.Len() != 0 {
		return nil, fmt.Errorf("unable to parse whole catalog data block: %d trailing bytes", tr.Len())
	}

	return node, nil
}

func catalogDecodeU64FromReader(r io.Reader) (uint64, error) {
	var val uint64
	for i := range 10 {
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
	for i := range 11 {
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
		return 0, fmt.Errorf("i64 varint overflow")
	}
	return -int64(val), nil
}
