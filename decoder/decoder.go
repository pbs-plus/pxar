// Package decoder reads pxar archives.
package decoder

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sonroyaalmerol/pxar/format"
	pxar "github.com/sonroyaalmerol/pxar"
)

// Decoder reads pxar archives sequentially.
type Decoder struct {
	input    io.Reader
	version  format.FormatVersion
	state    decoderState
	header   format.Header
	pathLens []int
	path     string
	payload  *limitedReader
	pending  []*pxar.Entry // buffered entries for version/prelude
}

type decoderState int

const (
	stateBegin decoderState = iota
	stateDefault
	stateInPayload
	stateInSpecialFile
	stateInDirectory
	stateInGoodbyeTable
	stateEOF
)

type limitedReader struct {
	reader io.Reader
	remain int64
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	if lr.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > lr.remain {
		p = p[:lr.remain]
	}
	n, err := lr.reader.Read(p)
	lr.remain -= int64(n)
	return n, err
}

// NewDecoder creates a new pxar decoder.
func NewDecoder(input io.Reader, payloadReader io.Reader) *Decoder {
	return &Decoder{
		input:   input,
		state:   stateBegin,
		version: format.FormatVersion1,
		path:    "/",
	}
}

// Next returns the next entry, or io.EOF when done.
func (d *Decoder) Next() (*pxar.Entry, error) {
	// Return any buffered entries first
	if len(d.pending) > 0 {
		e := d.pending[0]
		d.pending = d.pending[1:]
		return e, nil
	}

	switch d.state {
	case stateEOF:
		return nil, io.EOF
	case stateBegin:
		return d.readBegin()
	case stateDefault:
		return d.handleDefault()
	case stateInPayload:
		d.skipPayload()
		return d.handleDefault()
	case stateInSpecialFile:
		d.state = stateInDirectory
		return d.handleDirectory()
	case stateInDirectory:
		return d.handleDirectory()
	case stateInGoodbyeTable:
		return d.handleGoodbyeTable()
	}
	return nil, fmt.Errorf("unknown decoder state %d", d.state)
}

// Contents returns a reader for the current file's content.
func (d *Decoder) Contents() io.Reader {
	if d.payload != nil {
		return d.payload
	}
	return nil
}

func (d *Decoder) readBegin() (*pxar.Entry, error) {
	h, err := d.readHeader()
	if err != nil {
		return nil, err
	}
	d.header = h

	// Optional format version header
	if h.Type == format.PXARFormatVersion {
		verEntry, err := d.readFormatVersion()
		if err != nil {
			return nil, err
		}

		h2, err := d.readHeader()
		if err != nil {
			return nil, err
		}
		d.header = h2

		// Optional prelude
		if h2.Type == format.PXARPrelude {
			preludeEntry, err := d.readPreludeEntry()
			if err != nil {
				return nil, err
			}

			h3, err := d.readHeader()
			if err != nil {
				return nil, err
			}
			d.header = h3

			rootEntry, err := d.readEntryFromCurrentHeader()
			if err != nil {
				return nil, err
			}
			if d.state == stateBegin {
				d.state = stateDefault
			}
			// Buffer prelude and root, return version first
			d.pending = append(d.pending, preludeEntry, rootEntry)
			return verEntry, nil
		}

		rootEntry, err := d.readEntryFromCurrentHeader()
		if err != nil {
			return nil, err
		}
		if d.state == stateBegin {
			d.state = stateDefault
		}
		d.pending = append(d.pending, rootEntry)
		return verEntry, nil
	}

	// No version header, read root entry directly
	entry, err := d.readEntryFromCurrentHeader()
	if err != nil {
		return nil, err
	}
	if d.state == stateBegin {
		d.state = stateDefault
	}
	return entry, nil
}

func (d *Decoder) handleDefault() (*pxar.Entry, error) {
	h, err := d.readHeader()
	if err != nil {
		return nil, err
	}
	d.header = h
	return d.processDirectoryItem(h)
}

func (d *Decoder) handleDirectory() (*pxar.Entry, error) {
	return d.processDirectoryItem(d.header)
}

func (d *Decoder) processDirectoryItem(h format.Header) (*pxar.Entry, error) {
	switch h.Type {
	case format.PXARFilename:
		return d.handleFilename()
	case format.PXARGoodbye:
		d.state = stateInGoodbyeTable
		return d.handleGoodbyeTable()
	default:
		return nil, fmt.Errorf("expected FILENAME or GOODBYE, got %s", h.String())
	}
}

func (d *Decoder) handleGoodbyeTable() (*pxar.Entry, error) {
	contentSize := d.header.ContentSize()
	if contentSize > 0 {
		if _, err := io.CopyN(io.Discard, d.input, int64(contentSize)); err != nil {
			return nil, fmt.Errorf("skipping goodbye table: %w", err)
		}
	}

	if len(d.pathLens) == 0 {
		d.state = stateEOF
		return nil, io.EOF
	}

	d.pathLens = d.pathLens[:len(d.pathLens)-1]
	if len(d.pathLens) == 0 {
		d.state = stateEOF
		return nil, io.EOF
	}

	d.resetPath()
	d.state = stateDefault
	return d.Next()
}

func (d *Decoder) handleFilename() (*pxar.Entry, error) {
	data, err := d.readContent()
	if err != nil {
		return nil, err
	}
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	if err := format.CheckFilename(data); err != nil {
		return nil, err
	}

	// Reset path to current directory level before pushing new component
	d.resetPath()
	d.pushPath(string(data))

	h, err := d.readHeader()
	if err != nil {
		return nil, err
	}
	d.header = h
	return d.readEntryFromCurrentHeader()
}

func (d *Decoder) readEntryFromCurrentHeader() (*pxar.Entry, error) {
	switch d.header.Type {
	case format.PXARHardlink:
		return d.readHardlinkEntry()
	case format.PXAREntry:
		return d.readEntry()
	case format.PXAREntryV1:
		return d.readEntryV1()
	default:
		return nil, fmt.Errorf("unexpected entry header: %s", d.header.String())
	}
}

func (d *Decoder) readFormatVersion() (*pxar.Entry, error) {
	data, err := d.readContent()
	if err != nil {
		return nil, err
	}
	if len(data) != 8 {
		return nil, fmt.Errorf("invalid format version size: %d", len(data))
	}
	version := binary.LittleEndian.Uint64(data)
	v, err := format.DeserializeFormatVersion(version)
	if err != nil {
		return nil, err
	}
	d.version = v
	return &pxar.Entry{
		Kind:     pxar.KindVersion,
		Path:     "/",
		FileSize: uint64(v),
	}, nil
}

func (d *Decoder) readPreludeEntry() (*pxar.Entry, error) {
	data, err := d.readContent()
	if err != nil {
		return nil, err
	}
	return &pxar.Entry{
		Kind:       pxar.KindPrelude,
		Path:       "/",
		LinkTarget: string(data),
	}, nil
}

func (d *Decoder) readHardlinkEntry() (*pxar.Entry, error) {
	data, err := d.readContent()
	if err != nil {
		return nil, err
	}
	if len(data) <= 8 {
		return nil, fmt.Errorf("hardlink entry too small")
	}
	_ = binary.LittleEndian.Uint64(data[:8])
	target := data[8:]
	if len(target) > 0 && target[len(target)-1] == 0 {
		target = target[:len(target)-1]
	}
	return &pxar.Entry{
		Kind:       pxar.KindHardlink,
		Path:       d.path,
		LinkTarget: string(target),
	}, nil
}

func (d *Decoder) readEntry() (*pxar.Entry, error) {
	statData, err := d.readContent()
	if err != nil {
		return nil, err
	}
	if len(statData) != 40 {
		return nil, fmt.Errorf("invalid stat size: %d", len(statData))
	}

	stat := unmarshalStat(statData)
	entry := &pxar.Entry{
		Path:     d.path,
		Metadata: pxar.Metadata{Stat: stat},
	}

	for {
		h, err := d.readHeader()
		if err != nil {
			if err == io.EOF {
				if stat.IsFIFO() {
					entry.Kind = pxar.KindFifo
					return entry, nil
				}
				if stat.IsSocket() {
					entry.Kind = pxar.KindSocket
					return entry, nil
				}
				return nil, io.EOF
			}
			return nil, err
		}
		d.header = h

		done, err := d.readCurrentItem(entry)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
	}

	if entry.IsDir() {
		d.pathLens = append(d.pathLens, len(d.path))
	}

	return entry, nil
}

func (d *Decoder) readEntryV1() (*pxar.Entry, error) {
	data, err := d.readContent()
	if err != nil {
		return nil, err
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("invalid stat_v1 size: %d", len(data))
	}

	v1 := unmarshalStatV1(data)
	stat := v1.ToStat()
	entry := &pxar.Entry{
		Path:     d.path,
		Metadata: pxar.Metadata{Stat: stat},
	}

	for {
		h, err := d.readHeader()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		d.header = h

		done, err := d.readCurrentItem(entry)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
	}

	if entry.IsDir() {
		d.pathLens = append(d.pathLens, len(d.path))
	}

	return entry, nil
}

func (d *Decoder) readCurrentItem(entry *pxar.Entry) (bool, error) {
	h := d.header

	switch h.Type {
	case format.PXARXAttr:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		nameLen := 0
		for i, b := range data {
			if b == 0 {
				nameLen = i
				break
			}
		}
		entry.Metadata.XAttrs = append(entry.Metadata.XAttrs, format.XAttr{Data: data, NameLen: nameLen})
		return false, nil

	case format.PXARACLUser:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.ACL.Users = append(entry.Metadata.ACL.Users, format.ACLUser{
			UID:         binary.LittleEndian.Uint64(data[0:]),
			Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
		})
		return false, nil

	case format.PXARACLGroup:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.ACL.Groups = append(entry.Metadata.ACL.Groups, format.ACLGroup{
			GID:         binary.LittleEndian.Uint64(data[0:]),
			Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
		})
		return false, nil

	case format.PXARACLGroupObj:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		perms := format.ACLPermissions(binary.LittleEndian.Uint64(data))
		entry.Metadata.ACL.GroupObj = &format.ACLGroupObject{Permissions: perms}
		return false, nil

	case format.PXARACLDefault:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.ACL.Default = unmarshalACLDefault(data)
		return false, nil

	case format.PXARACLDefaultUser:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.ACL.DefaultUsers = append(entry.Metadata.ACL.DefaultUsers, format.ACLUser{
			UID:         binary.LittleEndian.Uint64(data[0:]),
			Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
		})
		return false, nil

	case format.PXARACLDefaultGroup:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.ACL.DefaultGroups = append(entry.Metadata.ACL.DefaultGroups, format.ACLGroup{
			GID:         binary.LittleEndian.Uint64(data[0:]),
			Permissions: format.ACLPermissions(binary.LittleEndian.Uint64(data[8:])),
		})
		return false, nil

	case format.PXARFCaps:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Metadata.FCaps = data
		return false, nil

	case format.PXARQuotaProjID:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		id := binary.LittleEndian.Uint64(data)
		entry.Metadata.QuotaProjectID = &id
		return false, nil

	case format.PXARSymlink:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
		}
		entry.Kind = pxar.KindSymlink
		entry.LinkTarget = string(data)
		d.state = stateDefault
		return true, nil

	case format.PXARDevice:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		entry.Kind = pxar.KindDevice
		entry.DeviceInfo = format.Device{
			Major: binary.LittleEndian.Uint64(data[0:]),
			Minor: binary.LittleEndian.Uint64(data[8:]),
		}
		d.state = stateDefault
		return true, nil

	case format.PXARPayload:
		contentSize := h.ContentSize()
		entry.Kind = pxar.KindFile
		entry.FileSize = contentSize
		// state will be set to stateDefault after payload is consumed
		d.state = stateInPayload
		d.payload = &limitedReader{reader: d.input, remain: int64(contentSize)}
		return true, nil

	case format.PXARPayloadRef:
		data, err := d.readContent()
		if err != nil {
			return false, err
		}
		pr := unmarshalPayloadRef(data)
		entry.Kind = pxar.KindFile
		entry.FileSize = pr.Size
		entry.PayloadOffset = pr.Offset
		d.state = stateInPayload
		d.payload = nil
		return true, nil

	case format.PXARFilename, format.PXARGoodbye:
		if entry.Metadata.IsFIFO() {
			entry.Kind = pxar.KindFifo
			d.state = stateInSpecialFile
		} else if entry.Metadata.IsSocket() {
			entry.Kind = pxar.KindSocket
			d.state = stateInSpecialFile
		} else {
			entry.Kind = pxar.KindDirectory
			d.state = stateInDirectory
		}
		return true, nil

	default:
		return false, fmt.Errorf("unexpected item type: %s", h.String())
	}
}

func (d *Decoder) readHeader() (format.Header, error) {
	var h format.Header
	err := binary.Read(d.input, binary.LittleEndian, &h)
	if err != nil {
		return h, err
	}
	if err := h.CheckHeaderSize(); err != nil {
		return h, err
	}
	return h, nil
}

func (d *Decoder) readContent() ([]byte, error) {
	size := d.header.ContentSize()
	if size == 0 {
		return nil, nil
	}
	data := make([]byte, size)
	_, err := io.ReadFull(d.input, data)
	if err != nil {
		return nil, fmt.Errorf("reading content: %w", err)
	}
	return data, nil
}

func (d *Decoder) skipPayload() {
	if d.payload != nil {
		io.CopyN(io.Discard, d.payload, d.payload.remain)
		d.payload = nil
	}
}

func (d *Decoder) pushPath(name string) {
	if d.path == "/" {
		d.path = "/" + name
	} else {
		d.path = d.path + "/" + name
	}
}

func (d *Decoder) resetPath() {
	if len(d.pathLens) > 0 {
		targetLen := d.pathLens[len(d.pathLens)-1]
		if targetLen <= len(d.path) {
			d.path = d.path[:targetLen]
		}
	}
}

func unmarshalStat(data []byte) format.Stat        { return format.UnmarshalStatBytes(data) }
func unmarshalStatV1(data []byte) format.StatV1     { return format.UnmarshalStatV1Bytes(data) }
func unmarshalACLDefault(data []byte) *format.ACLDefault { return format.UnmarshalACLDefaultBytes(data) }
func unmarshalPayloadRef(data []byte) format.PayloadRef { return format.UnmarshalPayloadRefBytes(data) }
