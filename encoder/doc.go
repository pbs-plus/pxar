// Package encoder creates pxar archives.
//
// Encoder writes pxar archive entries in a single sequential pass. It supports
// both unified (v1) and split (v2) formats via separate io.Writer outputs.
//
// # Usage
//
//	enc := encoder.NewEncoder(output, nil, &rootMeta, nil)
//	enc.AddFile(&fileMeta, "hello.txt", []byte("hello world"))
//	enc.Close()
//
// For split archives, provide a payload writer to NewEncoder and file content
// is written to the payload stream with PAYLOAD_REF entries in the metadata
// stream.
//
// # Directory Nesting
//
// Directories are created with CreateDirectory and finalized with Finish.
// The encoder maintains a state stack, so Finish closes the most recently
// opened directory and resumes the parent's context. Close finalizes the
// root directory and writes the goodbye table.
//
// # Hardlinks
//
// AddFile and AddPayloadRef return a LinkOffset token. Pass this token to
// AddHardlink to create a hardlink that references the original file via
// a relative offset in the wire format.
//
// # Streaming Files
//
// For large files, use CreateFile to obtain a *FileWriter (io.Writer).
// Write content via io.Copy or direct writes, then call Close on the writer.
//
// # Payload References
//
// AddPayloadRef writes a PAYLOAD_REF entry pointing to existing payload data
// without writing the content. Use PayloadPosition and Advance to track
// virtual payload positions for external chunk injection.
package encoder
