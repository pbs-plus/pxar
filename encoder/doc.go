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
// is written to the payload stream with PayloadRef entries in the metadata
// stream.
//
// # Directory Nesting
//
// Directories are created with CreateDirectory and finalized with Finish.
// The encoder maintains a state stack, so Finish closes the most recently
// opened directory and resumes the parent's context.
package encoder
