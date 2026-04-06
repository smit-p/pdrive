// Package chunker handles splitting files into fixed-size chunks,
// computing per-chunk SHA-256 hashes, and reassembling them.  It also
// provides AES-256-GCM encryption (with Argon2id key derivation) used
// to encrypt each chunk before upload and decrypt after download.
//
// Two chunking modes are provided:
//   - [Split] — reads the entire file into memory (suitable for tests or
//     small files).
//   - [ChunkReader] — streaming mode that reads one chunk at a time,
//     keeping peak memory bounded to chunkSize.
package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/google/uuid"
)

const (
	DefaultChunkSize = 32 * 1024 * 1024  // 32 MB — default / minimum
	MaxChunkSize     = 128 * 1024 * 1024 // 128 MB — upper bound
	targetChunkCount = 25                // aim for ~25 chunks per file
)

// ChunkSizeForFile returns an appropriate chunk size for the given file size.
// Larger files get proportionally larger chunks to keep the total chunk count
// near targetChunkCount, reducing the number of cloud API calls.
//
//	< 800 MB  → 32 MB  (~25 chunks or fewer)
//	1 GB      → 40 MB  (~25 chunks)
//	2 GB      → 80 MB  (~25 chunks)
//	≥ 3.2 GB  → 128 MB (capped)
func ChunkSizeForFile(fileSize int64) int {
	if fileSize <= 0 {
		return DefaultChunkSize
	}
	target := fileSize / targetChunkCount
	if target < DefaultChunkSize {
		return DefaultChunkSize
	}
	if target > MaxChunkSize {
		return MaxChunkSize
	}
	return int(target)
}

// Chunk represents a single piece of a split file.
type Chunk struct {
	ID       string
	Sequence int
	Data     []byte
	Size     int
	SHA256   string
}

// Split reads from r and produces fixed-size chunks.
// The last chunk may be smaller than chunkSize.
func Split(r io.Reader, chunkSize int) ([]Chunk, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	var chunks []Chunk
	buf := make([]byte, chunkSize)
	seq := 0

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			hash := sha256.Sum256(data)
			chunks = append(chunks, Chunk{
				ID:       uuid.New().String(),
				Sequence: seq,
				Data:     data,
				Size:     n,
				SHA256:   hex.EncodeToString(hash[:]),
			})
			seq++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return chunks, nil
}

// ChunkReader reads one chunk at a time from an io.Reader without buffering the whole file.
type ChunkReader struct {
	r         io.Reader
	chunkSize int
	buf       []byte
	seq       int
}

// NewChunkReader creates a streaming chunk reader.
func NewChunkReader(r io.Reader, chunkSize int) *ChunkReader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &ChunkReader{
		r:         r,
		chunkSize: chunkSize,
		buf:       make([]byte, chunkSize),
	}
}

// Next returns the next chunk, or nil when no more data.
func (cr *ChunkReader) Next() (*Chunk, error) {
	n, err := io.ReadFull(cr.r, cr.buf)
	if n == 0 {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	data := make([]byte, n)
	copy(data, cr.buf[:n])

	hash := sha256.Sum256(data)
	chunk := &Chunk{
		ID:       uuid.New().String(),
		Sequence: cr.seq,
		Data:     data,
		Size:     n,
		SHA256:   hex.EncodeToString(hash[:]),
	}
	cr.seq++

	if err == io.ErrUnexpectedEOF {
		// Last chunk (smaller than chunkSize) — mark reader exhausted.
		cr.r = io.LimitReader(cr.r, 0)
	} else if err != nil && err != io.EOF {
		return nil, err
	}

	return chunk, nil
}
