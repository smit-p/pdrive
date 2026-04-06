// Package chunker handles splitting files into variable-size chunks,
// computing per-chunk SHA-256 hashes, and reassembling them.  It also
// provides AES-256-GCM encryption (with Argon2id key derivation) used
// to encrypt each chunk before upload and decrypt after download.
//
// Three chunking modes are provided:
//   - [Split] — reads the entire file into memory (suitable for tests or
//     small files).
//   - [ChunkReader] — streaming mode with a fixed chunk size.
//   - [VariableChunkReader] — streaming mode with a per-chunk size
//     schedule (ramp-up strategy for large files).
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

// ChunkSizeForFile returns an appropriate fixed chunk size for the given file
// size.  Kept for backward compatibility and small-file uploads; large files
// benefit from ChunkSchedule instead.
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

// ChunkSchedule describes a variable-size chunking plan: a sequence of
// (Count, Size) tiers applied in order.  The last tier repeats for all
// remaining data.
type ChunkSchedule struct {
	Tiers []ChunkTier
}

// ChunkTier is one step in a ChunkSchedule.
type ChunkTier struct {
	Count int // number of chunks at this size (0 = unlimited / rest of file)
	Size  int // chunk size in bytes
}

// SizeForSeq returns the chunk size to use for the given 0-based sequence
// number.  If seq exceeds all tiers the last tier's size is used.
func (s *ChunkSchedule) SizeForSeq(seq int) int {
	if len(s.Tiers) == 0 {
		return DefaultChunkSize
	}
	offset := 0
	for _, t := range s.Tiers {
		if t.Count == 0 || seq < offset+t.Count {
			return t.Size
		}
		offset += t.Count
	}
	return s.Tiers[len(s.Tiers)-1].Size
}

// MaxSize returns the largest chunk size in the schedule.
func (s *ChunkSchedule) MaxSize() int {
	m := 0
	for _, t := range s.Tiers {
		if t.Size > m {
			m = t.Size
		}
	}
	if m == 0 {
		return DefaultChunkSize
	}
	return m
}

// EstimateChunks returns the approximate number of chunks the schedule
// produces for the given file size.
func (s *ChunkSchedule) EstimateChunks(fileSize int64) int {
	if fileSize <= 0 {
		return 0
	}
	remaining := fileSize
	count := 0
	for _, t := range s.Tiers {
		if remaining <= 0 {
			break
		}
		if t.Count == 0 {
			// Last tier: covers all remaining data.
			count += int((remaining + int64(t.Size) - 1) / int64(t.Size))
			return count
		}
		tierBytes := int64(t.Count) * int64(t.Size)
		if tierBytes >= remaining {
			count += int((remaining + int64(t.Size) - 1) / int64(t.Size))
			return count
		}
		count += t.Count
		remaining -= tierBytes
	}
	// All tiers exhausted — use last tier size.
	last := s.Tiers[len(s.Tiers)-1].Size
	count += int((remaining + int64(last) - 1) / int64(last))
	return count
}

// ScheduleForFile returns a variable chunk schedule optimised for the given
// file size.  Small files (≤ 256 MB) get a single fixed tier.  Larger files
// use a ramp-up strategy:
//
//	Tier 1:  4 × 16 MB  — fill the worker pipeline quickly
//	Tier 2:  8 × 32 MB  — warm up
//	Tier 3:  ∞ × 64 MB  — steady-state (good throughput with moderate API calls)
func ScheduleForFile(fileSize int64) *ChunkSchedule {
	if fileSize <= 256*1024*1024 {
		return &ChunkSchedule{Tiers: []ChunkTier{
			{Count: 0, Size: DefaultChunkSize},
		}}
	}
	return &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 4, Size: 16 * 1024 * 1024},  // 64 MB total
		{Count: 8, Size: 32 * 1024 * 1024},  // 256 MB total
		{Count: 0, Size: 64 * 1024 * 1024},  // rest of file
	}}
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

// VariableChunkReader reads one chunk at a time using a ChunkSchedule to
// determine the size of each chunk.  Peak memory is bounded to the largest
// tier size.
type VariableChunkReader struct {
	r        io.Reader
	schedule *ChunkSchedule
	buf      []byte // sized to schedule.MaxSize()
	seq      int
	done     bool
}

// NewVariableChunkReader creates a streaming reader that uses schedule to size
// each successive chunk.
func NewVariableChunkReader(r io.Reader, schedule *ChunkSchedule) *VariableChunkReader {
	if schedule == nil {
		schedule = &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: DefaultChunkSize}}}
	}
	return &VariableChunkReader{
		r:        r,
		schedule: schedule,
		buf:      make([]byte, schedule.MaxSize()),
	}
}

// Next returns the next chunk, or nil when no more data.
func (vr *VariableChunkReader) Next() (*Chunk, error) {
	if vr.done {
		return nil, nil
	}
	chunkSize := vr.schedule.SizeForSeq(vr.seq)
	// Read exactly chunkSize bytes (or fewer at EOF).
	n, err := io.ReadFull(vr.r, vr.buf[:chunkSize])
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
	copy(data, vr.buf[:n])

	hash := sha256.Sum256(data)
	chunk := &Chunk{
		ID:       uuid.New().String(),
		Sequence: vr.seq,
		Data:     data,
		Size:     n,
		SHA256:   hex.EncodeToString(hash[:]),
	}
	vr.seq++

	if err == io.ErrUnexpectedEOF {
		vr.done = true
	} else if err != nil && err != io.EOF {
		return nil, err
	}

	return chunk, nil
}
