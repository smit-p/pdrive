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
	"slices"

	"github.com/google/uuid"
)

const (
	DefaultChunkSize = 32 * 1024 * 1024       // 32 MB — default / minimum
	MaxChunkSize     = 4 * 1024 * 1024 * 1024 // 4 GiB — upper bound per chunk
	targetChunkCount = 25                     // aim for ~25 chunks per file (legacy)

	// EncOverheadPerChunk is the legacy fixed byte overhead for single-block
	// AES-256-GCM: 12-byte nonce + 16-byte authentication tag.
	// New uploads use EncStreamOverhead() instead; this constant is kept for
	// backward compatibility.
	EncOverheadPerChunk = 28
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

// ScheduleForFile returns a fixed chunk schedule using DefaultChunkSize.
// Kept as a fallback when remote capacity information is unavailable.
func ScheduleForFile(fileSize int64) *ChunkSchedule {
	return &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 0, Size: DefaultChunkSize},
	}}
}

// PlanChunks returns a chunk schedule that minimises the number of chunks for
// the given file size, using the supplied per-remote free-space values (bytes).
//
// Strategy:
//  1. If the file fits on a single remote and is within MaxChunkSize → 1 chunk.
//  2. If the file fits on one remote but exceeds MaxChunkSize → uniform
//     MaxChunkSize chunks (all on that remote).
//  3. Otherwise, greedily fill remotes largest-first: each chunk is sized to
//     use as much of the best available remote as possible, capped at
//     MaxChunkSize to keep peak temp-file size bounded.
//
// Streaming encryption overhead (EncStreamOverhead) is accounted for so that
// the encrypted chunk fits within the remote's free space.
func PlanChunks(fileSize int64, remoteFreeBytes []int64) *ChunkSchedule {
	if fileSize <= 0 {
		return &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: DefaultChunkSize}}}
	}

	// Minimum overhead for even a tiny chunk.
	minOverhead := EncStreamOverhead(1)

	// Sort free spaces descending; discard remotes too small to hold even
	// the encryption overhead.
	spaces := slices.Clone(remoteFreeBytes)
	slices.SortFunc(spaces, func(a, b int64) int {
		if b > a {
			return 1
		}
		if b < a {
			return -1
		}
		return 0
	})
	var usable []int64
	for _, s := range spaces {
		if s > minOverhead {
			usable = append(usable, s)
		}
	}
	if len(usable) == 0 {
		return &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: DefaultChunkSize}}}
	}

	// maxPlain returns the largest plaintext that fits in `space` bytes
	// after accounting for streaming encryption overhead.
	maxPlain := func(space int64) int64 {
		// Binary search: find largest p where p + EncStreamOverhead(p) <= space.
		lo, hi := int64(1), space
		for lo < hi {
			mid := lo + (hi-lo+1)/2
			if mid+EncStreamOverhead(mid) <= space {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		if lo+EncStreamOverhead(lo) > space {
			return 0
		}
		return lo
	}

	largest := maxPlain(usable[0])

	// Case 1 & 2: file fits on a single remote.
	if largest >= fileSize {
		size := fileSize
		if size > int64(MaxChunkSize) {
			size = int64(MaxChunkSize)
		}
		return &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: int(size)}}}
	}

	// Case 3: file spans multiple remotes — greedy fill.
	avail := slices.Clone(usable)
	remaining := fileSize
	var tiers []ChunkTier

	for remaining > 0 {
		// Pick the remote with the most available space.
		bestIdx := -1
		var bestSpace int64
		for i, s := range avail {
			if s > bestSpace {
				bestSpace = s
				bestIdx = i
			}
		}
		if bestIdx < 0 || bestSpace <= minOverhead {
			// Safety fallback — shouldn't happen if CheckSpace passed.
			tiers = append(tiers, ChunkTier{Count: 0, Size: DefaultChunkSize})
			break
		}

		plain := maxPlain(bestSpace)
		if plain > int64(MaxChunkSize) {
			plain = int64(MaxChunkSize)
		}
		if plain > remaining {
			plain = remaining
		}

		tiers = append(tiers, ChunkTier{Count: 1, Size: int(plain)})
		avail[bestIdx] -= plain + EncStreamOverhead(plain)
		remaining -= plain
	}

	// Collapse identical tiers into a single unlimited tier.
	allSame := true
	for i := 1; i < len(tiers); i++ {
		if tiers[i].Size != tiers[0].Size {
			allSame = false
			break
		}
	}
	if allSame && len(tiers) > 0 {
		return &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: tiers[0].Size}}}
	}

	return &ChunkSchedule{Tiers: tiers}
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
