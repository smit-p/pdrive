package chunker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
)

// DecryptedChunk holds a decrypted chunk ready for reassembly.
type DecryptedChunk struct {
	Sequence int
	Data     []byte
	SHA256   string // expected hash for verification
}

// Assemble takes decrypted chunks, orders them by sequence, verifies SHA-256, and returns a reader.
func Assemble(chunks []DecryptedChunk) (io.Reader, error) {
	sorted := make([]DecryptedChunk, len(chunks))
	copy(sorted, chunks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Sequence < sorted[j].Sequence
	})

	// Validate sequences are contiguous (0, 1, 2, ..., n-1).
	for i, c := range sorted {
		if c.Sequence != i {
			return nil, fmt.Errorf("chunk sequence gap: expected %d, got %d", i, c.Sequence)
		}
	}

	readers := make([]io.Reader, len(sorted))
	for i, c := range sorted {
		hash := sha256.Sum256(c.Data)
		actual := hex.EncodeToString(hash[:])
		if actual != c.SHA256 {
			return nil, fmt.Errorf("chunk %d hash mismatch: expected %s, got %s", c.Sequence, c.SHA256, actual)
		}
		readers[i] = bytes.NewReader(c.Data)
	}

	return io.MultiReader(readers...), nil
}
