package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/google/uuid"
)

const DefaultChunkSize = 4 * 1024 * 1024 // 4 MB

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
