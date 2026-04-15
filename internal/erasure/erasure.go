// Package erasure provides Reed-Solomon erasure coding for pdrive chunks.
//
// After a chunk is encrypted, Encode splits the ciphertext into DataShards
// equal-sized data pieces and computes ParityShards parity pieces, for a total
// of DataShards+ParityShards shards.  Any DataShards of those are sufficient
// to reconstruct the original ciphertext via Reconstruct.
//
// The storage overhead factor is (DataShards+ParityShards)/DataShards.
// Example configurations:
//
//	2+1 → 1.50× (tolerate 1 loss, need 3 providers)
//	3+1 → 1.33× (tolerate 1 loss, need 4 providers)
//	3+2 → 1.67× (tolerate 2 losses, need 5 providers)
//	4+2 → 1.50× (tolerate 2 losses, need 6 providers)
package erasure

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Encoder wraps a Reed-Solomon encoder with a fixed data/parity configuration.
type Encoder struct {
	enc          reedsolomon.Encoder
	dataShards   int
	parityShards int
}

// NewEncoder creates an Encoder with the given data and parity shard counts.
func NewEncoder(dataShards, parityShards int) (*Encoder, error) {
	if dataShards < 1 {
		return nil, fmt.Errorf("erasure: dataShards must be >= 1, got %d", dataShards)
	}
	if parityShards < 1 {
		return nil, fmt.Errorf("erasure: parityShards must be >= 1, got %d", parityShards)
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("erasure: creating encoder: %w", err)
	}
	return &Encoder{enc: enc, dataShards: dataShards, parityShards: parityShards}, nil
}

// DataShards returns the number of data shards.
func (e *Encoder) DataShards() int { return e.dataShards }

// ParityShards returns the number of parity shards.
func (e *Encoder) ParityShards() int { return e.parityShards }

// TotalShards returns dataShards + parityShards.
func (e *Encoder) TotalShards() int { return e.dataShards + e.parityShards }

// ShardSize returns the size of each shard for a given data length.
// All shards (data + parity) have the same size.
func (e *Encoder) ShardSize(dataLen int) int {
	if dataLen <= 0 {
		return 0
	}
	// Each data shard = ceil(dataLen / dataShards).
	return (dataLen + e.dataShards - 1) / e.dataShards
}

// Encode splits data into dataShards data pieces, pads the last data shard
// with zeros if needed, computes parityShards parity pieces, and returns all
// shards. shards[0..dataShards-1] are data, shards[dataShards..total-1] are parity.
func (e *Encoder) Encode(data []byte) ([][]byte, error) {
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("erasure: splitting data: %w", err)
	}
	if err := e.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("erasure: encoding parity: %w", err)
	}
	return shards, nil
}

// Reconstruct rebuilds missing shards in-place. Set missing shard entries to
// nil before calling. Returns an error if fewer than dataShards non-nil
// shards are provided.
func (e *Encoder) Reconstruct(shards [][]byte) error {
	if err := e.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("erasure: reconstruct: %w", err)
	}
	return nil
}

// Join reassembles the original data from data shards (indices 0..dataShards-1).
// originalLen is the length of the original data before padding.
func (e *Encoder) Join(shards [][]byte, originalLen int) ([]byte, error) {
	if len(shards) < e.dataShards {
		return nil, fmt.Errorf("erasure: need at least %d shards, got %d", e.dataShards, len(shards))
	}
	// Concatenate data shards and trim to original length.
	result := make([]byte, 0, originalLen)
	for i := 0; i < e.dataShards; i++ {
		result = append(result, shards[i]...)
	}
	if len(result) > originalLen {
		result = result[:originalLen]
	}
	return result, nil
}

// Verify checks whether the parity shards are consistent with the data shards.
func (e *Encoder) Verify(shards [][]byte) (bool, error) {
	return e.enc.Verify(shards)
}
