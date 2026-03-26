package broker

import (
	"errors"
	"time"

	"github.com/smit-p/pdrive/internal/metadata"
)

var ErrNoSpace = errors.New("no provider has enough free space")

// Broker assigns chunks to providers based on available space.
type Broker struct {
	db *metadata.DB
}

// NewBroker creates a new space broker.
func NewBroker(db *metadata.DB) *Broker {
	return &Broker{db: db}
}

// AssignChunk returns the provider ID with the most free space that can fit the chunk.
func (b *Broker) AssignChunk(chunkSizeBytes int64) (string, error) {
	providers, err := b.db.GetAllProviders()
	if err != nil {
		return "", err
	}

	now := time.Now().Unix()
	var bestID string
	var bestFree int64 = -1

	for _, p := range providers {
		// Skip rate-limited providers.
		if p.RateLimitedUntil != nil && *p.RateLimitedUntil > now {
			continue
		}
		// Skip providers without enough free space.
		if p.QuotaFreeBytes == nil || *p.QuotaFreeBytes < chunkSizeBytes {
			continue
		}
		if *p.QuotaFreeBytes > bestFree {
			bestFree = *p.QuotaFreeBytes
			bestID = p.ID
		}
	}

	if bestID == "" {
		return "", ErrNoSpace
	}
	return bestID, nil
}
