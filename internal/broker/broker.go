package broker

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/smit-p/pdrive/internal/metadata"
)

var ErrNoSpace = errors.New("no provider has enough free space")

// Policy controls how the broker selects a provider for new chunks.
type Policy string

const (
	// PolicyPFRD selects a provider randomly, weighted by free space.
	// A provider with 2× the free space is 2× as likely to be chosen.
	PolicyPFRD Policy = "pfrd"
	// PolicyMFS always picks the provider with the most free space.
	PolicyMFS Policy = "mfs"
)

// Broker assigns chunks to providers based on available space.
type Broker struct {
	db           *metadata.DB
	policy       Policy
	minFreeSpace int64 // bytes; providers below this threshold are skipped
}

// NewBroker creates a new space broker.
func NewBroker(db *metadata.DB, policy Policy, minFreeSpace int64) *Broker {
	if policy == "" {
		policy = PolicyPFRD
	}
	return &Broker{db: db, policy: policy, minFreeSpace: minFreeSpace}
}

// AssignChunk returns a provider ID chosen by the configured policy.
func (b *Broker) AssignChunk(chunkSizeBytes int64) (string, error) {
	candidates, err := b.eligible(chunkSizeBytes)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", ErrNoSpace
	}

	switch b.policy {
	case PolicyMFS:
		return b.pickMFS(candidates), nil
	default: // pfrd
		return b.pickPFRD(candidates), nil
	}
}

// eligible filters providers that can accept a chunk of the given size.
func (b *Broker) eligible(chunkSizeBytes int64) ([]metadata.Provider, error) {
	providers, err := b.db.GetAllProviders()
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	minRequired := chunkSizeBytes
	if b.minFreeSpace > minRequired {
		minRequired = b.minFreeSpace
	}

	var out []metadata.Provider
	for _, p := range providers {
		if p.RateLimitedUntil != nil && *p.RateLimitedUntil > now {
			continue
		}
		if p.QuotaFreeBytes == nil || *p.QuotaFreeBytes < minRequired {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// pickMFS returns the provider with the most free space.
func (b *Broker) pickMFS(candidates []metadata.Provider) string {
	var bestID string
	var bestFree int64 = -1
	for _, p := range candidates {
		if *p.QuotaFreeBytes > bestFree {
			bestFree = *p.QuotaFreeBytes
			bestID = p.ID
		}
	}
	return bestID
}

// pickPFRD selects a provider randomly, weighted by each provider's free space
// relative to the total free space across all candidates.
func (b *Broker) pickPFRD(candidates []metadata.Provider) string {
	var totalFree int64
	for _, p := range candidates {
		totalFree += *p.QuotaFreeBytes
	}
	if totalFree <= 0 {
		return candidates[0].ID
	}

	pick := rand.Int64N(totalFree)
	var cumulative int64
	for _, p := range candidates {
		cumulative += *p.QuotaFreeBytes
		if pick < cumulative {
			return p.ID
		}
	}
	return candidates[len(candidates)-1].ID
}
