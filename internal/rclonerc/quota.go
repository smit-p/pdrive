package rclonerc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const quotaCacheTTL = 15 * time.Minute

// QuotaInfo holds storage quota information for a remote.
type QuotaInfo struct {
	Total int64
	Used  int64
	Free  int64
}

type cachedQuota struct {
	info      QuotaInfo
	fetchedAt time.Time
}

// QuotaCache caches quota information per remote with a TTL.
type QuotaCache struct {
	client *Client
	mu     sync.RWMutex
	cache  map[string]cachedQuota
}

// NewQuotaCache creates a new quota cache.
func NewQuotaCache(client *Client) *QuotaCache {
	return &QuotaCache{
		client: client,
		cache:  make(map[string]cachedQuota),
	}
}

// GetQuota returns quota info for a remote, fetching from rclone RC if not cached.
func (qc *QuotaCache) GetQuota(remote string) (QuotaInfo, error) {
	qc.mu.RLock()
	if cached, ok := qc.cache[remote]; ok && time.Since(cached.fetchedAt) < quotaCacheTTL {
		qc.mu.RUnlock()
		return cached.info, nil
	}
	qc.mu.RUnlock()

	info, err := qc.fetchQuota(remote)
	if err != nil {
		return QuotaInfo{}, err
	}

	qc.mu.Lock()
	qc.cache[remote] = cachedQuota{info: info, fetchedAt: time.Now()}
	qc.mu.Unlock()

	return info, nil
}

// Invalidate removes a specific remote from the cache.
func (qc *QuotaCache) Invalidate(remote string) {
	qc.mu.Lock()
	delete(qc.cache, remote)
	qc.mu.Unlock()
}

func (qc *QuotaCache) fetchQuota(remote string) (QuotaInfo, error) {
	result, err := qc.client.call("operations/about", map[string]interface{}{
		"fs": remote + ":",
	})
	if err != nil {
		return QuotaInfo{}, fmt.Errorf("fetching quota for %s: %w", remote, err)
	}

	var resp struct {
		Total *int64 `json:"total"`
		Used  *int64 `json:"used"`
		Free  *int64 `json:"free"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return QuotaInfo{}, fmt.Errorf("parsing quota response: %w", err)
	}

	info := QuotaInfo{}
	if resp.Total != nil {
		info.Total = *resp.Total
	}
	if resp.Used != nil {
		info.Used = *resp.Used
	}
	if resp.Free != nil {
		info.Free = *resp.Free
	}
	return info, nil
}
