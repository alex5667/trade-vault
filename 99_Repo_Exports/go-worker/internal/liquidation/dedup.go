package liquidation

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

type dedupEntry struct {
	key string
	ts  int64
}

type dedupCache struct {
	ttl     time.Duration
	maxKeys int
	mu      sync.Mutex
	cache   map[string]*list.Element
	ll      *list.List
}

func newDedupCache(ttl time.Duration, maxKeys int) *dedupCache {
	if maxKeys <= 0 {
		maxKeys = 4096 // safety net: prevent removeOldest from evicting everything immediately
	}
	if ttl <= 0 {
		ttl = 2 * time.Second // safety net: prevent all entries from being instantly expired
	}
	return &dedupCache{
		ttl:     ttl,
		maxKeys: maxKeys,
		cache:   make(map[string]*list.Element),
		ll:      list.New(),
	}
}

func DefaultDQPolicy(allowSymbols []string) DQPolicy {
	m := map[string]struct{}{}
	for _, s := range allowSymbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			m[s] = struct{}{}
		}
	}
	// Инициализируем dedupCache eagerly с разумными дефолтами.
	// DedupEnabled=false — кеш создан, но не используется до явного включения.
	// Это исключает nil-panic при последующем policy.DedupEnabled = true,
	// а также проблему с нулевыми maxKeys/TTL при ленивой инициализации.
	return DQPolicy{
		MaxEventAge:      10 * time.Second,
		MaxFutureSkew:    2 * time.Second,
		MaxOutOfOrder:    2 * time.Second,
		AllowSymbols:     m,
		EnableQuarantine: true,
		DedupTTL:         2 * time.Second,
		DedupMaxKeys:     4096,
		lastTsMs:         map[string]int64{},
		dedup:            newDedupCache(2*time.Second, 4096),
	}
}

// SeenOrAdd checks if key exists and is less than TTL old.
// If not seen or expired, it adds it and returns false.
// If seen and within TTL, it updates the timestamp and returns true.
func (c *dedupCache) SeenOrAdd(key string, nowMs int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evict(nowMs)

	if ele, ok := c.cache[key]; ok {
		entry := ele.Value.(*dedupEntry)
		if nowMs-entry.ts <= c.ttl.Milliseconds() {
			entry.ts = nowMs
			c.ll.MoveToFront(ele)
			return true
		}
		// expired but not evicted yet
		entry.ts = nowMs
		c.ll.MoveToFront(ele)
		return false
	}

	entry := &dedupEntry{key: key, ts: nowMs}
	ele := c.ll.PushFront(entry)
	c.cache[key] = ele

	if c.ll.Len() > c.maxKeys {
		c.removeOldest()
	}

	return false
}

func (c *dedupCache) evict(nowMs int64) {
	ttlMs := c.ttl.Milliseconds()
	for {
		ele := c.ll.Back()
		if ele == nil {
			break
		}
		entry := ele.Value.(*dedupEntry)
		if nowMs-entry.ts > ttlMs {
			c.ll.Remove(ele)
			delete(c.cache, entry.key)
		} else {
			break
		}
	}
}

func (c *dedupCache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		entry := ele.Value.(*dedupEntry)
		delete(c.cache, entry.key)
	}
}
