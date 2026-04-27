package liquidation

import (
	"container/list"
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
	return &dedupCache{
		ttl:     ttl,
		maxKeys: maxKeys,
		cache:   make(map[string]*list.Element),
		ll:      list.New(),
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
