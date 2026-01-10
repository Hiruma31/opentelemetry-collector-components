// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"container/list"
	"sync"
)

// LRUCache is a thread-safe Least Recently Used cache with a bounded capacity.
// When capacity is exceeded, the least recently used item is evicted.
//
// This is optimized for the rate limiter use case where:
// - Keys are rate limit identifiers (strings)
// - Values are rate.Limiter instances
// - We want bounded memory usage for large numbers of unique clients
type LRUCache[K comparable, V any] struct {
	maxCapacity int
	mu          sync.Mutex
	cache       map[K]*list.Element
	list        *list.List
}

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRU cache with the specified capacity.
// If capacity <= 0, it creates an unbounded cache (for backward compatibility).
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		maxCapacity: capacity,
		cache:       make(map[K]*list.Element),
		list:        list.New(),
	}
}

// GetOrStore retrieves a value from the cache or stores it if it doesn't exist.
// The provided creator function is called only if the key is not found.
// Returns (value, found).
func (l *LRUCache[K, V]) GetOrStore(key K, creator func() (V, error)) (V, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if key exists
	if elem, exists := l.cache[key]; exists {
		// Move to front (most recently used)
		l.list.MoveToFront(elem)
		return elem.Value.(*lruEntry[K, V]).value, nil
	}

	// Key doesn't exist, create new value
	var zero V
	value, err := creator()
	if err != nil {
		return zero, err
	}

	// Add to cache
	entry := &lruEntry[K, V]{key: key, value: value}
	elem := l.list.PushFront(entry)
	l.cache[key] = elem

	// Evict oldest entry if over capacity
	if l.maxCapacity > 0 && l.list.Len() > l.maxCapacity {
		oldestElem := l.list.Back()
		l.list.Remove(oldestElem)
		oldestEntry := oldestElem.Value.(*lruEntry[K, V])
		delete(l.cache, oldestEntry.key)
	}

	return value, nil
}

// Len returns the current number of items in the cache.
func (l *LRUCache[K, V]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.cache)
}

// Clear removes all items from the cache.
func (l *LRUCache[K, V]) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cache = make(map[K]*list.Element)
	l.list.Init()
}
