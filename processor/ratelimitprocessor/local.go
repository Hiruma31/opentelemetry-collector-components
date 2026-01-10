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
	"context"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"golang.org/x/time/rate"
)

var _ RateLimiter = (*localRateLimiter)(nil)

type localRateLimiter struct {
	cfg            *Config
	set            processor.Settings
	limiters       *LRUCache[string, *rate.Limiter]
	defaultLimiter *rate.Limiter
	hasMetadataKey bool
}

// DefaultMaxLimiters is the maximum number of rate limiters to keep in memory per processor.
// This prevents unbounded memory growth when dealing with high-cardinality unique keys.
// When exceeded, least recently used limiters are evicted.
const DefaultMaxLimiters = 10000

func newLocalRateLimiter(cfg *Config, set processor.Settings) (*localRateLimiter, error) {
	maxLimiters := DefaultMaxLimiters
	if cfg.MaxLocalLimiters > 0 {
		maxLimiters = cfg.MaxLocalLimiters
	}

	hasMetadataKey := len(cfg.MetadataKeys) > 0 || len(cfg.ResourceAttributeKeys) > 0
	var defaultLimiter *rate.Limiter
	if !hasMetadataKey {
		// Fast path: if no metadata keys, create a single limiter for all traffic
		defaultLimiter = rate.NewLimiter(rate.Limit(cfg.Rate), cfg.Burst)
	}

	return &localRateLimiter{
		cfg:            cfg,
		set:            set,
		limiters:       NewLRUCache[string, *rate.Limiter](maxLimiters),
		defaultLimiter: defaultLimiter,
		hasMetadataKey: hasMetadataKey,
	}, nil
}

func (r *localRateLimiter) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (r *localRateLimiter) Shutdown(_ context.Context) error {
	r.limiters.Clear()
	return nil
}

func (r *localRateLimiter) RateLimit(ctx context.Context, hits int) error {
	// Fast path: no metadata keys configured, use single global limiter
	if !r.hasMetadataKey {
		cfg := r.cfg.RateLimitSettings
		return r.checkLimit(ctx, hits, r.defaultLimiter, cfg)
	}

	metadata := client.FromContext(ctx).Metadata
	key := getUniqueKey(ctx, metadata, r.cfg.MetadataKeys)

	// local rate limiter ignores classes (no resolver), so pass empty class.
	cfg, _, _ := resolveRateLimit(r.cfg, "", metadata)

	// Use LRU cache with bounded capacity
	limiter, err := r.limiters.GetOrStore(key, func() (*rate.Limiter, error) {
		return rate.NewLimiter(rate.Limit(cfg.Rate), cfg.Burst), nil
	})
	if err != nil {
		return err
	}

	return r.checkLimit(ctx, hits, limiter, cfg)
}

// checkLimit performs the actual rate limit check on a limiter.
// Extracted to reduce duplication between fast and slow paths.
func (r *localRateLimiter) checkLimit(ctx context.Context, hits int, limiter *rate.Limiter, cfg RateLimitSettings) error {
	switch cfg.ThrottleBehavior {
	case ThrottleBehaviorError:
		if ok := limiter.AllowN(time.Now(), hits); !ok {
			return errorWithDetails(errTooManyRequests, cfg)
		}
	case ThrottleBehaviorDelay:
		lr := limiter.ReserveN(time.Now(), hits)
		if !lr.OK() {
			return errorWithDetails(errTooManyRequests, cfg)
		}
		timer := time.NewTimer(lr.Delay())
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return nil
}
