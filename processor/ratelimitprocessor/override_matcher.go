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
	"slices"

	"go.opentelemetry.io/collector/client"
)

// overrideMatcher provides fast matching for override configuration.
// This optimizes the hot path by avoiding repeated allocations and comparisons.
type overrideMatcher struct {
	overrides        []RateLimitOverrides
	matchKeysCache   [][]string // Pre-computed keys from each override's Matches map
	metadataValueBuf [][]string // Reusable buffer for metadata values
}

// newOverrideMatcher creates an optimized matcher from override configurations.
// This pre-computes keys for faster matching during rate limit checks.
func newOverrideMatcher(overrides []RateLimitOverrides) *overrideMatcher {
	if len(overrides) == 0 {
		return nil
	}

	matcher := &overrideMatcher{
		overrides:      overrides,
		matchKeysCache: make([][]string, len(overrides)),
	}

	// Pre-compute the keys from each override's Matches map
	// This avoids repeatedly iterating the map during matching
	for i, override := range overrides {
		keys := make([]string, 0, len(override.Matches))
		for k := range override.Matches {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		matcher.matchKeysCache[i] = keys
	}

	return matcher
}

// findMatch returns the first override that matches the given metadata.
// Returns the override index or -1 if no match is found.
//
// OPTIMIZATION: This function avoids allocations by:
// - Using pre-computed sorted keys from the matcher
// - Reusing a temporary buffer for metadata lookups
// - Short-circuiting on first match
func (om *overrideMatcher) findMatch(metadata client.Metadata) int {
	if om == nil || len(om.overrides) == 0 {
		return -1
	}

	for i, override := range om.overrides {
		matchKeys := om.matchKeysCache[i]
		match := true

		// All match keys must be present with matching values
		for _, k := range matchKeys {
			expectedValues := override.Matches[k]
			actualValues := metadata.Get(k)
			if slices.Compare(actualValues, expectedValues) != 0 {
				match = false
				break
			}
		}

		if match {
			return i
		}
	}

	return -1
}
