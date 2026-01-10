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
	"fmt"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.opentelemetry.io/collector/client"
)

var (
	errTooManyRequests = status.Error(codes.ResourceExhausted, "too many requests")
)

// contextKeyResourceAttrs is used to store resource attributes in the context
type contextKeyResourceAttrs struct{}

var resourceAttrsKey = contextKeyResourceAttrs{}

// WithResourceAttributes stores resource attributes in the context
func WithResourceAttributes(ctx context.Context, attrs map[string]string) context.Context {
	return context.WithValue(ctx, resourceAttrsKey, attrs)
}

// getResourceAttributesFromContext retrieves resource attributes from the context
func getResourceAttributesFromContext(ctx context.Context) map[string]string {
	attrs, ok := ctx.Value(resourceAttrsKey).(map[string]string)
	if !ok {
		return nil
	}
	return attrs
}

// RateLimiter provides an interface for rate limiting by some number
// of things: requests, records, or bytes.
type RateLimiter interface {
	RateLimit(ctx context.Context, n int) error
}

// getUniqueKey returns a unique key based on client metadata stored
// in ctx and resource attributes with the given keys.
//
// The unique key is built by concatenating the metadata and attribute keys
// and their associated values. Being able to link a key back to a data source
// can be useful for observability purposes, so we use the full keys
// and values instead of hashing.
//
// If no metadata or attribute keys are specified, a special non-empty value
// "default" is returned.
//
// Metadata and attribute keys should be limited to ones that do not have
// extremely high cardinality: tenant ID would be a good choice. For rate
// limiting by IP (e.g. to avoid DDoS), consider running OpenTelemetry
// Collector behind a WAF/API Gateway/proxy.
//
// OPTIMIZATION: When no metadata keys are configured, this is a no-op and
// the string "default" is returned immediately without any allocations.
func getUniqueKey(ctx context.Context, metadata client.Metadata, metadataKeys []string) string {
	resourceAttrs := getResourceAttributesFromContext(ctx)

	if len(metadataKeys) == 0 && len(resourceAttrs) == 0 {
		return "default"
	}

	// Generate a unique key from client metadata and attributes.
	// Pre-allocate with estimated capacity to reduce allocations.
	var uniqueKey strings.Builder
	// Estimate: ~30 bytes per key-value pair
	estimatedCap := (len(metadataKeys) + len(resourceAttrs)) * 30
	uniqueKey.Grow(estimatedCap)

	index := 0

	// Add metadata keys first
	for _, metadataKey := range metadataKeys {
		values := metadata.Get(metadataKey)
		if index > 0 {
			uniqueKey.WriteByte(';')
		}
		uniqueKey.WriteString(metadataKey)
		uniqueKey.WriteByte(':')
		for i, value := range values {
			if i > 0 {
				uniqueKey.WriteByte(',')
			}
			uniqueKey.WriteString(value)
		}
		index++
	}

	// Add attribute keys in sorted order (pre-allocate to avoid repeated sorting)
	if len(resourceAttrs) > 0 {
		attrs := getResourceAttributeKeysSync(resourceAttrs, len(resourceAttrs))
		for _, attrKey := range attrs {
			if index > 0 {
				uniqueKey.WriteByte(';')
			}
			uniqueKey.WriteString(attrKey)
			uniqueKey.WriteByte(':')
			uniqueKey.WriteString(resourceAttrs[attrKey])
			index++
		}
	}

	return uniqueKey.String()
}

// getResourceAttributeKeysSync returns sorted keys of a resource attributes map.
// This function pre-allocates the slice to the exact size, reducing allocations.
func getResourceAttributeKeysSync(m map[string]string, expectedLen int) []string {
	keys := make([]string, 0, expectedLen)
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// sortedKeys returns the keys of a map in a consistent order
// to ensure deterministic unique key generation.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// getAttrsFromContext looks up for the metadata keys in the
// context and returns the values as attributes.
func getAttrsFromContext(ctx context.Context, metadataKeys []string) []attribute.KeyValue {
	clientInfo := client.FromContext(ctx)
	resourceAttrs := getResourceAttributesFromContext(ctx)

	attrs := make([]attribute.KeyValue, 0, len(metadataKeys)+len(resourceAttrs))
	for _, key := range metadataKeys {
		values := clientInfo.Metadata.Get(key)
		if len(values) > 0 {
			attrs = append(attrs, attribute.String(key, strings.Join(values, ",")))
		}
	}
	for _, key := range sortedKeys(resourceAttrs) {
		attrs = append(attrs, attribute.String(key, resourceAttrs[key]))
	}
	return attrs
}

// errorWithDetails provides a user friendly error with additional error details that
// can be later used to provide more detailed error information to the user.
func errorWithDetails(err error, cfg RateLimitSettings) error {
	st := status.Convert(err)
	if detailedSt, stErr := st.WithDetails(&errdetails.ErrorInfo{
		Domain: "ingest.elastic.co",
		Metadata: map[string]string{
			"component":         "ratelimitprocessor",
			"limit":             fmt.Sprintf("%d", cfg.Rate),
			"throttle_interval": cfg.ThrottleInterval.String(),
		},
	}, &errdetails.RetryInfo{
		RetryDelay: durationpb.New(cfg.RetryDelay),
	}); stErr == nil {
		return detailedSt.Err()
	}
	return st.Err()
}
