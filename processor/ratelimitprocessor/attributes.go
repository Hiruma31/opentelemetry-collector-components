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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// extractResourceAttributesFromLogs extracts resource attributes from log data
// by collecting attributes from all ResourceLogs.
func extractResourceAttributesFromLogs(ld plog.Logs, attributeKeys []string) map[string]string {
	if len(attributeKeys) == 0 {
		return nil
	}

	result := make(map[string]string)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		extractFromMap(resourceLogs.Resource().Attributes(), attributeKeys, result)
		// First resource wins for each key
		if len(result) == len(attributeKeys) {
			break
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// extractResourceAttributesFromTraces extracts resource attributes from trace data
// by collecting attributes from all ResourceSpans.
func extractResourceAttributesFromTraces(td ptrace.Traces, attributeKeys []string) map[string]string {
	if len(attributeKeys) == 0 {
		return nil
	}

	result := make(map[string]string)

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		extractFromMap(resourceSpans.Resource().Attributes(), attributeKeys, result)
		// First resource wins for each key
		if len(result) == len(attributeKeys) {
			break
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// extractResourceAttributesFromMetrics extracts resource attributes from metric data
// by collecting attributes from all ResourceMetrics.
func extractResourceAttributesFromMetrics(md pmetric.Metrics, attributeKeys []string) map[string]string {
	if len(attributeKeys) == 0 {
		return nil
	}

	result := make(map[string]string)

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		extractFromMap(resourceMetrics.Resource().Attributes(), attributeKeys, result)
		// First resource wins for each key
		if len(result) == len(attributeKeys) {
			break
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// extractResourceAttributesFromProfiles extracts resource attributes from profile data
// by collecting attributes from all ResourceProfiles.
func extractResourceAttributesFromProfiles(pd pprofile.Profiles, attributeKeys []string) map[string]string {
	if len(attributeKeys) == 0 {
		return nil
	}

	result := make(map[string]string)

	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		resourceProfiles := pd.ResourceProfiles().At(i)
		extractFromMap(resourceProfiles.Resource().Attributes(), attributeKeys, result)
		// First resource wins for each key
		if len(result) == len(attributeKeys) {
			break
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

// extractFromMap extracts specified keys from a pcommon.Map into the result map
// Only adds keys that haven't been set yet (first occurrence wins)
func extractFromMap(attrs pcommon.Map, attributeKeys []string, result map[string]string) {
	for _, key := range attributeKeys {
		if _, exists := result[key]; !exists {
			if val, ok := attrs.Get(key); ok {
				result[key] = val.AsString()
			}
		}
	}
}
