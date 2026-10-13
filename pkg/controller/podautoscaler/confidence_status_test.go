/*
Copyright 2025 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package podautoscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	autoscalingv1alpha1 "github.com/vllm-project/aibrix/api/autoscaling/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/decision"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
)

func suppressedOutcome() *decision.Outcome {
	return &decision.Outcome{
		Enabled:         true,
		Threshold:       0.6,
		CurrentReplicas: 4,
		DesiredReplicas: 4,
		Suppressed:      true,
		Representative:  "metric-a",
		Summary:         "scaling suppressed: no metric reached confidence threshold 0.60",
		Metrics: []decision.MetricDecision{{
			MetricName:        "metric-a",
			RawDesired:        10,
			AdjustedDesired:   4,
			Confidence:        0.42,
			Qualified:         false,
			SuppressionReason: "low confidence 0.42 below threshold 0.60: poor time continuity",
		}},
		MissingMetrics: []string{"metric-b"},
	}
}

func TestSetStatusRecordsSuppressedDecision(t *testing.T) {
	pa := &autoscalingv1alpha1.PodAutoscaler{}

	setStatus(pa, 4, 4, false, "stable", false, true, nil, suppressedOutcome())

	require.Len(t, pa.Status.ScalingHistory, 1, "suppression must be recorded even without a rescale")
	entry := pa.Status.ScalingHistory[0]
	assert.True(t, entry.Suppressed)
	assert.Equal(t, "metric-a", entry.Metric)
	assert.Equal(t, int32(42), entry.Confidence)
	assert.Contains(t, entry.SuppressionReason, "metric-a")
	assert.Contains(t, entry.SuppressionReason, "metric-b: metric unavailable")

	require.Len(t, entry.MetricContributions, 2)
	first := entry.MetricContributions[0]
	assert.Equal(t, "metric-a", first.Metric)
	assert.False(t, first.Qualified)
	assert.Equal(t, int32(42), first.Confidence)
	assert.Contains(t, first.SuppressionReason, "low confidence")
	missing := entry.MetricContributions[1]
	assert.Equal(t, "metric-b", missing.Metric)
	assert.False(t, missing.Qualified)
	assert.Equal(t, "metric unavailable", missing.SuppressionReason)

	cond := apimeta.FindStatusCondition(pa.Status.Conditions, ConditionConfidenceGuard)
	require.NotNil(t, cond)
	assert.Equal(t, "False", string(cond.Status))
	assert.Equal(t, ReasonLowConfidence, cond.Reason)
	assert.Contains(t, cond.Message, "scaling suppressed")
}

func TestSetStatusDeduplicatesRepeatedSuppression(t *testing.T) {
	pa := &autoscalingv1alpha1.PodAutoscaler{}

	setStatus(pa, 4, 4, false, "stable", false, true, nil, suppressedOutcome())
	// Immediate identical suppressed hold must not flood the history.
	setStatus(pa, 4, 4, false, "stable", false, true, nil, suppressedOutcome())
	assert.Len(t, pa.Status.ScalingHistory, 1)
}

func TestSetStatusRecordsQualifiedDecision(t *testing.T) {
	pa := &autoscalingv1alpha1.PodAutoscaler{}

	out := &decision.Outcome{
		Enabled:         true,
		Threshold:       0.6,
		CurrentReplicas: 4,
		DesiredReplicas: 6,
		QualifiedCount:  1,
		Representative:  "metric-a",
		Summary:         "confidence-weighted scaling: 1 metric source(s) above threshold 0.60",
		Metrics: []decision.MetricDecision{{
			MetricName:      "metric-a",
			RawDesired:      6,
			AdjustedDesired: 6,
			Confidence:      0.91,
			Trend:           0.15,
			Weight:          1,
			Qualified:       true,
		}},
	}
	setStatus(pa, 4, 6, true, "apa above target", false, true, nil, out)

	require.Len(t, pa.Status.ScalingHistory, 1)
	entry := pa.Status.ScalingHistory[0]
	assert.False(t, entry.Suppressed)
	assert.Equal(t, "metric-a", entry.Metric)
	assert.Equal(t, int32(91), entry.Confidence)
	assert.Equal(t, int32(15), entry.Trend)
	require.Len(t, entry.MetricContributions, 1)
	assert.True(t, entry.MetricContributions[0].Qualified)
	assert.Equal(t, int32(100), entry.MetricContributions[0].Weight)

	cond := apimeta.FindStatusCondition(pa.Status.Conditions, ConditionConfidenceGuard)
	require.NotNil(t, cond)
	assert.Equal(t, "True", string(cond.Status))
	assert.Equal(t, ReasonConfidenceQualified, cond.Reason)
}

func TestSetStatusWithoutDetailsRemovesGuardCondition(t *testing.T) {
	pa := &autoscalingv1alpha1.PodAutoscaler{}
	setStatus(pa, 4, 4, false, "stable", false, true, nil, suppressedOutcome())
	require.NotNil(t, apimeta.FindStatusCondition(pa.Status.Conditions, ConditionConfidenceGuard))

	// Feature disabled: stale guard condition must be dropped.
	setStatus(pa, 4, 4, false, "stable", false, true, nil, nil)
	assert.Nil(t, apimeta.FindStatusCondition(pa.Status.Conditions, ConditionConfidenceGuard))
}
