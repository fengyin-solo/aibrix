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

package decision

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const paKey = "default/test-pa"

func observation(metric string, confidence, trend float64, raw, current int32) MetricObservation {
	return MetricObservation{
		MetricKey:  paKey + "/" + metric,
		MetricName: metric,
		RawDesired: raw,
		Report: SourceReport{
			Confidence: confidence,
			Trend:      trend,
			Reasons:    []string{"weak factor"},
		},
	}
}

func TestGuardSuppressesLowConfidenceWithoutAnchor(t *testing.T) {
	now := time.Now()
	guard := NewGuard()

	out := guard.Decide(paKey, 4, DefaultConfidenceThreshold, now,
		[]MetricObservation{observation("m", 0.4, 0, 10, 4)}, nil)

	assert.True(t, out.Suppressed)
	assert.False(t, out.Anchored)
	assert.Equal(t, int32(4), out.DesiredReplicas)
	assert.Equal(t, 0, out.QualifiedCount)
	require.Len(t, out.Metrics, 1)
	md := out.Metrics[0]
	assert.False(t, md.Qualified)
	assert.Equal(t, int32(4), md.AdjustedDesired)
	assert.Contains(t, md.SuppressionReason, "low confidence 0.40 below threshold 0.60")
	assert.Contains(t, md.SuppressionReason, "weak factor")
	assert.Contains(t, out.Summary, "scaling suppressed")
}

func TestGuardConvergesTowardStableAnchor(t *testing.T) {
	now := time.Now()
	guard := NewGuard()

	// First cycle: qualified recommendation establishes a stable anchor at 6.
	first := guard.Decide(paKey, 4, DefaultConfidenceThreshold, now,
		[]MetricObservation{observation("m", 0.9, 0, 6, 4)}, nil)
	assert.False(t, first.Suppressed)
	assert.Equal(t, int32(6), first.DesiredReplicas)

	// Second cycle: noisy low-confidence raw recommendation wants to jump to
	// 10; convergence only allows reaching the anchor 6.
	out := guard.Decide(paKey, 5, DefaultConfidenceThreshold, now.Add(30*time.Second),
		[]MetricObservation{observation("m", 0.3, 0, 10, 5)}, nil)
	assert.True(t, out.Suppressed)
	assert.True(t, out.Anchored)
	assert.Equal(t, int32(6), out.DesiredReplicas)
	assert.Equal(t, int32(6), out.Metrics[0].AdjustedDesired)
	assert.True(t, out.Metrics[0].Anchored)

	// Low-confidence recommendation in the other direction is clamped too.
	down := guard.Decide(paKey, 7, DefaultConfidenceThreshold, now.Add(60*time.Second),
		[]MetricObservation{observation("m", 0.3, 0, 1, 7)}, nil)
	assert.True(t, down.Suppressed)
	assert.Equal(t, int32(6), down.DesiredReplicas)
}

func TestGuardAnchorExpires(t *testing.T) {
	now := time.Now()
	guard := NewGuard()

	first := guard.Decide(paKey, 4, DefaultConfidenceThreshold, now,
		[]MetricObservation{observation("m", 0.9, 0, 6, 4)}, nil)
	require.False(t, first.Suppressed)

	// After the anchor TTL the low-confidence decision simply holds current.
	out := guard.Decide(paKey, 8, DefaultConfidenceThreshold, now.Add(anchorTTL+time.Second),
		[]MetricObservation{observation("m", 0.3, 0, 20, 8)}, nil)
	assert.True(t, out.Suppressed)
	assert.False(t, out.Anchored)
	assert.Equal(t, int32(8), out.DesiredReplicas)
}

func TestGuardTrendAlignment(t *testing.T) {
	t.Run("aligned trend relaxes the step", func(t *testing.T) {
		guard := NewGuard()
		// confidence exactly at threshold -> min relaxation 0.25;
		// aligned trend adds 0.25 -> 0.5 of the raw step.
		out := guard.Decide(paKey, 4, DefaultConfidenceThreshold, time.Now(),
			[]MetricObservation{observation("m", 0.6, 0.3, 8, 4)}, nil)
		require.False(t, out.Suppressed)
		assert.Equal(t, int32(6), out.DesiredReplicas) // 4 + ceil(4*0.5)
		assert.Empty(t, out.Metrics[0].SuppressionReason)
	})

	t.Run("opposing trend suppresses the move", func(t *testing.T) {
		guard := NewGuard()
		out := guard.Decide(paKey, 4, DefaultConfidenceThreshold, time.Now(),
			[]MetricObservation{observation("m", 0.9, -0.5, 8, 4)}, nil)
		// Source stays qualified and votes, but its adjusted recommendation
		// is held at current because the trend contradicts the direction.
		assert.False(t, out.Suppressed)
		assert.Equal(t, int32(4), out.DesiredReplicas)
		assert.Equal(t, int32(4), out.Metrics[0].AdjustedDesired)
		assert.True(t, out.Metrics[0].Qualified)
		assert.Contains(t, out.Metrics[0].SuppressionReason, "opposes")
	})
}

func TestGuardMultiMetricWeightedVote(t *testing.T) {
	guard := NewGuard()

	out := guard.Decide(paKey, 10, DefaultConfidenceThreshold, time.Now(),
		[]MetricObservation{
			observation("strong", 1.0, 0, 20, 10), // fully relaxed -> 20
			observation("weak", 0.6, 0, 20, 10),   // 0.25 relaxed -> 13
		}, nil)

	require.False(t, out.Suppressed)
	assert.Equal(t, 2, out.QualifiedCount)
	// weighted: (20*1.0 + 13*0.6) / 1.6 = 17.375 -> 17
	assert.Equal(t, int32(17), out.DesiredReplicas)
	assert.Equal(t, "strong", out.Representative)

	var weightSum float64
	for _, md := range out.Metrics {
		assert.True(t, md.Qualified)
		weightSum += md.Weight
	}
	assert.InDelta(t, 1.0, weightSum, 1e-9)
}

func TestGuardAllSuppressedFallsBackToPAAnchor(t *testing.T) {
	now := time.Now()
	guard := NewGuard()

	qualified := guard.Decide(paKey, 4, DefaultConfidenceThreshold, now,
		[]MetricObservation{
			observation("a", 0.9, 0, 6, 4),
			observation("b", 0.8, 0, 6, 4),
		}, nil)
	require.False(t, qualified.Suppressed)
	assert.Equal(t, int32(6), qualified.DesiredReplicas)

	out := guard.Decide(paKey, 5, DefaultConfidenceThreshold, now.Add(30*time.Second),
		[]MetricObservation{
			observation("a", 0.2, 0, 1, 5),
			observation("b", 0.3, 0, 1, 5),
		}, nil)
	assert.True(t, out.Suppressed)
	assert.True(t, out.Anchored)
	assert.Equal(t, int32(6), out.DesiredReplicas)
}

func TestGuardThresholdBoundary(t *testing.T) {
	guard := NewGuard()

	atThreshold := guard.Decide(paKey, 8, 0.6, time.Now(),
		[]MetricObservation{observation("m", 0.6, 0, 16, 8)}, nil)
	assert.False(t, atThreshold.Suppressed, "confidence equal to threshold qualifies")
	assert.Equal(t, 1, atThreshold.QualifiedCount)
	// min relaxation 0.25 -> 8 + ceil(8*0.25)=10
	assert.Equal(t, int32(10), atThreshold.DesiredReplicas)

	belowThreshold := guard.Decide(paKey+"-other", 8, 0.6, time.Now(),
		[]MetricObservation{observation("m", 0.599, 0, 16, 8)}, nil)
	assert.True(t, belowThreshold.Suppressed, "confidence below threshold is suppressed")
	assert.Equal(t, int32(8), belowThreshold.DesiredReplicas)
}

func TestGuardRecordsMissingMetrics(t *testing.T) {
	guard := NewGuard()
	out := guard.Decide(paKey, 4, DefaultConfidenceThreshold, time.Now(),
		[]MetricObservation{observation("a", 0.9, 0, 6, 4)},
		[]string{"missing-metric"})
	assert.Equal(t, []string{"missing-metric"}, out.MissingMetrics)
	assert.True(t, strings.Contains(out.Summary, "missing metrics: missing-metric"))
}

func TestStepRelaxationAndScaleStep(t *testing.T) {
	assert.InDelta(t, minStepRelaxation, stepRelaxation(DefaultConfidenceThreshold, 0.6), 1e-9)
	assert.InDelta(t, 1.0, stepRelaxation(1.0, 0.6), 1e-9)
	assert.InDelta(t, 0.625, stepRelaxation(0.6+(1-0.6)/2, 0.6), 1e-9)

	assert.Equal(t, int32(4), scaleStep(4, 8, 0))
	assert.Equal(t, int32(6), scaleStep(4, 8, 0.5))
	assert.Equal(t, int32(2), scaleStep(4, 0, 0.5))
	assert.Equal(t, int32(5), scaleStep(4, 10, 0.1)) // minimum move is one replica
	assert.Equal(t, int32(3), scaleStep(4, 0, 0.1))

	assert.Equal(t, int32(5), clampIntoRange(10, 4, 5))
	assert.Equal(t, int32(5), clampIntoRange(1, 7, 5))
	assert.Equal(t, int32(6), clampIntoRange(6, 4, 8))
}
