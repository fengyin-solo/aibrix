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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/types"
)

func TestEvaluateSource(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	full := func(now time.Time, values []float64) types.WindowStats {
		return windowStats(base, 10*time.Second, values, now)
	}

	tests := []struct {
		name           string
		in             SourceInput
		now            time.Time
		wantConfidence func(t *testing.T, c float64)
		wantReasons    int
	}{
		{
			name: "full steady samples across five pods",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 5, ReadyReplicas: 5,
				ExpectedPods: 5, ReportingPods: 5,
				StableStats:  full(base.Add(30*time.Second), steady(50, 4)),
				StableWindow: 30 * time.Second,
			},
			now:            base.Add(30 * time.Second),
			wantConfidence: assertGE(0.99),
			wantReasons:    0,
		},
		{
			name: "cold start with a single sample",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 1, ReadyReplicas: 1,
				ExpectedPods: 1, ReportingPods: 1,
				StableStats:  full(base, steady(90, 1)),
				StableWindow: 180 * time.Second,
			},
			now:            base,
			wantConfidence: assertLT(DefaultConfidenceThreshold),
			wantReasons:    1,
		},
		{
			name: "high variance oscillation",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 5, ReadyReplicas: 5,
				ExpectedPods: 5, ReportingPods: 5,
				StableStats:  full(base.Add(180*time.Second), []float64{10, 90, 10, 90, 10, 90, 10, 90, 10, 90, 10, 90, 10, 90, 10, 90, 10, 90, 10}),
				StableWindow: 180 * time.Second,
			},
			now:            base.Add(180 * time.Second),
			wantConfidence: assertLT(DefaultConfidenceThreshold),
		},
		{
			name: "partial pod coverage",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 5, ReadyReplicas: 5,
				ExpectedPods: 5, ReportingPods: 2,
				StableStats:  full(base.Add(30*time.Second), steady(50, 4)),
				StableWindow: 30 * time.Second,
			},
			now: base.Add(30 * time.Second),
			wantConfidence: func(t *testing.T, c float64) {
				assert.Less(t, c, 0.9)
				assert.Greater(t, c, 0.0)
			},
		},
		{
			name: "pending replicas lower confidence",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 4, ReadyReplicas: 2, PendingReplicas: 2,
				ExpectedPods: 4, ReportingPods: 2,
				StableStats:  full(base.Add(30*time.Second), steady(50, 4)),
				StableWindow: 30 * time.Second,
			},
			now: base.Add(30 * time.Second),
			wantConfidence: func(t *testing.T, c float64) {
				assert.Less(t, c, 0.8)
			},
		},
		{
			name: "stale samples decay continuity",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 5, ReadyReplicas: 5,
				ExpectedPods: 5, ReportingPods: 5,
				// newest sample is one full window old
				StableStats:  windowStats(base.Add(-150*time.Second), 10*time.Second, steady(50, 4), base.Add(-150*time.Second)),
				StableWindow: 30 * time.Second,
			},
			now:            base.Add(30 * time.Second),
			wantConfidence: assertLT(0.3),
		},
		{
			name: "external source ignores pod factors",
			in: SourceInput{
				MetricName: "m", PodScoped: false,
				// pod-related fields are deliberately zero
				StableStats:  full(base.Add(30*time.Second), steady(50, 4)),
				StableWindow: 30 * time.Second,
			},
			now:            base.Add(30 * time.Second),
			wantConfidence: assertGE(0.99),
			wantReasons:    0,
		},
		{
			name: "no samples at all",
			in: SourceInput{
				MetricName: "m", PodScoped: true,
				CurrentReplicas: 5, ReadyReplicas: 5,
				ExpectedPods: 5, ReportingPods: 0,
				StableStats:  types.WindowStats{},
				StableWindow: 30 * time.Second,
			},
			now:            base,
			wantConfidence: func(t *testing.T, c float64) { assert.Equal(t, 0.0, c) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := EvaluateSource(tt.in, tt.now)
			assert.GreaterOrEqual(t, report.Confidence, 0.0)
			assert.LessOrEqual(t, report.Confidence, 1.0)
			if tt.wantConfidence != nil {
				tt.wantConfidence(t, report.Confidence)
			}
			if tt.wantReasons > 0 {
				assert.NotEmpty(t, report.Reasons)
			}
			// all applicable components must be bounded
			for name, score := range report.Components {
				if score.Applicable {
					assert.GreaterOrEqual(t, score.Value, 0.0, name)
					assert.LessOrEqual(t, score.Value, 1.0, name)
				}
			}
		})
	}
}

func TestComponentScores(t *testing.T) {
	base := time.Now()

	t.Run("sample coverage external not applicable", func(t *testing.T) {
		s := scoreSampleCoverage(SourceInput{PodScoped: false, ExpectedPods: 0, ReportingPods: 1})
		assert.False(t, s.Applicable)
	})

	t.Run("sample coverage zero pods reported", func(t *testing.T) {
		s := scoreSampleCoverage(SourceInput{PodScoped: true, ExpectedPods: 3, ReportingPods: 0})
		assert.True(t, s.Applicable)
		assert.Equal(t, 0.0, s.Value)
		assert.NotEmpty(t, s.Reason)
	})

	t.Run("variance needs at least two points", func(t *testing.T) {
		s := scoreValueVariance(types.WindowStats{DataPoints: 1, Mean: 50})
		assert.False(t, s.Applicable)
	})

	t.Run("variance score decreases with coefficient of variation", func(t *testing.T) {
		steady := scoreValueVariance(types.WindowStats{DataPoints: 4, Mean: 50, StdDev: 0})
		assert.Equal(t, 1.0, steady.Value)
		noisy := scoreValueVariance(types.WindowStats{DataPoints: 4, Mean: 50, StdDev: 50})
		assert.Equal(t, cvFloor, noisy.Value)
		assert.NotEmpty(t, noisy.Reason)
	})

	t.Run("pod count saturates at five replicas", func(t *testing.T) {
		assert.Equal(t, 0.2, scorePodCount(SourceInput{PodScoped: true, CurrentReplicas: 1}).Value)
		assert.Equal(t, 1.0, scorePodCount(SourceInput{PodScoped: true, CurrentReplicas: 5}).Value)
		assert.Equal(t, 1.0, scorePodCount(SourceInput{PodScoped: true, CurrentReplicas: 9}).Value)
	})

	t.Run("pending fraction", func(t *testing.T) {
		s := scorePendingPods(SourceInput{PodScoped: true, CurrentReplicas: 4, ReadyReplicas: 3, PendingReplicas: 1})
		assert.InDelta(t, 0.75, s.Value, 1e-9)
		assert.NotEmpty(t, s.Reason)
	})

	_ = base
}

func TestCalculateTrend(t *testing.T) {
	t.Run("rising recent half", func(t *testing.T) {
		stats := windowStats(time.Now(), 10*time.Second, []float64{30, 30, 90, 90}, time.Now())
		assert.Greater(t, CalculateTrend(stats, types.WindowStats{}), 1.0)
		assert.Equal(t, 1, TrendDirection(CalculateTrend(stats, types.WindowStats{})))
	})

	t.Run("falling recent half", func(t *testing.T) {
		stats := windowStats(time.Now(), 10*time.Second, []float64{90, 90, 30, 30}, time.Now())
		assert.Less(t, CalculateTrend(stats, types.WindowStats{}), 0.0)
		assert.Equal(t, -1, TrendDirection(CalculateTrend(stats, types.WindowStats{})))
	})

	t.Run("flat values within deadband", func(t *testing.T) {
		stats := windowStats(time.Now(), 10*time.Second, []float64{50, 50, 50, 50}, time.Now())
		assert.Equal(t, 0.0, CalculateTrend(stats, types.WindowStats{}))
		assert.Equal(t, 0, TrendDirection(CalculateTrend(stats, types.WindowStats{})))
	})

	t.Run("panic fallback for single sample", func(t *testing.T) {
		stable := types.WindowStats{DataPoints: 1, Mean: 40}
		panic := types.WindowStats{DataPoints: 1, Mean: 60}
		assert.InDelta(t, 0.5, CalculateTrend(stable, panic), 1e-9)
	})
}

func steady(value float64, n int) []float64 {
	values := make([]float64, n)
	for i := range values {
		values[i] = value
	}
	return values
}

// windowStats builds ordered samples starting at start every interval.
// DataPoints/Mean/Variance are derived the same way MetricsClient computes
// them so confidence tests exercise realistic statistics.
func windowStats(start time.Time, interval time.Duration, values []float64, now time.Time) types.WindowStats {
	stats := types.WindowStats{
		DataPoints: len(values),
		Values:     values,
	}
	timestamps := make([]time.Time, len(values))
	var sum, varianceSum float64
	for i, v := range values {
		timestamps[i] = start.Add(time.Duration(i) * interval)
		sum += v
	}
	mean := sum / float64(len(values))
	for _, v := range values {
		diff := v - mean
		varianceSum += diff * diff
	}
	stats.Mean = mean
	stats.Variance = varianceSum / float64(len(values))
	stats.StdDev = math.Sqrt(stats.Variance)
	stats.Min, stats.Max = values[0], values[0]
	for _, v := range values[1:] {
		if v < stats.Min {
			stats.Min = v
		}
		if v > stats.Max {
			stats.Max = v
		}
	}
	stats.Timestamps = timestamps
	stats.WindowStart = timestamps[0]
	stats.WindowEnd = timestamps[len(timestamps)-1]
	stats.LastUpdate = now
	return stats
}

func assertGE(threshold float64) func(*testing.T, float64) {
	return func(t *testing.T, c float64) {
		assert.GreaterOrEqual(t, c, threshold)
	}
}

func assertLT(threshold float64) func(*testing.T, float64) {
	return func(t *testing.T, c float64) {
		assert.Less(t, c, threshold)
	}
}
