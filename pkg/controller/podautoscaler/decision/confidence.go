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

// Package decision implements confidence and trend aware scaling decisions
// for the PodAutoscaler. It turns per-metric-source recommendations into a
// single replica decision, suppressing large moves when metric samples are
// few, missing, stale or noisy, and relaxing limits gradually when the
// observed trend supports the recommended direction.
package decision

import (
	"fmt"
	"math"
	"time"

	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/types"
)

const (
	// DefaultConfidenceThreshold is the minimum overall confidence required
	// for a metric source recommendation to drive scaling.
	DefaultConfidenceThreshold = 0.6

	// defaultScrapeInterval estimates the expected sampling cadence when too
	// few samples exist to derive an observed interval.
	defaultScrapeInterval = 10 * time.Second

	// trendDeadband ignores small trend values around zero.
	trendDeadband = 0.05

	// Component weights for the overall confidence. They sum to 1.
	weightSampleCoverage = 0.25
	weightValueVariance  = 0.25
	weightTimeContinuity = 0.20
	weightPendingPods    = 0.15
	weightPodCount       = 0.15

	// Coefficient-of-variation score bounds.
	cvLow   = 0.10
	cvHigh  = 0.80
	cvFloor = 0.10

	// scoreFloor keeps a weak but non-zero component from annihilating the
	// weighted geometric mean. A truly zero component (e.g. no reporting
	// pods) still forces the overall confidence to zero.
	scoreFloor = 0.05
)

// Confidence component names.
const (
	ComponentSampleCoverage = "SampleCoverage"
	ComponentValueVariance  = "ValueVariance"
	ComponentTimeContinuity = "TimeContinuity"
	ComponentPendingPods    = "PendingPods"
	ComponentPodCount       = "PodCount"
)

// SourceInput contains the observations used to score one metric source.
type SourceInput struct {
	MetricName string
	// PodScoped is true for sources scraped per pod (pod/resource/custom)
	// and false for external sources, for which pod coverage, pending and
	// pod count factors do not apply.
	PodScoped bool

	CurrentReplicas int32
	ReadyReplicas   int32
	PendingReplicas int32

	// ExpectedPods is the number of pods scraped in the latest cycle and
	// ReportingPods the number that returned a value.
	ExpectedPods  int
	ReportingPods int

	// StableStats describes the samples currently held in the stable window.
	StableStats  types.WindowStats
	StableWindow time.Duration
	// PanicStats is used only as a trend fallback for very short histories.
	PanicStats types.WindowStats
}

// ComponentScore is a single bounded confidence factor.
type ComponentScore struct {
	Value      float64
	Applicable bool
	Reason     string
}

// SourceReport is the confidence/trend evaluation of one metric source.
type SourceReport struct {
	Confidence float64
	Trend      float64
	Components map[string]ComponentScore
	// Reasons explains weak factors even when the overall confidence
	// happens to stay above the threshold.
	Reasons []string
}

// EvaluateSource computes a [0,1] confidence and a trend for one metric
// source from sample coverage, value variance, time continuity, pending-pod
// correction and pod count confidence.
func EvaluateSource(in SourceInput, now time.Time) SourceReport {
	components := map[string]ComponentScore{
		ComponentSampleCoverage: scoreSampleCoverage(in),
		ComponentValueVariance:  scoreValueVariance(in.StableStats),
		ComponentTimeContinuity: scoreTimeContinuity(in.StableStats, in.StableWindow, now),
		ComponentPendingPods:    scorePendingPods(in),
		ComponentPodCount:       scorePodCount(in),
	}

	confidence := combineComponents(components)

	var reasons []string
	for _, name := range []string{
		ComponentSampleCoverage,
		ComponentValueVariance,
		ComponentTimeContinuity,
		ComponentPendingPods,
		ComponentPodCount,
	} {
		score := components[name]
		if score.Applicable && score.Value < 0.6 && score.Reason != "" {
			reasons = append(reasons, score.Reason)
		}
	}

	return SourceReport{
		Confidence: confidence,
		Trend:      CalculateTrend(in.StableStats, in.PanicStats),
		Components: components,
		Reasons:    reasons,
	}
}

// CalculateTrend returns the relative change rate of the stable window
// series, comparing the recent half against the older half. When fewer than
// two samples exist, the panic/stable mean ratio is used as a fallback. The
// result is bounded to [-1, 2].
func CalculateTrend(stable, panic types.WindowStats) float64 {
	values := stable.Values
	if len(values) >= 2 {
		half := len(values) / 2
		older := average(values[:half])
		recent := average(values[half:])
		if older > 0 {
			return clamp((recent-older)/older, -1, 2)
		}
	}
	if stable.Mean > 0 && panic.DataPoints > 0 {
		return clamp((panic.Mean-stable.Mean)/stable.Mean, -1, 2)
	}
	return 0
}

// TrendDirection converts a trend value into -1 (down), 0 (flat) or 1 (up)
// using a small deadband.
func TrendDirection(trend float64) int {
	if trend > trendDeadband {
		return 1
	}
	if trend < -trendDeadband {
		return -1
	}
	return 0
}

// scoreSampleCoverage measures how many expected pods reported a value in
// the latest scrape. A missing scrape is a stronger signal than a noisy one.
func scoreSampleCoverage(in SourceInput) ComponentScore {
	if !in.PodScoped {
		return ComponentScore{Applicable: false}
	}
	if in.ExpectedPods <= 0 {
		if in.ReportingPods > 0 {
			return ComponentScore{Value: 1, Applicable: true}
		}
		return ComponentScore{
			Value:      0,
			Applicable: true,
			Reason:     "no pods available to report metrics",
		}
	}

	value := clamp(float64(in.ReportingPods)/float64(in.ExpectedPods), 0, 1)
	score := ComponentScore{Value: value, Applicable: true}
	if value < 1 {
		score.Reason = fmt.Sprintf("partial sample coverage: %d/%d pods reported metrics", in.ReportingPods, in.ExpectedPods)
	}
	return score
}

// scoreValueVariance converts the coefficient of variation of the window
// samples into a [cvFloor,1] score. Variance cannot be estimated with fewer
// than two samples, in which case the factor is not applicable.
func scoreValueVariance(stats types.WindowStats) ComponentScore {
	if stats.DataPoints < 2 || stats.Mean == 0 {
		return ComponentScore{Applicable: false}
	}

	cv := stats.StdDev / math.Abs(stats.Mean)
	var value float64
	switch {
	case cv <= cvLow:
		value = 1
	case cv >= cvHigh:
		value = cvFloor
	default:
		value = 1 - (1-cvFloor)*(cv-cvLow)/(cvHigh-cvLow)
	}

	score := ComponentScore{Value: value, Applicable: true}
	if value < 0.6 {
		score.Reason = fmt.Sprintf("high metric value variance: coefficient of variation %.2f", cv)
	}
	return score
}

// scoreTimeContinuity combines window fill ratio (expected sample slots
// observed) with staleness (time since the most recent sample).
func scoreTimeContinuity(stats types.WindowStats, window time.Duration, now time.Time) ComponentScore {
	if stats.DataPoints == 0 || window <= 0 {
		return ComponentScore{
			Value:      0,
			Applicable: true,
			Reason:     "no metric samples within the stable window",
		}
	}

	interval := defaultScrapeInterval
	if len(stats.Timestamps) >= 2 {
		if median := medianPositiveGap(stats.Timestamps); median > 0 {
			interval = median
		}
	}

	expectedSlots := float64(int(window/interval) + 1)
	fill := math.Min(1, float64(stats.DataPoints)/expectedSlots)

	staleness := 1.0
	lastSample := stats.Timestamps[len(stats.Timestamps)-1]
	if len(stats.Timestamps) == 0 {
		lastSample = stats.WindowEnd
	}
	if age := now.Sub(lastSample); age > interval {
		staleness = math.Max(0, 1-float64(age-interval)/float64(window))
	}

	value := fill * staleness
	score := ComponentScore{Value: value, Applicable: true}
	if value < 0.6 {
		score.Reason = fmt.Sprintf("poor time continuity: %d sample(s) over %s window, latest sample %s old",
			stats.DataPoints, window, now.Sub(lastSample).Round(time.Second))
	}
	return score
}

// scorePendingPods lowers confidence while a fraction of replicas are not
// ready, because their future load is still unknown.
func scorePendingPods(in SourceInput) ComponentScore {
	if !in.PodScoped || in.CurrentReplicas <= 0 {
		return ComponentScore{Applicable: false}
	}
	value := clamp(float64(in.ReadyReplicas)/float64(in.CurrentReplicas), 0, 1)
	score := ComponentScore{Value: value, Applicable: true}
	if in.PendingReplicas > 0 && value < 1 {
		score.Reason = fmt.Sprintf("pending replicas reduce confidence: %d/%d replicas not ready",
			in.PendingReplicas, in.CurrentReplicas)
	}
	return score
}

// scorePodCount shrinks confidence for very small replica sets where a
// single pod's jitter dominates the average.
func scorePodCount(in SourceInput) ComponentScore {
	if !in.PodScoped {
		return ComponentScore{Applicable: false}
	}
	value := math.Min(1, float64(in.CurrentReplicas)/5.0)
	score := ComponentScore{Value: value, Applicable: true}
	if in.CurrentReplicas < 3 {
		score.Reason = fmt.Sprintf("small replica set (%d pod(s)) is sensitive to single-pod jitter", in.CurrentReplicas)
	}
	return score
}

// combineComponents merges applicable factors with a weighted geometric
// mean: any genuinely zero factor forces a zero confidence.
func combineComponents(components map[string]ComponentScore) float64 {
	weights := map[string]float64{
		ComponentSampleCoverage: weightSampleCoverage,
		ComponentValueVariance:  weightValueVariance,
		ComponentTimeContinuity: weightTimeContinuity,
		ComponentPendingPods:    weightPendingPods,
		ComponentPodCount:       weightPodCount,
	}

	var weightSum, logSum float64
	for name, weight := range weights {
		score, ok := components[name]
		if !ok || !score.Applicable {
			continue
		}
		if score.Value <= 0 {
			return 0
		}
		weightSum += weight
		logSum += weight * math.Log(math.Max(score.Value, scoreFloor))
	}
	if weightSum == 0 {
		return 0
	}
	return math.Exp(logSum / weightSum)
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func medianPositiveGap(timestamps []time.Time) time.Duration {
	var gaps []time.Duration
	for i := 1; i < len(timestamps); i++ {
		if gap := timestamps[i].Sub(timestamps[i-1]); gap > 0 {
			gaps = append(gaps, gap)
		}
	}
	if len(gaps) == 0 {
		return 0
	}
	// Simple insertion sort; the sample count is small.
	for i := 1; i < len(gaps); i++ {
		for j := i; j > 0 && gaps[j-1] > gaps[j]; j-- {
			gaps[j-1], gaps[j] = gaps[j], gaps[j-1]
		}
	}
	mid := len(gaps) / 2
	if len(gaps)%2 == 1 {
		return gaps[mid]
	}
	return (gaps[mid-1] + gaps[mid]) / 2
}

func clamp(value, low, high float64) float64 {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}
