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
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// anchorTTL bounds how long a past recommendation is used as a
	// convergence target after fresh qualified decisions stop arriving.
	anchorTTL = 10 * time.Minute

	// minStepRelaxation is the fraction of a recommended step that is
	// allowed immediately after a source reaches the confidence threshold.
	minStepRelaxation = 0.25
	// trendAlignedRelaxation is added to the allowed fraction when the
	// observed trend points in the same direction as the recommendation.
	trendAlignedRelaxation = 0.25
)

// MetricObservation is one metric source's raw algorithm recommendation
// paired with its confidence report.
type MetricObservation struct {
	MetricKey  string
	MetricName string
	RawDesired int32
	Report     SourceReport
}

// MetricDecision explains how one metric source participated in the final
// decision.
type MetricDecision struct {
	MetricName        string
	RawDesired        int32
	AdjustedDesired   int32
	Confidence        float64
	Trend             float64
	Weight            float64
	Qualified         bool
	Anchored          bool
	SuppressionReason string
}

// Outcome is the confidence-aware aggregate decision for a PodAutoscaler.
type Outcome struct {
	Enabled         bool
	Threshold       float64
	CurrentReplicas int32
	DesiredReplicas int32
	// QualifiedCount is the number of sources whose confidence met the
	// threshold and therefore cast a vote.
	QualifiedCount int
	// Suppressed is true when no source qualified; the decision then only
	// converges to the current replicas or the last stable recommendation.
	Suppressed bool
	// Anchored indicates a historical stable recommendation was used as
	// the convergence target.
	Anchored bool
	// Representative is the metric that contributed most to the result.
	Representative string
	Metrics        []MetricDecision
	// MissingMetrics lists metric sources that failed to produce any
	// recommendation in this cycle.
	MissingMetrics []string
	Summary        string
}

type anchorEntry struct {
	replicas int32
	at       time.Time
}

// Guard holds recent stable recommendations per PodAutoscaler and per
// metric source. It is safe for concurrent use.
type Guard struct {
	mu            sync.Mutex
	sourceAnchors map[string]anchorEntry
	paAnchors     map[string]anchorEntry
}

// NewGuard creates an empty decision guard.
func NewGuard() *Guard {
	return &Guard{
		sourceAnchors: make(map[string]anchorEntry),
		paAnchors:     make(map[string]anchorEntry),
	}
}

// Decide turns per-source observations into a single replica decision.
//
// Qualified sources (confidence >= threshold) have their recommended step
// relaxed proportionally to confidence, with an extra allowance when the
// trend agrees, and cast confidence-weighted votes. Unqualified sources may
// only converge toward the current replicas or their last stable
// recommendation. If every source is unqualified, the PodAutoscaler-level
// anchor is used; without one the current replicas are held.
func (g *Guard) Decide(
	paKey string,
	current int32,
	threshold float64,
	now time.Time,
	observations []MetricObservation,
	missing []string,
) *Outcome {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.pruneExpiredLocked(now)

	outcome := &Outcome{
		Enabled:         true,
		Threshold:       threshold,
		CurrentReplicas: current,
		DesiredReplicas: current,
		MissingMetrics:  missing,
	}

	type vote struct {
		index  int
		weight float64
	}
	var votes []vote
	var weightSum float64

	for i, obs := range observations {
		md := MetricDecision{
			MetricName:      obs.MetricName,
			RawDesired:      obs.RawDesired,
			Confidence:      obs.Report.Confidence,
			Trend:           obs.Report.Trend,
			AdjustedDesired: current,
		}

		if obs.Report.Confidence >= threshold {
			md.Qualified = true
			allowance := stepRelaxation(obs.Report.Confidence, threshold)
			direction := sign64(int64(obs.RawDesired) - int64(current))
			trendDirection := TrendDirection(obs.Report.Trend)
			switch {
			case direction != 0 && trendDirection == direction:
				allowance = math.Min(1, allowance+trendAlignedRelaxation)
			case direction != 0 && trendDirection == -direction:
				allowance = 0
				md.SuppressionReason = fmt.Sprintf(
					"trend %.2f opposes the recommended scaling direction", obs.Report.Trend)
			}
			md.AdjustedDesired = scaleStep(current, obs.RawDesired, allowance)
			g.sourceAnchors[obs.MetricKey] = anchorEntry{replicas: md.AdjustedDesired, at: now}

			w := obs.Report.Confidence
			votes = append(votes, vote{index: i, weight: w})
			weightSum += w
		} else {
			if anchor, ok := g.freshSourceAnchorLocked(obs.MetricKey, now); ok {
				md.Anchored = true
				md.AdjustedDesired = clampIntoRange(obs.RawDesired, current, anchor)
			}
			md.SuppressionReason = fmt.Sprintf(
				"low confidence %.2f below threshold %.2f", obs.Report.Confidence, threshold)
			if len(obs.Report.Reasons) > 0 {
				md.SuppressionReason += ": " + strings.Join(obs.Report.Reasons, "; ")
			}
		}

		outcome.Metrics = append(outcome.Metrics, md)
	}

	if len(votes) > 0 {
		var weighted float64
		var best vote
		for i, v := range votes {
			weighted += float64(outcome.Metrics[v.index].AdjustedDesired) * v.weight
			if i == 0 || v.weight > best.weight {
				best = v
			}
			outcome.Metrics[v.index].Weight = v.weight / weightSum
		}
		outcome.DesiredReplicas = int32(math.Round(weighted / weightSum))
		outcome.QualifiedCount = len(votes)
		outcome.Representative = outcome.Metrics[best.index].MetricName
		g.paAnchors[paKey] = anchorEntry{replicas: outcome.DesiredReplicas, at: now}
	} else {
		outcome.Suppressed = true
		if anchor, ok := g.freshPAAnchorLocked(paKey, now); ok {
			outcome.Anchored = true
			outcome.DesiredReplicas = anchor
		}
		bestConfidence := -1.0
		for _, md := range outcome.Metrics {
			if md.Confidence > bestConfidence {
				bestConfidence = md.Confidence
				outcome.Representative = md.MetricName
			}
		}
	}

	outcome.Summary = buildSummary(outcome)
	return outcome
}

// stepRelaxation maps confidence to the allowed fraction of the raw step:
// minStepRelaxation at the threshold, scaling linearly to 1.0.
func stepRelaxation(confidence, threshold float64) float64 {
	if threshold >= 1 {
		if confidence >= 1 {
			return 1
		}
		return minStepRelaxation
	}
	relative := (confidence - threshold) / (1 - threshold)
	relative = clamp(relative, 0, 1)
	return minStepRelaxation + (1-minStepRelaxation)*relative
}

// scaleStep moves current toward raw by the allowed fraction. The minimum
// non-zero move is one replica.
func scaleStep(current, raw int32, allowance float64) int32 {
	delta := float64(raw - current)
	if delta == 0 || allowance <= 0 {
		return current
	}
	moved := delta * allowance
	if delta > 0 {
		return current + int32(math.Max(1, math.Ceil(moved)))
	}
	return current + int32(math.Min(-1, math.Floor(moved)))
}

// clampIntoRange restricts value to the interval formed by current and
// anchor, so a weak source can only converge, never overshoot.
func clampIntoRange(value, current, anchor int32) int32 {
	low, high := current, anchor
	if low > high {
		low, high = high, low
	}
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

func (g *Guard) freshSourceAnchorLocked(key string, now time.Time) (int32, bool) {
	anchor, ok := g.sourceAnchors[key]
	if !ok || now.Sub(anchor.at) > anchorTTL {
		return 0, false
	}
	return anchor.replicas, true
}

func (g *Guard) freshPAAnchorLocked(key string, now time.Time) (int32, bool) {
	anchor, ok := g.paAnchors[key]
	if !ok || now.Sub(anchor.at) > anchorTTL {
		return 0, false
	}
	return anchor.replicas, true
}

func (g *Guard) pruneExpiredLocked(now time.Time) {
	for key, anchor := range g.sourceAnchors {
		if now.Sub(anchor.at) > anchorTTL {
			delete(g.sourceAnchors, key)
		}
	}
	for key, anchor := range g.paAnchors {
		if now.Sub(anchor.at) > anchorTTL {
			delete(g.paAnchors, key)
		}
	}
}

func buildSummary(outcome *Outcome) string {
	var b strings.Builder
	if outcome.Suppressed {
		if outcome.Anchored {
			fmt.Fprintf(&b, "scaling suppressed: no metric reached confidence threshold %.2f, converging to last stable recommendation %d replicas",
				outcome.Threshold, outcome.DesiredReplicas)
		} else {
			fmt.Fprintf(&b, "scaling suppressed: no metric reached confidence threshold %.2f, holding at %d replicas",
				outcome.Threshold, outcome.CurrentReplicas)
		}
	} else {
		fmt.Fprintf(&b, "confidence-weighted scaling: %d metric source(s) above threshold %.2f, desired %d replicas",
			outcome.QualifiedCount, outcome.Threshold, outcome.DesiredReplicas)
	}
	for _, md := range outcome.Metrics {
		fmt.Fprintf(&b, "; metric=%s raw=%d adjusted=%d confidence=%.2f trend=%+.2f",
			md.MetricName, md.RawDesired, md.AdjustedDesired, md.Confidence, md.Trend)
		if md.Qualified {
			fmt.Fprintf(&b, " weight=%.2f qualified", md.Weight)
		} else {
			b.WriteString(" unqualified")
		}
		if md.SuppressionReason != "" {
			fmt.Fprintf(&b, " (%s)", md.SuppressionReason)
		}
	}
	if len(outcome.MissingMetrics) > 0 {
		fmt.Fprintf(&b, "; missing metrics: %s", strings.Join(outcome.MissingMetrics, ", "))
	}
	return b.String()
}

func sign64(v int64) int {
	switch {
	case v > 0:
		return 1
	case v < 0:
		return -1
	default:
		return 0
	}
}
