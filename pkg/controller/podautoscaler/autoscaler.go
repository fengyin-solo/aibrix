/*
Copyright 2024 The Aibrix Team.

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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	autoscalingv1alpha1 "github.com/vllm-project/aibrix/api/autoscaling/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/aggregation"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/algorithm"
	scalingctx "github.com/vllm-project/aibrix/pkg/controller/podautoscaler/context"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/decision"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/metrics"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AutoScaler provides scaling decision capabilities based on metrics and algorithms.
// This interface focuses purely on scaling logic without actual resource manipulation.
// All implementations are stateless and thread-safe, supporting concurrent reconciliation.
type AutoScaler interface {
	// ComputeDesiredReplicas performs metric-based scaling calculation.
	// This is the primary method for scaling decisions and returns only the recommendation.
	// It does NOT perform any actual scaling operations.
	// All per-PA configuration is extracted from the PodAutoscaler spec on each call.
	ComputeDesiredReplicas(ctx context.Context, request ReplicaComputeRequest) (*ReplicaComputeResult, error)
}

// ReplicaComputeRequest represents a request for replica calculation.
// This type is used both for the public interface and internal pipeline processing.
type ReplicaComputeRequest struct {
	PodAutoscaler   autoscalingv1alpha1.PodAutoscaler
	ScalingContext  scalingctx.ScalingContext // Single source of truth for PA-level configuration
	CurrentReplicas int32
	ReplicaState    ReplicaState
	Pods            []corev1.Pod
	Timestamp       time.Time
}

// ReplicaState captures scale target readiness state for pipeline-level guards.
type ReplicaState struct {
	ReadyReplicas   int32
	PendingReplicas int32
}

// ReplicaComputeResult represents the result of replica calculation
type ReplicaComputeResult struct {
	DesiredReplicas           int32
	Algorithm                 string
	MetricName                string
	MetricSourceType          autoscalingv1alpha1.MetricSourceType
	MetricValue               float64
	Reason                    string
	Valid                     bool
	PendingReplicaGuardActive bool
	// ReportingPods is the number of scraped pods that returned a value in
	// the latest cycle, used for sample coverage confidence.
	ReportingPods int
	// Decision carries confidence/trend diagnostics. It is nil when
	// confidence-aware scaling is disabled.
	Decision *decision.Outcome
}

// DefaultAutoScaler implements the complete scaling pipeline
// All components are stateless or thread-safe, allowing concurrent reconciliation
type DefaultAutoScaler struct {
	// Immutable shared components (all thread-safe)
	factory       metrics.MetricFetcherFactory
	client        client.Client
	metricsClient *metrics.MetricsClient
	aggregator    aggregation.MetricAggregator

	// Algorithm cache (algorithms are stateless structs, can be safely reused)
	mu             sync.RWMutex
	algorithmCache map[autoscalingv1alpha1.ScalingStrategyType]algorithm.ScalingAlgorithm

	// guard holds recent stable recommendations for confidence-aware decisions
	guard *decision.Guard
}

// NewDefaultAutoScaler creates a new default autoscaler
func NewDefaultAutoScaler(
	factory metrics.MetricFetcherFactory,
	client client.Client,
) *DefaultAutoScaler {
	// Create stable metrics client with default window durations
	// Windows auto-initialize on first use based on the PodAutoscaler config
	metricsClient := metrics.NewMetricsClient(time.Second)

	// Create single aggregator for all strategies (stateless, just delegates to client)
	aggregator := aggregation.NewMetricAggregator(metricsClient)

	return &DefaultAutoScaler{
		factory:        factory,
		client:         client,
		metricsClient:  metricsClient,
		aggregator:     aggregator,
		algorithmCache: make(map[autoscalingv1alpha1.ScalingStrategyType]algorithm.ScalingAlgorithm),
		guard:          decision.NewGuard(),
	}
}

// getOrCreateAlgorithm retrieves or creates a stateless algorithm for the given strategy
// The algorithm struct is stateless and can be safely cached and reused
func (a *DefaultAutoScaler) getOrCreateAlgorithm(strategy autoscalingv1alpha1.ScalingStrategyType) algorithm.ScalingAlgorithm {
	// Try read lock first (common case)
	a.mu.RLock()
	algo, exists := a.algorithmCache[strategy]
	a.mu.RUnlock()
	if exists {
		return algo
	}

	// Need to create, acquire write lock
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock
	if algo, exists := a.algorithmCache[strategy]; exists {
		return algo
	}

	// Create new stateless algorithm instance
	algo = algorithm.NewScalingAlgorithm(strategy)
	a.algorithmCache[strategy] = algo
	return algo
}

// ComputeDesiredReplicas computes desired replicas based on all metrics in MetricsSources.
// When confidence-aware scaling is disabled it returns the maximum recommended
// replicas across all valid metrics (historical behavior). When enabled,
// qualified metric sources cast confidence-weighted votes while low-confidence
// sources may only converge to the current or last stable recommendation.
func (a *DefaultAutoScaler) ComputeDesiredReplicas(ctx context.Context, request ReplicaComputeRequest) (*ReplicaComputeResult, error) {
	pa := request.PodAutoscaler
	metricsSources := pa.Spec.MetricsSources

	if len(metricsSources) == 0 {
		return &ReplicaComputeResult{Valid: false}, fmt.Errorf(
			"no metricsSources defined in PodAutoscaler %s/%s", pa.Namespace, pa.Name)
	}

	confidenceAware := request.ScalingContext.GetConfidenceAwareScalingEnabled()

	var validResults []*ReplicaComputeResult // to record the recommended result calculated in the current round
	var observations []decision.MetricObservation
	var missingMetrics []string
	var anyValid bool

	for _, metricSource := range metricsSources {
		// Extract per-request configuration
		metricKey := types.MetricKey{
			Namespace:   pa.Namespace,
			Name:        pa.Spec.ScaleTargetRef.Name,
			MetricName:  metricSource.TargetMetric,
			PaNamespace: pa.Namespace,
			PaName:      pa.Name,
		}

		result, err := a.computeReplicasForSingleMetric(ctx, request, metricKey, metricSource)
		if err != nil {
			klog.ErrorS(err, "Failed to compute replicas for metric",
				"PodAutoscaler", klog.KObj(&pa),
				"metric", metricSource.TargetMetric,
				"metricSourceType", metricSource.MetricSourceType)
			// Continue processing other metrics — one failure shouldn't block all
			if confidenceAware {
				missingMetrics = append(missingMetrics, metricSource.TargetMetric)
			}
			continue
		}

		if result.Valid {
			anyValid = true
			validResults = append(validResults, result)
			klog.V(4).InfoS("Computed replicas for metric",
				"PodAutoscaler", klog.KObj(&pa),
				"metricName", metricSource.TargetMetric,
				"desiredReplicas", result.DesiredReplicas,
				"algorithm", result.Algorithm,
				"reason", result.Reason,
				"currentReplicas", request.CurrentReplicas,
			)
			if confidenceAware {
				observations = append(observations,
					a.buildMetricObservation(request, metricKey, metricSource, result))
			}
		}
	}

	if !anyValid {
		return &ReplicaComputeResult{Valid: false}, fmt.Errorf("all %d metric sources failed for "+
			"PodAutoscaler %s/%s", len(metricsSources), pa.Namespace, pa.Name)
	}

	var bestResult *ReplicaComputeResult
	var outcome *decision.Outcome

	if confidenceAware {
		paKey := fmt.Sprintf("%s/%s", pa.Namespace, pa.Name)
		outcome = a.guard.Decide(
			paKey,
			request.CurrentReplicas,
			request.ScalingContext.GetConfidenceThreshold(),
			request.Timestamp,
			observations,
			missingMetrics,
		)

		representative := findResultByMetric(validResults, outcome.Representative)
		klog.V(2).InfoS("Confidence-aware autoscaling computed result",
			"podAutoscaler", klog.KObj(&pa),
			"desiredReplicas", outcome.DesiredReplicas,
			"suppressed", outcome.Suppressed,
			"qualifiedCount", outcome.QualifiedCount,
			"currentReplicas", request.CurrentReplicas,
			"summary", outcome.Summary,
		)
		bestResult = &ReplicaComputeResult{
			DesiredReplicas:  outcome.DesiredReplicas,
			Algorithm:        representative.Algorithm,
			MetricName:       representative.MetricName,
			MetricSourceType: representative.MetricSourceType,
			MetricValue:      representative.MetricValue,
			Reason:           outcome.Summary,
			Valid:            true,
			Decision:         outcome,
		}
	} else {
		// compare the recommended values of the current round and use the maximum value
		bestResult = validResults[0]
		for _, r := range validResults[1:] {
			if r.DesiredReplicas > bestResult.DesiredReplicas {
				bestResult = r
			}
		}

		allValidReplicas := make([]int32, len(validResults))
		for i, res := range validResults {
			allValidReplicas[i] = res.DesiredReplicas
		}

		klog.V(2).InfoS("Multi metric autoscaling computed result",
			"podAutoscaler", klog.KObj(&pa),
			"metricCount", len(allValidReplicas),
			"desiredReplicas", bestResult.DesiredReplicas,
			"validDesiredReplicas", allValidReplicas,
			"currentReplicas", request.CurrentReplicas,
		)

		bestResult = &ReplicaComputeResult{
			DesiredReplicas:           bestResult.DesiredReplicas,
			Algorithm:                 bestResult.Algorithm,
			MetricName:                bestResult.MetricName,
			MetricSourceType:          bestResult.MetricSourceType,
			MetricValue:               bestResult.MetricValue,
			Reason:                    bestResult.Reason,
			Valid:                     true,
			PendingReplicaGuardActive: bestResult.PendingReplicaGuardActive,
		}
	}

	bestResult = applyPendingReplicaGuard(request, bestResult)
	bestResult.Decision = outcome

	return bestResult, nil
}

// findResultByMetric returns the valid result for the given metric name, or
// the first result when the name cannot be matched.
func findResultByMetric(results []*ReplicaComputeResult, metricName string) *ReplicaComputeResult {
	for _, r := range results {
		if r.MetricName == metricName {
			return r
		}
	}
	return results[0]
}

// buildMetricObservation scores one metric source and pairs it with the raw
// algorithm recommendation for the confidence-aware decision guard.
func (a *DefaultAutoScaler) buildMetricObservation(
	request ReplicaComputeRequest,
	metricKey types.MetricKey,
	metricSource autoscalingv1alpha1.MetricSource,
	result *ReplicaComputeResult,
) decision.MetricObservation {
	stableWindow, _ := metricWindowDurations(request.PodAutoscaler)

	stableStats, panicStats, err := a.metricsClient.GetUnifiedStats(metricKey, request.Timestamp)
	if err != nil {
		klog.V(4).InfoS("No window stats available for confidence evaluation",
			"metricKey", metricKey.String(), "err", err)
		stableStats = types.WindowStats{}
		panicStats = types.WindowStats{}
	}

	podScoped := metricSource.MetricSourceType != autoscalingv1alpha1.EXTERNAL &&
		metricSource.MetricSourceType != autoscalingv1alpha1.DOMAIN

	report := decision.EvaluateSource(decision.SourceInput{
		MetricName:      metricSource.TargetMetric,
		PodScoped:       podScoped,
		CurrentReplicas: request.CurrentReplicas,
		ReadyReplicas:   request.ReplicaState.ReadyReplicas,
		PendingReplicas: request.ReplicaState.PendingReplicas,
		ExpectedPods:    len(request.Pods),
		ReportingPods:   result.ReportingPods,
		StableStats:     stableStats,
		StableWindow:    stableWindow,
		PanicStats:      panicStats,
	}, request.Timestamp)

	return decision.MetricObservation{
		MetricKey:  metricKey.String(),
		MetricName: metricSource.TargetMetric,
		RawDesired: result.DesiredReplicas,
		Report:     report,
	}
}

func applyPendingReplicaGuard(request ReplicaComputeRequest, result *ReplicaComputeResult) *ReplicaComputeResult {
	if result == nil || !result.Valid {
		return result
	}
	if request.PodAutoscaler.Spec.ScalingStrategy != autoscalingv1alpha1.KPA &&
		request.PodAutoscaler.Spec.ScalingStrategy != autoscalingv1alpha1.APA {
		return result
	}
	if result.MetricSourceType != autoscalingv1alpha1.POD &&
		result.MetricSourceType != autoscalingv1alpha1.RESOURCE {
		return result
	}
	if request.ReplicaState.PendingReplicas <= 0 || result.DesiredReplicas == request.CurrentReplicas {
		return result
	}
	if request.CurrentReplicas <= 0 {
		return result
	}

	targetValue, ok := request.ScalingContext.GetTargetValueForMetric(result.MetricName)
	if !ok || targetValue <= 0 {
		return result
	}

	readyReplicas := request.ReplicaState.ReadyReplicas
	if readyReplicas < 0 {
		readyReplicas = 0
	}
	if readyReplicas > request.CurrentReplicas {
		readyReplicas = request.CurrentReplicas
	}

	pendingReplicas := request.CurrentReplicas - readyReplicas
	if pendingReplicas <= 0 {
		return result
	}

	currentReplicas := float64(request.CurrentReplicas)
	readyCount := float64(readyReplicas)
	pendingCount := float64(pendingReplicas)

	var desiredReplicas int32
	var reason string

	if result.DesiredReplicas > request.CurrentReplicas {
		adjustedMetricValue := result.MetricValue * readyCount / currentReplicas
		desiredReplicas = computePendingAdjustedReplicas(request, result, adjustedMetricValue, targetValue, readyReplicas, pendingReplicas)
		if desiredReplicas < request.CurrentReplicas {
			desiredReplicas = request.CurrentReplicas
		}
		if desiredReplicas > result.DesiredReplicas {
			desiredReplicas = result.DesiredReplicas
		}
		reason = "scale-up adjusted: pending replicas treated as missing metrics"
	} else {
		adjustedMetricValue := (result.MetricValue*readyCount + targetValue*pendingCount) / currentReplicas
		desiredReplicas = computePendingAdjustedReplicas(request, result, adjustedMetricValue, targetValue, readyReplicas, pendingReplicas)
		if desiredReplicas > request.CurrentReplicas {
			desiredReplicas = request.CurrentReplicas
		}
		if desiredReplicas < result.DesiredReplicas {
			desiredReplicas = result.DesiredReplicas
		}
		reason = "scale-down adjusted: pending replicas treated at target utilization"
	}

	if desiredReplicas == result.DesiredReplicas {
		return result
	}

	return &ReplicaComputeResult{
		DesiredReplicas:           desiredReplicas,
		Algorithm:                 result.Algorithm,
		MetricName:                result.MetricName,
		MetricSourceType:          result.MetricSourceType,
		MetricValue:               result.MetricValue,
		Reason:                    reason,
		Valid:                     true,
		PendingReplicaGuardActive: true,
	}
}

func computePendingAdjustedReplicas(
	request ReplicaComputeRequest,
	result *ReplicaComputeResult,
	adjustedMetricValue float64,
	targetValue float64,
	readyReplicas int32,
	pendingReplicas int32,
) int32 {
	switch request.PodAutoscaler.Spec.ScalingStrategy {
	case autoscalingv1alpha1.APA:
		return int32(math.Ceil(float64(request.CurrentReplicas) * adjustedMetricValue / targetValue))
	case autoscalingv1alpha1.KPA:
		if result.DesiredReplicas < request.CurrentReplicas {
			return int32(math.Ceil((result.MetricValue*float64(readyReplicas) + targetValue*float64(pendingReplicas)) / targetValue))
		}
		return int32(math.Ceil(adjustedMetricValue / targetValue))
	default:
		return result.DesiredReplicas
	}
}

// computeReplicasForSingleMetric computes desired replicas for a single MetricSource.
// It wraps executeScalingPipeline and formats the result.
func (a *DefaultAutoScaler) computeReplicasForSingleMetric(
	ctx context.Context,
	request ReplicaComputeRequest,
	metricKey types.MetricKey,
	metricSource autoscalingv1alpha1.MetricSource,
) (*ReplicaComputeResult, error) {

	// Get algorithm based on the PA-level scaling strategy (shared across all metrics)
	algo := a.getOrCreateAlgorithm(request.PodAutoscaler.Spec.ScalingStrategy)

	// Execute the full pipeline for this single metric
	recommendation, err := a.executeScalingPipeline(ctx, request, metricKey, metricSource, algo)
	if err != nil {
		return &ReplicaComputeResult{Valid: false}, fmt.Errorf("failed to compute recommendation for metric %q: %w", metricSource.TargetMetric, err)
	}

	if !recommendation.ScaleValid {
		return &ReplicaComputeResult{Valid: false}, fmt.Errorf("scaling recommendation invalid for metric %q", metricSource.TargetMetric)
	}

	return &ReplicaComputeResult{
		DesiredReplicas:  recommendation.DesiredReplicas,
		Algorithm:        recommendation.Algorithm,
		MetricName:       metricSource.TargetMetric,
		MetricSourceType: metricSource.MetricSourceType,
		MetricValue:      recommendationMetricValue(recommendation),
		Reason:           recommendation.Reason,
		Valid:            true,
		ReportingPods:    int(recommendationIntMetadata(recommendation, "reporting_pods")),
	}, nil
}

// recommendationIntMetadata extracts an integer metadata value from a
// recommendation, supporting common numeric types.
func recommendationIntMetadata(recommendation *algorithm.ScalingRecommendation, key string) int64 {
	if recommendation == nil || recommendation.Metadata == nil {
		return 0
	}
	switch value := recommendation.Metadata[key].(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case float32:
		return int64(value)
	default:
		return 0
	}
}

func recommendationMetricValue(recommendation *algorithm.ScalingRecommendation) float64 {
	if recommendation == nil || recommendation.Metadata == nil {
		return 0
	}
	switch value := recommendation.Metadata["current_value"].(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int32:
		return float64(value)
	case int64:
		return float64(value)
	case json.Number:
		floatValue, err := value.Float64()
		if err == nil {
			return floatValue
		}
		return 0
	default:
		return 0
	}
}

// executeScalingPipeline contains the common scaling logic for replica computation
func (a *DefaultAutoScaler) executeScalingPipeline(
	ctx context.Context,
	request ReplicaComputeRequest,
	metricKey types.MetricKey,
	metricSource autoscalingv1alpha1.MetricSource,
	algo algorithm.ScalingAlgorithm,
) (*algorithm.ScalingRecommendation, error) {
	workloadKey := fmt.Sprintf("%s/%s", request.PodAutoscaler.Namespace, request.PodAutoscaler.Name)

	// Step 1: Collect metrics
	collectionSpec := types.CollectionSpec{
		Namespace:    metricKey.Namespace,
		TargetName:   metricKey.Name,
		MetricName:   metricKey.MetricName,
		MetricSource: metricSource,
		Pods:         request.Pods,
		Timestamp:    request.Timestamp,
	}

	klog.InfoS("Collecting metrics", "source", workloadKey, "pods", len(request.Pods))
	snapshot, err := metrics.CollectMetrics(ctx, collectionSpec, a.factory)
	if err != nil {
		return nil, fmt.Errorf("failed to collect metrics for %s: %w", workloadKey, err)
	}

	// Use scaling context from request (single source of truth)
	scalingContext := request.ScalingContext

	// Step 2: Process and aggregate metrics
	stableWindow, panicWindow := metricWindowDurations(request.PodAutoscaler)
	if err := a.metricsClient.ConfigureMetricWindows(metricKey, stableWindow, panicWindow); err != nil {
		return nil, fmt.Errorf("failed to configure metric windows for %s: %w", workloadKey, err)
	}

	klog.InfoS("Processing metrics snapshot", "source", workloadKey, "healthy metrics pods", len(snapshot.Values), "values", snapshot.Values)
	if err := a.aggregator.ProcessSnapshot(metricKey, snapshot); err != nil {
		return nil, fmt.Errorf("failed to process metrics snapshot for %s: %w", workloadKey, err)
	}
	// Use the full metricKey with PaNamespace and PaName for proper multi-tenancy
	aggregatedMetrics, err := a.aggregator.GetAggregatedMetrics(metricKey, request.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregated metrics for %s %s: %w", workloadKey, metricKey, err)
	}

	// Step 3: confidence and trend are computed by the decision layer after
	// the algorithm produces its raw recommendation; the aggregator values
	// are passed through unchanged here.
	klog.InfoS("Metrics aggregated",
		"averageValue", aggregatedMetrics.StableValue,
		"trend", aggregatedMetrics.Trend,
		"confidence", aggregatedMetrics.Confidence,
		"podCount", len(request.Pods))

	// Step 4: Make scaling decision
	scalingRequest := algorithm.ScalingRequest{
		Target: types.ScaleTarget{
			Namespace:  request.PodAutoscaler.Namespace,
			Name:       request.PodAutoscaler.Spec.ScaleTargetRef.Name,
			Kind:       request.PodAutoscaler.Spec.ScaleTargetRef.Kind,
			APIVersion: request.PodAutoscaler.Spec.ScaleTargetRef.APIVersion,
			MetricKey:  metricKey,
		},
		CurrentReplicas:   request.CurrentReplicas,
		AggregatedMetrics: aggregatedMetrics,
		ScalingContext:    scalingContext,
		Timestamp:         request.Timestamp,
	}

	klog.InfoS("Computing scaling recommendation", "source", workloadKey,
		"algorithm", algo.GetAlgorithmType())
	recommendation, err := algo.ComputeRecommendation(ctx, scalingRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to compute scaling recommendation for %s: %w", workloadKey, err)
	}
	// Record cross-sectional sample coverage from the latest scrape so the
	// decision layer can score sample coverage confidence.
	if recommendation.Metadata == nil {
		recommendation.Metadata = make(map[string]interface{})
	}
	recommendation.Metadata["reporting_pods"] = len(snapshot.Values)

	klog.InfoS("Scaling recommendation computed",
		"source", workloadKey,
		"algorithm", algo.GetAlgorithmType(),
		"recommendation", recommendation)

	return recommendation, nil
}

func metricWindowDurations(pa autoscalingv1alpha1.PodAutoscaler) (time.Duration, time.Duration) {
	stableWindow := metrics.DefaultStableWindowDuration
	if pa.Spec.ObserveWindowSeconds != nil {
		stableWindow = time.Duration(*pa.Spec.ObserveWindowSeconds) * time.Second
	}

	panicWindow := metrics.DefaultPanicWindowDuration
	if pa.Spec.PanicWindowSeconds != nil {
		panicWindow = time.Duration(*pa.Spec.PanicWindowSeconds) * time.Second
	}

	return stableWindow, panicWindow
}

// Helper methods
