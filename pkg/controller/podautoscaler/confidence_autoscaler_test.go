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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	autoscalingv1alpha1 "github.com/vllm-project/aibrix/api/autoscaling/v1alpha1"
	scalingctx "github.com/vllm-project/aibrix/pkg/controller/podautoscaler/context"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/metrics"
	"github.com/vllm-project/aibrix/pkg/controller/podautoscaler/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type metricValueFetcherFactory struct {
	values map[string]float64
	fail   map[string]bool
}

func (f *metricValueFetcherFactory) For(source autoscalingv1alpha1.MetricSource) metrics.MetricFetcher {
	return &metricValueFetcher{f: f}
}

type metricValueFetcher struct {
	f *metricValueFetcherFactory
}

func (f *metricValueFetcher) FetchPodMetrics(_ context.Context, _ corev1.Pod, source autoscalingv1alpha1.MetricSource) (float64, error) {
	if f.f.fail[source.TargetMetric] {
		return 0, errors.New("scrape failed")
	}
	return f.f.values[source.TargetMetric], nil
}

func confidencePA(t *testing.T, name string, sources []autoscalingv1alpha1.MetricSource,
	annotations map[string]string, observeWindow, panicWindow int64) autoscalingv1alpha1.PodAutoscaler {
	t.Helper()
	pa := autoscalingv1alpha1.PodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        name,
			Annotations: annotations,
		},
		Spec: autoscalingv1alpha1.PodAutoscalerSpec{
			ScaleTargetRef:       corev1.ObjectReference{Kind: "Deployment", Name: "test-llm"},
			MaxReplicas:          20,
			ScalingStrategy:      autoscalingv1alpha1.APA,
			ObserveWindowSeconds: ptr.To(observeWindow),
			PanicWindowSeconds:   ptr.To(panicWindow),
			MetricsSources:       sources,
		},
	}
	return pa
}

func podMetric(name, target string) autoscalingv1alpha1.MetricSource {
	return autoscalingv1alpha1.MetricSource{
		MetricSourceType: autoscalingv1alpha1.POD,
		TargetMetric:     name,
		TargetValue:      target,
	}
}

func confidenceRequest(pa autoscalingv1alpha1.PodAutoscaler, ctx scalingctx.ScalingContext, replicas int32, now time.Time) ReplicaComputeRequest {
	pods := make([]corev1.Pod, replicas)
	for i := range pods {
		pods[i] = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "metric-pod"}}
	}
	return ReplicaComputeRequest{
		PodAutoscaler:   pa,
		ScalingContext:  ctx,
		CurrentReplicas: replicas,
		ReplicaState:    ReplicaState{ReadyReplicas: replicas, PendingReplicas: 0},
		Pods:            pods,
		Timestamp:       now,
	}
}

func enableAnnotations(threshold string) map[string]string {
	ann := map[string]string{types.ConfidenceAwareScalingLabel: "true"}
	if threshold != "" {
		ann[types.ConfidenceThresholdLabel] = threshold
	}
	return ann
}

// runCycles executes scrape cycles 10s apart, updating the mock values before
// each cycle. values[i] holds metric -> value for cycle i.
func runCycles(t *testing.T, autoScaler *DefaultAutoScaler, pa autoscalingv1alpha1.PodAutoscaler,
	ctx scalingctx.ScalingContext, replicas int32, start time.Time, values []map[string]float64,
	factory *metricValueFetcherFactory) *ReplicaComputeResult {
	t.Helper()
	var result *ReplicaComputeResult
	for i, cycleValues := range values {
		factory.values = cycleValues
		request := confidenceRequest(pa, ctx, replicas, start.Add(time.Duration(i)*10*time.Second))
		var err error
		result, err = autoScaler.ComputeDesiredReplicas(context.TODO(), request)
		require.NoError(t, err)
		require.NotNil(t, result)
	}
	return result
}

func TestConfidenceAwareDisabledKeepsHistoricalBehavior(t *testing.T) {
	// No annotation: even a cold single-sample recommendation scales
	// immediately using the maximum across sources.
	pa := confidencePA(t, "disabled-pa",
		[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
		nil, 60, 10)
	factory := &metricValueFetcherFactory{values: map[string]float64{"gpu_cache_usage_perc": 70}}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	result, err := autoScaler.ComputeDesiredReplicas(context.TODO(),
		confidenceRequest(pa, ctx, 1, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.NoError(t, err)
	assert.Equal(t, int32(2), result.DesiredReplicas)
	assert.Nil(t, result.Decision)
}

func TestConfidenceAwareColdStartSuppressed(t *testing.T) {
	pa := confidencePA(t, "cold-start",
		[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
		enableAnnotations(""), 180, 60)
	factory := &metricValueFetcherFactory{values: map[string]float64{"gpu_cache_usage_perc": 90}}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := autoScaler.ComputeDesiredReplicas(context.TODO(), confidenceRequest(pa, ctx, 1, start))
	require.NoError(t, err)

	require.NotNil(t, result.Decision)
	assert.True(t, result.Decision.Suppressed)
	assert.Equal(t, int32(1), result.DesiredReplicas, "cold start must not scale on a single instantaneous sample")
	assert.Less(t, result.Decision.Metrics[0].Confidence, 0.6)
}

func TestConfidenceAwareHighVarianceSinglePodSuppressed(t *testing.T) {
	pa := confidencePA(t, "jitter-pod",
		[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
		enableAnnotations(""), 60, 10)
	factory := &metricValueFetcherFactory{}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Latest sample is 90 (would scale to 2), but the short history contains
	// a 10 -> 90 jump: high variance and poor window fill suppress scaling.
	result := runCycles(t, autoScaler, pa, ctx, 1, start, []map[string]float64{
		{"gpu_cache_usage_perc": 10},
		{"gpu_cache_usage_perc": 90},
		{"gpu_cache_usage_perc": 90},
	}, factory)

	require.NotNil(t, result.Decision)
	assert.True(t, result.Decision.Suppressed)
	assert.Equal(t, int32(1), result.DesiredReplicas)
	assert.Contains(t, result.Decision.Summary, "scaling suppressed")
}

func TestConfidenceAwareWarmSteadyMetricsScale(t *testing.T) {
	pa := confidencePA(t, "warm-steady",
		[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
		enableAnnotations(""), 30, 10)
	factory := &metricValueFetcherFactory{}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cycles := make([]map[string]float64, 4)
	for i := range cycles {
		cycles[i] = map[string]float64{"gpu_cache_usage_perc": 70}
	}
	result := runCycles(t, autoScaler, pa, ctx, 5, start, cycles, factory)

	require.NotNil(t, result.Decision)
	assert.False(t, result.Decision.Suppressed)
	assert.Equal(t, int32(7), result.DesiredReplicas)
	assert.GreaterOrEqual(t, result.Decision.Metrics[0].Confidence, 0.9)
}

func TestConfidenceAwareMultiMetricWeightedVote(t *testing.T) {
	sources := []autoscalingv1alpha1.MetricSource{
		podMetric("metric-a", "50"),
		podMetric("metric-b", "50"),
	}
	pa := confidencePA(t, "weighted-vote", sources, enableAnnotations(""), 30, 10)
	factory := &metricValueFetcherFactory{}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cycles := make([]map[string]float64, 4)
	for i := range cycles {
		// metric-a wants 7, metric-b stays within tolerance (hold at 5)
		cycles[i] = map[string]float64{"metric-a": 70, "metric-b": 54}
	}
	result := runCycles(t, autoScaler, pa, ctx, 5, start, cycles, factory)

	require.NotNil(t, result.Decision)
	assert.False(t, result.Decision.Suppressed)
	// Weighted vote is (7 + 5) / 2 = 6; historical max behavior would pick 7.
	assert.Equal(t, int32(6), result.DesiredReplicas)
	assert.Equal(t, 2, result.Decision.QualifiedCount)
	require.Len(t, result.Decision.Metrics, 2)
	for _, md := range result.Decision.Metrics {
		assert.True(t, md.Qualified)
		assert.InDelta(t, 0.5, md.Weight, 1e-9)
	}
}

func TestConfidenceAwareMissingMetricRecorded(t *testing.T) {
	sources := []autoscalingv1alpha1.MetricSource{
		podMetric("metric-a", "50"),
		podMetric("metric-b", "50"),
	}
	pa := confidencePA(t, "missing-metric", sources, enableAnnotations(""), 30, 10)
	factory := &metricValueFetcherFactory{fail: map[string]bool{"metric-b": true}}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cycles := make([]map[string]float64, 4)
	for i := range cycles {
		cycles[i] = map[string]float64{"metric-a": 70}
	}
	result := runCycles(t, autoScaler, pa, ctx, 5, start, cycles, factory)

	require.NotNil(t, result.Decision)
	assert.Equal(t, []string{"metric-b"}, result.Decision.MissingMetrics)
	assert.Equal(t, int32(7), result.DesiredReplicas)
	assert.Contains(t, result.Reason, "missing metrics: metric-b")
}

func TestConfidenceAwareTrendOpposingRecommendationHeld(t *testing.T) {
	// Stable mean is above target (wants scale up) but the recent trend is
	// sharply down: a qualified source is held because trend contradicts the
	// recommended direction.
	pa := confidencePA(t, "trend-opposed",
		[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
		enableAnnotations(""), 30, 10)
	factory := &metricValueFetcherFactory{}
	autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
	ctx := testScalingContext(t, &pa)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result := runCycles(t, autoScaler, pa, ctx, 5, start, []map[string]float64{
		{"gpu_cache_usage_perc": 90},
		{"gpu_cache_usage_perc": 90},
		{"gpu_cache_usage_perc": 30},
		{"gpu_cache_usage_perc": 30},
	}, factory)

	require.NotNil(t, result.Decision)
	assert.False(t, result.Decision.Suppressed)
	require.Len(t, result.Decision.Metrics, 1)
	md := result.Decision.Metrics[0]
	assert.True(t, md.Qualified)
	assert.Equal(t, int32(5), result.DesiredReplicas)
	assert.Contains(t, md.SuppressionReason, "opposes")
}

func TestConfidenceThresholdAnnotationControlsQualification(t *testing.T) {
	// Same 10 -> 90 -> 90 single-pod series has confidence ~0.5: below the
	// default 0.6 threshold but above an explicitly lower 0.5 threshold.
	build := func(threshold string) *ReplicaComputeResult {
		pa := confidencePA(t, "threshold-boundary",
			[]autoscalingv1alpha1.MetricSource{podMetric("gpu_cache_usage_perc", "50")},
			enableAnnotations(threshold), 60, 10)
		factory := &metricValueFetcherFactory{}
		autoScaler := NewDefaultAutoScaler(factory, fake.NewClientBuilder().Build())
		ctx := testScalingContext(t, &pa)

		return runCycles(t, autoScaler, pa, ctx, 1,
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), []map[string]float64{
				{"gpu_cache_usage_perc": 10},
				{"gpu_cache_usage_perc": 90},
				{"gpu_cache_usage_perc": 90},
			}, factory)
	}

	suppressed := build("0.6")
	require.NotNil(t, suppressed.Decision)
	assert.True(t, suppressed.Decision.Suppressed)
	assert.Equal(t, int32(1), suppressed.DesiredReplicas)

	qualified := build("0.5")
	require.NotNil(t, qualified.Decision)
	assert.False(t, qualified.Decision.Suppressed)
	assert.Greater(t, qualified.DesiredReplicas, int32(1))
}
