/*
Copyright 2026 The Aibrix Team.

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

package modelclaim

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
)

func namedPod(name string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestSelectPodForActivation_LeastLoaded(t *testing.T) {
	cands := []corev1.Pod{namedPod("a"), namedPod("b"), namedPod("c")}
	load := map[string]int{"a": 2, "b": 0, "c": 1}
	got, err := selectPodForActivation(cands, map[string]bool{}, load, "m", uniformLocality{})
	require.NoError(t, err)
	assert.Equal(t, "b", got.Name)
}

func TestSelectPodForActivation_SkipsAlreadyOn(t *testing.T) {
	cands := []corev1.Pod{namedPod("a"), namedPod("b")}
	load := map[string]int{"a": 0, "b": 5}
	got, err := selectPodForActivation(cands, map[string]bool{"a": true}, load, "m", uniformLocality{})
	require.NoError(t, err)
	assert.Equal(t, "b", got.Name, "a is excluded even though least loaded")
}

func TestSelectPodForActivation_TieBreakByName(t *testing.T) {
	cands := []corev1.Pod{namedPod("z"), namedPod("a")}
	load := map[string]int{"z": 0, "a": 0}
	got, err := selectPodForActivation(cands, map[string]bool{}, load, "m", uniformLocality{})
	require.NoError(t, err)
	assert.Equal(t, "a", got.Name)
}

func TestSelectPodForActivation_NoCapacity(t *testing.T) {
	cands := []corev1.Pod{namedPod("a")}
	_, err := selectPodForActivation(cands, map[string]bool{"a": true}, map[string]int{}, "m", uniformLocality{})
	assert.Error(t, err)
}

func TestServedModelName(t *testing.T) {
	pm := &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	assert.Equal(t, "foo", servedModelName(pm))
	name := "bar"
	pm.Spec.ModelName = &name
	assert.Equal(t, "bar", servedModelName(pm))
}

func TestIpcNameFor(t *testing.T) {
	pm := &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	assert.Equal(t, "kvc_foo", ipcNameFor(pm))

	// Sanitized to match kvcached's normalization (verified on real hardware):
	// '.' and '/' become '-', existing '-' is kept.
	dotted := &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Name: "qwen3-0.6b"}}
	assert.Equal(t, "kvc_qwen3-0-6b", ipcNameFor(dotted))
	slashed := &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Name: "Qwen/Qwen2-7B"}}
	assert.Equal(t, "kvc_Qwen-Qwen2-7B", ipcNameFor(slashed))
}

func TestDesiredReplicas(t *testing.T) {
	pm := &modelv1alpha1.ModelClaim{}
	assert.Equal(t, int32(1), desiredReplicas(pm))
	one := int32(1)
	pm.Spec.Replicas = &one
	assert.Equal(t, int32(1), desiredReplicas(pm))
}

// fakeLocality maps nodeName -> load cost for tests (0 = weights already hot).
type fakeLocality map[string]float64

func (f fakeLocality) Cost(model, nodeName string) float64 { return f[nodeName] }

func podOnNode(name, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
	}
}

func TestSelectPodForActivation_LocalityDominatesLoad(t *testing.T) {
	// "hot" sits on a node whose store already has the weights (cost 0) but is
	// busier; "cold" is idle but on a node that must stage weights (cost 5).
	cands := []corev1.Pod{podOnNode("cold", "n-cold"), podOnNode("hot", "n-hot")}
	load := map[string]int{"cold": 0, "hot": 3}
	loc := fakeLocality{"n-hot": 0, "n-cold": 5}
	got, err := selectPodForActivation(cands, map[string]bool{}, load, "m", loc)
	require.NoError(t, err)
	assert.Equal(t, "hot", got.Name, "lower locality cost wins over lower load")
}

func TestSelectPodForActivation_LoadBreaksEqualLocality(t *testing.T) {
	// Two nodes equally hot (cost 0): fall back to least-loaded.
	cands := []corev1.Pod{podOnNode("a", "n1"), podOnNode("b", "n2")}
	load := map[string]int{"a": 2, "b": 1}
	loc := fakeLocality{"n1": 0, "n2": 0}
	got, err := selectPodForActivation(cands, map[string]bool{}, load, "m", loc)
	require.NoError(t, err)
	assert.Equal(t, "b", got.Name)
}

func TestSelectPodForActivation_NilLocalityIsUniform(t *testing.T) {
	// A nil provider must not panic and must behave like load-only selection.
	cands := []corev1.Pod{podOnNode("a", "n1"), podOnNode("b", "n2")}
	load := map[string]int{"a": 5, "b": 0}
	got, err := selectPodForActivation(cands, map[string]bool{}, load, "m", nil)
	require.NoError(t, err)
	assert.Equal(t, "b", got.Name)
}

func TestSelectPodForActivationWithStatePrefersLiveRuntimeState(t *testing.T) {
	candidates := []corev1.Pod{namedPod("cold"), namedPod("hot")}
	states := map[string]PodPlacementState{
		"cold": {
			SnapshotKnown: true,
			MemoryKnown:   true,
			HBMFreeBytes:  900,
			KVUsedBytes:   10,
			ModelCount:    1,
		},
		"hot": {
			SnapshotKnown:  true,
			ArtifactCached: true,
			MemoryKnown:    true,
			HBMFreeBytes:   100,
			KVUsedBytes:    100,
			ModelCount:     3,
		},
	}

	got, err := selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 0,
	)
	require.NoError(t, err)
	assert.Equal(t, "hot", got.Name, "cached artifact wins before live memory tie-breakers")
}

func TestSelectPodForActivationWithStateRanksMemoryAndKV(t *testing.T) {
	candidates := []corev1.Pod{namedPod("busy"), namedPod("free")}
	states := map[string]PodPlacementState{
		"busy": {
			SnapshotKnown: true,
			MemoryKnown:   true,
			HBMFreeBytes:  500,
			KVUsedBytes:   10,
			ModelCount:    1,
		},
		"free": {
			SnapshotKnown: true,
			MemoryKnown:   true,
			HBMFreeBytes:  600,
			KVUsedBytes:   100,
			ModelCount:    3,
		},
	}

	got, err := selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 0,
	)
	require.NoError(t, err)
	assert.Equal(t, "free", got.Name, "higher free HBM wins before KV/model-count tie-breakers")
}

func TestSelectPodForActivationWithStateRanksKVThenModelCount(t *testing.T) {
	// Equal artifact/snapshot/HBM: lower KV usage wins before model count.
	candidates := []corev1.Pod{namedPod("kv-busy"), namedPod("kv-free")}
	states := map[string]PodPlacementState{
		"kv-busy": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 500, KVUsedBytes: 300, ModelCount: 1},
		"kv-free": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 500, KVUsedBytes: 100, ModelCount: 3},
	}

	got, err := selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 0,
	)
	require.NoError(t, err)
	assert.Equal(t, "kv-free", got.Name, "lower KV usage wins even with more models")

	// Equal KV: fewer resident models wins.
	equalKV := states["kv-free"]
	equalKV.KVUsedBytes = 300
	states["kv-free"] = equalKV
	got, err = selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 0,
	)
	require.NoError(t, err)
	assert.Equal(t, "kv-busy", got.Name, "fewer resident models wins on equal KV")
}

func TestSelectPodForActivationWithStateFallsBackForUnknownSnapshots(t *testing.T) {
	candidates := []corev1.Pod{namedPod("busy"), namedPod("idle")}
	got, err := selectPodForActivationWithState(
		candidates,
		map[string]bool{},
		map[string]int{"busy": 2, "idle": 0},
		"m",
		uniformLocality{},
		map[string]PodPlacementState{},
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, "idle", got.Name)
}

func TestSelectPodForActivationWithStateFiltersPodsBelowHBMRequirement(t *testing.T) {
	candidates := []corev1.Pod{namedPod("tight"), namedPod("roomy")}
	states := map[string]PodPlacementState{
		"tight": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 500},
		// Artifact cached here, but the HBM floor must still be enforced.
		"roomy": {SnapshotKnown: true, ArtifactCached: true, MemoryKnown: true, HBMFreeBytes: 1000},
	}

	got, err := selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 800,
	)
	require.NoError(t, err)
	assert.Equal(t, "roomy", got.Name, "a cached-artifact pod still must satisfy the HBM floor")

	// Both pods short on HBM: a distinct error explains the scheduling stall.
	_, err = selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 5000,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HBM")
}

func TestSelectPodForActivationWithStateUnknownMemoryBypassesHBMRequirement(t *testing.T) {
	// A requirement estimate exists, but a pod without readable free HBM must
	// stay eligible (missing observability must not wedge scheduling).
	candidates := []corev1.Pod{namedPod("dark")}
	states := map[string]PodPlacementState{"dark": {SnapshotKnown: false, MemoryKnown: false}}

	got, err := selectPodForActivationWithState(
		candidates, map[string]bool{}, map[string]int{}, "m", uniformLocality{}, states, 800,
	)
	require.NoError(t, err)
	assert.Equal(t, "dark", got.Name)
}

func TestSelectPodsForMultipleReplicasUseDistinctPods(t *testing.T) {
	// Simulate the controller's sequential activation loop: each pick updates
	// the used set and model-count view, so replicas never share a pod.
	candidates := []corev1.Pod{namedPod("a"), namedPod("b"), namedPod("c")}
	load := map[string]int{}
	states := map[string]PodPlacementState{
		"a": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 100, KVUsedBytes: 10},
		"b": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 100, KVUsedBytes: 10},
		"c": {SnapshotKnown: true, MemoryKnown: true, HBMFreeBytes: 100, KVUsedBytes: 10},
	}
	used := map[string]bool{}
	var picked []string
	for range 3 {
		pod, err := selectPodForActivationWithState(
			candidates, used, load, "m", uniformLocality{}, states, 0,
		)
		require.NoError(t, err)
		picked = append(picked, pod.Name)
		used[pod.Name] = true
		load[pod.Name]++
		state := states[pod.Name]
		state.ModelCount++
		states[pod.Name] = state
	}
	assert.ElementsMatch(t, []string{"a", "b", "c"}, picked)

	// Fourth pick with every pod already used fails.
	_, err := selectPodForActivationWithState(
		candidates, used, load, "m", uniformLocality{}, states, 0,
	)
	require.Error(t, err)
}

func TestHbmRequirementFromSnapshots(t *testing.T) {
	snapshots := map[string]*RuntimeSnapshot{
		"p1": {Models: []RuntimeSnapshotModel{
			{ArtifactURL: "hf://m", HBMPeakBytes: 800, KVCapacityBytes: 600},
			{ArtifactURL: "hf://other", HBMPeakBytes: 9999},
		}},
		"p2": {Models: []RuntimeSnapshotModel{
			{ArtifactURL: "hf://m", HBMPeakBytes: 700, KVCapacityBytes: 900},
		}},
	}
	assert.Equal(t, int64(900), hbmRequirementFromSnapshots(snapshots, "hf://m"))
	assert.Zero(t, hbmRequirementFromSnapshots(snapshots, "hf://unseen"))
	assert.Zero(t, hbmRequirementFromSnapshots(nil, ""))
}

func TestUniformLocality_AlwaysZero(t *testing.T) {
	assert.Zero(t, uniformLocality{}.Cost("m", "any-node"))
}

func TestPruneDeadInstances(t *testing.T) {
	pm := &modelv1alpha1.ModelClaim{}
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{
		{Pod: "alive", Port: 20000},
		{Pod: "gone", Port: 20001},
	}
	pruneDeadInstances(pm, []corev1.Pod{namedPod("alive")})
	require.Len(t, pm.Status.Instances, 1)
	assert.Equal(t, "alive", pm.Status.Instances[0].Pod,
		"instance on a vanished warm pod must be dropped so re-activation can run")

	// No candidates at all: every instance is stale.
	pruneDeadInstances(pm, nil)
	assert.Empty(t, pm.Status.Instances)
}
