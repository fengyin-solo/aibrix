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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
)

// warmPodIP builds a running warm pool pod with an explicit IP so per-pod fake
// runtimes can report distinct snapshots in multi-replica tests.
func warmPodIP(name, ip string) *corev1.Pod {
	pod := warmPod(name, "b300-pool-a", true, corev1.PodRunning)
	pod.Status.PodIP = ip
	return pod
}

func claimWithReplicas(n int32) *modelv1alpha1.ModelClaim {
	pm := withFinalizer(sampleModelClaim())
	pm.Spec.Replicas = &n
	return pm
}

func reconcileResult(t *testing.T, r *ModelClaimReconciler, name string) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name},
	})
	require.NoError(t, err)
	return result
}

func instancePodsSet(got *modelv1alpha1.ModelClaim) map[string]modelv1alpha1.ModelClaimInstance {
	out := map[string]modelv1alpha1.ModelClaimInstance{}
	for _, in := range got.Status.Instances {
		out[in.Pod] = in
	}
	return out
}

func assertClaimAnnotationState(t *testing.T, r *ModelClaimReconciler, podName, claimName, state string, port int32) {
	t.Helper()
	got := &corev1.Pod{}
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: podName}, got))
	val := got.Annotations[constants.ModelClaimPodAnnotationPrefix+claimName]
	assert.Contains(t, val, fmt.Sprintf(`"port":%d`, port))
	assert.Contains(t, val, fmt.Sprintf(`"state":"%s"`, state))
}

// AC-2: replicas=3 lands on three distinct warm pods in one pass, all become
// Active/Ready with per-pod active annotations.
func TestReconcileMultiReplicaPlacesOnDistinctPods(t *testing.T) {
	pm := claimWithReplicas(3)
	r, runtime := newReconciler(t,
		pm,
		warmPodIP("warm-1", "10.0.0.1"),
		warmPodIP("warm-2", "10.0.0.2"),
		warmPodIP("warm-3", "10.0.0.3"),
	)

	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 3)
	assert.Equal(t, int32(3), got.Status.DesiredReplicas)
	assert.Equal(t, int32(3), got.Status.ReadyReplicas)
	assert.Equal(t, modelv1alpha1.ModelClaimActive, got.Status.Phase)
	assert.ElementsMatch(t, []string{"warm-1", "warm-2", "warm-3"},
		[]string{got.Status.Instances[0].Pod, got.Status.Instances[1].Pod, got.Status.Instances[2].Pod})
	require.Len(t, runtime.activateCalls, 3)
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, runtime.activatePodIPs)
	for _, in := range got.Status.Instances {
		assert.True(t, in.Ready, "instance on %s must report ready", in.Pod)
		assert.NotZero(t, in.Port)
		assert.Equal(t, modelv1alpha1.ModelClaimActive, in.Phase)
		assertClaimAnnotationState(t, r, in.Pod, pm.Name, constants.ModelClaimRoutingStateActive, in.Port)
	}
}

// AC-3: with fewer warm pods than replicas the claim activates what it can and
// fills remaining slots as new warm pods appear, never re-activating.
func TestReconcileMultiReplicaWaitsThenFills(t *testing.T) {
	pm := claimWithReplicas(3)
	r, runtime := newReconciler(t, pm, warmPodIP("warm-1", "10.0.0.1"))

	reconcileOnce(t, r, pm.Name)
	require.Len(t, getModel(t, r, pm.Name).Status.Instances, 1)
	require.Len(t, runtime.activateCalls, 1)

	require.NoError(t, r.Create(context.Background(), warmPodIP("warm-2", "10.0.0.2")))
	require.NoError(t, r.Create(context.Background(), warmPodIP("warm-3", "10.0.0.3")))
	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 3)
	assert.Equal(t, int32(3), got.Status.ReadyReplicas)
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, runtime.activatePodIPs)
	assert.Len(t, runtime.activateCalls, 3, "each replica activates exactly once")
}

// AC-4 (controller path): same-artifact observed peak establishes an HBM
// floor; a pod below it is skipped even when the artifact is cached elsewhere.
func TestReconcilePlacementEnforcesObservedHBMFloor(t *testing.T) {
	pm := claimWithReplicas(1)
	r, runtime := newReconciler(t,
		pm,
		warmPodIP("tight", "10.0.0.1"),
		warmPodIP("roomy", "10.0.0.2"),
	)
	runtime.snapshots = map[string]*RuntimeSnapshot{
		"10.0.0.1": {
			Accelerators: []RuntimeAcceleratorSnapshot{{ID: "GPU-0", HBMFreeBytes: 500}},
			Models: []RuntimeSnapshotModel{{
				ModelName: "co-tenant-same-artifact", ArtifactURL: pm.Spec.ArtifactURL,
				HBMPeakBytes: 800,
			}},
		},
		"10.0.0.2": {
			Accelerators: []RuntimeAcceleratorSnapshot{{ID: "GPU-0", HBMFreeBytes: 1000}},
		},
	}

	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 1)
	assert.Equal(t, "roomy", got.Status.Instances[0].Pod)
}

// AC-8/FR-5: an engine that fails on its pod is replaced on a DIFFERENT warm
// pod first; the failed instance is only stopped after the replacement is
// Ready. No deactivate may happen while the replacement is booting.
func TestReconcileFailedInstanceReplaysToDifferentPod(t *testing.T) {
	pm := claimWithReplicas(1)
	pm.UID = "claim-uid"
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{{
		Pod: "warm-a", Port: 9001, Phase: modelv1alpha1.ModelClaimActive, Ready: true,
	}}
	a := warmPodIP("warm-a", "10.0.0.1")
	a.Annotations = map[string]string{
		constants.ModelClaimPodAnnotationPrefix + pm.Name: `{"model":"qwen2-7b","port":9001,"state":"active"}`,
	}
	b := warmPodIP("warm-b", "10.0.0.2")
	r, runtime := newReconciler(t, pm, a, b)
	runtime.putModel("10.0.0.1", ModelInfo{ModelName: "qwen2-7b", Port: 9001, Phase: "active", Ready: true})
	runtime.snapshots = map[string]*RuntimeSnapshot{
		"10.0.0.1": {Models: []RuntimeSnapshotModel{{
			ModelName: "qwen2-7b", Port: 9001,
			ClaimRef: &ModelClaimRef{Namespace: pm.Namespace, Name: pm.Name, UID: string(pm.UID)},
			Phase:    "failed", Alive: false, Ready: false,
			LastError: "restart budget exhausted",
		}}},
	}

	// Pass 1: health observes terminal failure; converge uses pre-pass state
	// so the failed record exists from this pass on.
	reconcileOnce(t, r, pm.Name)
	afterFailure := getModel(t, r, pm.Name)
	require.Len(t, afterFailure.Status.Instances, 1)
	assert.Equal(t, modelv1alpha1.ModelClaimFailed, afterFailure.Status.Instances[0].Phase)
	assert.Empty(t, runtime.deactivateCalls, "failure observation alone must not stop anything")

	// Pass 2: replacement activates on warm-b while the failed record stays.
	reconcileOnce(t, r, pm.Name)
	replacing := getModel(t, r, pm.Name)
	require.Len(t, replacing.Status.Instances, 2)
	byPod := instancePodsSet(replacing)
	require.Contains(t, byPod, "warm-a")
	require.Contains(t, byPod, "warm-b")
	assert.Equal(t, modelv1alpha1.ModelClaimFailed, byPod["warm-a"].Phase)
	assert.Equal(t, []string{"10.0.0.2"}, runtime.activatePodIPs)
	assert.Empty(t, runtime.deactivateCalls, "failed instance is not stopped while replacement is in flight")

	// Pass 3: replacement is Ready; now the old failed engine gets stopped and
	// its record/annotation removed.
	reconcileOnce(t, r, pm.Name)
	done := getModel(t, r, pm.Name)
	require.Len(t, done.Status.Instances, 1)
	assert.Equal(t, "warm-b", done.Status.Instances[0].Pod)
	assert.Equal(t, modelv1alpha1.ModelClaimActive, done.Status.Instances[0].Phase)
	require.Len(t, runtime.deactivateCalls, 1)
	assert.Equal(t, "10.0.0.1", runtime.deactivatePodIPs[0])
	oldPod := &corev1.Pod{}
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: "warm-a"}, oldPod))
	_, stillAnnotated := oldPod.Annotations[constants.ModelClaimPodAnnotationPrefix+pm.Name]
	assert.False(t, stillAnnotated)
}

// FR-6: with no distinct pod available, a failed instance is released and the
// activation retried on the same warm pod (one engine at a time, no duplicate).
func TestReconcileFailedInstanceRetriesSamePod(t *testing.T) {
	pm := claimWithReplicas(1)
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{{
		Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimFailed,
	}}
	r, runtime := newReconciler(t, pm, warmPodIP("warm-1", "10.0.0.1"))
	runtime.snapshots = map[string]*RuntimeSnapshot{
		"10.0.0.1": {Models: []RuntimeSnapshotModel{{
			ModelName: "qwen2-7b", Port: 9001, Phase: "failed", Alive: false,
		}}},
	}

	// Pass 1: no distinct candidate -> failed record released after stop.
	reconcileOnce(t, r, pm.Name)
	require.Len(t, runtime.deactivateCalls, 1)
	assert.Empty(t, getModel(t, r, pm.Name).Status.Instances)

	// Fresh engine state on the only pod: re-activation lands on warm-1 again.
	runtime.snapshots = nil
	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 1)
	assert.Equal(t, "warm-1", got.Status.Instances[0].Pod)
	assert.Equal(t, modelv1alpha1.ModelClaimActive, got.Status.Instances[0].Phase)
	require.Len(t, runtime.activateCalls, 1)
	assert.Equal(t, "10.0.0.1", runtime.activatePodIPs[0])
}

// AC-7: deleting a pod holding one replica of a multi-replica claim prunes the
// record and a new pod carries the replacement; the surviving replica is
// untouched.
func TestReconcileMultiReplicaReplaysAfterPodDeletion(t *testing.T) {
	pm := claimWithReplicas(2)
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{
		{Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive, Ready: true},
		{Pod: "warm-2", Port: 9002, Phase: modelv1alpha1.ModelClaimActive, Ready: true},
	}
	r, runtime := newReconciler(t,
		pm,
		warmPodIP("warm-1", "10.0.0.1"),
		warmPodIP("warm-2", "10.0.0.2"),
	)
	runtime.putModel("10.0.0.1", ModelInfo{ModelName: "qwen2-7b", Port: 9001, Phase: "active", Ready: true})
	runtime.putModel("10.0.0.2", ModelInfo{ModelName: "qwen2-7b", Port: 9002, Phase: "active", Ready: true})

	require.NoError(t, r.Delete(context.Background(),
		warmPodIP("warm-2", "10.0.0.2")))
	reconcileOnce(t, r, pm.Name)
	orphaned := getModel(t, r, pm.Name)
	require.Len(t, orphaned.Status.Instances, 1)
	assert.Equal(t, "warm-1", orphaned.Status.Instances[0].Pod)

	require.NoError(t, r.Create(context.Background(), warmPodIP("warm-3", "10.0.0.3")))
	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 2)
	assert.Equal(t, int32(2), got.Status.ReadyReplicas)
	byPod := instancePodsSet(got)
	assert.Contains(t, byPod, "warm-1")
	assert.Contains(t, byPod, "warm-3")
	assert.Equal(t, []string{"10.0.0.3"}, runtime.activatePodIPs,
		"only the missing replica is activated; the surviving pod is untouched")
	assert.Empty(t, runtime.deactivateCalls)
}

// AC-6: scaling 3->1 stops exactly two healthy engines (latest-named first)
// and leaves one Active instance serving.
func TestReconcileScaleDownThreeToOne(t *testing.T) {
	pm := claimWithReplicas(1)
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{
		{Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive, Ready: true},
		{Pod: "warm-2", Port: 9002, Phase: modelv1alpha1.ModelClaimActive, Ready: true},
		{Pod: "warm-3", Port: 9003, Phase: modelv1alpha1.ModelClaimActive, Ready: true},
	}
	r, runtime := newReconciler(t,
		pm,
		warmPodIP("warm-1", "10.0.0.1"),
		warmPodIP("warm-2", "10.0.0.2"),
		warmPodIP("warm-3", "10.0.0.3"),
	)
	for ip, port := range map[string]int32{"10.0.0.1": 9001, "10.0.0.2": 9002, "10.0.0.3": 9003} {
		runtime.putModel(ip, ModelInfo{ModelName: "qwen2-7b", Port: port, Phase: "active", Ready: true})
	}

	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 1)
	assert.Equal(t, "warm-1", got.Status.Instances[0].Pod)
	assert.Equal(t, modelv1alpha1.ModelClaimActive, got.Status.Instances[0].Phase)
	assert.Len(t, runtime.deactivateCalls, 2)
	assert.ElementsMatch(t, []string{"10.0.0.2", "10.0.0.3"}, runtime.deactivatePodIPs)
	assert.Empty(t, runtime.activateCalls)
}

// AC-9: instance ready/restartCount mirror runtime snapshot and stay aligned
// with the routing annotation.
func TestReconcileInstanceStatusReflectsReadyAndRestarts(t *testing.T) {
	pm := withFinalizer(sampleModelClaim())
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{{
		Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive,
	}}
	r, runtime := newReconciler(t, pm, warmPodIP("warm-1", "10.0.0.1"))
	runtime.snapshots = map[string]*RuntimeSnapshot{
		"10.0.0.1": {Models: []RuntimeSnapshotModel{{
			ModelName: "qwen2-7b", Port: 9200, Phase: "active", Alive: true, Ready: true, RestartCount: 2,
		}}},
	}

	reconcileOnce(t, r, pm.Name)
	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 1)
	in := got.Status.Instances[0]
	assert.True(t, in.Ready)
	assert.Equal(t, int32(2), in.RestartCount)
	assert.Equal(t, int32(9200), in.Port)
	assertClaimAnnotationState(t, r, "warm-1", pm.Name, constants.ModelClaimRoutingStateActive, 9200)

	// Runtime demotes the engine after another crash: ready clears, route is
	// withdrawn, restart counter advances.
	runtime.snapshots["10.0.0.1"] = &RuntimeSnapshot{Models: []RuntimeSnapshotModel{{
		ModelName: "qwen2-7b", Port: 9200, Phase: "active", Alive: true, Ready: false, RestartCount: 3,
	}}}
	reconcileOnce(t, r, pm.Name)
	got = getModel(t, r, pm.Name)
	in = got.Status.Instances[0]
	assert.False(t, in.Ready)
	assert.Equal(t, int32(3), in.RestartCount)
	assert.Equal(t, modelv1alpha1.ModelClaimActivating, in.Phase)
	assert.Equal(t, int32(0), got.Status.ReadyReplicas)
	assertClaimAnnotationState(t, r, "warm-1", pm.Name, constants.ModelClaimRoutingStateActivating, 0)
}

// FR-4: a successful snapshot that no longer lists a previously Active engine
// marks it terminal (EngineGone) so replay takes over.
func TestReconcileEngineVanishedMarksFailed(t *testing.T) {
	pm := withFinalizer(sampleModelClaim())
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{{
		Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive, Ready: true,
	}}
	r, _ := newReconciler(t, pm, warmPodIP("warm-1", "10.0.0.1"))
	// No models and no per-pod snapshot: Snapshot succeeds but reports nothing.

	reconcileOnce(t, r, pm.Name)

	got := getModel(t, r, pm.Name)
	require.Len(t, got.Status.Instances, 1)
	assert.Equal(t, modelv1alpha1.ModelClaimFailed, got.Status.Instances[0].Phase)
	assert.False(t, got.Status.Instances[0].Ready)
	assert.Equal(t, modelv1alpha1.ModelClaimFailed, got.Status.Phase)
	assertClaimAnnotationState(t, r, "warm-1", pm.Name, constants.ModelClaimRoutingStateFailed, 0)
}

// AC-11: the finalizer stays until every replica is stopped; a deactivate
// failure (or an engine still visible in snapshot) keeps it, then release
// proceeds once all instances are confirmed gone.
func TestReconcileDeletionWaitsForAllInstancesStopped(t *testing.T) {
	now := metav1.Now()
	pm := withFinalizer(sampleModelClaim())
	pm.DeletionTimestamp = &now
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{
		{Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive},
		{Pod: "warm-2", Port: 9002, Phase: modelv1alpha1.ModelClaimActive},
	}
	r, runtime := newReconciler(t,
		pm,
		withClaimRouteAnno(warmPodIP("warm-1", "10.0.0.1"), pm.Name, 9001),
		withClaimRouteAnno(warmPodIP("warm-2", "10.0.0.2"), pm.Name, 9002),
	)

	// First pass: teardown RPC fails on both pods -> finalizer retained.
	runtime.failDeactivate = true
	result := reconcileResult(t, r, pm.Name)
	assert.Greater(t, result.RequeueAfter.Milliseconds(), int64(0))
	require.True(t, controllerutil.ContainsFinalizer(getModel(t, r, pm.Name), ModelClaimFinalizer))
	require.Len(t, getModel(t, r, pm.Name).Status.Instances, 2)

	// Second pass: both engines stop and disappear from snapshots -> release.
	runtime.failDeactivate = false
	reconcileOnce(t, r, pm.Name)
	final := &modelv1alpha1.ModelClaim{}
	err := r.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: pm.Name}, final)
	assert.True(t, err != nil || !controllerutil.ContainsFinalizer(final, ModelClaimFinalizer))
	assert.Len(t, runtime.deactivateCalls, 4) // 2 failed attempts + 2 successful
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.2"}, runtime.deactivatePodIPs)
	for _, podName := range []string{"warm-1", "warm-2"} {
		pod := &corev1.Pod{}
		require.NoError(t, r.Get(context.Background(),
			types.NamespacedName{Namespace: testNamespace, Name: podName}, pod))
		_, ok := pod.Annotations[constants.ModelClaimPodAnnotationPrefix+pm.Name]
		assert.False(t, ok, "routing annotation must be removed on %s", podName)
	}
}

// AC-11 variant: a successful deactivate whose engine is still observed by the
// next snapshot keeps the finalizer until the runtime confirms removal.
func TestReconcileDeletionWaitsForSnapshotConfirmation(t *testing.T) {
	now := metav1.Now()
	pm := withFinalizer(sampleModelClaim())
	pm.DeletionTimestamp = &now
	pm.Status.Instances = []modelv1alpha1.ModelClaimInstance{
		{Pod: "warm-1", Port: 9001, Phase: modelv1alpha1.ModelClaimActive},
	}
	r, runtime := newReconciler(t, pm, withClaimRouteAnno(warmPodIP("warm-1", "10.0.0.1"), pm.Name, 9001))
	// Deactivate removes f.models, but the snapshot still reports the engine.
	runtime.snapshots = map[string]*RuntimeSnapshot{
		"10.0.0.1": {Models: []RuntimeSnapshotModel{{
			ModelName: "qwen2-7b", Port: 9001, Phase: "active", Alive: true, Ready: true,
		}}},
	}

	result := reconcileResult(t, r, pm.Name)
	assert.True(t, result.RequeueAfter > 0, "claim should requeue while instances remain")
	stillThere := getModel(t, r, pm.Name)
	require.True(t, controllerutil.ContainsFinalizer(stillThere, ModelClaimFinalizer))
	require.Len(t, stillThere.Status.Instances, 1, "engine still visible -> instance retained")

	runtime.snapshots = nil
	reconcileOnce(t, r, pm.Name)
	final := &modelv1alpha1.ModelClaim{}
	err := r.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: pm.Name}, final)
	assert.True(t, err != nil || !controllerutil.ContainsFinalizer(final, ModelClaimFinalizer))
}

func withClaimRouteAnno(pod *corev1.Pod, claimName string, port int32) *corev1.Pod {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[constants.ModelClaimPodAnnotationPrefix+claimName] =
		fmt.Sprintf(`{"model":"qwen2-7b","port":%d,"state":"active"}`, port)
	return pod
}
