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

package roleset

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	aibrixconst "github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/controller/constants"
	controllerdrain "github.com/vllm-project/aibrix/pkg/controller/drain"
	ctrlutil "github.com/vllm-project/aibrix/pkg/controller/util"
)

const (
	rbNamespace = "rb-ns"
	imageV1     = "nginx:v1"
	imageV2     = "nginx:v2"
	imageV3     = "nginx:v3"
)

type rollbackFixture struct {
	t        *testing.T
	ctx      context.Context
	cli      client.Client
	rs       *orchestrationv1alpha1.RoleSet
	recorder *record.FakeRecorder
	now      time.Time
}

func newRollbackScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(scheme))
	require.NoError(t, orchestrationv1alpha1.AddToScheme(scheme))
	return scheme
}

func newRollbackFixture(t *testing.T, rs *orchestrationv1alpha1.RoleSet, objs ...client.Object) *rollbackFixture {
	scheme := newRollbackScheme(t)
	allObjs := append([]client.Object{rs}, objs...)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(allObjs...).
		WithStatusSubresource(&orchestrationv1alpha1.PodSet{}).
		Build()
	recorder := record.NewFakeRecorder(512)
	go func() {
		for range recorder.Events {
		}
	}()
	t.Cleanup(func() { close(recorder.Events) })
	return &rollbackFixture{
		t:        t,
		ctx:      context.Background(),
		cli:      cli,
		rs:       rs,
		recorder: recorder,
		now:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func newRollbackRoleSet(strategy orchestrationv1alpha1.RoleSetUpdateStrategyType, thresholdSeconds int32, roles ...*orchestrationv1alpha1.RoleSpec) *orchestrationv1alpha1.RoleSet {
	roleList := make([]orchestrationv1alpha1.RoleSpec, 0, len(roles))
	for _, role := range roles {
		roleList = append(roleList, *role)
	}
	return &orchestrationv1alpha1.RoleSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rb-roleset",
			Namespace: rbNamespace,
			Labels:    map[string]string{constants.StormServiceNameLabelKey: "storm"},
		},
		Spec: orchestrationv1alpha1.RoleSetSpec{
			Roles:          roleList,
			UpdateStrategy: strategy,
			RollbackStrategy: &orchestrationv1alpha1.RollbackStrategy{
				FailureThresholdSeconds: ptr.To(thresholdSeconds),
			},
		},
	}
}

func newRBRole(name string, replicas int32, image string, surge, unavailable int) *orchestrationv1alpha1.RoleSpec {
	return &orchestrationv1alpha1.RoleSpec{
		Name:     name,
		Replicas: ptr.To(replicas),
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{{Name: "main", Image: image}},
			},
		},
		UpdateStrategy: orchestrationv1alpha1.RoleUpdateStrategy{
			MaxSurge:       ptr.To(intstr.FromInt(surge)),
			MaxUnavailable: ptr.To(intstr.FromInt(unavailable)),
		},
	}
}

func (f *rollbackFixture) role(name string) *orchestrationv1alpha1.RoleSpec {
	for i := range f.rs.Spec.Roles {
		if f.rs.Spec.Roles[i].Name == name {
			return &f.rs.Spec.Roles[i]
		}
	}
	f.t.Fatalf("role %s not found", name)
	return nil
}

func (f *rollbackFixture) planner() *rollbackPlanner {
	return &rollbackPlanner{cli: f.cli, recorder: f.recorder, now: f.now}
}

func (f *rollbackFixture) plan() rolloutStatuses {
	statuses, err := f.planner().plan(f.ctx, f.rs)
	require.NoError(f.t, err)
	// Mimic the production status update: the controller persists the overlay
	// produced by the planner, so the next reconcile carries start times,
	// counters and history.
	if statuses != nil {
		byName := map[string]*orchestrationv1alpha1.RoleStatus{}
		for i := range f.rs.Status.Roles {
			byName[f.rs.Status.Roles[i].Name] = &f.rs.Status.Roles[i]
		}
		for _, role := range f.rs.Spec.Roles {
			overlay := statuses[role.Name]
			if overlay == nil {
				continue
			}
			st := byName[role.Name]
			if st == nil {
				f.rs.Status.Roles = append(f.rs.Status.Roles, orchestrationv1alpha1.RoleStatus{Name: role.Name})
				st = &f.rs.Status.Roles[len(f.rs.Status.Roles)-1]
			}
			st.CurrentRevision = overlay.CurrentRevision
			st.TargetRevision = overlay.TargetRevision
			st.RolloutState = overlay.RolloutState
			st.UpdateStartedAt = overlay.UpdateStartedAt
			st.RollbackCount = overlay.RollbackCount
			st.FailureReason = overlay.FailureReason
			st.FailureMessage = overlay.FailureMessage
			st.RollbackHistory = overlay.RollbackHistory
		}
	}
	return statuses
}

func (f *rollbackFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func (f *rollbackFixture) rollingNext() controllerdrain.Result {
	var manager RollingManager
	switch f.rs.Spec.UpdateStrategy {
	case orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType:
		manager = &RollingManagerParallel{cli: f.cli, recorder: f.recorder}
	case orchestrationv1alpha1.InterleaveRoleSetStrategyType:
		manager = &RollingManagerInterleave{cli: f.cli, recorder: f.recorder}
	default:
		manager = &RollingManagerSequential{cli: f.cli, recorder: f.recorder}
	}
	result, err := manager.Next(f.ctx, f.rs)
	require.NoError(f.t, err)
	return result
}

func roleHash(role *orchestrationv1alpha1.RoleSpec) string {
	return ctrlutil.ComputeHash(&role.Template, nil)
}

// renderRolePod renders a pod exactly like the production syncer would.
func renderRolePod(t *testing.T, rs *orchestrationv1alpha1.RoleSet, role *orchestrationv1alpha1.RoleSpec, index int, ready bool) *v1.Pod {
	pod, err := ctrlutil.GetPodFromTemplate(&role.Template, rs, metav1.NewControllerRef(rs, orchestrationv1alpha1.SchemeGroupVersion.WithKind(orchestrationv1alpha1.RoleSetKind)))
	require.NoError(t, err)
	idx := index
	renderStormServicePod(rs, role, pod, &idx)
	pod.Status.Phase = v1.PodRunning
	pod.Status.PodIP = "10.0.0.1"
	status := v1.ConditionFalse
	if ready {
		status = v1.ConditionTrue
	}
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: status}}
	return pod
}

func (f *rollbackFixture) seedReadyPods(roleName string) {
	role := f.role(roleName)
	for i := int32(0); i < getRoleReplicas(role); i++ {
		require.NoError(f.t, f.cli.Create(f.ctx, renderRolePod(f.t, f.rs, role, int(i), true)))
	}
}

func (f *rollbackFixture) listPods(roleName string) []*v1.Pod {
	pods, err := getRolePods(f.ctx, f.cli, f.rs.Namespace, f.rs.Name, roleName)
	require.NoError(f.t, err)
	return pods
}

func (f *rollbackFixture) listPodSets(roleName string) []*orchestrationv1alpha1.PodSet {
	podSets, err := getRolePodSets(f.ctx, f.cli, f.rs.Namespace, f.rs.Name, roleName)
	require.NoError(f.t, err)
	return podSets
}

// setReady flips the readiness of active pods. When roleFilter is non-empty,
// only pods of that role are touched.
func (f *rollbackFixture) setReady(roleName, hash string, ready bool) {
	for _, pod := range f.listPods(roleName) {
		if !podutilIsActive(pod) {
			continue
		}
		if hash != "" && pod.Labels[constants.RoleTemplateHashLabelKey] != hash {
			continue
		}
		updated := pod.DeepCopy()
		desired := v1.ConditionFalse
		if ready {
			desired = v1.ConditionTrue
		}
		updated.Status.Phase = v1.PodRunning
		found := false
		for i := range updated.Status.Conditions {
			if updated.Status.Conditions[i].Type == v1.PodReady {
				updated.Status.Conditions[i].Status = desired
				found = true
			}
		}
		if !found {
			updated.Status.Conditions = append(updated.Status.Conditions, v1.PodCondition{Type: v1.PodReady, Status: desired})
		}
		// fake client treats Pod as an in-tree status subresource, so status
		// changes must go through the status writer.
		require.NoError(f.t, f.cli.Status().Update(f.ctx, updated))
	}
}

func podutilIsActive(pod *v1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed
}

func (f *rollbackFixture) countByHash(roleName string) map[string]int {
	counts := map[string]int{}
	for _, pod := range f.listPods(roleName) {
		if podutilIsActive(pod) {
			counts[pod.Labels[constants.RoleTemplateHashLabelKey]]++
		}
	}
	return counts
}

// changeImage mutates the role template like a user applying a new revision.
func (f *rollbackFixture) changeImage(roleName, image string) {
	role := f.role(roleName)
	role.Template.Spec.Containers[0].Image = image
	require.NoError(f.t, f.cli.Update(f.ctx, f.rs))
}

func (f *rollbackFixture) fetchedRoleSet() *orchestrationv1alpha1.RoleSet {
	rs := &orchestrationv1alpha1.RoleSet{}
	require.NoError(f.t, f.cli.Get(f.ctx, types.NamespacedName{Name: f.rs.Name, Namespace: f.rs.Namespace}, rs))
	return rs
}

func (f *rollbackFixture) requestRecovery(value string) {
	if f.rs.Annotations == nil {
		f.rs.Annotations = map[string]string{}
	}
	f.rs.Annotations[constants.RoleSetRollbackRecoveryAnnotationKey] = value
	require.NoError(f.t, f.cli.Update(f.ctx, f.rs))
}

// driveUntil runs plan + rolling manager ticks until done() is true or the
// tick budget is exhausted. Pods of good revisions become ready; pods whose
// hash matches neverReadyHashes[roleName] stay not ready.
func (f *rollbackFixture) driveUntil(maxTicks int, tick time.Duration, neverReadyHashes map[string]string, done func(rolloutStatuses) bool) rolloutStatuses {
	var statuses rolloutStatuses
	for i := 0; i < maxTicks; i++ {
		f.advance(tick)
		statuses = f.plan()
		if done != nil && done(statuses) {
			return statuses
		}
		f.rollingNext()
		// Simulate the scheduler/kubelet: new pods report ready, except pods
		// of revisions that intentionally fail.
		for _, role := range f.rs.Spec.Roles {
			pods := f.listPods(role.Name)
			for _, pod := range pods {
				if !podutilIsActive(pod) {
					continue
				}
				hash := pod.Labels[constants.RoleTemplateHashLabelKey]
				if bad, ok := neverReadyHashes[role.Name]; ok && hash == bad {
					continue
				}
				if pod.Status.Phase != v1.PodRunning || !isPodReadyCondition(pod) {
					f.setReady(role.Name, hash, true)
				}
			}
		}
		statuses = f.plan()
		if done != nil && done(statuses) {
			return statuses
		}
	}
	require.True(f.t, done == nil || done(statuses), "driveUntil exhausted its tick budget")
	return statuses
}

func isPodReadyCondition(pod *v1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == v1.PodReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}

func waitForStable(t *testing.T, statuses rolloutStatuses, roleName string) *orchestrationv1alpha1.RoleStatus {
	st := statuses[roleName]
	require.NotNil(t, st, "no rollout status for role %s", roleName)
	require.Equal(t, orchestrationv1alpha1.RoleRolloutStable, st.RolloutState, "role %s state is %s: %s", roleName, st.RolloutState, st.FailureMessage)
	return st
}

func TestRollbackDisabledIsNoop(t *testing.T) {
	role := newRBRole("worker", 1, imageV2, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	rs.Spec.RollbackStrategy = nil
	// new not-ready pod of a different hash
	pod := renderRolePod(t, rs, role, 0, false)
	pod.Labels[constants.RoleTemplateHashLabelKey] = "some-other-hash"
	fixture := newRollbackFixture(t, rs, pod)

	statuses, err := fixture.planner().plan(context.Background(), rs)
	require.NoError(t, err)
	assert.Nil(t, statuses)
	assert.Nil(t, rs.Spec.Roles[0].StableTemplate)
}

func TestRollbackRecordsStableRevision(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")

	statuses := fixture.plan()

	st := statuses["worker"]
	require.Equal(t, orchestrationv1alpha1.RoleRolloutStable, st.RolloutState)
	hash := roleHash(role)
	assert.Equal(t, hash, st.CurrentRevision)
	assert.Equal(t, hash, st.TargetRevision)
	assert.Nil(t, st.UpdateStartedAt)

	// stable template is persisted in spec
	persisted := fixture.fetchedRoleSet()
	require.NotNil(t, persisted.Spec.Roles[0].StableTemplate)
	assert.Equal(t, hash, persisted.Spec.Roles[0].StableRevision)
	assert.Equal(t, imageV1, persisted.Spec.Roles[0].StableTemplate.Spec.Containers[0].Image)
}

func TestRollbackDoesNotTriggerBeforeThreshold(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 30, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	// apply a bad revision
	fixture.changeImage("worker", imageV2)
	badHash := roleHash(fixture.role("worker"))
	require.NotEqual(t, stableHash, badHash)

	// surge pod of the bad revision appears and stays not ready
	fixture.plan()
	fixture.rollingNext()
	assert.Len(t, fixture.countByHash("worker"), 2)

	// still within threshold: progressing, no rollback
	fixture.advance(29 * time.Second)
	statuses := fixture.plan()
	st := statuses["worker"]
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutProgressing, st.RolloutState)
	assert.Zero(t, st.RollbackCount)
	assert.Empty(t, st.FailureReason)
	assert.Equal(t, imageV2, fixture.fetchedRoleSet().Spec.Roles[0].Template.Spec.Containers[0].Image)
}

func TestParallelRollbackFailureRestoresStableAndDoesNotDuplicatePods(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	fixture.changeImage("worker", imageV2)
	badHash := roleHash(fixture.role("worker"))
	neverReady := map[string]string{"worker": badHash}

	// one second before the threshold nothing has happened
	statuses := fixture.driveUntil(9, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutProgressing
	})
	require.Equal(t, orchestrationv1alpha1.RoleRolloutProgressing, statuses["worker"].RolloutState)

	// crossing the threshold triggers the rollback
	statuses = fixture.driveUntil(20, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutRollingBack
	})
	st := statuses["worker"]
	require.Equal(t, orchestrationv1alpha1.RoleRolloutRollingBack, st.RolloutState)
	assert.Equal(t, int32(1), st.RollbackCount)
	assert.Equal(t, FailureReasonProgressDeadlineExceeded, st.FailureReason)
	assert.Contains(t, st.FailureMessage, shortHash(badHash))
	require.Len(t, st.RollbackHistory, 1)
	assert.Equal(t, badHash, st.RollbackHistory[0].TargetRevision)
	assert.Equal(t, stableHash, st.RollbackHistory[0].RollbackRevision)
	require.NotNil(t, st.RollbackHistory[0].FailedTemplate)
	assert.Equal(t, imageV2, st.RollbackHistory[0].FailedTemplate.Spec.Containers[0].Image)

	// spec template swapped back to the stable revision
	persisted := fixture.fetchedRoleSet()
	assert.Equal(t, imageV1, persisted.Spec.Roles[0].Template.Spec.Containers[0].Image)
	require.NotNil(t, persisted.Spec.Roles[0].StableTemplate)
	assert.Equal(t, stableHash, persisted.Spec.Roles[0].StableRevision)

	// drive the rollback to completion: bad pod removed, stable pod kept
	statuses = fixture.driveUntil(20, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})
	st = waitForStable(t, statuses, "worker")
	assert.Equal(t, int32(1), st.RollbackCount)
	assert.Equal(t, FailureReasonProgressDeadlineExceeded, st.FailureReason, "failure reason is retained until manual recovery")
	counts := fixture.countByHash("worker")
	assert.Equal(t, map[string]int{stableHash: 1}, counts, "old pods must not be recreated by the rollback")
}

func TestRollbackDrainTimeoutReason(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 0, 1)
	drainTimeout := int32(3600)
	role.Drain = &orchestrationv1alpha1.RoleDrainSpec{TimeoutSeconds: &drainTimeout}
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	fixture.changeImage("worker", imageV2)
	badHash := roleHash(fixture.role("worker"))

	// no surge allowed: the ready stable pod is sent to drain and blocks there
	fixture.plan()
	result := fixture.rollingNext()
	assert.True(t, result.Changed)
	draining := 0
	for _, pod := range fixture.listPods("worker") {
		if pod.Annotations[aibrixconst.PodDrainingAnnotationKey] == "true" {
			draining++
			assert.Equal(t, aibrixconst.PodDrainReasonRollout, pod.Annotations[aibrixconst.PodDrainReasonAnnotationKey])
		}
	}
	require.Equal(t, 1, draining)
	// no new revision pod can be created while the stable pod drains
	assert.Equal(t, map[string]int{stableHash: 1}, fixture.countByHash("worker"))

	// the next reconcile observes the blocked drain and arms the deadline
	fixture.plan()

	// threshold crossed while drain is stuck -> DrainTimeout rollback
	fixture.advance(11 * time.Second)
	statuses := fixture.plan()
	st := statuses["worker"]
	require.Equal(t, orchestrationv1alpha1.RoleRolloutRollingBack, st.RolloutState)
	assert.Equal(t, FailureReasonDrainTimeout, st.FailureReason)
	assert.Equal(t, int32(1), st.RollbackCount)
	require.NotEmpty(t, st.RollbackHistory)
	assert.Equal(t, badHash, st.RollbackHistory[0].TargetRevision)

	// simulate the drain finally completing: the draining pod is deleted
	for _, pod := range fixture.listPods("worker") {
		if pod.Annotations[aibrixconst.PodDrainingAnnotationKey] == "true" {
			require.NoError(t, fixture.cli.Delete(fixture.ctx, pod))
		}
	}
	// the regular Scale path recreates a pod from the stable template, still
	// honoring PodGroupSize/budgets; then it becomes ready
	neverReady := map[string]string{"worker": "hash-that-never-appears"}
	statuses = fixture.driveUntil(20, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})
	waitForStable(t, statuses, "worker")
	assert.Equal(t, map[string]int{stableHash: 1}, fixture.countByHash("worker"))
}

func TestParallelPartialRoleFailure(t *testing.T) {
	bad := newRBRole("bad", 1, imageV1, 1, 0)
	good := newRBRole("good", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, bad, good)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("bad")
	fixture.seedReadyPods("good")
	fixture.plan()
	badStable := roleHash(fixture.role("bad"))
	goodStable := roleHash(fixture.role("good"))
	require.Equal(t, badStable, goodStable) // identical templates share a hash

	fixture.changeImage("bad", imageV2)
	fixture.changeImage("good", imageV2)
	badHash := roleHash(fixture.role("bad"))
	goodHash := roleHash(fixture.role("good"))
	require.Equal(t, badHash, goodHash)
	neverReady := map[string]string{"bad": badHash}

	statuses := fixture.driveUntil(40, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["good"].RolloutState == orchestrationv1alpha1.RoleRolloutStable &&
			s["bad"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})

	goodStatus := waitForStable(t, statuses, "good")
	assert.Equal(t, badHash, goodStatus.CurrentRevision, "healthy role completes the update")
	assert.Zero(t, goodStatus.RollbackCount)

	badStatus := waitForStable(t, statuses, "bad")
	assert.Equal(t, badStable, badStatus.CurrentRevision, "failed role is back on its stable revision")
	assert.Equal(t, int32(1), badStatus.RollbackCount)
	assert.Equal(t, FailureReasonProgressDeadlineExceeded, badStatus.FailureReason)
	assert.Equal(t, map[string]int{badStable: 1}, fixture.countByHash("bad"))
	assert.Equal(t, map[string]int{badHash: 1}, fixture.countByHash("good"))
}

func TestSequentialRollbackDoesNotArmRoleWaitingForTurn(t *testing.T) {
	first := newRBRole("first", 1, imageV1, 1, 0)
	first.UpgradeOrder = ptr.To[int32](1)
	second := newRBRole("second", 1, imageV1, 1, 0)
	second.UpgradeOrder = ptr.To[int32](2)
	rs := newRollbackRoleSet(orchestrationv1alpha1.SequentialRoleSetStrategyType, 10, first, second)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("first")
	fixture.seedReadyPods("second")
	fixture.plan()
	firstStable := roleHash(fixture.role("first"))

	fixture.changeImage("first", imageV2)
	fixture.changeImage("second", imageV2)
	secondBadHash := roleHash(fixture.role("second"))

	// first role fails; the second never starts (sequential), so its deadline
	// must not be armed even long past the threshold.
	neverReady := map[string]string{"first": roleHash(fixture.role("first")), "second": "impossible"}
	statuses := fixture.driveUntil(30, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["first"] != nil && s["first"].RolloutState == orchestrationv1alpha1.RoleRolloutStable &&
			s["first"].RollbackCount == 1
	})
	firstStatus := waitForStable(t, statuses, "first")
	assert.Equal(t, firstStable, firstStatus.CurrentRevision)
	secondStatus := statuses["second"]
	require.NotNil(t, secondStatus)
	assert.Nil(t, secondStatus.UpdateStartedAt, "role waiting for its turn must not age out")
	assert.Zero(t, secondStatus.RollbackCount)
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutProgressing, secondStatus.RolloutState)

	// after the first role is stable, the second proceeds and succeeds
	neverReady = map[string]string{"first": "impossible"}
	statuses = fixture.driveUntil(30, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["second"] != nil && s["second"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})
	secondFinal := waitForStable(t, statuses, "second")
	assert.Equal(t, secondBadHash, secondFinal.CurrentRevision)
	assert.Zero(t, secondFinal.RollbackCount)
}

func TestInterleavePartialRoleFailure(t *testing.T) {
	bad := newRBRole("bad", 1, imageV1, 1, 1)
	good := newRBRole("good", 1, imageV1, 1, 1)
	rs := newRollbackRoleSet(orchestrationv1alpha1.InterleaveRoleSetStrategyType, 10, bad, good)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("bad")
	fixture.seedReadyPods("good")
	fixture.plan()
	badStable := roleHash(fixture.role("bad"))

	fixture.changeImage("bad", imageV2)
	fixture.changeImage("good", imageV2)
	badHash := roleHash(fixture.role("bad"))
	neverReady := map[string]string{"bad": badHash}

	statuses := fixture.driveUntil(60, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["good"] != nil && s["good"].RolloutState == orchestrationv1alpha1.RoleRolloutStable &&
			s["bad"] != nil && s["bad"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})

	goodStatus := waitForStable(t, statuses, "good")
	assert.Equal(t, badHash, goodStatus.CurrentRevision)
	assert.Zero(t, goodStatus.RollbackCount)
	badStatus := waitForStable(t, statuses, "bad")
	assert.Equal(t, badStable, badStatus.CurrentRevision)
	assert.Equal(t, int32(1), badStatus.RollbackCount)
	assert.Equal(t, FailureReasonProgressDeadlineExceeded, badStatus.FailureReason)
	assert.Equal(t, map[string]int{badStable: 1}, fixture.countByHash("bad"))
}

func TestConsecutiveFailuresAndManualRecovery(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	// first failure
	fixture.changeImage("worker", imageV2)
	badHash := roleHash(fixture.role("worker"))
	neverReady := map[string]string{"worker": badHash}
	statuses := fixture.driveUntil(30, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"] != nil && s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable && s["worker"].RollbackCount == 1
	})
	waitForStable(t, statuses, "worker")
	assert.Equal(t, map[string]int{stableHash: 1}, fixture.countByHash("worker"))

	// administrator requests recovery: failure cleared and the same revision
	// is retried without re-applying the template.
	fixture.requestRecovery("worker")
	statuses = fixture.plan()
	st := statuses["worker"]
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutProgressing, st.RolloutState)
	assert.Empty(t, st.FailureReason)
	assert.Empty(t, st.FailureMessage)
	assert.Equal(t, badHash, st.TargetRevision)
	require.NotNil(t, st.UpdateStartedAt)
	// annotation consumed
	persisted := fixture.fetchedRoleSet()
	_, hasAnnotation := persisted.Annotations[constants.RoleSetRollbackRecoveryAnnotationKey]
	assert.False(t, hasAnnotation)
	assert.Equal(t, imageV2, persisted.Spec.Roles[0].Template.Spec.Containers[0].Image)
	require.Len(t, st.RollbackHistory, 1)
	require.NotNil(t, st.RollbackHistory[0].RecoveredAt)

	// second failure of the same revision
	statuses = fixture.driveUntil(30, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"] != nil && s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable && s["worker"].RollbackCount == 2
	})
	st = waitForStable(t, statuses, "worker")
	assert.Equal(t, int32(2), st.RollbackCount)
	require.Len(t, st.RollbackHistory, 2)
	assert.Equal(t, badHash, st.RollbackHistory[0].TargetRevision)
	assert.Equal(t, badHash, st.RollbackHistory[1].TargetRevision)
	assert.Nil(t, st.RollbackHistory[1].FailedTemplate, "only the newest record keeps the failed template")
	// rollback must not create a second copy of the stable pod
	assert.Equal(t, map[string]int{stableHash: 1}, fixture.countByHash("worker"))

	// recovering with "*" also works and retries the revision again
	fixture.requestRecovery("*")
	statuses = fixture.plan()
	assert.Equal(t, badHash, statuses["worker"].TargetRevision)
	assert.Empty(t, statuses["worker"].FailureReason)
}

func TestRecoveryDoesNotOverwriteNewerInFlightRevision(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()

	// v2 fails and is rolled back to v1
	fixture.changeImage("worker", imageV2)
	neverReady := map[string]string{"worker": roleHash(fixture.role("worker"))}
	statuses := fixture.driveUntil(30, time.Second, neverReady, func(s rolloutStatuses) bool {
		return s["worker"] != nil && s["worker"].RollbackCount == 1 &&
			s["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable
	})
	waitForStable(t, statuses, "worker")
	require.Equal(t, imageV1, fixture.fetchedRoleSet().Spec.Roles[0].Template.Spec.Containers[0].Image)

	// administrator applies a newer v3 revision, then requests recovery
	fixture.changeImage("worker", imageV3)
	v3Hash := roleHash(fixture.role("worker"))
	fixture.requestRecovery("worker")

	statuses = fixture.plan()
	st := statuses["worker"]
	assert.Empty(t, st.FailureReason, "failure is cleared")
	assert.Equal(t, v3Hash, st.TargetRevision, "the in-flight revision is preserved")
	assert.Equal(t, imageV3, fixture.rs.Spec.Roles[0].Template.Spec.Containers[0].Image)
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutProgressing, st.RolloutState)
	// history of the old failure is retained for observability
	require.Len(t, st.RollbackHistory, 1)
}

func TestRecoveryClearsFailureWithoutRetriableTemplate(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	// synthesize a completed rollback without a stored failed template
	fixture.rs.Status.Roles = []orchestrationv1alpha1.RoleStatus{{
		Name:            "worker",
		Replicas:        1,
		ReadyReplicas:   1,
		RolloutState:    orchestrationv1alpha1.RoleRolloutRollingBack,
		CurrentRevision: stableHash,
		TargetRevision:  stableHash,
		RollbackCount:   1,
		FailureReason:   FailureReasonProgressDeadlineExceeded,
		FailureMessage:  "synthetic failure",
	}}

	fixture.requestRecovery("worker")
	statuses := fixture.plan()
	st := statuses["worker"]
	assert.Empty(t, st.FailureReason)
	assert.Empty(t, st.FailureMessage)
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutStable, st.RolloutState)
	assert.Equal(t, int32(1), st.RollbackCount, "counter is preserved")
}

func TestRollbackNormalizesInPlacePods(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	role.UpdateStrategy.Type = orchestrationv1alpha1.InPlaceIfPossibleRoleUpdateStrategyType
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()
	stableHash := roleHash(role)

	// simulate an in-place patch toward v2: pod keeps the stable hash label
	// but carries the target-hash annotation.
	pods := fixture.listPods("worker")
	require.Len(t, pods, 1)
	patched := pods[0].DeepCopy()
	patched.Annotations[constants.RoleInPlaceUpdateTargetHashAnnotationKey] = roleHash(&orchestrationv1alpha1.RoleSpec{
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "main", Image: imageV2}}}},
	})
	badHash := patched.Annotations[constants.RoleInPlaceUpdateTargetHashAnnotationKey]
	require.NotEqual(t, stableHash, badHash)
	require.NoError(t, fixture.cli.Update(fixture.ctx, patched))

	fixture.changeImage("worker", imageV2)
	require.Equal(t, badHash, roleHash(fixture.role("worker")))

	// first reconcile observes the in-place target and arms the deadline; the
	// threshold is crossed on a later reconcile.
	fixture.plan()
	fixture.advance(11 * time.Second)
	statuses := fixture.plan()
	st := statuses["worker"]
	assert.Equal(t, orchestrationv1alpha1.RoleRolloutRollingBack, st.RolloutState)
	pods = fixture.listPods("worker")
	require.Len(t, pods, 1)
	assert.Equal(t, badHash, pods[0].Labels[constants.RoleTemplateHashLabelKey])
	assert.Empty(t, pods[0].Annotations[constants.RoleInPlaceUpdateTargetHashAnnotationKey])
	assert.Empty(t, pods[0].Annotations[constants.RoleInPlaceUpdateStateAnnotationKey])
	assert.Equal(t, imageV1, fixture.rs.Spec.Roles[0].Template.Spec.Containers[0].Image)
}

func TestRollbackWithPodGroupSize(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	role.PodGroupSize = ptr.To[int32](2)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)

	// seed a ready PodSet of the stable revision
	syncer := GetRoleSyncerWithRecorder(fixture.cli, fixture.role("worker"), nil)
	podSetSyncer, ok := syncer.(*PodSetRoleSyncer)
	require.True(t, ok)
	podSet := podSetSyncer.createPodSetForRole(rs, fixture.role("worker"), ptr.To(0))
	podSet.Status.Phase = orchestrationv1alpha1.PodSetPhaseReady
	require.NoError(t, fixture.cli.Create(fixture.ctx, podSet))
	stableHash := roleHash(fixture.role("worker"))

	fixture.plan()
	fixture.changeImage("worker", imageV2)
	badHash := roleHash(fixture.role("worker"))

	// rollout creates a not-ready PodSet of the new revision
	fixture.plan()
	fixture.rollingNext()
	podSets := fixture.listPodSets("worker")
	var hasBad bool
	for _, ps := range podSets {
		if ps.Labels[constants.RoleTemplateHashLabelKey] == badHash {
			hasBad = true
		}
	}
	require.True(t, hasBad)

	// the new-revision PodSet is now observed and arms the deadline
	fixture.plan()

	// threshold triggers rollback; subsequent Scale/Rollout replaces the bad
	// PodSet through the same PodSet path (PodGroupSize preserved).
	fixture.advance(11 * time.Second)
	statuses := fixture.plan()
	require.Equal(t, orchestrationv1alpha1.RoleRolloutRollingBack, statuses["worker"].RolloutState)
	for i := 0; i < 20; i++ {
		fixture.rollingNext()
		for _, ps := range fixture.listPodSets("worker") {
			if ps.Labels[constants.RoleTemplateHashLabelKey] == stableHash && ps.Status.Phase != orchestrationv1alpha1.PodSetPhaseReady {
				updated := ps.DeepCopy()
				updated.Status.Phase = orchestrationv1alpha1.PodSetPhaseReady
				require.NoError(t, fixture.cli.Status().Update(fixture.ctx, updated))
			}
		}
		statuses = fixture.plan()
		if statuses["worker"].RolloutState == orchestrationv1alpha1.RoleRolloutStable {
			break
		}
	}
	st := waitForStable(t, statuses, "worker")
	assert.Equal(t, int32(1), st.RollbackCount)
	var readyStablePods int
	for _, ps := range fixture.listPodSets("worker") {
		assert.NotEqual(t, badHash, ps.Labels[constants.RoleTemplateHashLabelKey])
		if ps.Labels[constants.RoleTemplateHashLabelKey] == stableHash && ps.Status.Phase == orchestrationv1alpha1.PodSetPhaseReady {
			readyStablePods++
		}
	}
	assert.Equal(t, 1, readyStablePods)
}

func TestProgressingConditionReflectsRollback(t *testing.T) {
	role := newRBRole("worker", 1, imageV1, 1, 0)
	rs := newRollbackRoleSet(orchestrationv1alpha1.ParallelRoleSetUpdateStrategyType, 10, role)
	fixture := newRollbackFixture(t, rs)
	fixture.seedReadyPods("worker")
	fixture.plan()

	status := &orchestrationv1alpha1.RoleSetStatus{}
	setProgressingCondition(status, rs, fixture.plan())
	cond := getConditionForTest(status, orchestrationv1alpha1.RoleSetProgressing)
	require.NotNil(t, cond)
	assert.Equal(t, v1.ConditionFalse, cond.Status)
	assert.Equal(t, ProgressingRolloutCompletedReason, cond.Reason)

	// mutate state to RollingBack
	statuses := rolloutStatuses{"worker": {RolloutState: orchestrationv1alpha1.RoleRolloutRollingBack}}
	setProgressingCondition(status, rs, statuses)
	cond = getConditionForTest(status, orchestrationv1alpha1.RoleSetProgressing)
	require.NotNil(t, cond)
	assert.Equal(t, v1.ConditionTrue, cond.Status)
	assert.Equal(t, ProgressingRollbackInProgressReason, cond.Reason)
}

func getConditionForTest(status *orchestrationv1alpha1.RoleSetStatus, condType orchestrationv1alpha1.ConditionType) *orchestrationv1alpha1.Condition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == condType {
			return &status.Conditions[i]
		}
	}
	return nil
}

func TestRecoveryAnnotationParsing(t *testing.T) {
	roles := []orchestrationv1alpha1.RoleSpec{{Name: "a"}, {Name: "b"}}
	assert.Equal(t, map[string]bool{"a": true, "b": true}, parseRecoveryRoles("*", roles))
	assert.Equal(t, map[string]bool{"a": true, "b": true}, parseRecoveryRoles(" ", roles))
	assert.Equal(t, map[string]bool{"a": true}, parseRecoveryRoles("a", roles))
	assert.Equal(t, map[string]bool{"a": true, "b": true}, parseRecoveryRoles(" a ,b ", roles))
	assert.Empty(t, parseRecoveryRoles("unknown", roles))
}

func TestFailureThresholdDefault(t *testing.T) {
	rs := &orchestrationv1alpha1.RoleSet{Spec: orchestrationv1alpha1.RoleSetSpec{
		RollbackStrategy: &orchestrationv1alpha1.RollbackStrategy{},
	}}
	assert.Equal(t, 10*time.Minute, failureThreshold(rs))

	rs.Spec.RollbackStrategy.FailureThresholdSeconds = ptr.To[int32](5)
	assert.Equal(t, 5*time.Second, failureThreshold(rs))
}
