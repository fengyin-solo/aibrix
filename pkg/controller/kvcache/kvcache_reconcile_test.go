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

package kvcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/controller/kvcache/backends"
)

func reconcileTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	require.NoError(t, orchestrationv1alpha1.AddToScheme(s))
	return s
}

func newReconcileTestKVCache(name, backend string) *orchestrationv1alpha1.KVCache {
	annotations := map[string]string{}
	if backend != "" {
		annotations[constants.KVCacheLabelKeyBackend] = backend
	}
	return &orchestrationv1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			UID:         types.UID(name + "-uid"),
			Annotations: annotations,
		},
		Spec: orchestrationv1alpha1.KVCacheSpec{
			Cache: orchestrationv1alpha1.RuntimeSpec{
				Replicas: 3,
				Image:    "registry.example/kvcache:v1",
			},
			Service: orchestrationv1alpha1.ServiceSpec{
				Type: corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{
					{Name: "service", Port: 18512, Protocol: corev1.ProtocolTCP},
				},
			},
		},
	}
}

func newKVCacheTestReconciler(c client.Client, s *runtime.Scheme) *KVCacheReconciler {
	return &KVCacheReconciler{
		Client: c,
		Scheme: s,
		Backends: map[string]backends.BackendReconciler{
			constants.KVCacheBackendVineyard:    backends.NewVineyardReconciler(c),
			constants.KVCacheBackendHPKV:        backends.NewDistributedReconciler(c, constants.KVCacheBackendHPKV),
			constants.KVCacheBackendInfinistore: backends.NewDistributedReconciler(c, constants.KVCacheBackendInfinistore),
		},
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func reconcileRequest(kv *orchestrationv1alpha1.KVCache) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: kv.Namespace, Name: kv.Name}}
}

func setSTSStatus(t *testing.T, ctx context.Context, c client.Client, name string, status appsv1.StatefulSetStatus) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, sts))
	sts.Status = status
	require.NoError(t, c.Status().Update(ctx, sts))
}

func setDeploymentStatus(t *testing.T, ctx context.Context, c client.Client, name string, status appsv1.DeploymentStatus) {
	t.Helper()
	deployment := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, deployment))
	deployment.Status = status
	require.NoError(t, c.Status().Update(ctx, deployment))
}

// -- Status derivation --

func TestReconcile_StatusDerivation_StatefulSet(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("status-sts", constants.KVCacheBackendHPKV)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "status-sts"}

	// Initial reconcile: StatefulSet was just created, no replicas ready.
	result, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	updated := &orchestrationv1alpha1.KVCache{}
	require.NoError(t, c.Get(ctx, key, updated))
	assert.Equal(t, int32(0), updated.Status.ReadyReplicas)
	ready := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, reasonReplicasNotReady, ready.Reason)
	assert.Equal(t, "0 of 3 replicas ready, 3 more required", ready.Message)
	progressing := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	require.NotNil(t, progressing)
	assert.Equal(t, metav1.ConditionTrue, progressing.Status)

	// Partial rollout: 1 of 3 replicas ready and updated.
	setSTSStatus(t, ctx, c, kv.Name, appsv1.StatefulSetStatus{
		Replicas:        3,
		ReadyReplicas:   1,
		UpdatedReplicas: 3,
		CurrentRevision: "rev-1",
		UpdateRevision:  "rev-1",
	})
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, updated))
	assert.Equal(t, int32(1), updated.Status.ReadyReplicas)
	ready = findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, "1 of 3 replicas ready, 2 more required", ready.Message)
	progressing = findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	assert.Equal(t, metav1.ConditionTrue, progressing.Status)

	// Rolling update in progress even though the currently ready pods are all
	// on the old revision.
	setSTSStatus(t, ctx, c, kv.Name, appsv1.StatefulSetStatus{
		Replicas:        3,
		ReadyReplicas:   3,
		UpdatedReplicas: 1,
		CurrentRevision: "rev-1",
		UpdateRevision:  "rev-2",
	})
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, updated))
	progressing = findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	assert.Equal(t, metav1.ConditionTrue, progressing.Status)
	assert.Contains(t, progressing.Message, "Rolling update in progress")

	// Fully converged: Ready=True and Progressing=False.
	setSTSStatus(t, ctx, c, kv.Name, appsv1.StatefulSetStatus{
		Replicas:        3,
		ReadyReplicas:   3,
		UpdatedReplicas: 3,
		CurrentRevision: "rev-2",
		UpdateRevision:  "rev-2",
	})
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, updated))
	assert.Equal(t, int32(3), updated.Status.ReadyReplicas)
	ready = findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
	assert.Equal(t, reasonAllReplicasReady, ready.Reason)
	progressing = findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	require.NotNil(t, progressing)
	assert.Equal(t, metav1.ConditionFalse, progressing.Status)
	assert.Equal(t, reasonReconcileComplete, progressing.Reason)
}

func TestReconcile_StatusDerivation_Deployment(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("status-deploy", constants.KVCacheBackendVineyard)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "status-deploy"}

	_, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)

	deployment := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, key, deployment))

	setDeploymentStatus(t, ctx, c, kv.Name, appsv1.DeploymentStatus{
		Replicas:          3,
		ReadyReplicas:     3,
		UpdatedReplicas:   3,
		AvailableReplicas: 3,
	})
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)

	updated := &orchestrationv1alpha1.KVCache{}
	require.NoError(t, c.Get(ctx, key, updated))
	assert.Equal(t, int32(3), updated.Status.ReadyReplicas)
	ready := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
	progressing := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	require.NotNil(t, progressing)
	assert.Equal(t, metav1.ConditionFalse, progressing.Status)
}

// -- Unknown backend --

func TestReconcile_UnsupportedBackendRecordsCondition(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("bad-backend", "not-a-real-backend")
	statusWrites := 0
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cc client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				statusWrites++
				return cc.Status().Update(ctx, obj, opts...)
			},
		}).Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "bad-backend"}

	// No error: the object is not retried hot for a configuration error.
	result, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.Equal(t, 1, statusWrites)

	updated := &orchestrationv1alpha1.KVCache{}
	require.NoError(t, c.Get(ctx, key, updated))
	ready := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, reasonUnsupportedBackend, ready.Reason)
	assert.Contains(t, ready.Message, "not-a-real-backend")
	progressing := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionProgressing)
	require.NotNil(t, progressing)
	assert.Equal(t, metav1.ConditionFalse, progressing.Status)
	assert.Equal(t, reasonUnsupportedBackend, progressing.Reason)

	// An unsupported backend must remain inert: no workload resources created.
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &appsv1.StatefulSet{})))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &appsv1.Deployment{})))

	// Repeated reconcile is idempotent: the conditions already describe the
	// problem, so no further status write happens.
	result, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.Equal(t, 1, statusWrites, "repeated reconcile must not rewrite an unchanged status")
}

// -- Cleanup failure retries without blocking the current backend --

func TestReconcile_ForeignCleanupFailureRequeuesAndStillReconciles(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("cleanup-fail", constants.KVCacheBackendHPKV)

	// A resource from a previous backend (vineyard Deployment) that fails to delete.
	oldDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kv.Name,
			Namespace: kv.Namespace,
			Labels:    map[string]string{constants.KVCacheLabelKeyIdentifier: kv.Name},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "old"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "old"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "old", Image: "registry.example/old:v1",
					ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(80)},
					}},
				}}},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv, oldDeployment).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cc client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok {
					return apierrors.NewInternalError(fmt.Errorf("simulated apiserver failure"))
				}
				return cc.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "cleanup-fail"}

	// The reconcile returns a requeue (not an error) and the current backend's
	// StatefulSet is still created.
	result, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.True(t, result.Requeue, "cleanup failure should request a retry without returning an error")

	require.NoError(t, c.Get(ctx, key, &appsv1.StatefulSet{}), "current backend resources must still converge")
	require.NoError(t, c.Get(ctx, key, &appsv1.Deployment{}), "the failing-to-delete resource is still present")

	// Status was still derived from the current backend's workload.
	updated := &orchestrationv1alpha1.KVCache{}
	require.NoError(t, c.Get(ctx, key, updated))
	ready := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)

	// A failed cleanup is not cached: the next reconcile retries it.
	result, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.True(t, result.Requeue, "cleanup failure keeps requesting a retry")
}

// -- End-to-end backend switch cleanup --

func TestReconcile_ForeignCleanupRunsOncePerBackend(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("cleanup-cache", constants.KVCacheBackendHPKV)
	deletes := 0
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cc client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				return cc.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := newKVCacheTestReconciler(c, s)

	// First reconcile: foreign cleanup probes the vineyard resource names (3)
	// and the absent watcher resources (4).
	_, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, 7, deletes)

	// Second reconcile: foreign cleanup is cached for the hpkv backend, so only
	// the watcher-removal probes (4) remain.
	deletes = 0
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, 4, deletes)

	// Switching the backend invalidates the cache and re-runs foreign cleanup.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: kv.Name}, kv))
	kv.Annotations[constants.KVCacheLabelKeyBackend] = constants.KVCacheBackendVineyard
	require.NoError(t, c.Update(ctx, kv))
	deletes = 0
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deletes, 4, "vineyard foreign cleanup probes the distributed resource names")
}

func TestReconcile_BackendSwitchCleansUpOldResources(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("backend-switch", constants.KVCacheBackendHPKV)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "backend-switch"}

	// First reconcile with hpkv: StatefulSet + headless Service exist.
	_, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, &appsv1.StatefulSet{}))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "backend-switch-headless-service"}, &corev1.Service{}))

	// Switch the annotation to vineyard and update the object.
	require.NoError(t, c.Get(ctx, key, kv))
	kv.Annotations[constants.KVCacheLabelKeyBackend] = constants.KVCacheBackendVineyard
	require.NoError(t, c.Update(ctx, kv))

	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)

	// Vineyard resources exist.
	require.NoError(t, c.Get(ctx, key, &appsv1.Deployment{}))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "backend-switch-rpc"}, &corev1.Service{}))
	// Distributed resources are gone.
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &appsv1.StatefulSet{})))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "backend-switch-headless-service"}, &corev1.Service{})))
}

// -- Repeated reconcile idempotency at the controller level --

func TestReconcile_RepeatedReconcileDoesNotWriteSteadyStatus(t *testing.T) {
	ctx := context.Background()
	s := reconcileTestScheme(t)
	kv := newReconcileTestKVCache("steady", constants.KVCacheBackendHPKV)
	creates, updates, deletes, statusUpdates := 0, 0, 0, 0
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(kv).
		WithStatusSubresource(&orchestrationv1alpha1.KVCache{}, &appsv1.StatefulSet{}, &appsv1.Deployment{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cc client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				creates++
				return cc.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cc client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return cc.Update(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, cc client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				return cc.Delete(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cc client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				statusUpdates++
				return cc.Status().Update(ctx, obj, opts...)
			},
		}).Build()
	r := newKVCacheTestReconciler(c, s)
	key := types.NamespacedName{Namespace: "default", Name: "steady"}

	_, err := r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Positive(t, creates)
	assert.Equal(t, 1, statusUpdates, "the initial reconcile writes status once")

	// Drive the workload to a fully ready state, so the next observe settles.
	setSTSStatus(t, ctx, c, kv.Name, appsv1.StatefulSetStatus{
		Replicas:        3,
		ReadyReplicas:   3,
		UpdatedReplicas: 3,
		CurrentRevision: "rev-1",
		UpdateRevision:  "rev-1",
	})
	creates, updates, deletes, statusUpdates = 0, 0, 0, 0

	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "Ready flip produces exactly one status write")
	creates, updates, deletes, statusUpdates = 0, 0, 0, 0

	// Nothing changed: no writes of any kind. Foreign cleanup still probes the
	// well-known names (NotFound deletes), but those carry no state change.
	_, err = r.Reconcile(ctx, reconcileRequest(kv))
	require.NoError(t, err)
	assert.Zero(t, creates, "no creates once converged")
	assert.Zero(t, updates, "no spec updates once converged")
	assert.Zero(t, statusUpdates, "no status writes when the observed state is unchanged")

	// The converged status is intact.
	updated := &orchestrationv1alpha1.KVCache{}
	require.NoError(t, c.Get(ctx, key, updated))
	assert.Equal(t, int32(3), updated.Status.ReadyReplicas)
	ready := findCondition(updated.Status.Conditions, orchestrationv1alpha1.KVCacheConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
}
