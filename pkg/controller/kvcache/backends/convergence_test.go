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

package backends

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
)

func backendsTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	require.NoError(t, v1alpha1.AddToScheme(s))
	return s
}

func newDistributedKVCache(name string) *v1alpha1.KVCache {
	return &v1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
			Annotations: map[string]string{
				constants.KVCacheLabelKeyBackend: constants.KVCacheBackendHPKV,
			},
		},
		Spec: v1alpha1.KVCacheSpec{
			Cache: v1alpha1.RuntimeSpec{
				Replicas:        1,
				Image:           "registry.example/kvcache:v1",
				ImagePullPolicy: "IfNotPresent",
				Env:             []corev1.EnvVar{{Name: "CACHE_ENV", Value: "v1"}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				},
			},
			Service: v1alpha1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Name: "service", Port: 18512, Protocol: corev1.ProtocolTCP}},
			},
		},
	}
}

// -- StatefulSet template drift convergence --

func TestReconcileStatefulSet_TemplateDriftConverged(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("drift-sts")
	desired := HpKVBackend{}.BuildCacheStatefulSet(kv)
	require.NoError(t, r.ReconcileStatefulsetObject(ctx, desired))

	// Simulate changes to spec.cache: image pull policy, env, resources and
	// replicas must all be converged onto the existing StatefulSet.
	kv.Spec.Cache.Image = "registry.example/kvcache:v2"
	kv.Spec.Cache.ImagePullPolicy = "Always"
	kv.Spec.Cache.Env = append(kv.Spec.Cache.Env, corev1.EnvVar{Name: "CACHE_ENV_NEW", Value: "v2"})
	kv.Spec.Cache.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
	}
	kv.Spec.Cache.Replicas = 3

	require.NoError(t, r.ReconcileStatefulsetObject(ctx, HpKVBackend{}.BuildCacheStatefulSet(kv)))

	found := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "drift-sts"}, found))
	require.Len(t, found.Spec.Template.Spec.Containers, 1)
	container := found.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "registry.example/kvcache:v2", container.Image)
	assert.Equal(t, corev1.PullAlways, container.ImagePullPolicy)
	assert.Contains(t, container.Env, corev1.EnvVar{Name: "CACHE_ENV", Value: "v1"})
	assert.Contains(t, container.Env, corev1.EnvVar{Name: "CACHE_ENV_NEW", Value: "v2"})
	assert.Equal(t, "2", container.Resources.Requests.Cpu().String())
	require.NotNil(t, found.Spec.Replicas)
	assert.Equal(t, int32(3), *found.Spec.Replicas)
}

func TestReconcileStatefulSet_TemplateOverrideProbeAndVolumeConverged(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("template-sts")
	template := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "kvcache-server",
				Image: "registry.example/kvcache:v1",
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{Path: "/ready", Port: intstr.FromInt32(9100)},
					},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
	kv.Spec.Cache.Template = template
	require.NoError(t, r.ReconcileStatefulsetObject(ctx, HpKVBackend{}.BuildCacheStatefulSet(kv)))

	// Change probe thresholds, add another volume mount/volume through the
	// user-provided template and expect the drift to converge.
	template.Spec.Containers[0].ReadinessProbe.FailureThreshold = 9
	template.Spec.Containers[0].ReadinessProbe.PeriodSeconds = 30
	template.Spec.Containers[0].VolumeMounts = append(template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: "extra", MountPath: "/extra"})
	template.Spec.Volumes = append(template.Spec.Volumes, corev1.Volume{Name: "extra",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}})

	require.NoError(t, r.ReconcileStatefulsetObject(ctx, HpKVBackend{}.BuildCacheStatefulSet(kv)))

	found := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "template-sts"}, found))
	probe := found.Spec.Template.Spec.Containers[0].ReadinessProbe
	require.NotNil(t, probe)
	assert.Equal(t, int32(9), probe.FailureThreshold)
	assert.Equal(t, int32(30), probe.PeriodSeconds)
	assert.Len(t, found.Spec.Template.Spec.Containers[0].VolumeMounts, 2)
	assert.Len(t, found.Spec.Template.Spec.Volumes, 2)

	// Repeated reconciliation with no further drift must not produce an update.
	updates := 0
	c2 := fake.NewClientBuilder().WithScheme(s).WithObjects(found).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
				updates++
				return nil
			},
		}).Build()
	r2 := NewDistributedReconciler(c2, constants.KVCacheBackendHPKV)
	require.NoError(t, r2.ReconcileStatefulsetObject(ctx, HpKVBackend{}.BuildCacheStatefulSet(kv)))
	assert.Zero(t, updates, "no update should be issued when the template already matches")
}

// -- Deployment template drift convergence --

func TestReconcileDeployment_TemplateDriftConverged(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := NewVineyardReconciler(c)

	kv := &v1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{Name: "vineyard-drift", Namespace: "default", UID: "vineyard-drift-uid"},
		Spec: v1alpha1.KVCacheSpec{
			Cache: v1alpha1.RuntimeSpec{
				Replicas:        1,
				Image:           "registry.example/vineyard:v1",
				ImagePullPolicy: "IfNotPresent",
				Env:             []corev1.EnvVar{{Name: "EXTRA_ENV", Value: "v1"}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
				},
			},
		},
	}
	require.NoError(t, r.ReconcileDeploymentObject(ctx, buildVineyardDeployment(kv)))

	kv.Spec.Cache.Image = "registry.example/vineyard:v2"
	kv.Spec.Cache.ImagePullPolicy = "Always"
	kv.Spec.Cache.Env = []corev1.EnvVar{{Name: "EXTRA_ENV", Value: "v2"}}
	kv.Spec.Cache.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	kv.Spec.Cache.Replicas = 2
	require.NoError(t, r.ReconcileDeploymentObject(ctx, buildVineyardDeployment(kv)))

	found := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "vineyard-drift"}, found))
	container := found.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "registry.example/vineyard:v2", container.Image)
	assert.Equal(t, corev1.PullAlways, container.ImagePullPolicy)
	assert.Contains(t, container.Env, corev1.EnvVar{Name: "EXTRA_ENV", Value: "v2"})
	assert.NotContains(t, container.Env, corev1.EnvVar{Name: "EXTRA_ENV", Value: "v1"})
	assert.Equal(t, "2Gi", container.Resources.Requests.Memory().String())
	require.NotNil(t, found.Spec.Replicas)
	assert.Equal(t, int32(2), *found.Spec.Replicas)
}

// -- Bare Pod convergence: image is mutable, everything else requires recreate --

func TestReconcilePodObject_ImageUpdatedInPlace(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	creates, updates, deletes := 0, 0, 0
	c := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			creates++
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updates++
			return c.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes++
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("pod-image")
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{
		Image:           "registry.example/watcher:v1",
		ImagePullPolicy: "IfNotPresent",
	}
	require.NoError(t, r.ReconcilePodObject(ctx, HpKVBackend{}.BuildWatcherPod(kv)))
	creates, updates, deletes = 0, 0, 0

	kv.Spec.Watcher.Image = "registry.example/watcher:v2"
	require.NoError(t, r.ReconcilePodObject(ctx, HpKVBackend{}.BuildWatcherPod(kv)))
	assert.Equal(t, 1, updates, "image is mutable and must be updated in place")
	assert.Zero(t, creates, "image-only drift must not recreate the Pod")
	assert.Zero(t, deletes, "image-only drift must not delete the Pod")

	updated := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: watcherPodName("pod-image")}, updated))
	assert.Equal(t, "registry.example/watcher:v2", updated.Spec.Containers[0].Image)
}

func TestReconcilePodObject_ImmutableDriftRecreatesPod(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	creates, deletes := 0, 0
	c := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			creates++
			return c.Create(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes++
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("pod-recreate")
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{
		Image:           "registry.example/watcher:v1",
		ImagePullPolicy: "IfNotPresent",
		Env:             []corev1.EnvVar{{Name: "WATCH_ENV", Value: "v1"}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	}
	require.NoError(t, r.ReconcilePodObject(ctx, HpKVBackend{}.BuildWatcherPod(kv)))
	creates, deletes = 0, 0

	// Env and resources are immutable Pod fields: the Pod must be recreated.
	kv.Spec.Watcher.Env = []corev1.EnvVar{{Name: "WATCH_ENV", Value: "v2"}}
	kv.Spec.Watcher.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
	}
	require.NoError(t, r.ReconcilePodObject(ctx, HpKVBackend{}.BuildWatcherPod(kv)))
	assert.Equal(t, 1, deletes, "immutable drift deletes the existing Pod")
	assert.Equal(t, 1, creates, "immutable drift recreates the Pod")

	recreated := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: watcherPodName("pod-recreate")}, recreated))
	assert.Contains(t, recreated.Spec.Containers[0].Env, corev1.EnvVar{Name: "WATCH_ENV", Value: "v2"})
	assert.Equal(t, "200m", recreated.Spec.Containers[0].Resources.Requests.Cpu().String())
}

// -- Watcher removal cleanup --

func TestDistributedReconciler_WatcherRemovedCleansUpResources(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("watcher-removal")
	kv.Spec.Metadata = &v1alpha1.MetadataSpec{
		Redis: &v1alpha1.MetadataConfig{
			Runtime: &v1alpha1.RuntimeSpec{Replicas: 1, Image: "redis:7"},
		},
	}
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{Image: "registry.example/watcher:v1"}

	_, err := r.Reconcile(ctx, kv)
	require.NoError(t, err)

	key := types.NamespacedName{Namespace: "default", Name: "watcher-removal"}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: watcherPodName("watcher-removal")}, &corev1.Pod{}))
	require.NoError(t, c.Get(ctx, key, &corev1.ServiceAccount{}))
	require.NoError(t, c.Get(ctx, key, &rbacv1.Role{}))
	require.NoError(t, c.Get(ctx, key, &rbacv1.RoleBinding{}))
	// Cache resources are present too.
	require.NoError(t, c.Get(ctx, key, &appsv1.StatefulSet{}))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: headlessServiceName("watcher-removal")}, &corev1.Service{}))

	// spec.watcher disappears: watcher resources must be deleted, cache kept.
	kv.Spec.Watcher = nil
	_, err = r.Reconcile(ctx, kv)
	require.NoError(t, err)

	assert.True(t, apierrors.IsNotFound(c.Get(ctx,
		types.NamespacedName{Namespace: "default", Name: watcherPodName("watcher-removal")}, &corev1.Pod{})),
		"watcher Pod should be deleted")
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &corev1.ServiceAccount{})), "watcher ServiceAccount should be deleted")
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &rbacv1.Role{})), "watcher Role should be deleted")
	assert.True(t, apierrors.IsNotFound(c.Get(ctx, key, &rbacv1.RoleBinding{})), "watcher RoleBinding should be deleted")
	assert.NoError(t, c.Get(ctx, key, &appsv1.StatefulSet{}), "cache StatefulSet should remain")
	assert.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: headlessServiceName("watcher-removal")}, &corev1.Service{}),
		"cache Service should remain")

	// Repeated reconcile must stay idempotent (no error, nothing recreated).
	_, err = r.Reconcile(ctx, kv)
	require.NoError(t, err)
	assert.True(t, apierrors.IsNotFound(c.Get(ctx,
		types.NamespacedName{Namespace: "default", Name: watcherPodName("watcher-removal")}, &corev1.Pod{})))
}

// -- Backend switch cleanup --

func TestVineyardCleanup_RemovesDistributedResources(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	kvName := "switch-to-vineyard"
	kv := newDistributedKVCache(kvName)
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{Image: "registry.example/watcher:v1"}
	kv.Spec.Metadata = &v1alpha1.MetadataSpec{
		Redis: &v1alpha1.MetadataConfig{
			Runtime: &v1alpha1.RuntimeSpec{Replicas: 1, Image: "redis:7"},
		},
	}

	hpkv := HpKVBackend{}
	initial := []client.Object{
		hpkv.BuildCacheStatefulSet(kv),
		hpkv.BuildService(kv),
		hpkv.BuildMetadataPod(kv),
		hpkv.BuildMetadataService(kv),
		hpkv.BuildWatcherPod(kv),
		hpkv.BuildWatcherPodServiceAccount(kv),
		hpkv.BuildWatcherPodRole(kv),
		hpkv.BuildWatcherPodRoleBinding(kv),
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(initial...).Build()
	r := NewVineyardReconciler(c)

	require.NoError(t, r.CleanupForeignResources(ctx, kv))

	assertNotFound(t, ctx, c, &appsv1.StatefulSet{}, kvName)
	assertNotFound(t, ctx, c, &corev1.Service{}, headlessServiceName(kvName))
	assertNotFound(t, ctx, c, &corev1.Pod{}, redisName(kvName))
	assertNotFound(t, ctx, c, &corev1.Service{}, redisName(kvName))
	assertNotFound(t, ctx, c, &corev1.Pod{}, watcherPodName(kvName))
	assertNotFound(t, ctx, c, &corev1.ServiceAccount{}, kvName)
	assertNotFound(t, ctx, c, &rbacv1.Role{}, kvName)
	assertNotFound(t, ctx, c, &rbacv1.RoleBinding{}, kvName)

	// Cleanup is idempotent when the resources are already gone.
	require.NoError(t, r.CleanupForeignResources(ctx, kv))
}

func TestDistributedCleanup_RemovesVineyardResources(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	kvName := "switch-to-hpkv"
	kv := &v1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{Name: kvName, Namespace: "default", UID: types.UID(kvName + "-uid")},
		Spec: v1alpha1.KVCacheSpec{
			Metadata: &v1alpha1.MetadataSpec{
				Etcd: &v1alpha1.MetadataConfig{
					Runtime: &v1alpha1.RuntimeSpec{Replicas: 2, Image: "etcd:3.5"},
				},
			},
			Cache: v1alpha1.RuntimeSpec{Replicas: 1, Image: "registry.example/vineyard:v1"},
		},
	}

	// An in-cluster Redis Pod/Service carries the same metadata role label but is
	// not an etcd resource: it must survive the etcd prefix filter.
	redisMeta := metav1.ObjectMeta{
		Name:      redisName(kvName),
		Namespace: "default",
		Labels: map[string]string{
			constants.KVCacheLabelKeyIdentifier: kvName,
			constants.KVCacheLabelKeyRole:       constants.KVCacheLabelValueRoleMetadata,
		},
	}
	redisPod := &corev1.Pod{
		ObjectMeta: redisMeta,
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}}},
	}
	redisSvc := &corev1.Service{
		ObjectMeta: redisMeta,
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 6379, Protocol: corev1.ProtocolTCP}}},
	}
	initial := []client.Object{
		buildVineyardDeployment(kv),
		buildVineyardRpcService(kv),
		buildEtcdPod(kv, kvName+"-etcd-0"),
		buildEtcdPod(kv, kvName+"-etcd-1"),
		buildEtcdPodService(kvName+"-etcd-0", kv),
		buildEtcdPodService(kvName+"-etcd-1", kv),
		buildEtcdAggregateService(kv),
		redisPod,
		redisSvc,
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(initial...).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	require.NoError(t, r.CleanupForeignResources(ctx, kv))

	assertNotFound(t, ctx, c, &appsv1.Deployment{}, kvName)
	assertNotFound(t, ctx, c, &corev1.Service{}, rpcServiceName(kvName))
	assertNotFound(t, ctx, c, &corev1.Service{}, etcdAggregateServiceName(kvName))
	assertNotFound(t, ctx, c, &corev1.Pod{}, kvName+"-etcd-0")
	assertNotFound(t, ctx, c, &corev1.Pod{}, kvName+"-etcd-1")
	assertNotFound(t, ctx, c, &corev1.Service{}, kvName+"-etcd-0")
	assertNotFound(t, ctx, c, &corev1.Service{}, kvName+"-etcd-1")
	// Redis resources are not vineyard resources and stay untouched.
	assert.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: redisName(kvName)}, &corev1.Pod{}))
	assert.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: redisName(kvName)}, &corev1.Service{}))

	require.NoError(t, r.CleanupForeignResources(ctx, kv))
}

// hpkv and infinistore share resource names; switching between them must not
// delete the StatefulSet but converge the backend-specific template instead.
func TestDistributedBackendSwitch_SharedResourcesConverged(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()

	kv := newDistributedKVCache("hpkv-to-infini")
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{Image: "registry.example/watcher:v1"}

	hpkv := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)
	_, err := hpkv.Reconcile(ctx, kv)
	require.NoError(t, err)

	stsBefore := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: kv.Name}, stsBefore))

	// Switch annotation to infinistore.
	kv.Annotations[constants.KVCacheLabelKeyBackend] = constants.KVCacheBackendInfinistore
	infinistore := NewDistributedReconciler(c, constants.KVCacheBackendInfinistore)
	_, err = infinistore.Reconcile(ctx, kv)
	require.NoError(t, err)

	stsAfter := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: kv.Name}, stsAfter))
	assert.Equal(t, stsBefore.UID, stsAfter.UID, "the StatefulSet must be updated, not recreated")
	envMap := map[string]string{}
	for _, env := range stsAfter.Spec.Template.Spec.Containers[0].Env {
		envMap[env.Name] = env.Value
	}
	assert.Equal(t, constants.KVCacheBackendInfinistore, envMap["AIBRIX_KVCACHE_BACKEND"])
	assert.Equal(t, "infinistore", stsAfter.Spec.Template.Spec.Containers[0].Command[0])
	assert.Equal(t, int32(12345), stsAfter.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)

	// The watcher Pod is a bare Pod: its backend arg is immutable, so it must be
	// recreated with the infinistore argument.
	watcher := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: watcherPodName(kv.Name)}, watcher))
	assert.Contains(t, watcher.Spec.Containers[0].Args, constants.KVCacheBackendInfinistore)
	assert.NotContains(t, watcher.Spec.Containers[0].Args, constants.KVCacheBackendHPKV)
}

// -- External metadata path: no Redis Pod/Service are created --

func TestDistributedReconciler_ExternalConnectionCreatesNoRedis(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "valkey-creds", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("secret")},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)

	kv := newDistributedKVCache("external-meta")
	kv.Spec.Metadata = &v1alpha1.MetadataSpec{
		Redis: &v1alpha1.MetadataConfig{
			ExternalConnection: &v1alpha1.ExternalConnectionConfig{
				Address:           "valkey.example.com:6379",
				PasswordSecretRef: "valkey-creds",
			},
		},
	}
	_, err := r.Reconcile(ctx, kv)
	require.NoError(t, err)

	assert.True(t, apierrors.IsNotFound(c.Get(ctx,
		types.NamespacedName{Namespace: "default", Name: redisName("external-meta")}, &corev1.Pod{})))
	assert.True(t, apierrors.IsNotFound(c.Get(ctx,
		types.NamespacedName{Namespace: "default", Name: redisName("external-meta")}, &corev1.Service{})))
	// Current backend resources are still reconciled.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "external-meta"}, &appsv1.StatefulSet{}))
}

// -- Repeated reconcile idempotency --

func TestDistributedReconciler_RepeatedReconcileIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := backendsTestScheme(t)

	creates, updates, deletes := 0, 0, 0
	c := fake.NewClientBuilder().WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				creates++
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return c.Update(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()

	r := NewDistributedReconciler(c, constants.KVCacheBackendHPKV)
	kv := newDistributedKVCache("idempotent")
	kv.Spec.Metadata = &v1alpha1.MetadataSpec{
		Redis: &v1alpha1.MetadataConfig{
			Runtime: &v1alpha1.RuntimeSpec{Replicas: 1, Image: "redis:7"},
		},
	}
	kv.Spec.Watcher = &v1alpha1.RuntimeSpec{Image: "registry.example/watcher:v1"}

	_, err := r.Reconcile(ctx, kv)
	require.NoError(t, err)
	assert.Positive(t, creates, "first reconcile creates resources")
	assert.Zero(t, updates)
	assert.Zero(t, deletes)
	firstCreates := creates

	// A second reconcile must converge without any create, update or delete.
	_, err = r.Reconcile(ctx, kv)
	require.NoError(t, err)
	assert.Equal(t, firstCreates, creates, "second reconcile must not create anything")
	assert.Zero(t, updates, "second reconcile must not update anything")
	assert.Zero(t, deletes, "second reconcile must not delete anything")

	// All resources created by the first reconcile are still in place.
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "idempotent"}, sts))
	watcher := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: watcherPodName("idempotent")}, watcher))
}

func assertNotFound(t *testing.T, ctx context.Context, c client.Client, obj client.Object, name string) {
	t.Helper()
	err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, obj)
	assert.True(t, apierrors.IsNotFound(err), "expected %T %s to be deleted, got err=%v", obj, name, err)
}
