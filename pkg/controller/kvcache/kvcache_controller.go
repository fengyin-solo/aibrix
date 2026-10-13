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

package kvcache

import (
	"context"
	"fmt"
	"sync"

	"github.com/vllm-project/aibrix/pkg/constants"
	"k8s.io/apimachinery/pkg/types"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/config"
	"github.com/vllm-project/aibrix/pkg/controller/kvcache/backends"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	controllerKind = orchestrationv1alpha1.GroupVersion.WithKind("KVCache")
	controllerName = "kv-cache-controller"
)

// Condition reasons reported on the KVCache object.
const (
	reasonAllReplicasReady    = "AllReplicasReady"
	reasonReplicasNotReady    = "ReplicasNotReady"
	reasonReplicasProgressing = "ReplicasProgressing"
	reasonReconcileComplete   = "ReconcileComplete"
	reasonWorkloadNotFound    = "WorkloadNotFound"
	reasonUnsupportedBackend  = "UnsupportedBackend"
)

// Add creates a new ModelAdapter Controller and adds it to the Manager with default RBAC.
// The Manager will set fields on the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, runtimeConfig config.RuntimeConfig) error {
	r, err := newReconciler(mgr, runtimeConfig)
	if err != nil {
		return err
	}
	return add(mgr, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, runtimeConfig config.RuntimeConfig) (reconcile.Reconciler, error) {
	reconciler := &KVCacheReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor(controllerName),
		RuntimeConfig:   runtimeConfig,
		cleanedBackends: map[types.NamespacedName]string{},
		Backends: map[string]backends.BackendReconciler{
			constants.KVCacheBackendVineyard:    backends.NewVineyardReconciler(mgr.GetClient()),
			constants.KVCacheBackendHPKV:        backends.NewDistributedReconciler(mgr.GetClient(), constants.KVCacheBackendHPKV),
			constants.KVCacheBackendInfinistore: backends.NewDistributedReconciler(mgr.GetClient(), constants.KVCacheBackendInfinistore),
		},
	}
	return reconciler, nil
}

func podWithLabelFilter(labelKey string) predicate.Predicate {
	hasLabelAndModelIdentifier := func(labels map[string]string, labelKey string) bool {
		_, exists := labels[labelKey]
		return exists
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return hasLabelAndModelIdentifier(e.Object.GetLabels(), labelKey)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return hasLabelAndModelIdentifier(e.ObjectNew.GetLabels(), labelKey)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return hasLabelAndModelIdentifier(e.Object.GetLabels(), labelKey)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return hasLabelAndModelIdentifier(e.Object.GetLabels(), labelKey)
		},
	}
}

// kvCacheRequestsForPod maps a Pod carrying the KVCache identifier label back to
// the KVCache it belongs to. Pods without the label enqueue nothing.
func kvCacheRequestsForPod(_ context.Context, obj client.Object) []reconcile.Request {
	cacheName, ok := obj.GetLabels()[constants.KVCacheLabelKeyIdentifier]
	if !ok {
		return nil
	}

	return []ctrl.Request{
		{
			NamespacedName: types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      cacheName,
			},
		},
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// use the builder fashion. If we need more fine grain control later, we can switch to `controller.New()`
	err := ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		For(&orchestrationv1alpha1.KVCache{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(kvCacheRequestsForPod),
			builder.WithPredicates(podWithLabelFilter(constants.KVCacheLabelKeyIdentifier))).
		Complete(r)
	if err != nil {
		return err
	}

	klog.InfoS("Finished to add kv-cache-controller")
	return nil
}

// KVCacheReconciler reconciles a KVCache object
type KVCacheReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	RuntimeConfig config.RuntimeConfig
	Backends      map[string]backends.BackendReconciler

	// cleanedBackends records, per KVCache, the backend whose foreign resources
	// have already been cleaned up. Labeled Pod events trigger frequent
	// reconciles, so cleanup only runs again when the backend annotation value
	// changes. A failed cleanup is never recorded and is retried.
	cleanupMu       sync.Mutex
	cleanedBackends map[types.NamespacedName]string
}

// foreignCleanupPending reports whether cleanup for currentBackend still needs
// to run.
func (r *KVCacheReconciler) foreignCleanupPending(key types.NamespacedName, currentBackend string) bool {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	return r.cleanedBackends[key] != currentBackend
}

// markForeignCleanupDone records a successful cleanup for currentBackend.
func (r *KVCacheReconciler) markForeignCleanupDone(key types.NamespacedName, currentBackend string) {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	if r.cleanedBackends == nil {
		r.cleanedBackends = map[types.NamespacedName]string{}
	}
	r.cleanedBackends[key] = currentBackend
}

// +kubebuilder:rbac:groups=orchestration.aibrix.ai,resources=kvcaches,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=orchestration.aibrix.ai,resources=kvcaches/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=orchestration.aibrix.ai,resources=kvcaches/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=get;list;watch;create;delete;update
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list

// Reconcile reconciles a KVCache to desired state.
func (r *KVCacheReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the KVCache instance
	kvCache := &orchestrationv1alpha1.KVCache{}
	err := r.Get(ctx, req.NamespacedName, kvCache)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return. Created objects are automatically garbage collected.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	backend := getKVCacheBackendFromAnnotations(kvCache)
	backendHandler, ok := r.Backends[backend]
	if !ok {
		// An unsupported backend is a configuration error: surface it on the
		// object instead of returning an error and hot-looping retries.
		return r.reconcileUnsupportedBackend(ctx, kvCache, backend)
	}

	// Remove resources created by a previously selected backend first. A
	// cleanup failure must not block convergence of the current backend; it is
	// retried via a requeue after the normal reconcile path below. Successful
	// cleanup for a given backend is cached so Pod-event-driven reconciles do not
	// reissue deletes on every invocation.
	var cleanupErr error
	if r.foreignCleanupPending(req.NamespacedName, backend) {
		cleanupErr = backendHandler.CleanupForeignResources(ctx, kvCache)
		if cleanupErr != nil {
			klog.ErrorS(cleanupErr, "Failed to clean up resources from a previous backend",
				"KVCache", req.NamespacedName, "backend", backend)
		} else {
			r.markForeignCleanupDone(req.NamespacedName, backend)
		}
	}

	result, err := backendHandler.Reconcile(ctx, kvCache)
	if err != nil {
		return result, err
	}

	if err := r.syncKVCacheStatus(ctx, kvCache); err != nil {
		return ctrl.Result{}, err
	}

	if cleanupErr != nil {
		// Requeue through the rate-limited queue so the foreign-resource cleanup
		// is retried without a tight error loop.
		return ctrl.Result{Requeue: true}, nil
	}
	return result, nil
}

// reconcileUnsupportedBackend writes the unsupported-backend reason into the
// Ready/Progressing conditions and returns no error, so the controller does not
// hot-retry an object the user must fix.
func (r *KVCacheReconciler) reconcileUnsupportedBackend(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache, backend string) (ctrl.Result, error) {
	message := fmt.Sprintf("Unsupported backend %q: supported backends are %q, %q and %q",
		backend, constants.KVCacheBackendVineyard, constants.KVCacheBackendHPKV, constants.KVCacheBackendInfinistore)
	conditions := []metav1.Condition{
		{
			Type:               orchestrationv1alpha1.KVCacheConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: kvCache.Generation,
			Reason:             reasonUnsupportedBackend,
			Message:            message,
		},
		{
			Type:               orchestrationv1alpha1.KVCacheConditionProgressing,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: kvCache.Generation,
			Reason:             reasonUnsupportedBackend,
			Message:            "No progress while the backend is unsupported",
		},
	}

	if err := r.updateConditions(ctx, kvCache, conditions, false); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// cacheWorkloadObservation summarizes the cache workload (StatefulSet for the
// distributed backends, Deployment for vineyard) used to derive status.
type cacheWorkloadObservation struct {
	desiredReplicas int32
	readyReplicas   int32
	updatedReplicas int32
	// exists reports whether the workload object has been created yet.
	exists bool
	// rolloutComplete reports whether the workload is not in the middle of a
	// rolling update.
	rolloutComplete bool
}

// observeCacheWorkload reads the live cache workload. Both the distributed
// backends and vineyard name the workload exactly like the KVCache, so the kind
// alone distinguishes them.
func (r *KVCacheReconciler) observeCacheWorkload(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) (cacheWorkloadObservation, error) {
	desired := kvCache.Spec.Cache.Replicas
	key := types.NamespacedName{Namespace: kvCache.Namespace, Name: kvCache.Name}

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, key, sts); err == nil {
		if sts.Spec.Replicas != nil {
			desired = *sts.Spec.Replicas
		}
		// Empty revisions mean the StatefulSet controller has not reported yet;
		// treat that as "no rollout in progress" rather than an endless one.
		rolloutComplete := sts.Status.CurrentRevision == sts.Status.UpdateRevision
		return cacheWorkloadObservation{
			desiredReplicas: desired,
			readyReplicas:   sts.Status.ReadyReplicas,
			updatedReplicas: sts.Status.UpdatedReplicas,
			exists:          true,
			rolloutComplete: rolloutComplete,
		}, nil
	} else if !errors.IsNotFound(err) {
		return cacheWorkloadObservation{}, err
	}

	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, key, deployment); err == nil {
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		return cacheWorkloadObservation{
			desiredReplicas: desired,
			readyReplicas:   deployment.Status.ReadyReplicas,
			updatedReplicas: deployment.Status.UpdatedReplicas,
			exists:          true,
			rolloutComplete: true,
		}, nil
	} else if !errors.IsNotFound(err) {
		return cacheWorkloadObservation{}, err
	}

	return cacheWorkloadObservation{
		desiredReplicas: desired,
		rolloutComplete: false,
	}, nil
}

// syncKVCacheStatus derives readyReplicas and the Ready/Progressing conditions
// from the live cache workload and persists them only when something changed.
func (r *KVCacheReconciler) syncKVCacheStatus(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) error {
	observation, err := r.observeCacheWorkload(ctx, kvCache)
	if err != nil {
		return err
	}

	readyCondition := buildReadyCondition(kvCache.Generation, observation)
	progressingCondition := buildProgressingCondition(kvCache.Generation, observation)

	replicasChanged := kvCache.Status.ReadyReplicas != observation.readyReplicas
	kvCache.Status.ReadyReplicas = observation.readyReplicas

	return r.updateConditions(ctx, kvCache,
		[]metav1.Condition{readyCondition, progressingCondition}, replicasChanged)
}

// updateConditions writes the status only when readyReplicas or one of the
// conditions changed, so a steady-state reconcile performs no writes.
func (r *KVCacheReconciler) updateConditions(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache, conditions []metav1.Condition, replicasChanged bool) error {
	changed := replicasChanged
	for _, condition := range conditions {
		if meta.SetStatusCondition(&kvCache.Status.Conditions, condition) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	klog.InfoS("Updating KVCache status", "KVCache",
		types.NamespacedName{Namespace: kvCache.Namespace, Name: kvCache.Name},
		"readyReplicas", kvCache.Status.ReadyReplicas)
	return r.Status().Update(ctx, kvCache)
}

func buildReadyCondition(generation int64, obs cacheWorkloadObservation) metav1.Condition {
	if !obs.exists {
		return metav1.Condition{
			Type:               orchestrationv1alpha1.KVCacheConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonWorkloadNotFound,
			Message: fmt.Sprintf("Cache workload has not been created yet: 0 of %d replicas ready, %d more required",
				obs.desiredReplicas, obs.desiredReplicas),
		}
	}

	missing := obs.desiredReplicas - obs.readyReplicas
	if missing <= 0 && obs.readyReplicas >= obs.desiredReplicas {
		return metav1.Condition{
			Type:               orchestrationv1alpha1.KVCacheConditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             reasonAllReplicasReady,
			Message:            fmt.Sprintf("All %d replicas are ready", obs.desiredReplicas),
		}
	}

	if missing < 0 {
		missing = 0
	}
	return metav1.Condition{
		Type:               orchestrationv1alpha1.KVCacheConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             reasonReplicasNotReady,
		Message: fmt.Sprintf("%d of %d replicas ready, %d more required",
			obs.readyReplicas, obs.desiredReplicas, missing),
	}
}

func buildProgressingCondition(generation int64, obs cacheWorkloadObservation) metav1.Condition {
	progressing := !obs.exists || !obs.rolloutComplete ||
		obs.readyReplicas != obs.desiredReplicas ||
		obs.updatedReplicas != obs.desiredReplicas
	if !progressing {
		return metav1.Condition{
			Type:               orchestrationv1alpha1.KVCacheConditionProgressing,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonReconcileComplete,
			Message:            "All replicas are ready and up to date",
		}
	}

	reason := reasonReplicasProgressing
	message := fmt.Sprintf("%d of %d replicas ready, %d updated",
		obs.readyReplicas, obs.desiredReplicas, obs.updatedReplicas)
	if !obs.exists {
		reason = reasonWorkloadNotFound
		message = fmt.Sprintf("Cache workload has not been created yet; 0 of %d replicas ready", obs.desiredReplicas)
	} else if !obs.rolloutComplete {
		message = fmt.Sprintf("Rolling update in progress: %d of %d replicas updated",
			obs.updatedReplicas, obs.desiredReplicas)
	}
	return metav1.Condition{
		Type:               orchestrationv1alpha1.KVCacheConditionProgressing,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	}
}

// getKVCacheBackendFromAnnotations returns the backend based on labels and annotations.
func getKVCacheBackendFromAnnotations(kv *orchestrationv1alpha1.KVCache) string {
	backend := kv.Annotations[constants.KVCacheLabelKeyBackend]
	if backend == "" {
		// In some case where webhooks are disabled (e.g., via --disable-webhook),
		// mutating webhooks cannot inject default values into the spec. Using annotations
		// ensures that users can still explicitly specify the backend, even when no webhook is active.
		return constants.KVCacheBackendDefault
	}
	return backend
}
