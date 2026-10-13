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

// Package modelclaim implements the controller for the ModelClaim CRD: a model
// runtime claim that attaches to a selected GPU pod and is served as its own
// engine process through the aibrix-runtime sidecar. Multiple claims may share
// a GPU through kvcached.
package modelclaim

import (
	"context"
	"fmt"
	"time"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/config"
	"github.com/vllm-project/aibrix/pkg/constants"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	controllerName = "model-claim-controller"

	runtimePhaseActive   = "active"
	runtimePhaseFailed   = "failed"
	runtimePhaseSleeping = "sleeping"

	// ModelClaimFinalizer ensures attached engine processes are deactivated and
	// routing is deregistered before the ModelClaim object is removed.
	ModelClaimFinalizer = "model.aibrix.ai/modelclaim-finalizer"

	// ScheduledPodsAnnotationKey records the warm pods this model has been
	// bin-packed onto, mirroring the ModelAdapter scheduled-pods convention so a
	// binding survives controller restarts. N models may be recorded per pod.
	ScheduledPodsAnnotationKey = "model.aibrix.ai/scheduled-pods"

	// DefaultRequeueDuration paces periodic reconciliation (placement retries
	// and readiness checks).
	DefaultRequeueDuration = 10 * time.Second
)

// ModelClaimReconciler reconciles a ModelClaim object.
type ModelClaimReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Runtime drives the per-runtime sidecar (activate/deactivate). Injected so
	// the reconcile loop is testable with an in-process fake.
	Runtime RuntimeClient
	// Locality scores how cheap it is to load a model's weights on a node (the
	// Layer 2 node weight-cache signal). Phase 1 uses uniformLocality, so
	// placement is load-only until node state reporting is added back.
	Locality LocalityProvider
	// SnapshotCache stores bounded runtime observations for placement. It is
	// process-local so a leader restart naturally rehydrates from sidecars.
	SnapshotCache *runtimeSnapshotCache
	// PoolPolicy serializes controller-local policy ticks and retains only the
	// request-counter deltas needed for conservative KV allocation. It is not a
	// desired-state store; runtime snapshots remain authoritative after restart.
	PoolPolicy *poolPolicyManager
}

// Add creates a new ModelClaim controller and registers it with the Manager.
func Add(mgr manager.Manager, _ config.RuntimeConfig) error {
	r := &ModelClaimReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Recorder:   mgr.GetEventRecorderFor(controllerName),
		Runtime:    NewRuntimeClient(),
		Locality:   uniformLocality{},
		PoolPolicy: newPoolPolicyManager(time.Now),
		SnapshotCache: newRuntimeSnapshotCache(
			defaultRuntimeSnapshotTTL, time.Now,
		),
	}

	err := ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		For(&modelv1alpha1.ModelClaim{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		))).
		// React to warm GPU pods coming and going so models pending placement can
		// attach as soon as an eligible pod appears.
		Watches(&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(enqueueModelClaimsForPod(mgr.GetClient())),
			builder.WithPredicates(modelPoolPodFilter())).
		Complete(r)
	if err != nil {
		return err
	}

	klog.InfoS("Finished to add model-claim-controller")
	return nil
}

//+kubebuilder:rbac:groups=model.aibrix.ai,resources=modelclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=model.aibrix.ai,resources=modelclaims/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=model.aibrix.ai,resources=modelclaims/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=apps,resources=deployments;replicasets,verbs=get;list;watch

// Reconcile drives a ModelClaim towards its desired state: select a warm pod,
// activate a runtime engine, hold routing at port 0 until ready, and stop the
// engine on scale-down or deletion.
func (r *ModelClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pm := &modelv1alpha1.ModelClaim{}
	if err := r.Get(ctx, req.NamespacedName, pm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion: every attached instance must be stopped and confirmed
	// gone before the finalizer is released.
	if !pm.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, pm)
	}

	// Ensure the finalizer is present before doing any external-effecting work.
	if !controllerutil.ContainsFinalizer(pm, ModelClaimFinalizer) {
		controllerutil.AddFinalizer(pm, ModelClaimFinalizer)
		if err := r.Update(ctx, pm); err != nil {
			return requeueOnConflict(err)
		}
		// A finalizer-only update changes neither generation, labels, nor
		// annotations, so our watch predicate (GenerationChanged / LabelChanged /
		// AnnotationChanged) filters the resulting event out and would NOT
		// re-enqueue. Requeue explicitly so reconciliation proceeds to placement
		// and activation instead of stalling until an unrelated event arrives.
		return ctrl.Result{Requeue: true}, nil
	}

	if _, err := modelParallelism(pm); err != nil {
		message := fmt.Sprintf("invalid engineConfig parallelism: %v", err)
		r.Recorder.Event(pm, corev1.EventTypeWarning, "InvalidEngineConfig", message)
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "InvalidEngineConfig",
			Message: message,
		})
		pm.Status.Phase = modelv1alpha1.ModelClaimFailed
		setClaimGauges(pm)
		if uerr := r.Status().Update(ctx, pm); uerr != nil {
			return requeueOnConflict(uerr)
		}
		return ctrl.Result{}, nil
	}
	candidates, err := r.listCandidateWarmPods(ctx, pm)
	if err != nil {
		klog.ErrorS(err, "failed to list candidate warm pods", "modelClaim", req.NamespacedName)
		return ctrl.Result{}, err
	}

	pruneDeadInstances(pm, candidates)
	r.setStatusFields(pm, candidates)

	// Convene the desired instance set: activate replacements/extra replicas
	// on distinct warm pods, replay terminal failures, and stop surplus
	// instances under the healthy-instance guard (see scaling.go).
	if err := r.convergeInstances(ctx, pm, candidates); err != nil {
		r.Recorder.Event(pm, corev1.EventTypeWarning, "ActivateFailed", err.Error())
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "ActivateFailed",
			Message: err.Error(),
		})
		pm.Status.Phase = modelv1alpha1.ModelClaimFailed
		setClaimGauges(pm)
		if uerr := r.Status().Update(ctx, pm); uerr != nil {
			return requeueOnConflict(uerr)
		}
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
	}

	// Reconcile instance routability against live engine readiness (promote
	// ready Activating instances, demote Active instances that went unhealthy).
	r.reconcileInstanceHealth(ctx, pm)
	r.recomputeReadiness(pm)
	setClaimGauges(pm)
	if err := r.Status().Update(ctx, pm); err != nil {
		return requeueOnConflict(err)
	}
	// Pool policy is an optional, Deployment-scoped control loop. It runs after
	// claim status is persisted so a policy failure cannot block activation
	// or route-health convergence for this claim.
	r.reconcilePoolPolicies(ctx, candidates)
	return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
}

// requeueOnConflict lets the next reconcile work from the latest API object.
// Status updates can race with deletion/finalizer updates and must not surface
// as a controller error when Kubernetes reports the expected resource-version
// conflict.
func requeueOnConflict(err error) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}

// listCandidateWarmPods returns running pods that match the model's PodSelector
// and advertise themselves as warm GPU pool members accepting attachments.
func (r *ModelClaimReconciler) listCandidateWarmPods(ctx context.Context, pm *modelv1alpha1.ModelClaim) ([]corev1.Pod, error) {
	if pm.Spec.PodSelector == nil {
		return nil, fmt.Errorf("spec.podSelector must not be nil")
	}
	parallelism, err := modelParallelism(pm)
	if err != nil {
		return nil, fmt.Errorf("invalid engineConfig parallelism: %w", err)
	}
	selector, err := metav1.LabelSelectorAsSelector(pm.Spec.PodSelector)
	if err != nil {
		return nil, err
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(pm.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, err
	}

	candidates := make([]corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := podList.Items[i]
		if pod.Labels[constants.ModelPoolLabelEnabled] != constants.ModelPoolLabelEnabledValue {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if isVLLMModel(pm) && !podSupportsVLLMParallelism(pod, parallelism) {
			continue
		}
		candidates = append(candidates, pod)
	}
	return candidates, nil
}

// pruneDeadInstances drops status instances whose warm pod is no longer a live
// candidate (deleted or replaced). Without this, a recreated warm pod would
// never be re-activated: desired == len(stale instances), so the model silently
// stops being served after pod churn. The pod is gone, so there is no runtime
// to deactivate and no annotation left to clean; dropping the record is the
// reconcile-correct move; the normal activation path then re-places the model
// (and, with the node weight cache, prefers the node already holding weights).
func pruneDeadInstances(pm *modelv1alpha1.ModelClaim, candidates []corev1.Pod) {
	alive := make(map[string]bool, len(candidates))
	for i := range candidates {
		alive[candidates[i].Name] = true
	}
	kept := pm.Status.Instances[:0]
	for _, inst := range pm.Status.Instances {
		if alive[inst.Pod] {
			kept = append(kept, inst)
		}
	}
	pm.Status.Instances = kept
}

// setStatusFields refreshes candidate/desired counts and the Initialized
// condition. It only mutates the in-memory object; the caller persists once.
func (r *ModelClaimReconciler) setStatusFields(pm *modelv1alpha1.ModelClaim, candidates []corev1.Pod) {
	pm.Status.Candidates = int32(len(candidates))
	pm.Status.DesiredReplicas = desiredReplicas(pm)
	meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
		Type:    string(modelv1alpha1.ModelClaimConditionTypeInitialized),
		Status:  metav1.ConditionTrue,
		Reason:  "ModelClaimInitialized",
		Message: "ModelClaim has entered the reconciliation loop",
	})
}

// recomputeReadiness derives ReadyReplicas, the high-level phase, and the Ready
// condition from the current instance set.
func (r *ModelClaimReconciler) recomputeReadiness(pm *modelv1alpha1.ModelClaim) {
	// Only instances whose engine is serveable (Active) count as ready. An
	// Activating instance has a spawned-but-not-serveable engine, and its
	// warm-pod annotation is still the non-routable marker (port 0), so it is
	// NOT routable and must not inflate ReadyReplicas.
	active := 0
	sleeping := 0
	failed := 0
	for i := range pm.Status.Instances {
		switch pm.Status.Instances[i].Phase {
		case modelv1alpha1.ModelClaimActive:
			active++
		case modelv1alpha1.ModelClaimSleeping:
			sleeping++
		case modelv1alpha1.ModelClaimFailed:
			failed++
		}
	}
	pm.Status.ReadyReplicas = int32(active)
	switch {
	case len(pm.Status.Instances) > 0 && active == len(pm.Status.Instances):
		// Every instance is serveable: the claim is fully Active.
		pm.Status.Phase = modelv1alpha1.ModelClaimActive
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionTrue,
			Reason:  "ModelClaimActive",
			Message: "all model engine instances are active and routable",
		})
	case active > 0 && failed == 0 && sleeping > 0:
		// Some replicas serve while others are intentionally asleep: report
		// Sleeping; the gateway keeps routing to the Active instances.
		pm.Status.Phase = modelv1alpha1.ModelClaimSleeping
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "EngineSleeping",
			Message: "one or more model engine instances are sleeping while others serve",
		})
	case active > 0:
		// Partially serving: other replicas are booting or being replayed.
		// The gateway keeps routing to the ready instances while the claim
		// converges; the Ready condition stays False until every desired
		// instance is serveable.
		pm.Status.Phase = modelv1alpha1.ModelClaimActivating
		reason, message := "EngineStarting", "one or more engine instances are not ready yet"
		if failed > 0 {
			reason, message = "InstanceFailed", "one or more engine instances failed and are being replayed"
		}
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		})
	case failed > 0:
		// No serveable instance remains: the claim is unavailable until a
		// replay activation becomes ready.
		pm.Status.Phase = modelv1alpha1.ModelClaimFailed
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "EngineFailed",
			Message: "all model engine instances are failed; waiting for replay",
		})
	case sleeping > 0:
		pm.Status.Phase = modelv1alpha1.ModelClaimSleeping
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "EngineSleeping",
			Message: "one or more model engine instances are sleeping and non-routable",
		})
	case len(pm.Status.Instances) > 0:
		// Engine(s) spawned but at least one is still booting/compiling. The
		// model is not yet routable.
		pm.Status.Phase = modelv1alpha1.ModelClaimActivating
		meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
			Type:    string(modelv1alpha1.ModelClaimConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  "EngineStarting",
			Message: "engine(s) spawned, waiting for readiness probe",
		})
	default:
		pm.Status.Phase = modelv1alpha1.ModelClaimPending
	}
}

// placementInputs gathers the cross-claim load map and the per-pod runtime
// placement summaries (including the estimated HBM requirement) once per
// converge pass, so activation, replay, and scale-down all decide on the same
// point-in-time observation.
func (r *ModelClaimReconciler) placementInputs(
	ctx context.Context,
	pm *modelv1alpha1.ModelClaim,
	candidates []corev1.Pod,
) (map[string]int, map[string]PodPlacementState, int64, error) {
	load := r.computePodLoad(ctx, pm.Namespace)
	parallelism, err := modelParallelism(pm)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("invalid engineConfig parallelism: %w", err)
	}
	states, hbmRequired := r.collectPlacementStates(ctx, candidates, pm.Spec.ArtifactURL, parallelism)
	return load, states, hbmRequired, nil
}

// convergeInstances drives the claim towards its desired instance set.
//
// Safety order within one pass:
//  1. activate scale-up slots and terminal-failure replacements on DISTINCT
//     warm pods first (a failed instance keeps occupying its slot, and its pod
//     is excluded, until the replacement is up);
//  2. a terminal failure with no distinct candidate is released for a
//     same-pod retry only when no replacement is still booting and the claim
//     is not already serving desired from healthy instances;
//  3. stop surplus instances through selectScaleDownVictim, which never
//     removes a serveable instance while ready count would drop below desired.
//
// Lack of an available warm pod is not an error (Pending + requeue); runtime
// activation failures propagate to the caller's Failed/Retry handling.
func (r *ModelClaimReconciler) convergeInstances(ctx context.Context, pm *modelv1alpha1.ModelClaim, candidates []corev1.Pod) error {
	desired := desiredReplicas(pm)
	load, states, hbmRequired, err := r.placementInputs(ctx, pm, candidates)
	if err != nil {
		return err
	}

	// 1. Activate up to desired + failed: each failed record reserves a
	// temporary extra slot for its replacement so the old instance can be
	// stopped only after the new one is ready.
	target := desired + int32(countPhase(pm, modelv1alpha1.ModelClaimFailed))
	if err := r.activateToTarget(ctx, pm, candidates, target, load, states, hbmRequired); err != nil {
		return err
	}

	// 2. Release terminal failures that cannot get a distinct replacement, so
	// the next pass can re-activate (possibly on the same warm pod).
	r.releaseFailedForSamePodRetry(ctx, pm, candidates, load, states, hbmRequired)

	// 3. Stop surplus instances under the healthy-instance guard.
	r.scaleDownGuarded(ctx, pm, desired)
	return nil
}

// activateToTarget activates engines on distinct warm pods until the instance
// count reaches target. No available pod is a non-error scheduling wait.
func (r *ModelClaimReconciler) activateToTarget(
	ctx context.Context,
	pm *modelv1alpha1.ModelClaim,
	candidates []corev1.Pod,
	target int32,
	load map[string]int,
	placementStates map[string]PodPlacementState,
	hbmRequired int64,
) error {
	for int32(len(pm.Status.Instances)) < target {
		pod, selectErr := selectPodForActivationWithState(
			candidates, instancePods(pm), load, servedModelName(pm), r.Locality, placementStates, hbmRequired,
		)
		if selectErr != nil {
			// No available warm pod right now; remain Pending and retry on requeue.
			r.Recorder.Event(pm, corev1.EventTypeWarning, "NoMatchingPods", selectErr.Error())
			meta.SetStatusCondition(&pm.Status.Conditions, metav1.Condition{
				Type:    string(modelv1alpha1.ModelClaimConditionTypeScheduled),
				Status:  metav1.ConditionFalse,
				Reason:  "NoMatchingPods",
				Message: selectErr.Error(),
			})
			return nil
		}

		resp, aerr := r.Runtime.Activate(ctx, pod.Status.PodIP, DefaultRuntimePort, &ActivateRequest{
			ModelName:    servedModelName(pm),
			ArtifactURL:  pm.Spec.ArtifactURL,
			Engine:       pm.Spec.Engine,
			IPCName:      ipcNameFor(pm),
			EngineConfig: pm.Spec.EngineConfig,
			ClaimRef: &ModelClaimRef{
				Namespace: pm.Namespace,
				Name:      pm.Name,
				UID:       string(pm.UID),
			},
		})
		if aerr != nil {
			recordActivation(pm.Namespace, servedModelName(pm), false)
			return aerr
		}

		// The engine is spawned but not yet serveable (boot/compile). Keep the
		// model NOT routable — stamp the non-routable marker (port 0), record the
		// instance as Activating with its real port — until reconcileInstanceHealth
		// confirms the engine is ready, then it flips the annotation to the real
		// port. This means the gateway never routes to a still-booting engine.
		if err := r.annotateWarmPod(ctx, pm, pod, 0); err != nil {
			return err
		}

		pm.Status.Instances = append(pm.Status.Instances, modelv1alpha1.ModelClaimInstance{
			Pod:   pod.Name,
			Port:  resp.Port,
			Phase: modelv1alpha1.ModelClaimActivating,
		})
		load[pod.Name]++
		// The engine is booting, so its HBM/KV footprint is not yet observable.
		// Account for the placement itself (one more resident model on this pod)
		// so a second replica activated in the same pass cannot be ranked onto
		// the pod that was just selected. alreadyOn excludes it outright; the
		// model-count bump keeps cross-pod ranking consistent.
		if state, ok := placementStates[pod.Name]; ok {
			state.ModelCount++
			placementStates[pod.Name] = state
		}
		r.Recorder.Eventf(pm, corev1.EventTypeNormal, "Activating",
			"model %s engine starting on pod %s:%d", servedModelName(pm), pod.Name, resp.Port)
	}
	return nil
}

// releaseFailedForSamePodRetry handles terminal-failed instances for which no
// DISTINCT warm pod exists: the failed engine is stopped and its record dropped
// so the normal activation path can retry on the same pod on the next pass.
//
// It deliberately does nothing while a replacement is still booting on another
// pod (the replacement-first ordering) or while desired is already served by
// healthy instances (scale-down reaps the failed record instead). At most one
// record is released per pass per pod-set to keep retries paced by requeue.
func (r *ModelClaimReconciler) releaseFailedForSamePodRetry(
	ctx context.Context,
	pm *modelv1alpha1.ModelClaim,
	candidates []corev1.Pod,
	load map[string]int,
	states map[string]PodPlacementState,
	hbmRequired int64,
) {
	desired := desiredReplicas(pm)
	ready := int32(countPhase(pm, modelv1alpha1.ModelClaimActive))
	activating := countPhase(pm, modelv1alpha1.ModelClaimActivating)
	if ready >= desired || activating > 0 {
		return
	}
	for i := range pm.Status.Instances {
		inst := &pm.Status.Instances[i]
		if inst.Phase != modelv1alpha1.ModelClaimFailed {
			continue
		}
		// A distinct candidate (excluding every pod this claim already occupies)
		// means the replacement-first path is in charge, not same-pod retry.
		if _, err := selectPodForActivationWithState(
			candidates, instancePods(pm), load, servedModelName(pm), r.Locality, states, hbmRequired,
		); err == nil {
			return
		}
		r.Recorder.Eventf(pm, corev1.EventTypeNormal, "ReplayFailedInstance",
			"model %s has no distinct warm pod; releasing failed instance on pod %s for same-pod retry",
			servedModelName(pm), inst.Pod)
		r.stopInstance(ctx, pm, *inst)
		pm.Status.Instances = removeInstanceAt(pm.Status.Instances, i)
		return
	}
}

// collectPlacementStates summarizes runtime snapshots for every candidate and
// additionally estimates the per-instance HBM requirement from engines of the
// same artifact already observed in the pool.
func (r *ModelClaimReconciler) collectPlacementStates(
	ctx context.Context,
	candidates []corev1.Pod,
	artifactURL string,
	parallelism int64,
) (map[string]PodPlacementState, int64) {
	states := make(map[string]PodPlacementState, len(candidates))
	snapshots := make(map[string]*RuntimeSnapshot, len(candidates))
	if r.SnapshotCache == nil {
		return states, 0
	}
	for i := range candidates {
		pod := &candidates[i]
		key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		snapshot, ok := r.SnapshotCache.Get(key, pod.UID, func() (*RuntimeSnapshot, error) {
			return r.Runtime.Snapshot(ctx, pod.Status.PodIP, DefaultRuntimePort)
		})
		if !ok {
			continue
		}
		snapshots[pod.Name] = snapshot
		states[pod.Name] = placementStateFromSnapshot(snapshot, artifactURL, parallelism)
	}
	return states, hbmRequirementFromSnapshots(snapshots, artifactURL)
}

// reconcileInstanceHealth reconciles routing from fresh runtime snapshot data.
// Snapshot state is authoritative for the engine port and readiness: the
// controller never promotes an engine merely because its old status entry was
// Active. A snapshot failure leaves the last known routing in place rather
// than guessing that a live engine has disappeared.
func (r *ModelClaimReconciler) reconcileInstanceHealth(ctx context.Context, pm *modelv1alpha1.ModelClaim) {
	served := servedModelName(pm)
	for i := range pm.Status.Instances {
		inst := &pm.Status.Instances[i]
		if inst.Phase != modelv1alpha1.ModelClaimActivating &&
			inst.Phase != modelv1alpha1.ModelClaimActive &&
			inst.Phase != modelv1alpha1.ModelClaimSleeping &&
			inst.Phase != modelv1alpha1.ModelClaimFailed {
			continue
		}
		ip := r.podIP(ctx, pm.Namespace, inst.Pod)
		if ip == "" {
			continue // pod gone; pruneDeadInstances will drop it
		}
		snapshot, err := r.Runtime.Snapshot(ctx, ip, DefaultRuntimePort)
		if err != nil {
			klog.V(4).InfoS("runtime snapshot failed", "model", pm.Name, "pod", inst.Pod, "phase", inst.Phase, "err", err)
			continue
		}
		observed := snapshotModelForClaim(snapshot, pm, served)
		observedPort := inst.Port
		if observed != nil {
			observedPort = observed.Port
		}

		desiredPhase := modelv1alpha1.ModelClaimActivating
		routingPort := int32(0)
		switch {
		case observed != nil && observed.Phase == runtimePhaseFailed:
			desiredPhase = modelv1alpha1.ModelClaimFailed
		case observed != nil && observed.Phase == runtimePhaseSleeping:
			desiredPhase = modelv1alpha1.ModelClaimSleeping
		case observed != nil && observed.Ready && observedPort > 0:
			desiredPhase = modelv1alpha1.ModelClaimActive
			routingPort = observedPort
		case observed != nil:
			// Alive but not serveable (booting, compiling, restarting): keep
			// it off the routable set until the next readiness observation.
			desiredPhase = modelv1alpha1.ModelClaimActivating
		case inst.Phase == modelv1alpha1.ModelClaimActive ||
			inst.Phase == modelv1alpha1.ModelClaimSleeping:
			// The snapshot succeeded but a previously resident engine is no
			// longer reported by the runtime: the process vanished. Treat it
			// as terminal so the failure-replay path replaces it.
			desiredPhase = modelv1alpha1.ModelClaimFailed
		default:
			// An Activating engine may briefly lag the runtime's model list
			// right after activation; do not flap it to Failed on one
			// observation. A Failed record stays Failed until the converge
			// pass releases or replaces it.
			desiredPhase = inst.Phase
		}

		// Observed readiness/restart counters mirror the runtime; on a missing
		// engine the instance is plainly not ready while the last restart
		// count is retained for status continuity.
		inst.Ready = observed != nil && observed.Ready && observedPort > 0
		if observed != nil {
			inst.RestartCount = int32(observed.RestartCount)
		}

		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: pm.Namespace, Name: inst.Pod}, pod); err != nil {
			continue
		}
		if err := r.annotateWarmPodWithState(
			ctx, pm, pod, routingPort, routingStateForPhase(desiredPhase),
		); err != nil {
			klog.ErrorS(err, "routability annotation failed", "model", pm.Name, "pod", inst.Pod, "ready", desiredPhase == modelv1alpha1.ModelClaimActive)
			continue
		}
		inst.Port = observedPort
		previousPhase := inst.Phase
		if previousPhase == desiredPhase {
			continue
		}
		inst.Phase = desiredPhase
		switch desiredPhase {
		case modelv1alpha1.ModelClaimFailed:
			reason, message := "EngineFailed", "runtime reported terminal engine failure"
			switch {
			case observed != nil && observed.LastError != "":
				message = observed.LastError
			case observed == nil:
				reason, message = "EngineGone", "engine process disappeared from the runtime model list"
			}
			r.Recorder.Eventf(pm, corev1.EventTypeWarning, reason,
				"model %s failed on pod %s: %s", served, inst.Pod, message)
		case modelv1alpha1.ModelClaimActive:
			if previousPhase == modelv1alpha1.ModelClaimActivating {
				recordActivation(pm.Namespace, served, true)
				r.Recorder.Eventf(pm, corev1.EventTypeNormal, "Activated",
					"model %s ready and routable on pod %s:%d", served, inst.Pod, inst.Port)
			} else {
				r.Recorder.Eventf(pm, corev1.EventTypeNormal, "Woken",
					"model %s woke and is routable on pod %s:%d", served, inst.Pod, inst.Port)
			}
		case modelv1alpha1.ModelClaimSleeping:
			r.Recorder.Eventf(pm, corev1.EventTypeNormal, "Sleeping",
				"model %s is sleeping on pod %s and marked non-routable", served, inst.Pod)
		case modelv1alpha1.ModelClaimActivating:
			if previousPhase != modelv1alpha1.ModelClaimActivating {
				r.Recorder.Eventf(pm, corev1.EventTypeWarning, "Unhealthy",
					"model %s no longer ready on pod %s; marked non-routable", served, inst.Pod)
			}
		}
	}
}

// snapshotModelForClaim resolves runtime state by ClaimRef UID when the
// sidecar provides one. The served-name fallback keeps old runtime images
// interoperable while avoiding a match to a snapshot explicitly owned by a
// different ModelClaim.
func snapshotModelForClaim(snapshot *RuntimeSnapshot, pm *modelv1alpha1.ModelClaim, served string) *RuntimeSnapshotModel {
	if snapshot == nil {
		return nil
	}
	claimUID := string(pm.UID)
	var legacy *RuntimeSnapshotModel
	for i := range snapshot.Models {
		model := &snapshot.Models[i]
		if claimUID != "" && model.ClaimRef != nil && model.ClaimRef.UID != "" {
			if model.ClaimRef.UID == claimUID {
				return model
			}
			continue
		}
		if model.ModelName == served {
			legacy = model
		}
	}
	return legacy
}

// annotateWarmPod records the active or activating served-model binding for
// this ModelClaim. Lifecycle reconciliation uses annotateWarmPodWithState for
// sleeping and failed observations.
func (r *ModelClaimReconciler) annotateWarmPod(ctx context.Context, pm *modelv1alpha1.ModelClaim, pod *corev1.Pod, port int32) error {
	state := constants.ModelClaimRoutingStateActive
	if port == 0 {
		state = constants.ModelClaimRoutingStateActivating
	}
	return r.annotateWarmPodWithState(ctx, pm, pod, port, state)
}

func (r *ModelClaimReconciler) annotateWarmPodWithState(
	ctx context.Context,
	pm *modelv1alpha1.ModelClaim,
	pod *corev1.Pod,
	port int32,
	state string,
) error {
	key := constants.ModelClaimPodAnnotationPrefix + pm.Name
	value := fmt.Sprintf(`{"model":%q,"port":%d,"state":%q}`, servedModelName(pm), port, state)
	if pod.Annotations[key] == value {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[key] = value
	return r.Patch(ctx, pod, patch)
}

func routingStateForPhase(phase modelv1alpha1.ModelClaimPhase) string {
	switch phase {
	case modelv1alpha1.ModelClaimActive:
		return constants.ModelClaimRoutingStateActive
	case modelv1alpha1.ModelClaimSleeping:
		return constants.ModelClaimRoutingStateSleeping
	case modelv1alpha1.ModelClaimFailed:
		return constants.ModelClaimRoutingStateFailed
	default:
		return constants.ModelClaimRoutingStateActivating
	}
}

// deannotateWarmPod removes this ModelClaim's routing annotation from a warm
// pod (best-effort), used on deactivation/deletion so the gateway stops routing.
func (r *ModelClaimReconciler) deannotateWarmPod(ctx context.Context, namespace, podName, pmName string) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		return // pod already gone
	}
	key := constants.ModelClaimPodAnnotationPrefix + pmName
	if _, ok := pod.Annotations[key]; !ok {
		return
	}
	patch := client.MergeFrom(pod.DeepCopy())
	delete(pod.Annotations, key)
	if err := r.Patch(ctx, pod, patch); err != nil {
		klog.ErrorS(err, "failed to remove model-claim routing annotation", "pod", podName, "model", pmName)
	}
}

// scaleDownGuarded deactivates surplus instances one at a time using
// selectScaleDownVictim's ordering and safety guards. A -1 victim means the
// claim must wait (for a replacement to become ready, or for a healthy
// replacement to exist): the surplus record is kept rather than stopping the
// only healthy engine.
func (r *ModelClaimReconciler) scaleDownGuarded(ctx context.Context, pm *modelv1alpha1.ModelClaim, target int32) {
	for int32(len(pm.Status.Instances)) > target {
		idx := selectScaleDownVictim(pm.Status.Instances, target)
		if idx < 0 {
			return
		}
		inst := pm.Status.Instances[idx]
		r.stopInstance(ctx, pm, inst)
		pm.Status.Instances = removeInstanceAt(pm.Status.Instances, idx)
	}
}

// stopInstance de-routes and stops one engine instance. Runtime teardown is
// best-effort: the pod/engine may already be gone, and the next health pass
// reconciles any residual state. The caller is responsible for removing the
// status record.
func (r *ModelClaimReconciler) stopInstance(ctx context.Context, pm *modelv1alpha1.ModelClaim, inst modelv1alpha1.ModelClaimInstance) {
	r.deannotateWarmPod(ctx, pm.Namespace, inst.Pod, pm.Name)
	ip := r.podIP(ctx, pm.Namespace, inst.Pod)
	if ip == "" {
		return
	}
	if err := r.Runtime.Deactivate(ctx, ip, DefaultRuntimePort, &DeactivateRequest{
		ModelName: servedModelName(pm),
		Mode:      DeactivateStop,
	}); err != nil {
		klog.ErrorS(err, "deactivate failed", "pod", inst.Pod, "model", pm.Name)
	}
}

// reconcileDeletion stops every engine instance and only releases the
// finalizer after each stop is confirmed: a vanished pod counts as confirmed,
// otherwise the runtime deactivate must succeed AND a follow-up snapshot must
// no longer report the claim's engine. Unconfirmed instances are retained in
// status (persisted so a controller restart resumes the wait) and the claim
// requeues. This guarantees a deleting ModelClaim never leaves live engines
// behind just because one teardown failed or lagged.
func (r *ModelClaimReconciler) reconcileDeletion(ctx context.Context, pm *modelv1alpha1.ModelClaim) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pm, ModelClaimFinalizer) {
		return ctrl.Result{}, nil
	}
	served := servedModelName(pm)
	remaining := make([]modelv1alpha1.ModelClaimInstance, 0, len(pm.Status.Instances))
	for _, inst := range pm.Status.Instances {
		r.deannotateWarmPod(ctx, pm.Namespace, inst.Pod, pm.Name)
		ip := r.podIP(ctx, pm.Namespace, inst.Pod)
		if ip == "" {
			continue // pod already gone; nothing left to stop
		}
		if err := r.Runtime.Deactivate(ctx, ip, DefaultRuntimePort, &DeactivateRequest{
			ModelName: served,
			Mode:      DeactivateStop,
		}); err != nil {
			klog.ErrorS(err, "deactivate on delete failed", "pod", inst.Pod, "model", pm.Name)
			remaining = append(remaining, inst)
			continue
		}
		// Confirm the engine actually disappeared rather than assuming the
		// stop was synchronous. A snapshot error keeps the finalizer in
		// place; the next pass retries.
		snapshot, err := r.Runtime.Snapshot(ctx, ip, DefaultRuntimePort)
		if err != nil {
			klog.ErrorS(err, "post-deactivate snapshot failed; keeping finalizer", "pod", inst.Pod, "model", pm.Name)
			remaining = append(remaining, inst)
			continue
		}
		if snapshotModelForClaim(snapshot, pm, served) != nil {
			klog.InfoS("engine still reported after deactivate; keeping finalizer", "pod", inst.Pod, "model", pm.Name)
			remaining = append(remaining, inst)
		}
	}
	pm.Status.Instances = remaining

	if len(remaining) > 0 {
		r.Recorder.Eventf(pm, corev1.EventTypeWarning, "DeletionWaiting",
			"model %s deletion is waiting for %d engine instance(s) to stop", served, len(remaining))
		setClaimGauges(pm)
		_ = r.Status().Update(ctx, pm) // best-effort persistence for restart resume
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
	}

	clearClaimMetrics(pm.Namespace, served)
	controllerutil.RemoveFinalizer(pm, ModelClaimFinalizer)
	if err := r.Update(ctx, pm); err != nil {
		return requeueOnConflict(err)
	}
	return ctrl.Result{}, nil
}

// computePodLoad tallies how many model instances each warm pod currently hosts,
// across all ModelClaims in the namespace, for least-loaded bin-packing.
func (r *ModelClaimReconciler) computePodLoad(ctx context.Context, namespace string) map[string]int {
	load := map[string]int{}
	list := &modelv1alpha1.ModelClaimList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		klog.ErrorS(err, "compute pod load: list model claims", "namespace", namespace)
		return load
	}
	for i := range list.Items {
		for _, inst := range list.Items[i].Status.Instances {
			load[inst.Pod]++
		}
	}
	return load
}

// podIP resolves a pod's IP by name, returning "" if the pod is missing or has
// no IP yet.
func (r *ModelClaimReconciler) podIP(ctx context.Context, namespace, name string) string {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pod); err != nil {
		return ""
	}
	return pod.Status.PodIP
}

// instancePods returns the set of pod names this model is already attached to.
func instancePods(pm *modelv1alpha1.ModelClaim) map[string]bool {
	pods := make(map[string]bool, len(pm.Status.Instances))
	for _, inst := range pm.Status.Instances {
		pods[inst.Pod] = true
	}
	return pods
}

// modelPoolPodFilter restricts pod events to GPU pool members so the
// controller only reacts to pods that can host ModelClaims.
func modelPoolPodFilter() predicate.Predicate {
	isModelPoolPod := func(labels map[string]string) bool {
		if labels == nil {
			return false
		}
		if _, ok := labels[constants.ModelPoolLabelName]; !ok {
			return false
		}
		return labels[constants.ModelPoolLabelEnabled] == constants.ModelPoolLabelEnabledValue
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return isModelPoolPod(e.Object.GetLabels()) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return isModelPoolPod(e.ObjectNew.GetLabels()) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return isModelPoolPod(e.Object.GetLabels()) },
		GenericFunc: func(e event.GenericEvent) bool { return isModelPoolPod(e.Object.GetLabels()) },
	}
}

// enqueueModelClaimsForPod re-reconciles every ModelClaim in a pod's namespace
// when warm-pool membership changes. A pod add may let a pending model attach; a
// pod delete may strand instances that must be rescheduled.
func enqueueModelClaimsForPod(c client.Client) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		list := &modelv1alpha1.ModelClaimList{}
		if err := c.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
			klog.ErrorS(err, "unable to list model claims in namespace", "namespace", obj.GetNamespace())
			return nil
		}
		requests := make([]reconcile.Request, 0, len(list.Items))
		for i := range list.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: list.Items[i].Namespace,
					Name:      list.Items[i].Name,
				},
			})
		}
		return requests
	}
}
