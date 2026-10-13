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
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	aibrixconst "github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/controller/constants"
	ctrlutil "github.com/vllm-project/aibrix/pkg/controller/util"
	orchestrationctrl "github.com/vllm-project/aibrix/pkg/controller/util/orchestration"
)

const (
	// DefaultFailureThresholdSeconds is used when rollbackStrategy is enabled
	// but failureThresholdSeconds is not set.
	DefaultFailureThresholdSeconds int32 = 600
	// RollbackHistoryLimit is the maximum number of rollback records kept per role.
	RollbackHistoryLimit = 10

	// Event types emitted for automatic rollback.
	RollbackTriggeredEventType = "RollbackTriggered"
	RollbackCompletedEventType = "RollbackCompleted"
	RollbackRecoveredEventType = "RollbackRecovered"

	// Failure reasons recorded in role status.
	FailureReasonProgressDeadlineExceeded = "ProgressDeadlineExceeded"
	FailureReasonDrainTimeout             = "DrainTimeout"

	// Reasons used on the RoleSet Progressing condition.
	ProgressingRolloutInProgressReason  = "RolloutInProgress"
	ProgressingRollbackInProgressReason = "RollbackInProgress"
	ProgressingRolloutCompletedReason   = "RolloutCompleted"
)

// rolloutStatuses maps role name to the rollout-specific part of RoleStatus
// produced by the rollback planner and consumed by the status calculation.
type rolloutStatuses map[string]*orchestrationv1alpha1.RoleStatus

type rollbackPlanner struct {
	cli      client.Client
	recorder record.EventRecorder
	now      time.Time
}

// plan reconciles the per-role rollout state machine. It records the last
// stable template in spec, swaps the active template back to the last stable
// revision when an update crosses the failure threshold, and handles manual
// recovery. The RoleSet passed in is mutated in memory; spec changes are also
// persisted so that the regular rolling managers drive the (possibly rolled
// back) template through the normal Scale/Rollout path, preserving drain,
// maxSurge, maxUnavailable and PodGroupSize constraints.
// It returns nil when automatic rollback is disabled.
func (p *rollbackPlanner) plan(ctx context.Context, roleSet *orchestrationv1alpha1.RoleSet) (rolloutStatuses, error) {
	if roleSet.Spec.RollbackStrategy == nil {
		return nil, nil
	}
	threshold := failureThreshold(roleSet)

	recoveryRoles, err := p.consumeRecoveryAnnotation(ctx, roleSet)
	if err != nil {
		return nil, err
	}

	// Snapshot after the annotation patch so the spec patch carries a fresh
	// resource version.
	specBase := roleSet.DeepCopy()
	specChanged := false
	statuses := make(rolloutStatuses, len(roleSet.Spec.Roles))

	for i := range roleSet.Spec.Roles {
		role := &roleSet.Spec.Roles[i]
		syncer := GetRoleSyncerWithRecorder(p.cli, role, p.recorder)
		st := carryRolloutStatus(roleSet.Status.Roles, role.Name)
		prevTargetRevision := st.TargetRevision

		desiredHash := ctrlutil.ComputeHash(&role.Template, nil)
		stableHash := ""
		if role.StableTemplate != nil {
			stableHash = ctrlutil.ComputeHash(role.StableTemplate, nil)
		}

		ready, err := syncer.AllReady(ctx, roleSet, role)
		if err != nil {
			return nil, fmt.Errorf("check readiness of role %s: %w", role.Name, err)
		}
		hasTargetPod, blockedOnDrain, err := p.observeRevision(ctx, roleSet, role, desiredHash)
		if err != nil {
			return nil, fmt.Errorf("observe revision of role %s: %w", role.Name, err)
		}
		// A rollout drain that is waiting to remove a stable pod also proves
		// the update has started, even before any target-revision pod exists.
		updateObserved := hasTargetPod || blockedOnDrain

		forceStart := false
		if recoveryRoles[role.Name] {
			recovered := p.recoverRole(ctx, roleSet, role, st, desiredHash, stableHash)
			forceStart = recovered
			if recovered {
				desiredHash = ctrlutil.ComputeHash(&role.Template, nil)
				// readiness was evaluated against the pre-recovery template;
				// re-check against the retried revision.
				ready, err = syncer.AllReady(ctx, roleSet, role)
				if err != nil {
					return nil, fmt.Errorf("check readiness of role %s after recovery: %w", role.Name, err)
				}
				hasTargetPod, blockedOnDrain, err = p.observeRevision(ctx, roleSet, role, desiredHash)
				if err != nil {
					return nil, fmt.Errorf("observe revision of role %s after recovery: %w", role.Name, err)
				}
				updateObserved = hasTargetPod || blockedOnDrain
			}
			specChanged = specChanged || recovered
		}

		st.TargetRevision = desiredHash
		switch {
		case stableHash == "":
			// No stable revision recorded yet: initial creation or first rollout.
			// Failures cannot trigger a rollback until a stable revision exists.
			if ready {
				recordStableRevision(role, desiredHash)
				specChanged = true
				st.CurrentRevision = desiredHash
				st.RolloutState = orchestrationv1alpha1.RoleRolloutStable
				st.UpdateStartedAt = nil
			} else {
				st.RolloutState = orchestrationv1alpha1.RoleRolloutProgressing
				if st.CurrentRevision == "" {
					st.CurrentRevision = desiredHash
				}
				st.UpdateStartedAt = armUpdateStart(st.UpdateStartedAt, prevTargetRevision, desiredHash, updateObserved, forceStart, p.now)
			}
		case desiredHash == stableHash:
			// Active template is the stable revision.
			if ready {
				wasRollingBack := st.RolloutState == orchestrationv1alpha1.RoleRolloutRollingBack
				st.CurrentRevision = desiredHash
				st.RolloutState = orchestrationv1alpha1.RoleRolloutStable
				st.UpdateStartedAt = nil
				if wasRollingBack {
					klog.Infof("[rollback] roleset %s/%s role %s rollback to revision %s completed",
						roleSet.Namespace, roleSet.Name, role.Name, shortHash(stableHash))
					if p.recorder != nil {
						p.recorder.Eventf(roleSet, v1.EventTypeNormal, RollbackCompletedEventType,
							"role %s rolled back to stable revision %s and is ready", role.Name, shortHash(stableHash))
					}
				}
			} else if st.RolloutState == orchestrationv1alpha1.RoleRolloutRollingBack {
				// Keep replacing failed-revision pods through the regular rolling path.
				st.CurrentRevision = stableHash
			} else {
				// Stable template pods are not ready without an active update
				// (e.g. external pod deletion). Observe but never auto-rollback.
				st.RolloutState = orchestrationv1alpha1.RoleRolloutProgressing
				if st.CurrentRevision == "" {
					st.CurrentRevision = stableHash
				}
				st.UpdateStartedAt = armUpdateStart(st.UpdateStartedAt, prevTargetRevision, desiredHash, updateObserved, forceStart, p.now)
			}
		default:
			// A new revision (desiredHash) is being rolled out over stableHash.
			if ready {
				recordStableRevision(role, desiredHash)
				specChanged = true
				st.CurrentRevision = desiredHash
				st.RolloutState = orchestrationv1alpha1.RoleRolloutStable
				st.UpdateStartedAt = nil
				// A successful update of a new revision clears prior failure state.
				st.FailureReason = ""
				st.FailureMessage = ""
			} else {
				st.RolloutState = orchestrationv1alpha1.RoleRolloutProgressing
				if st.CurrentRevision == "" {
					st.CurrentRevision = stableHash
				}
				st.UpdateStartedAt = armUpdateStart(st.UpdateStartedAt, prevTargetRevision, desiredHash, updateObserved, forceStart, p.now)
				if st.UpdateStartedAt != nil && p.now.Sub(st.UpdateStartedAt.Time) >= threshold {
					specChanged = true
					p.triggerRollback(ctx, roleSet, role, st, stableHash, desiredHash, blockedOnDrain, threshold)
				}
			}
		}
		statuses[role.Name] = st
	}

	if specChanged {
		if err := orchestrationctrl.Patch(ctx, p.cli, roleSet, client.MergeFrom(specBase)); err != nil {
			return nil, fmt.Errorf("persist rollback spec changes: %w", err)
		}
	}
	return statuses, nil
}

// armUpdateStart returns the start time of the current update. The deadline is
// only armed once a pod of the target revision is observed, so roles waiting
// for their turn under Sequential/Interleave strategies do not age out early.
func armUpdateStart(prev *metav1.Time, prevTarget, desired string, targetObserved, forced bool, now time.Time) *metav1.Time {
	if prev != nil && prevTarget == desired {
		return prev
	}
	if targetObserved || forced {
		t := metav1.NewTime(now)
		return &t
	}
	return nil
}

func recordStableRevision(role *orchestrationv1alpha1.RoleSpec, hash string) {
	role.StableTemplate = role.Template.DeepCopy()
	role.StableRevision = hash
}

func failureThreshold(roleSet *orchestrationv1alpha1.RoleSet) time.Duration {
	seconds := DefaultFailureThresholdSeconds
	if roleSet.Spec.RollbackStrategy != nil && roleSet.Spec.RollbackStrategy.FailureThresholdSeconds != nil &&
		*roleSet.Spec.RollbackStrategy.FailureThresholdSeconds > 0 {
		seconds = *roleSet.Spec.RollbackStrategy.FailureThresholdSeconds
	}
	return time.Duration(seconds) * time.Second
}

// triggerRollback swaps the active role template back to the last stable
// template and records the failure. Pod replacement afterwards is performed by
// the regular rolling managers, so drain and disruption budgets still apply.
func (p *rollbackPlanner) triggerRollback(
	ctx context.Context,
	roleSet *orchestrationv1alpha1.RoleSet,
	role *orchestrationv1alpha1.RoleSpec,
	st *orchestrationv1alpha1.RoleStatus,
	stableHash, failedHash string,
	blockedOnDrain bool,
	threshold time.Duration,
) {
	reason := FailureReasonProgressDeadlineExceeded
	if blockedOnDrain {
		reason = FailureReasonDrainTimeout
	}
	message := fmt.Sprintf("role %s update to revision %s did not become ready within %s; rolling back to stable revision %s",
		role.Name, shortHash(failedHash), threshold, shortHash(stableHash))
	if blockedOnDrain {
		message = fmt.Sprintf("role %s update to revision %s is blocked by a stuck drain/terminating pod for %s; rolling back to stable revision %s",
			role.Name, shortHash(failedHash), threshold, shortHash(stableHash))
	}

	// Normalize pods that were patched in place toward the failed revision:
	// relabel them to the failed hash so the recreate/in-place rollout path
	// replaces them through the same budgeted and drain-aware machinery.
	if err := normalizeInPlacePodsForRollback(ctx, p.cli, roleSet, role, failedHash); err != nil {
		klog.Errorf("[rollback] roleset %s/%s role %s failed to normalize in-place pods: %v",
			roleSet.Namespace, roleSet.Name, role.Name, err)
	}

	failedTemplate := role.Template.DeepCopy()
	role.Template = *role.StableTemplate.DeepCopy()

	// The failed template is only retained on the newest record for manual
	// recovery; older records keep the revision metadata only.
	for i := range st.RollbackHistory {
		st.RollbackHistory[i].FailedTemplate = nil
	}
	record := orchestrationv1alpha1.RollbackRecord{
		TargetRevision:   failedHash,
		RollbackRevision: stableHash,
		FailedTemplate:   failedTemplate,
		Reason:           reason,
		Message:          message,
		RolledAt:         metav1.NewTime(p.now),
	}
	st.RollbackHistory = append([]orchestrationv1alpha1.RollbackRecord{record}, st.RollbackHistory...)
	if len(st.RollbackHistory) > RollbackHistoryLimit {
		st.RollbackHistory = st.RollbackHistory[:RollbackHistoryLimit]
	}
	st.RollbackCount++
	st.FailureReason = reason
	st.FailureMessage = message
	st.CurrentRevision = stableHash
	st.TargetRevision = stableHash
	st.RolloutState = orchestrationv1alpha1.RoleRolloutRollingBack
	st.UpdateStartedAt = nil

	klog.Warningf("[rollback] roleset %s/%s role %s: %s", roleSet.Namespace, roleSet.Name, role.Name, message)
	if p.recorder != nil {
		p.recorder.Eventf(roleSet, v1.EventTypeWarning, RollbackTriggeredEventType,
			"%s (rollback #%d)", message, st.RollbackCount)
	}
}

// recoverRole retries the latest failed revision when the recovery annotation
// requests it. It returns true when the active template was changed.
func (p *rollbackPlanner) recoverRole(
	ctx context.Context,
	roleSet *orchestrationv1alpha1.RoleSet,
	role *orchestrationv1alpha1.RoleSpec,
	st *orchestrationv1alpha1.RoleStatus,
	desiredHash, stableHash string,
) bool {
	clearFailure := func() {
		st.FailureReason = ""
		st.FailureMessage = ""
	}
	latest := latestFailedRecord(st.RollbackHistory)
	if latest == nil || latest.FailedTemplate == nil {
		// Nothing to retry; just clear the recorded failure.
		if st.FailureReason != "" || st.FailureMessage != "" {
			klog.Infof("[rollback] roleset %s/%s role %s failure state cleared by administrator",
				roleSet.Namespace, roleSet.Name, role.Name)
		}
		clearFailure()
		return false
	}
	targetHash := ctrlutil.ComputeHash(latest.FailedTemplate, nil)
	if targetHash == desiredHash && st.RolloutState != orchestrationv1alpha1.RoleRolloutRollingBack {
		clearFailure()
		return false
	}
	// A different revision than the failed one is already in flight; do not
	// overwrite the administrator's newer template, just clear the failure.
	if desiredHash != stableHash && desiredHash != targetHash {
		klog.Infof("[rollback] roleset %s/%s role %s recovery requested while revision %s is in flight; clearing failure without restoring %s",
			roleSet.Namespace, roleSet.Name, role.Name, shortHash(desiredHash), shortHash(targetHash))
		clearFailure()
		return false
	}
	role.Template = *latest.FailedTemplate.DeepCopy()
	latest.RecoveredAt = ptrTime(metav1.NewTime(p.now))
	st.FailureReason = ""
	st.FailureMessage = ""
	st.RolloutState = orchestrationv1alpha1.RoleRolloutProgressing
	st.TargetRevision = targetHash
	start := metav1.NewTime(p.now)
	st.UpdateStartedAt = &start
	klog.Infof("[rollback] roleset %s/%s role %s manual recovery: retrying revision %s",
		roleSet.Namespace, roleSet.Name, role.Name, shortHash(targetHash))
	if p.recorder != nil {
		p.recorder.Eventf(roleSet, v1.EventTypeNormal, RollbackRecoveredEventType,
			"role %s manual recovery: retrying revision %s", role.Name, shortHash(targetHash))
	}
	return true
}

func latestFailedRecord(records []orchestrationv1alpha1.RollbackRecord) *orchestrationv1alpha1.RollbackRecord {
	for i := range records {
		if records[i].TargetRevision != "" {
			return &records[i]
		}
	}
	return nil
}

func ptrTime(t metav1.Time) *metav1.Time {
	return &t
}

// observeRevision reports whether pods of the desired revision already exist and
// whether the rollout is blocked on a draining or terminating pod.
func (p *rollbackPlanner) observeRevision(
	ctx context.Context,
	roleSet *orchestrationv1alpha1.RoleSet,
	role *orchestrationv1alpha1.RoleSpec,
	desiredHash string,
) (hasTargetPod bool, blockedOnDrain bool, err error) {
	if role.PodGroupSize != nil && *role.PodGroupSize > 1 {
		podSets, err := getRolePodSets(ctx, p.cli, roleSet.Namespace, roleSet.Name, role.Name)
		if err != nil {
			return false, false, err
		}
		for _, podSet := range podSets {
			if podSet.DeletionTimestamp != nil {
				blockedOnDrain = true
			}
			if isPodSetActive(podSet) && podSet.Labels[constants.RoleTemplateHashLabelKey] == desiredHash {
				hasTargetPod = true
			}
		}
		return hasTargetPod, blockedOnDrain, nil
	}

	pods, err := getRolePods(ctx, p.cli, roleSet.Namespace, roleSet.Name, role.Name)
	if err != nil {
		return false, false, err
	}
	activePods, _ := filterActivePods(pods)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Annotations[aibrixconst.PodDrainingAnnotationKey] == "true" {
			blockedOnDrain = true
		}
	}
	for _, pod := range activePods {
		if pod.Labels[constants.RoleTemplateHashLabelKey] == desiredHash ||
			pod.Annotations[constants.RoleInPlaceUpdateTargetHashAnnotationKey] == desiredHash {
			hasTargetPod = true
		}
	}
	return hasTargetPod, blockedOnDrain, nil
}

// normalizeInPlacePodsForRollback relabels pods that were image-patched toward
// the failed revision so subsequent rollout logic treats them as outdated
// failed-revision pods instead of ready stable-revision pods.
func normalizeInPlacePodsForRollback(
	ctx context.Context,
	cli client.Client,
	roleSet *orchestrationv1alpha1.RoleSet,
	role *orchestrationv1alpha1.RoleSpec,
	failedHash string,
) error {
	pods, err := getRolePods(ctx, cli, roleSet.Namespace, roleSet.Name, role.Name)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if pod.Annotations[constants.RoleInPlaceUpdateTargetHashAnnotationKey] != failedHash {
			continue
		}
		updated := pod.DeepCopy()
		if updated.Labels == nil {
			updated.Labels = make(map[string]string)
		}
		updated.Labels[constants.RoleTemplateHashLabelKey] = failedHash
		delete(updated.Annotations, constants.RoleInPlaceUpdateTargetHashAnnotationKey)
		delete(updated.Annotations, constants.RoleInPlaceUpdateStateAnnotationKey)
		delete(updated.Annotations, constants.RoleInPlaceUpdatePendingReasonAnnotationKey)
		if err := cli.Patch(ctx, updated, client.MergeFrom(pod)); err != nil {
			return err
		}
	}
	return nil
}

// consumeRecoveryAnnotation removes the rollback recovery annotation (if any)
// and returns the set of role names selected for recovery.
func (p *rollbackPlanner) consumeRecoveryAnnotation(ctx context.Context, roleSet *orchestrationv1alpha1.RoleSet) (map[string]bool, error) {
	raw, ok := roleSet.Annotations[constants.RoleSetRollbackRecoveryAnnotationKey]
	if !ok {
		return nil, nil
	}
	requested := parseRecoveryRoles(raw, roleSet.Spec.Roles)
	if len(requested) == 0 {
		klog.Warningf("[rollback] roleset %s/%s has recovery annotation %q matching no role; ignoring",
			roleSet.Namespace, roleSet.Name, raw)
	}
	base := roleSet.DeepCopy()
	delete(roleSet.Annotations, constants.RoleSetRollbackRecoveryAnnotationKey)
	if err := orchestrationctrl.Patch(ctx, p.cli, roleSet, client.MergeFrom(base)); err != nil {
		return nil, fmt.Errorf("consume rollback recovery annotation: %w", err)
	}
	return requested, nil
}

func parseRecoveryRoles(raw string, roles []orchestrationv1alpha1.RoleSpec) map[string]bool {
	raw = strings.TrimSpace(raw)
	selected := make(map[string]bool)
	if raw == "" || raw == "*" {
		for i := range roles {
			selected[roles[i].Name] = true
		}
		return selected
	}
	known := make(map[string]bool, len(roles))
	for i := range roles {
		known[roles[i].Name] = true
	}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name != "" && known[name] {
			selected[name] = true
		}
	}
	return selected
}

// carryRolloutStatus clones the rollout-specific status of a role from the
// previous reconcile so timestamps, counters and history survive.
func carryRolloutStatus(roles []orchestrationv1alpha1.RoleStatus, name string) *orchestrationv1alpha1.RoleStatus {
	st := &orchestrationv1alpha1.RoleStatus{Name: name}
	for i := range roles {
		if roles[i].Name == name {
			previous := roles[i].DeepCopy()
			st.CurrentRevision = previous.CurrentRevision
			st.TargetRevision = previous.TargetRevision
			st.RolloutState = previous.RolloutState
			st.UpdateStartedAt = previous.UpdateStartedAt.DeepCopy()
			st.RollbackCount = previous.RollbackCount
			st.FailureReason = previous.FailureReason
			st.FailureMessage = previous.FailureMessage
			st.RollbackHistory = previous.RollbackHistory
			return st
		}
	}
	return st
}

func shortHash(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}

// mergeRolloutStatus overlays planner-owned rollout fields onto a RoleStatus
// rebuilt from observed pod counts.
func mergeRolloutStatus(target *orchestrationv1alpha1.RoleStatus, overlay *orchestrationv1alpha1.RoleStatus) {
	if overlay == nil {
		return
	}
	target.CurrentRevision = overlay.CurrentRevision
	target.TargetRevision = overlay.TargetRevision
	target.RolloutState = overlay.RolloutState
	target.UpdateStartedAt = overlay.UpdateStartedAt
	target.RollbackCount = overlay.RollbackCount
	target.FailureReason = overlay.FailureReason
	target.FailureMessage = overlay.FailureMessage
	target.RollbackHistory = overlay.RollbackHistory
}

// setProgressingCondition reflects the per-role rollout states on the RoleSet
// Progressing condition. An active rollback takes precedence over updates.
func setProgressingCondition(status *orchestrationv1alpha1.RoleSetStatus, rs *orchestrationv1alpha1.RoleSet, statuses rolloutStatuses) {
	var progressingRoles, rollingBackRoles []string
	for _, role := range rs.Spec.Roles {
		st := statuses[role.Name]
		if st == nil {
			continue
		}
		switch st.RolloutState {
		case orchestrationv1alpha1.RoleRolloutRollingBack:
			rollingBackRoles = append(rollingBackRoles, role.Name)
		case orchestrationv1alpha1.RoleRolloutProgressing:
			progressingRoles = append(progressingRoles, role.Name)
		}
	}
	switch {
	case len(rollingBackRoles) > 0:
		message := fmt.Sprintf("role(s) %s rolling back to the last stable revision", strings.Join(rollingBackRoles, ","))
		if len(progressingRoles) > 0 {
			message = fmt.Sprintf("%s; role(s) %s still updating", message, strings.Join(progressingRoles, ","))
		}
		SetRoleSetCondition(status, *orchestrationctrl.NewCondition(orchestrationv1alpha1.RoleSetProgressing, v1.ConditionTrue, ProgressingRollbackInProgressReason, message))
	case len(progressingRoles) > 0:
		condition := orchestrationctrl.NewCondition(orchestrationv1alpha1.RoleSetProgressing, v1.ConditionTrue, ProgressingRolloutInProgressReason,
			fmt.Sprintf("role(s) %s updating", strings.Join(progressingRoles, ",")))
		SetRoleSetCondition(status, *condition)
	default:
		condition := orchestrationctrl.NewCondition(orchestrationv1alpha1.RoleSetProgressing, v1.ConditionFalse, ProgressingRolloutCompletedReason, "all roles are at their stable revision")
		SetRoleSetCondition(status, *condition)
	}
}
