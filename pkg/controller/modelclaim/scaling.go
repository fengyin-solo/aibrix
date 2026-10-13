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
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
)

// Multi-replica convergence primitives. The reconcile loop drives the claim
// through three safety-ordered steps every pass:
//
//  1. activate replacements/extra replicas on DISTINCT warm pods first;
//  2. only abandon a terminal-failed instance without a replacement once it is
//     clear no distinct pod can take it (same-pod retry path);
//  3. stop surplus instances, never taking a serveable instance down while it
//     would leave fewer than desired ready engines.
//
// The selection rules in this file are pure functions so the ordering and
// guards are table-tested without a kube API or runtime fakes.

func countPhase(pm *modelv1alpha1.ModelClaim, phase modelv1alpha1.ModelClaimPhase) int {
	count := 0
	for i := range pm.Status.Instances {
		if pm.Status.Instances[i].Phase == phase {
			count++
		}
	}
	return count
}

// scaleDownVictimRank orders surplus-instance removal: terminal failures are
// reaped first, then non-routable sleeping/booting instances, and healthy
// engines last. Unknown phases are treated as non-ready.
func scaleDownVictimRank(phase modelv1alpha1.ModelClaimPhase) int {
	switch phase {
	case modelv1alpha1.ModelClaimFailed:
		return 0
	case modelv1alpha1.ModelClaimSleeping:
		return 1
	case modelv1alpha1.ModelClaimActive:
		return 3
	default: // Activating and any not-yet-classified phase
		return 2
	}
}

// selectScaleDownVictim chooses which surplus instance to stop, or returns -1
// when stopping must wait.
//
// Precondition: len(instances) > desired (there is at least one surplus slot).
// The deterministic victim is the lowest-ranked phase; within a phase the pod
// with the lexicographically later name is removed first, so the kept set is
// the earliest-named stable across passes.
//
// Guards:
//   - An Active victim may only be stopped when another ready engine remains
//     at/above desired: the only (or last-required) healthy instance is never
//     stopped first.
//   - A Failed victim is removed once ready instances already satisfy desired
//     (its replacement finished activating); while the claim is still short on
//     ready engines a failed record is either replaced on another pod or
//     handled by the same-pod retry path, never reaped here.
//   - A Sleeping/Activating victim may be removed only while enough other
//     non-failed slots remain to satisfy desired: a replacement still booting
//     for a gated Failed instance is protected, so the old instance is never
//     stopped before its replacement can take over.
func selectScaleDownVictim(instances []modelv1alpha1.ModelClaimInstance, desired int32) int {
	ready := int32(countPhaseList(instances, modelv1alpha1.ModelClaimActive))
	failed := 0
	for i := range instances {
		if instances[i].Phase == modelv1alpha1.ModelClaimFailed {
			failed++
		}
	}
	nonFailed := int32(len(instances) - failed)
	victim := -1
	victimRank := 0
	for i := range instances {
		rank := scaleDownVictimRank(instances[i].Phase)
		switch {
		case rank == 3 && ready <= desired:
			// Stopping this healthy instance would leave fewer than desired
			// ready engines. Never do that while a surplus convergence runs.
			continue
		case rank == 0 && ready < desired:
			// Replacement has not finished activating yet; keep the failed
			// record so activation logic does not launch a duplicate.
			continue
		case (rank == 1 || rank == 2) && nonFailed-1 < desired:
			// This sleeping/booting slot is still needed to reach desired
			// (e.g. it is the in-flight replacement for a failed instance).
			continue
		}
		if victim == -1 || rank < victimRank ||
			(rank == victimRank && instances[i].Pod > instances[victim].Pod) {
			victim = i
			victimRank = rank
		}
	}
	return victim
}

func countPhaseList(instances []modelv1alpha1.ModelClaimInstance, phase modelv1alpha1.ModelClaimPhase) int {
	count := 0
	for i := range instances {
		if instances[i].Phase == phase {
			count++
		}
	}
	return count
}

// removeInstanceAt drops the instance at idx while preserving order of the
// remaining entries (status order is used as a stable identity by tests and
// by scale-down's deterministic tie-breaks).
func removeInstanceAt(instances []modelv1alpha1.ModelClaimInstance, idx int) []modelv1alpha1.ModelClaimInstance {
	return append(instances[:idx], instances[idx+1:]...)
}
