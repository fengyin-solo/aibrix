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

package cache

import (
	"sort"
	"sync"

	"github.com/vllm-project/aibrix/pkg/constants"
	v1 "k8s.io/api/core/v1"
)

type modelClaimBinding struct {
	podKey string
	port   int
	state  string
}

// modelClaimState tracks all ModelClaim advertisements, including port-0
// bindings. It never participates in normal model-to-pod routing.
type modelClaimState struct {
	mu       sync.RWMutex
	bindings map[string]map[string]modelClaimBinding
}

func (s *modelClaimState) set(podKey, model string, port int, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings == nil {
		s.bindings = make(map[string]map[string]modelClaimBinding)
	}
	byPod := s.bindings[model]
	if byPod == nil {
		byPod = make(map[string]modelClaimBinding)
		s.bindings[model] = byPod
	}
	byPod[podKey] = modelClaimBinding{podKey: podKey, port: port, state: state}
}

func (s *modelClaimState) clearPod(podKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for model, byPod := range s.bindings {
		delete(byPod, podKey)
		if len(byPod) == 0 {
			delete(s.bindings, model)
		}
	}
}

func (s *modelClaimState) get(model string) []modelClaimBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byPod := s.bindings[model]
	out := make([]modelClaimBinding, 0, len(byPod))
	for _, binding := range byPod {
		out = append(out, binding)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].podKey < out[j].podKey })
	return out
}

// modelClaimWakeStateRank orders the non-routable binding the gateway falls
// back to when a model has no Ready replica: a sleeping engine can be woken,
// so it wins over a still-booting one, then a failed one, then any stray
// active binding. Within one state the lowest podKey wins deterministically.
func modelClaimWakeStateRank(state string) int {
	switch state {
	case constants.ModelClaimRoutingStateSleeping:
		return 0
	case constants.ModelClaimRoutingStateActivating:
		return 1
	case constants.ModelClaimRoutingStateFailed:
		return 2
	case constants.ModelClaimRoutingStateActive:
		return 3
	default:
		return 4
	}
}

// ModelClaimBinding returns one deterministic non-routable advertisement for a
// served model. It is consulted only when no Active replica registers the
// model in the normal routing set; the preferred binding is a sleeping engine
// (so the gateway can trigger a wake), then activating, then failed. With
// multiple replicas sharing one served name the result is deterministic within
// a state (lowest pod key).
func (c *Store) ModelClaimBinding(modelName string) (*v1.Pod, int, string, bool) {
	bestState := 5
	var best modelClaimBinding
	found := false
	// get() returns bindings sorted by pod key ascending, so the first
	// occurrence of the best-ranked state is also the deterministic lowest-key
	// choice within that state.
	for _, binding := range c.modelClaims.get(modelName) {
		if rank := modelClaimWakeStateRank(binding.state); rank < bestState {
			bestState = rank
			best = binding
			found = true
		}
	}
	if !found {
		return nil, 0, "", false
	}
	metaPod, ok := c.metaPods.Load(best.podKey)
	if !ok || metaPod == nil || metaPod.Pod == nil {
		return nil, 0, "", false
	}
	return metaPod.Pod, best.port, best.state, true
}

var _ ModelClaimBindingProvider = (*Store)(nil)
