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

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
)

func inst(pod string, phase modelv1alpha1.ModelClaimPhase) modelv1alpha1.ModelClaimInstance {
	return modelv1alpha1.ModelClaimInstance{Pod: pod, Phase: phase}
}

func TestSelectScaleDownVictimOrderingAndGuards(t *testing.T) {
	tests := []struct {
		name      string
		instances []modelv1alpha1.ModelClaimInstance
		desired   int32
		wantPod   string // "" means no victim (must wait)
	}{
		{
			name:      "failed kept while replacement is not ready",
			instances: []modelv1alpha1.ModelClaimInstance{inst("a", modelv1alpha1.ModelClaimFailed), inst("b", modelv1alpha1.ModelClaimActive)},
			desired:   1,
			wantPod:   "a", // ready=1 already satisfies desired
		},
		{
			name:      "failed gated when no ready replacement",
			instances: []modelv1alpha1.ModelClaimInstance{inst("a", modelv1alpha1.ModelClaimFailed)},
			desired:   1,
			wantPod:   "",
		},
		{
			name: "booting replacement protected while failed waits",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimFailed),
				inst("c", modelv1alpha1.ModelClaimActivating),
			},
			desired: 2,
			wantPod: "", // neither the failed record nor its booting replacement may go
		},
		{
			name: "failed removed after replacement becomes ready",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimFailed),
				inst("c", modelv1alpha1.ModelClaimActive),
			},
			desired: 2,
			wantPod: "b",
		},
		{
			name: "healthy instances never stopped while ready equals desired",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimActive),
			},
			desired: 2,
			wantPod: "", // both are required ready engines
		},
		{
			name: "surplus booting instance removable while desired stays served",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimActivating),
			},
			desired: 1,
			wantPod: "b",
		},
		{
			name: "surplus sleeping instance removable",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimSleeping),
			},
			desired: 1,
			wantPod: "b",
		},
		{
			name: "pure scale down three active to one removes later names first",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("c", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimActive),
			},
			desired: 1,
			wantPod: "c",
		},
		{
			name: "partial availability scale down keeps required ready",
			instances: []modelv1alpha1.ModelClaimInstance{
				inst("a", modelv1alpha1.ModelClaimActive),
				inst("b", modelv1alpha1.ModelClaimActive),
				inst("c", modelv1alpha1.ModelClaimActivating),
			},
			desired: 2,
			wantPod: "c", // non-failed after removal = 2 >= desired
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := selectScaleDownVictim(tc.instances, tc.desired)
			if tc.wantPod == "" {
				assert.Equal(t, -1, idx, "expected convergence to wait")
				return
			}
			assert.GreaterOrEqual(t, idx, 0)
			assert.Equal(t, tc.wantPod, tc.instances[idx].Pod)
		})
	}
}

func TestSelectScaleDownVictimNeverPicksHealthyOverFailed(t *testing.T) {
	// One active engine satisfying desired and one failed surplus record:
	// the failed record is the victim, never the sole healthy instance.
	instances := []modelv1alpha1.ModelClaimInstance{
		inst("a", modelv1alpha1.ModelClaimActive),
		inst("b", modelv1alpha1.ModelClaimFailed),
	}
	idx := selectScaleDownVictim(instances, 1)
	assert.Equal(t, 1, idx)
	assert.Equal(t, "b", instances[idx].Pod)
}
