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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/constants"
)

func warmModelClaimPod(name, namespace string, models map[string]int) *v1.Pod {
	anns := map[string]string{}
	i := 0
	for served, port := range models {
		anns[fmt.Sprintf("%smc%d", constants.ModelClaimPodAnnotationPrefix, i)] =
			fmt.Sprintf(`{"model":%q,"port":%d}`, served, port)
		i++
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      map[string]string{constants.ModelPoolLabelName: "pool-a", constants.ModelPoolLabelEnabled: "true"},
			Annotations: anns,
		},
		Status: v1.PodStatus{
			PodIP:      "10.0.0.9",
			Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
		},
	}
}

func TestAddPodRegistersModelClaim(t *testing.T) {
	c := NewForTest()
	c.addPod(warmModelClaimPod("p1", "default", map[string]int{"qwen-0.5b": 9001}))

	_, exist := c.metaPods.Load("default/p1")
	assert.True(t, exist, "warm pod should be admitted to the cache")

	pods, err := c.ListPodsByModel("qwen-0.5b")
	require.NoError(t, err)
	assert.Equal(t, 1, pods.Len(), "served model should resolve to the warm pod")
}

func TestAddPodMultipleModelClaimsOnePod(t *testing.T) {
	c := NewForTest()
	c.addPod(warmModelClaimPod("p1", "default", map[string]int{"m-a": 9001, "m-b": 9002}))

	a, err := c.ListPodsByModel("m-a")
	require.NoError(t, err)
	assert.Equal(t, 1, a.Len())

	b, err := c.ListPodsByModel("m-b")
	require.NoError(t, err)
	assert.Equal(t, 1, b.Len())
}

func TestUpdatePodAddsAndRemovesModelClaim(t *testing.T) {
	c := NewForTest()
	old := warmModelClaimPod("p1", "default", map[string]int{"m-a": 9001})
	c.addPod(old)

	both := warmModelClaimPod("p1", "default", map[string]int{"m-a": 9001, "m-b": 9002})
	c.updatePod(old, both)
	a, err := c.ListPodsByModel("m-a")
	require.NoError(t, err)
	assert.Equal(t, 1, a.Len())
	b, err := c.ListPodsByModel("m-b")
	require.NoError(t, err)
	assert.Equal(t, 1, b.Len())

	onlyB := warmModelClaimPod("p1", "default", map[string]int{"m-b": 9002})
	c.updatePod(both, onlyB)
	_, err = c.ListPodsByModel("m-a")
	assert.Error(t, err, "m-a should no longer be registered after the annotation is dropped")
	b, err = c.ListPodsByModel("m-b")
	require.NoError(t, err)
	assert.Equal(t, 1, b.Len())
}

func TestModelClaimPortZeroHasNoRoutableBackend(t *testing.T) {
	c := NewForTest()
	pod := warmModelClaimPod("p1", "default", map[string]int{"m-live": 9001})
	pod.Annotations[constants.ModelClaimPodAnnotationPrefix+"sleeping"] =
		`{"model":"m-sleeping","port":0,"state":"sleeping"}`
	c.addPod(pod)

	assert.True(t, c.HasModel("m-live"))
	assert.False(t, c.HasModel("m-sleeping"), "port 0 ModelClaim must have no routable pods")
	_, err := c.ListPodsByModel("m-sleeping")
	assert.Error(t, err)

	provider, ok := any(c).(interface {
		ModelClaimBinding(string) (*v1.Pod, int, string, bool)
	})
	require.True(t, ok, "cache must expose non-routable ModelClaim bindings")
	boundPod, port, state, found := provider.ModelClaimBinding("m-sleeping")
	require.True(t, found)
	assert.Equal(t, pod.Name, boundPod.Name)
	assert.Zero(t, port)
	assert.Equal(t, constants.ModelClaimRoutingStateSleeping, state)
}

func TestModelClaimBindingTracksAnnotationUpdatesAndDeletion(t *testing.T) {
	c := NewForTest()
	active := warmModelClaimPod("p1", "default", map[string]int{"m": 9001})
	c.addPod(active)

	provider, ok := any(c).(interface {
		ModelClaimBinding(string) (*v1.Pod, int, string, bool)
	})
	require.True(t, ok, "cache must expose ModelClaim bindings")
	_, port, state, found := provider.ModelClaimBinding("m")
	require.True(t, found)
	assert.Equal(t, 9001, port)
	assert.Equal(t, constants.ModelClaimRoutingStateActive, state)

	sleeping := warmModelClaimPod("p1", "default", nil)
	sleeping.Annotations[constants.ModelClaimPodAnnotationPrefix+"sleeping"] =
		`{"model":"m","port":0,"state":"sleeping"}`
	c.updatePod(active, sleeping)
	_, port, state, found = provider.ModelClaimBinding("m")
	require.True(t, found)
	assert.Zero(t, port)
	assert.Equal(t, constants.ModelClaimRoutingStateSleeping, state)
	assert.False(t, c.HasModel("m"))

	c.deletePod(sleeping)
	_, _, _, found = provider.ModelClaimBinding("m")
	assert.False(t, found)
}

func TestModelClaimRoutabilityViaAnnotationUpdate(t *testing.T) {
	c := NewForTest()
	live := warmModelClaimPod("p1", "default", map[string]int{"m": 9001})
	c.addPod(live)
	require.True(t, c.HasModel("m"))

	notRoutable := warmModelClaimPod("p1", "default", map[string]int{"m": 0})
	c.updatePod(live, notRoutable)
	assert.False(t, c.HasModel("m"))

	c.updatePod(notRoutable, warmModelClaimPod("p1", "default", map[string]int{"m": 9002}))
	assert.True(t, c.HasModel("m"))
}

// Multi-replica routing: only Active (state=active, port>0) bindings register
// the model as a routable backend; Activating/Sleeping replicas stay out of
// ListPodsByModel even while siblings serve traffic.
func TestModelClaimMultiReplicaOnlyActiveBindingsAreRoutable(t *testing.T) {
	c := NewForTest()
	anno := func(port int, state string) string {
		return fmt.Sprintf(`{"model":"m-multi","port":%d,"state":"%s"}`, port, state)
	}
	mk := func(name, ip string, port int, state string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    map[string]string{constants.ModelPoolLabelName: "pool-a", constants.ModelPoolLabelEnabled: "true"},
				Annotations: map[string]string{
					constants.ModelClaimPodAnnotationPrefix + name: anno(port, state),
				},
			},
			Status: v1.PodStatus{
				PodIP:      ip,
				Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
			},
		}
	}
	activeA := mk("pa", "10.0.0.1", 9001, constants.ModelClaimRoutingStateActive)
	activeB := mk("pb", "10.0.0.2", 9002, constants.ModelClaimRoutingStateActive)
	booting := mk("pc", "10.0.0.3", 0, constants.ModelClaimRoutingStateActivating)
	sleeping := mk("pd", "10.0.0.4", 0, constants.ModelClaimRoutingStateSleeping)
	for _, p := range []*v1.Pod{activeA, activeB, booting, sleeping} {
		c.addPod(p)
	}

	assert.True(t, c.HasModel("m-multi"))
	pods, err := c.ListPodsByModel("m-multi")
	require.NoError(t, err)
	got := pods.All()
	require.Len(t, got, 2)
	names := []string{got[0].Name, got[1].Name}
	assert.ElementsMatch(t, []string{"pa", "pb"}, names)
}

// With no Active replica, the fallback binding prefers a sleeping engine (the
// gateway can wake it) over an activating or failed one.
func TestModelClaimBindingWakePreference(t *testing.T) {
	c := NewForTest()
	mk := func(name, state string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    map[string]string{constants.ModelPoolLabelName: "pool-a", constants.ModelPoolLabelEnabled: "true"},
				Annotations: map[string]string{
					constants.ModelClaimPodAnnotationPrefix + name: fmt.Sprintf(`{"model":"m-mix","port":0,"state":"%s"}`, state),
				},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.9"},
		}
	}
	for _, p := range []*v1.Pod{
		mk("p-failed", constants.ModelClaimRoutingStateFailed),
		mk("p-sleep", constants.ModelClaimRoutingStateSleeping),
		mk("p-boot", constants.ModelClaimRoutingStateActivating),
	} {
		c.addPod(p)
	}

	assert.False(t, c.HasModel("m-mix"), "port-0 bindings never register a routable model")
	provider := any(c).(interface {
		ModelClaimBinding(string) (*v1.Pod, int, string, bool)
	})
	pod, port, state, found := provider.ModelClaimBinding("m-mix")
	require.True(t, found)
	assert.Equal(t, "p-sleep", pod.Name)
	assert.Zero(t, port)
	assert.Equal(t, constants.ModelClaimRoutingStateSleeping, state)

	// Without a sleeping binding, activating wins over failed.
	c = NewForTest()
	for _, p := range []*v1.Pod{
		mk("p-failed", constants.ModelClaimRoutingStateFailed),
		mk("p-boot", constants.ModelClaimRoutingStateActivating),
	} {
		c.addPod(p)
	}
	provider = any(c).(interface {
		ModelClaimBinding(string) (*v1.Pod, int, string, bool)
	})
	pod, _, state, found = provider.ModelClaimBinding("m-mix")
	require.True(t, found)
	assert.Equal(t, "p-boot", pod.Name)
	assert.Equal(t, constants.ModelClaimRoutingStateActivating, state)
}

func TestModelClaimStateClearedOnPodDelete(t *testing.T) {
	c := NewForTest()
	pod := warmModelClaimPod("p1", "default", map[string]int{"m": 0})
	c.addPod(pod)
	require.False(t, c.HasModel("m"))

	c.deletePod(pod)
	assert.False(t, c.HasModel("m"))
}
