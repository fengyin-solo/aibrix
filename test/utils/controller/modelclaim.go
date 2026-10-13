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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modelapi "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
	modelclaimcontroller "github.com/vllm-project/aibrix/pkg/controller/modelclaim"
)

const modelClaimRuntimeListenPort = modelclaimcontroller.DefaultRuntimePort

// ModelClaimFixture owns the client dependencies and a fleet of per-pod fake
// runtimes for ModelClaim scenarios. Every warm pod gets its own runtime
// simulation on a distinct loopback alias (127.0.0.x:8080), so multi-replica
// placements observe independent per-pod engine state.
type ModelClaimFixture struct {
	ctx      context.Context
	client   client.Client
	timeout  time.Duration
	interval time.Duration

	mu           sync.Mutex
	pods         map[string]*fakePodRuntime
	defaultPhase string
	defaultReady bool
	nextPort     int32
	nextAlias    int

	activateFailures   int
	deactivateFailures int
}

// NewModelClaimFixture configures polling helpers and an empty runtime fleet.
func NewModelClaimFixture(ctx context.Context, c client.Client, timeout, interval time.Duration) *ModelClaimFixture {
	ginkgo.GinkgoHelper()
	f := &ModelClaimFixture{
		ctx:          ctx,
		client:       c,
		timeout:      timeout,
		interval:     interval,
		pods:         map[string]*fakePodRuntime{},
		defaultPhase: "active",
		defaultReady: true,
		nextPort:     19000,
		nextAlias:    2, // 127.0.0.2, .3, ... each bound to the runtime port
	}
	ginkgo.DeferCleanup(f.Close)
	return f
}

// Runtime returns the fleet-wide facade used to steer and observe all per-pod
// fake runtimes.
func (f *ModelClaimFixture) Runtime() *FakeModelClaimFleet {
	ginkgo.GinkgoHelper()
	return &FakeModelClaimFleet{fixture: f}
}

// Close stops every per-pod fake runtime.
func (f *ModelClaimFixture) Close() {
	ginkgo.GinkgoHelper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rt := range f.pods {
		rt.close()
	}
	f.pods = map[string]*fakePodRuntime{}
}

// CreateClaim creates a ModelClaim; the trailing variadic sets spec.replicas
// (defaults to 1).
func (f *ModelClaimFixture) CreateClaim(
	namespace, name, pool string,
	modelName *string,
	engineArgs map[string]string,
	replicas ...int32,
) *modelapi.ModelClaim {
	ginkgo.GinkgoHelper()
	claim := &modelapi.ModelClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: modelapi.ModelClaimSpec{
			ModelName:   modelName,
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{constants.ModelPoolLabelName: pool}},
			ArtifactURL: "huggingface://integration/" + name,
			Engine:      "vllm",
		},
	}
	if len(replicas) > 0 {
		r := replicas[0]
		claim.Spec.Replicas = &r
	}
	if engineArgs != nil {
		claim.Spec.EngineConfig = &modelapi.ModelClaimEngineConfig{Args: engineArgs}
	}
	gomega.Expect(f.client.Create(f.ctx, claim)).To(gomega.Succeed())
	gomega.Expect(claim.UID).NotTo(gomega.BeEmpty())
	return claim
}

// CreateWarmPod creates a running pool pod backed by its own fake runtime.
func (f *ModelClaimFixture) CreateWarmPod(namespace, name, pool string) *corev1.Pod {
	ginkgo.GinkgoHelper()
	rt := f.newPodRuntime(name)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				constants.ModelPoolLabelName:    pool,
				constants.ModelPoolLabelEnabled: constants.ModelPoolLabelEnabledValue,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runtime", Image: "aibrix-runtime"}}},
	}
	gomega.Expect(f.client.Create(f.ctx, pod)).To(gomega.Succeed())
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = rt.ip
	gomega.Expect(f.client.Status().Update(f.ctx, pod)).To(gomega.Succeed())
	return pod
}

func (f *ModelClaimFixture) newPodRuntime(name string) *fakePodRuntime {
	f.mu.Lock()
	defer f.mu.Unlock()
	gomega.Expect(f.pods).NotTo(gomega.HaveKey(name), "duplicate warm pod name in fixture")
	ip := "127.0.0." + strconv.Itoa(f.nextAlias)
	f.nextAlias++
	rt := &fakePodRuntime{
		name:         name,
		ip:           ip,
		fixture:      f,
		defaultPhase: f.defaultPhase,
		defaultReady: f.defaultReady,
		models:       map[string]modelclaimcontroller.RuntimeSnapshotModel{},
	}
	rt.server = StartFixedPortHTTPServer(
		ip, modelClaimRuntimeListenPort, rt, 5*time.Second, 50*time.Millisecond,
	)
	f.pods[name] = rt
	return rt
}

func (f *ModelClaimFixture) allocPort() int32 {
	f.nextPort++
	return f.nextPort
}

// runtimeForPod finds the fake runtime behind a pod name.
func (f *ModelClaimFixture) runtimeForPod(name string) (*fakePodRuntime, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rt, ok := f.pods[name]
	return rt, ok
}

func (f *ModelClaimFixture) forEachPodRuntime(fn func(*fakePodRuntime)) {
	f.mu.Lock()
	names := make([]string, 0, len(f.pods))
	for name := range f.pods {
		names = append(names, name)
	}
	sort.Strings(names)
	runtimes := make([]*fakePodRuntime, 0, len(names))
	for _, name := range names {
		runtimes = append(runtimes, f.pods[name])
	}
	f.mu.Unlock()
	for _, rt := range runtimes {
		fn(rt)
	}
}

// GetClaim retrieves the latest claim using the supplied polling assertions.
func (f *ModelClaimFixture) GetClaim(g gomega.Gomega, claim *modelapi.ModelClaim) *modelapi.ModelClaim {
	ginkgo.GinkgoHelper()
	latest := &modelapi.ModelClaim{}
	g.Expect(f.client.Get(f.ctx, client.ObjectKeyFromObject(claim), latest)).To(gomega.Succeed())
	return latest
}

// TriggerReconcile updates a claim annotation to trigger reconciliation.
func (f *ModelClaimFixture) TriggerReconcile(claim *modelapi.ModelClaim) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		latest := &modelapi.ModelClaim{}
		if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(claim), latest); err != nil {
			return err
		}
		patch := client.MergeFrom(latest.DeepCopy())
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations["test.aibrix.ai/reconcile"] = fmt.Sprintf("%d", time.Now().UnixNano())
		if err := f.client.Patch(f.ctx, latest, patch); err != nil {
			if apierrors.IsConflict(err) {
				return err
			}
			return gomega.StopTrying("patch ModelClaim reconcile annotation").Wrap(err)
		}
		return nil
	}, f.timeout, f.interval).Should(gomega.Succeed())
}

// ExpectRoute checks the pod routing annotation for a claim's port and state.
func (f *ModelClaimFixture) ExpectRoute(
	g gomega.Gomega,
	namespace, podName, claimName string,
	port int32,
	state string,
) {
	ginkgo.GinkgoHelper()
	pod := &corev1.Pod{}
	g.Expect(f.client.Get(f.ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)).To(gomega.Succeed())
	annotation := pod.Annotations[constants.ModelClaimPodAnnotationPrefix+claimName]
	route, err := decodeModelClaimRouteAnnotation(annotation)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(route.Port).To(gomega.BeNumerically("==", port))
	g.Expect(route.State).To(gomega.Equal(state))
}

type modelClaimRouteAnnotation struct {
	Port  int32  `json:"port"`
	State string `json:"state"`
}

func decodeModelClaimRouteAnnotation(annotation string) (modelClaimRouteAnnotation, error) {
	route := modelClaimRouteAnnotation{}
	err := json.Unmarshal([]byte(annotation), &route)
	return route, err
}

// ExpectEvent waits for an event matching the claim UID, type, and reason.
func (f *ModelClaimFixture) ExpectEvent(claim *modelapi.ModelClaim, eventType, reason string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() bool {
		events := &corev1.EventList{}
		if err := f.client.List(f.ctx, events, client.InNamespace(claim.Namespace)); err != nil {
			return false
		}
		for i := range events.Items {
			event := &events.Items[i]
			if event.InvolvedObject.UID == claim.UID && event.Type == eventType && event.Reason == reason {
				return true
			}
		}
		return false
	}, f.timeout, f.interval).Should(gomega.BeTrue())
}

// fakePodRuntime simulates one warm pod's aibrix-runtime process.
type fakePodRuntime struct {
	name    string
	ip      string
	fixture *ModelClaimFixture
	server  *httptest.Server

	mu           sync.Mutex
	defaultPhase string
	defaultReady bool
	models       map[string]modelclaimcontroller.RuntimeSnapshotModel

	activateCalls   []modelclaimcontroller.ActivateRequest
	deactivateCalls []modelclaimcontroller.DeactivateRequest
}

func (r *fakePodRuntime) close() {
	if r.server != nil {
		r.server.Close()
	}
}

// ServeHTTP handles the runtime activation, deactivate, snapshot, and model-list endpoints.
func (r *fakePodRuntime) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/v1/runtime/models/activate":
		r.handleActivate(w, req)
	case req.Method == http.MethodPost && req.URL.Path == "/v1/runtime/models/deactivate":
		r.handleDeactivate(w, req)
	case req.Method == http.MethodGet && req.URL.Path == "/v1/runtime/snapshot":
		r.handleSnapshot(w)
	case req.Method == http.MethodGet && req.URL.Path == "/v1/runtime/models":
		r.handleModelList(w)
	default:
		http.Error(w, "unsupported fake runtime operation", http.StatusNotFound)
	}
}

func (r *fakePodRuntime) handleActivate(w http.ResponseWriter, req *http.Request) {
	in := modelclaimcontroller.ActivateRequest{}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.activateCalls = append(r.activateCalls, in)

	r.fixture.mu.Lock()
	fail := r.fixture.activateFailures > 0
	if fail {
		r.fixture.activateFailures--
	}
	port := r.fixture.allocPort()
	phase, ready := r.defaultPhase, r.defaultReady
	r.fixture.mu.Unlock()

	if fail {
		http.Error(w, "injected activation failure", http.StatusServiceUnavailable)
		return
	}

	uid := ""
	if in.ClaimRef != nil {
		uid = in.ClaimRef.UID
	}
	r.models[uid] = modelclaimcontroller.RuntimeSnapshotModel{
		ModelName:   in.ModelName,
		ArtifactURL: in.ArtifactURL,
		ClaimRef:    in.ClaimRef,
		Port:        port,
		IPCName:     in.IPCName,
		Phase:       phase,
		Alive:       phase != "failed",
		Ready:       ready,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(modelclaimcontroller.ActivateResponse{
		Status: "success", ModelName: in.ModelName, Port: port, IPCName: in.IPCName,
	})
}

func (r *fakePodRuntime) handleDeactivate(w http.ResponseWriter, req *http.Request) {
	in := modelclaimcontroller.DeactivateRequest{}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.deactivateCalls = append(r.deactivateCalls, in)

	r.fixture.mu.Lock()
	fail := r.fixture.deactivateFailures > 0
	if fail {
		r.fixture.deactivateFailures--
	}
	r.fixture.mu.Unlock()
	if fail {
		http.Error(w, "injected deactivate failure", http.StatusServiceUnavailable)
		return
	}
	for uid, model := range r.models {
		if model.ModelName == in.ModelName {
			delete(r.models, uid)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (r *fakePodRuntime) handleSnapshot(w http.ResponseWriter) {
	r.mu.Lock()
	models := make([]modelclaimcontroller.RuntimeSnapshotModel, 0, len(r.models))
	for _, model := range r.models {
		models = append(models, model)
	}
	r.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(modelclaimcontroller.RuntimeSnapshot{
		ObservedAt: time.Now(),
		Models:     models,
	})
}

func (r *fakePodRuntime) handleModelList(w http.ResponseWriter) {
	r.mu.Lock()
	models := make([]modelclaimcontroller.ModelInfo, 0, len(r.models))
	for _, model := range r.models {
		models = append(models, modelclaimcontroller.ModelInfo{
			ModelName: model.ModelName,
			Port:      model.Port,
			IPCName:   model.IPCName,
			Phase:     model.Phase,
			Ready:     model.Ready,
		})
	}
	r.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Models []modelclaimcontroller.ModelInfo `json:"models"`
	}{Models: models})
}

func (r *fakePodRuntime) setClaimState(uid, phase string, ready bool, lastError string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	model, found := r.models[uid]
	if !found {
		return
	}
	model.Phase = phase
	model.Ready = ready
	model.Alive = phase != "failed"
	model.LastError = lastError
	r.models[uid] = model
}

// FakeModelClaimFleet is the test-facing facade over all per-pod runtimes.
type FakeModelClaimFleet struct {
	fixture *ModelClaimFixture
}

// SetDefaultState sets the initial phase/readiness for future activations.
func (fl *FakeModelClaimFleet) SetDefaultState(phase string, ready bool) {
	fl.fixture.mu.Lock()
	defer fl.fixture.mu.Unlock()
	fl.fixture.defaultPhase = phase
	fl.fixture.defaultReady = ready
}

// FailNextActivations injects failures into the next count activations fleet-wide.
func (fl *FakeModelClaimFleet) FailNextActivations(count int) {
	fl.fixture.mu.Lock()
	defer fl.fixture.mu.Unlock()
	fl.fixture.activateFailures = count
}

// FailNextDeactivations injects failures into the next count deactivations fleet-wide.
func (fl *FakeModelClaimFleet) FailNextDeactivations(count int) {
	fl.fixture.mu.Lock()
	defer fl.fixture.mu.Unlock()
	fl.fixture.deactivateFailures = count
}

// SetClaimState updates runtime state for a claim UID on every hosting pod.
func (fl *FakeModelClaimFleet) SetClaimState(uid, phase string, ready bool, lastError string) {
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.setClaimState(uid, phase, ready, lastError)
	})
}

// SetPodClaimState updates runtime state for a claim UID on one specific pod.
func (fl *FakeModelClaimFleet) SetPodClaimState(podName, uid, phase string, ready bool, lastError string) {
	rt, ok := fl.fixture.runtimeForPod(podName)
	gomega.Expect(ok).To(gomega.BeTrue(), "no fake runtime for pod %s", podName)
	rt.setClaimState(uid, phase, ready, lastError)
}

// ActivateCallCount returns total activation attempts across all pods.
func (fl *FakeModelClaimFleet) ActivateCallCount() int {
	total := 0
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.mu.Lock()
		total += len(rt.activateCalls)
		rt.mu.Unlock()
	})
	return total
}

// DeactivateRequests returns all recorded deactivation requests fleet-wide.
func (fl *FakeModelClaimFleet) DeactivateRequests() []modelclaimcontroller.DeactivateRequest {
	var out []modelclaimcontroller.DeactivateRequest
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.mu.Lock()
		out = append(out, rt.deactivateCalls...)
		rt.mu.Unlock()
	})
	return out
}

// ActivatePodIPs returns pod IPs (deduplicated) that received activations.
func (fl *FakeModelClaimFleet) ActivatePodIPs() []string {
	seen := map[string]struct{}{}
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.mu.Lock()
		if len(rt.activateCalls) > 0 {
			seen[rt.ip] = struct{}{}
		}
		rt.mu.Unlock()
	})
	out := make([]string, 0, len(seen))
	for ip := range seen {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out
}

// ActivatePods returns the names of pods that received at least one activation.
func (fl *FakeModelClaimFleet) ActivatePods() []string {
	seen := map[string]struct{}{}
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.mu.Lock()
		if len(rt.activateCalls) > 0 {
			seen[rt.name] = struct{}{}
		}
		rt.mu.Unlock()
	})
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ClaimUIDs returns unique claim UIDs recorded in activation requests.
func (fl *FakeModelClaimFleet) ClaimUIDs() []string {
	unique := map[string]struct{}{}
	fl.fixture.forEachPodRuntime(func(rt *fakePodRuntime) {
		rt.mu.Lock()
		for i := range rt.activateCalls {
			if rt.activateCalls[i].ClaimRef != nil {
				unique[rt.activateCalls[i].ClaimRef.UID] = struct{}{}
			}
		}
		rt.mu.Unlock()
	})
	out := make([]string, 0, len(unique))
	for uid := range unique {
		out = append(out, uid)
	}
	sort.Strings(out)
	return out
}
