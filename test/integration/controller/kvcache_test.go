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

package controller

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	orchestrationapi "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
	controllerutils "github.com/vllm-project/aibrix/test/utils/controller"
)

const (
	kvCacheTimeout  = time.Second * 15
	kvCacheInterval = time.Millisecond * 250
	// kvCacheQuietWindow is how long a resource must stay absent before we accept
	// that nothing else re-triggered reconciliation. Reconciles in envtest land in
	// milliseconds, so this leaves a wide margin without slowing the suite.
	kvCacheQuietWindow = time.Second * 1
)

// makeKVCache builds a minimal KVCache. An empty backend leaves the annotation
// unset, which exercises the controller's default-backend path.
func makeKVCache(namespace, name, backend string) *orchestrationapi.KVCache {
	kv := &orchestrationapi.KVCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: orchestrationapi.KVCacheSpec{
			Cache: orchestrationapi.RuntimeSpec{
				Replicas: 1,
				Image:    "aibrix/kvcache:test",
			},
			// spec.service.ports is required by the CRD schema.
			Service: orchestrationapi.ServiceSpec{
				Type: corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{
					{Name: "service", Port: 9600, Protocol: corev1.ProtocolTCP},
				},
			},
		},
	}
	if backend != "" {
		kv.Annotations = map[string]string{
			constants.KVCacheLabelKeyBackend: backend,
		}
	}
	return kv
}

// makeKVCacheWithWatcher builds a KVCache that also runs a watcher Pod.
func makeKVCacheWithWatcher(namespace, name, backend string) *orchestrationapi.KVCache {
	kv := makeKVCache(namespace, name, backend)
	kv.Spec.Watcher = &orchestrationapi.RuntimeSpec{
		Replicas: 1,
		Image:    "aibrix/kvcache-watcher:test",
	}
	return kv
}

var _ = ginkgo.Describe("KVCache controller test", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		ns = nil
		ns = controllerutils.CreateNamespace(ctx, k8sClient, "test-kvcache-", 3*time.Second)
	})

	ginkgo.AfterEach(func() {
		controllerutils.DeleteNamespace(ctx, k8sClient, ns)
	})

	// expectControllerOwnedBy asserts obj carries exactly one owner reference and
	// that it is a controller reference back to the given KVCache.
	expectControllerOwnedBy := func(obj metav1.Object, owner *orchestrationapi.KVCache) {
		// Report failures at the caller's line rather than inside this helper.
		ginkgo.GinkgoHelper()

		refs := obj.GetOwnerReferences()
		gomega.Expect(refs).To(gomega.HaveLen(1))

		ref := refs[0]
		gomega.Expect(ref.Kind).To(gomega.Equal("KVCache"))
		gomega.Expect(ref.APIVersion).To(gomega.Equal(orchestrationapi.GroupVersion.String()))
		gomega.Expect(ref.Name).To(gomega.Equal(owner.Name))
		gomega.Expect(ref.UID).To(gomega.Equal(owner.UID))
		gomega.Expect(ref.Controller).NotTo(gomega.BeNil())
		gomega.Expect(*ref.Controller).To(gomega.BeTrue())
		gomega.Expect(ref.BlockOwnerDeletion).NotTo(gomega.BeNil())
		gomega.Expect(*ref.BlockOwnerDeletion).To(gomega.BeTrue())
	}

	// eventuallyGet waits until the named object exists.
	eventuallyGet := func(name string, obj client.Object) {
		ginkgo.GinkgoHelper()

		gomega.Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: name}, obj)
		}, kvCacheTimeout, kvCacheInterval).Should(gomega.Succeed())
	}

	// consistentlyAbsent asserts the named object is not (re)created for the
	// duration of the quiet window.
	consistentlyAbsent := func(name string, obj client.Object) {
		ginkgo.GinkgoHelper()

		gomega.Consistently(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: name}, obj)
			return apierrors.IsNotFound(err)
		}, kvCacheQuietWindow, kvCacheInterval).Should(gomega.BeTrue())
	}

	// waitForDistributedKVCacheToSettle ensures the initial reconcile has
	// created both distributed resources, then gives owner-triggered Service
	// events a quiet window to drain before tests remove the unwatched
	// StatefulSet. Without this stabilization, a delayed Service-owner reconcile
	// can recreate the StatefulSet before the Pod event under test arrives.
	waitForDistributedKVCacheToSettle := func(name string) {
		ginkgo.GinkgoHelper()

		serviceName := fmt.Sprintf("%s-headless-service", name)
		eventuallyGet(name, &appsv1.StatefulSet{})
		eventuallyGet(serviceName, &corev1.Service{})

		gomega.Consistently(func() error {
			statefulSetKey := types.NamespacedName{Namespace: ns.Name, Name: name}
			if err := k8sClient.Get(ctx, statefulSetKey, &appsv1.StatefulSet{}); err != nil {
				return err
			}
			serviceKey := types.NamespacedName{Namespace: ns.Name, Name: serviceName}
			return k8sClient.Get(ctx, serviceKey, &corev1.Service{})
		}, kvCacheQuietWindow, kvCacheInterval).Should(gomega.Succeed())
	}

	ginkgo.Context("backend selection", func() {
		ginkgo.It("defaults to the vineyard backend when no backend annotation is set", func() {
			kv := makeKVCache(ns.Name, "default-backend", "")
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			// The vineyard backend is the configured default, so it must produce
			// the vineyard deployment and its rpc service without any annotation.
			deploy := &appsv1.Deployment{}
			eventuallyGet(kv.Name, deploy)
			gomega.Expect(deploy.Labels).To(gomega.HaveKeyWithValue(constants.KVCacheLabelKeyIdentifier, kv.Name))
			gomega.Expect(deploy.Labels).To(gomega.HaveKeyWithValue(
				constants.KVCacheLabelKeyRole, constants.KVCacheLabelValueRoleCache))
			gomega.Expect(deploy.Spec.Replicas).NotTo(gomega.BeNil())
			gomega.Expect(*deploy.Spec.Replicas).To(gomega.Equal(int32(1)))
			gomega.Expect(deploy.Spec.Selector.MatchLabels).To(
				gomega.HaveKeyWithValue(constants.KVCacheLabelKeyIdentifier, kv.Name))

			svc := &corev1.Service{}
			eventuallyGet(fmt.Sprintf("%s-rpc", kv.Name), svc)
			// The rpc Service must select the cache pods this KVCache owns.
			gomega.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(constants.KVCacheLabelKeyIdentifier, kv.Name))
			gomega.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
				constants.KVCacheLabelKeyRole, constants.KVCacheLabelValueRoleCache))

			// The default path must not fall through to a distributed backend.
			sts := &appsv1.StatefulSet{}
			consistentlyAbsent(kv.Name, sts)
		})

		ginkgo.It("produces the same resources when vineyard is requested explicitly", func() {
			kv := makeKVCache(ns.Name, "explicit-vineyard", constants.KVCacheBackendVineyard)
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			deploy := &appsv1.Deployment{}
			eventuallyGet(kv.Name, deploy)

			svc := &corev1.Service{}
			eventuallyGet(fmt.Sprintf("%s-rpc", kv.Name), svc)
		})

		ginkgo.It("records an UnsupportedBackend condition and creates nothing for an unsupported backend", func() {
			kv := makeKVCache(ns.Name, "bad-backend", "not-a-real-backend")
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			// The manager drives the reconcile: an unsupported backend is a
			// configuration error, so the reason is written to the object's
			// conditions instead of being hot-retried as an error.
			gomega.Eventually(func(g gomega.Gomega) {
				fetched := &orchestrationapi.KVCache{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: kv.Name}, fetched)).
					To(gomega.Succeed())
				ready := meta.FindStatusCondition(fetched.Status.Conditions, orchestrationapi.KVCacheConditionReady)
				g.Expect(ready).NotTo(gomega.BeNil())
				g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(gomega.Equal("UnsupportedBackend"))
				g.Expect(ready.Message).To(gomega.ContainSubstring("not-a-real-backend"))
				progressing := meta.FindStatusCondition(fetched.Status.Conditions, orchestrationapi.KVCacheConditionProgressing)
				g.Expect(progressing).NotTo(gomega.BeNil())
				g.Expect(progressing.Status).To(gomega.Equal(metav1.ConditionFalse))
			}, kvCacheTimeout, kvCacheInterval).Should(gomega.Succeed())

			// An unsupported backend must be inert, not partially applied.
			consistentlyAbsent(kv.Name, &appsv1.Deployment{})
			consistentlyAbsent(kv.Name, &appsv1.StatefulSet{})
			consistentlyAbsent(fmt.Sprintf("%s-rpc", kv.Name), &corev1.Service{})

			// Stop the manager's retry loop so it cannot add noise to later specs.
			gomega.Expect(k8sClient.Delete(ctx, kv)).To(gomega.Succeed())
		})
	})

	ginkgo.Context("owned resource ownership", func() {
		ginkgo.It("sets controller owner references on the vineyard Deployment and Service", func() {
			kv := makeKVCache(ns.Name, "owner-vineyard", constants.KVCacheBackendVineyard)
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			deploy := &appsv1.Deployment{}
			eventuallyGet(kv.Name, deploy)
			expectControllerOwnedBy(deploy, kv)

			svc := &corev1.Service{}
			eventuallyGet(fmt.Sprintf("%s-rpc", kv.Name), svc)
			expectControllerOwnedBy(svc, kv)
		})

		ginkgo.It("sets controller owner references on the infinistore StatefulSet and Service", func() {
			kv := makeKVCache(ns.Name, "owner-infinistore", constants.KVCacheBackendInfinistore)
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			sts := &appsv1.StatefulSet{}
			eventuallyGet(kv.Name, sts)
			expectControllerOwnedBy(sts, kv)
			gomega.Expect(sts.Labels).To(gomega.HaveKeyWithValue(constants.KVCacheLabelKeyIdentifier, kv.Name))
			gomega.Expect(sts.Labels).To(gomega.HaveKeyWithValue(
				constants.KVCacheLabelKeyRole, constants.KVCacheLabelValueRoleCache))
			gomega.Expect(sts.Spec.Replicas).NotTo(gomega.BeNil())
			gomega.Expect(*sts.Spec.Replicas).To(gomega.Equal(int32(1)))

			svc := &corev1.Service{}
			eventuallyGet(fmt.Sprintf("%s-headless-service", kv.Name), svc)
			expectControllerOwnedBy(svc, kv)
			// The distributed backends front their pods with a headless Service.
			gomega.Expect(svc.Spec.ClusterIP).To(gomega.Equal(corev1.ClusterIPNone))
			gomega.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(constants.KVCacheLabelKeyIdentifier, kv.Name))
		})
	})

	// The controller owns the cache StatefulSet, so StatefulSet events re-enqueue
	// the KVCache and self-heal a deleted workload.
	ginkgo.Context("statefulset-triggered reconciliation", func() {
		ginkgo.It("re-creates a deleted cache StatefulSet through the ownership watch", func() {
			kv := makeKVCache(ns.Name, "sts-trigger", constants.KVCacheBackendInfinistore)
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			waitForDistributedKVCacheToSettle(kv.Name)

			sts := &appsv1.StatefulSet{}
			eventuallyGet(kv.Name, sts)
			gomega.Expect(k8sClient.Delete(ctx, sts)).To(gomega.Succeed())

			// The ownership event re-enqueues the KVCache and the StatefulSet is
			// recreated even with no Pod activity.
			eventuallyGet(kv.Name, &appsv1.StatefulSet{})
		})
	})

	// The controller watches Pods carrying the KVCache identifier label and maps
	// them back to the named KVCache. Bare Pods (the watcher Pod) are not part of
	// Owns(); the labeled Pod watch is their only event path, so deleting the
	// watcher Pod proves the mapping drives the reconcile. Predicate filtering of
	// unlabeled Pods is covered by Test_podWithLabelFilter in the unit suite.
	ginkgo.Context("pod-triggered reconciliation", func() {
		ginkgo.It("re-creates a deleted watcher Pod through the labeled Pod watch", func() {
			kv := makeKVCacheWithWatcher(ns.Name, "watcher-trigger", constants.KVCacheBackendInfinistore)
			gomega.Expect(k8sClient.Create(ctx, kv)).To(gomega.Succeed())

			waitForDistributedKVCacheToSettle(kv.Name)
			watcherName := fmt.Sprintf("%s-kvcache-watcher-pod", kv.Name)
			watcher := &corev1.Pod{}
			eventuallyGet(watcherName, watcher)

			gomega.Expect(k8sClient.Delete(ctx, watcher)).To(gomega.Succeed())

			// The labeled Pod delete event re-enqueues the KVCache and a fresh
			// watcher Pod is created.
			gomega.Eventually(func() types.UID {
				recreated := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: watcherName}, recreated); err != nil {
					return ""
				}
				return recreated.UID
			}, kvCacheTimeout, kvCacheInterval).ShouldNot(gomega.Equal(watcher.UID))
		})

		ginkgo.It("routes the Pod event only to the KVCache named by the label", func() {
			target := makeKVCacheWithWatcher(ns.Name, "route-target", constants.KVCacheBackendInfinistore)
			other := makeKVCacheWithWatcher(ns.Name, "route-other", constants.KVCacheBackendInfinistore)
			gomega.Expect(k8sClient.Create(ctx, target)).To(gomega.Succeed())
			gomega.Expect(k8sClient.Create(ctx, other)).To(gomega.Succeed())

			waitForDistributedKVCacheToSettle(target.Name)
			waitForDistributedKVCacheToSettle(other.Name)

			targetWatcherName := fmt.Sprintf("%s-kvcache-watcher-pod", target.Name)
			otherWatcherName := fmt.Sprintf("%s-kvcache-watcher-pod", other.Name)
			targetWatcher := &corev1.Pod{}
			otherWatcher := &corev1.Pod{}
			eventuallyGet(targetWatcherName, targetWatcher)
			eventuallyGet(otherWatcherName, otherWatcher)
			otherUID := otherWatcher.UID

			gomega.Expect(k8sClient.Delete(ctx, targetWatcher)).To(gomega.Succeed())

			// The target KVCache gets reconciled (its watcher Pod is recreated)...
			gomega.Eventually(func() types.UID {
				recreated := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: targetWatcherName}, recreated); err != nil {
					return ""
				}
				return recreated.UID
			}, kvCacheTimeout, kvCacheInterval).ShouldNot(gomega.Equal(targetWatcher.UID))

			// ...while the other KVCache is not reconciled (its watcher Pod is
			// untouched).
			unchanged := &corev1.Pod{}
			gomega.Consistently(func(g gomega.Gomega) {
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: otherWatcherName}, unchanged)).
					To(gomega.Succeed())
				g.Expect(unchanged.UID).To(gomega.Equal(otherUID))
			}, kvCacheQuietWindow, kvCacheInterval).Should(gomega.Succeed())
		})
	})
})
