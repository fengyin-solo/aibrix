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
	"errors"
	"fmt"
	"reflect"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type BackendReconciler interface {
	Reconcile(ctx context.Context, kv *orchestrationv1alpha1.KVCache) (reconcile.Result, error)
	// CleanupForeignResources removes resources created by the other backends when
	// the backend annotation of a KVCache changes. Resources owned by the current
	// backend are left untouched.
	CleanupForeignResources(ctx context.Context, kv *orchestrationv1alpha1.KVCache) error
}

type BaseReconciler struct {
	client.Client
}

// ReconcilePodObject creates the desired Pod or converges an existing one.
//
// Only a Pod's container image is mutable after creation; env vars, resources,
// probes, volumes (and volume mounts), image pull policy, command/args, ports
// and security context are immutable. Drift on an immutable field is converged
// by deleting the found Pod and recreating it from the desired spec.
func (r *BaseReconciler) ReconcilePodObject(ctx context.Context, desired *corev1.Pod) error {
	found := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new Pod", "Pod.Namespace", desired.Namespace, "Pod.Name", desired.Name)
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}

	// A Pod that is still terminating cannot be replaced with a new Pod carrying
	// the same name yet. Its final delete event re-enqueues the KVCache.
	if found.DeletionTimestamp != nil {
		return nil
	}

	// Normalize both sides with the defaults kube-apiserver applies on write, so
	// drift detection is identical against a live cluster and the defaulter-free
	// fake client.
	normalizePodSpecDefaults(&desired.Spec)
	normalizePodSpecDefaults(&found.Spec)

	if !podManagedFieldsEqual(desired, found) {
		if !podContainerImagesEqual(desired, found) {
			// Image is mutable: update it in place whenever it is the only drift.
			if podOnlyImageDrifts(desired, found) {
				applyContainerImages(desired, found)
				klog.InfoS("Updating Pod", "Pod.Namespace", found.Namespace, "Pod.Name", found.Name)
				return r.Update(ctx, found)
			}
		}

		// Immutable fields drifted; the Pod must be recreated to pick them up.
		klog.InfoS("Recreating Pod to converge immutable pod spec fields",
			"Pod.Namespace", found.Namespace, "Pod.Name", found.Name)
		if err := r.Delete(ctx, found); err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		// Against the fake client (and once kube-apiserver has finished removing the
		// Pod) the object is already gone; recreate it immediately. In a real
		// cluster the Pod may still be terminating here, in which case we wait for
		// the delete event to re-enqueue reconciliation.
		check := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, check); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return r.Create(ctx, desired)
	}

	return nil
}

func (r *BaseReconciler) ReconcileDeploymentObject(ctx context.Context, desired *appsv1.Deployment) error {
	found := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating Deployment", "Name", desired.Name)
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}

	// Normalize the stored template with the defaults kube-apiserver applies on
	// write, so repeated reconciles do not churn against a defaulter-free client.
	normalizePodSpecDefaults(&found.Spec.Template.Spec)

	changed := false
	if replicasChanged(desired.Spec.Replicas, found.Spec.Replicas) {
		replicas := *desired.Spec.Replicas
		found.Spec.Replicas = &replicas
		changed = true
	}
	if convergePodTemplate(&desired.Spec.Template, &found.Spec.Template) {
		changed = true
	}

	if changed {
		klog.InfoS("Updating Deployment", "Name", desired.Name)
		return r.Update(ctx, found)
	}
	return nil
}

func (r *BaseReconciler) ReconcileServiceObject(ctx context.Context, service *corev1.Service) error {
	found := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new Service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
		return r.Create(ctx, service)
	} else if err != nil {
		return err
	}

	// Update the found object and write the result back if there are any changes
	if needsUpdateService(service, found) {
		found.Spec.Ports = service.Spec.Ports
		found.Spec.Selector = service.Spec.Selector
		found.Spec.Type = service.Spec.Type
		klog.InfoS("Updating Service", "Service.Namespace", found.Namespace, "Service.Name", found.Name)
		return r.Update(ctx, found)
	}

	return nil
}

func (r *BaseReconciler) ReconcileStatefulsetObject(ctx context.Context, sts *appsv1.StatefulSet) error {
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new StatefulSet", "Sts.Namespace", sts.Namespace, "Sts.Name", sts.Name)
		return r.Create(ctx, sts)
	} else if err != nil {
		return err
	}

	// Normalize the stored template with the defaults kube-apiserver applies on
	// write, so repeated reconciles do not churn against a defaulter-free client.
	normalizePodSpecDefaults(&found.Spec.Template.Spec)

	changed := false
	if replicasChanged(sts.Spec.Replicas, found.Spec.Replicas) {
		replicas := *sts.Spec.Replicas
		found.Spec.Replicas = &replicas
		changed = true
	}
	if convergePodTemplate(&sts.Spec.Template, &found.Spec.Template) {
		changed = true
	}

	if changed {
		klog.InfoS("Updating Statefulset", "Sts.Namespace", found.Namespace, "Sts.Name", found.Name)
		return r.Update(ctx, found)
	}

	return nil
}

func (r *BaseReconciler) reconcileWatcherPodServiceAccount(ctx context.Context, sa *corev1.ServiceAccount) error {
	found := &corev1.ServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{Name: sa.Name, Namespace: sa.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new ServiceAccount", "SA.Namespace", sa.Namespace, "SA.Name", sa.Name)
		return r.Create(ctx, sa)
	} else if err != nil {
		return err
	}
	return nil
}

func (r *BaseReconciler) reconcileWatcherPodRole(ctx context.Context, role *rbacv1.Role) error {
	found := &rbacv1.Role{}
	err := r.Get(ctx, types.NamespacedName{Name: role.Name, Namespace: role.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new Role", "Role.Namespace", role.Namespace, "Role.Name", role.Name)
		return r.Create(ctx, role)
	} else if err != nil {
		return err
	}
	return nil
}

func (r *BaseReconciler) reconcileWatcherPodRoleBinding(ctx context.Context, rolebinding *rbacv1.RoleBinding) error {
	found := &rbacv1.RoleBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: rolebinding.Name, Namespace: rolebinding.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		klog.InfoS("Creating a new RoleBinding", "RoleBinding.Namespace", rolebinding.Namespace, "RoleBinding.Name", rolebinding.Name)
		return r.Create(ctx, rolebinding)
	} else if err != nil {
		return err
	}
	return nil
}

// deleteObjectIfExists deletes obj, treating NotFound as success so repeated
// reconciles stay idempotent.
func (r *BaseReconciler) deleteObjectIfExists(ctx context.Context, obj client.Object) error {
	if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete %s %s/%s: %w",
			obj.GetObjectKind().GroupVersionKind().Kind, obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

// cleanupWatcherResources removes the watcher Pod and the ServiceAccount, Role
// and RoleBinding created for it. It is invoked when spec.watcher is removed.
func (r *BaseReconciler) cleanupWatcherResources(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) error {
	namespace := kvCache.Namespace
	objects := []client.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: watcherPodName(kvCache.Name), Namespace: namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: kvCache.Name, Namespace: namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: kvCache.Name, Namespace: namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: kvCache.Name, Namespace: namespace}},
	}
	var errs []error
	for _, obj := range objects {
		if err := r.deleteObjectIfExists(ctx, obj); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// needsUpdateService checks if the service spec of the new service differs from the existing one
func needsUpdateService(service, found *corev1.Service) bool {
	// Compare relevant spec fields
	return !reflect.DeepEqual(service.Spec.Ports, found.Spec.Ports) ||
		!reflect.DeepEqual(service.Spec.Selector, found.Spec.Selector) ||
		service.Spec.Type != found.Spec.Type
}

func replicasChanged(desired, found *int32) bool {
	if desired == nil || found == nil {
		return desired != found
	}
	return *desired != *found
}

// convergePodTemplate applies the managed fields of the desired pod template to
// the found pod template and reports whether anything changed. Only fields the
// KVCache spec drives are converged; fields the API server populates with
// defaults (probe thresholds, port protocol, volume defaultMode) are normalized
// on the desired side first, so a stable object does not trigger repeated
// updates.
func convergePodTemplate(desired, found *corev1.PodTemplateSpec) bool {
	changed := false

	if mergeStringMap(desired.Labels, &found.Labels) {
		changed = true
	}
	if mergeStringMap(desired.Annotations, &found.Annotations) {
		changed = true
	}

	if convergeContainers(desired, found) {
		changed = true
	}

	desiredPodSpec := desired.Spec.DeepCopy()
	normalizePodSpecDefaults(desiredPodSpec)

	if !apiequality.Semantic.DeepEqual(found.Spec.Volumes, desiredPodSpec.Volumes) {
		found.Spec.Volumes = desiredPodSpec.Volumes
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(found.Spec.Affinity, desiredPodSpec.Affinity) {
		found.Spec.Affinity = desiredPodSpec.Affinity
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(found.Spec.Tolerations, desiredPodSpec.Tolerations) {
		found.Spec.Tolerations = desiredPodSpec.Tolerations
		changed = true
	}
	if found.Spec.HostIPC != desiredPodSpec.HostIPC {
		found.Spec.HostIPC = desiredPodSpec.HostIPC
		changed = true
	}
	if found.Spec.ServiceAccountName != desiredPodSpec.ServiceAccountName {
		found.Spec.ServiceAccountName = desiredPodSpec.ServiceAccountName
		changed = true
	}

	return changed
}

// convergeContainers matches containers by their index and converges every
// managed container field in place. The container list itself is fully
// controller-managed, so a container count change replaces the whole list.
func convergeContainers(desired, found *corev1.PodTemplateSpec) bool {
	if len(desired.Spec.Containers) != len(found.Spec.Containers) {
		containers := make([]corev1.Container, len(desired.Spec.Containers))
		for i := range desired.Spec.Containers {
			normalizeContainerDefaults(&desired.Spec.Containers[i])
			containers[i] = *desired.Spec.Containers[i].DeepCopy()
		}
		found.Spec.Containers = containers
		return true
	}

	changed := false
	for i := range desired.Spec.Containers {
		if convergeContainer(&desired.Spec.Containers[i], &found.Spec.Containers[i]) {
			changed = true
		}
	}
	return changed
}

func convergeContainer(desired, found *corev1.Container) bool {
	changed := false

	if found.Image != desired.Image {
		found.Image = desired.Image
		changed = true
	}
	if found.ImagePullPolicy != desired.ImagePullPolicy {
		found.ImagePullPolicy = desired.ImagePullPolicy
		changed = true
	}

	normalizeContainerDefaults(desired)

	containerFields := []struct {
		desired interface{}
		found   interface{}
		assign  func()
	}{
		{desired.Command, found.Command, func() { found.Command = desired.Command }},
		{desired.Args, found.Args, func() { found.Args = desired.Args }},
		{desired.WorkingDir, found.WorkingDir, func() { found.WorkingDir = desired.WorkingDir }},
		{desired.Env, found.Env, func() { found.Env = desired.Env }},
		{desired.EnvFrom, found.EnvFrom, func() { found.EnvFrom = desired.EnvFrom }},
		{desired.Resources, found.Resources, func() { found.Resources = desired.Resources }},
		{desired.Ports, found.Ports, func() { found.Ports = desired.Ports }},
		{desired.SecurityContext, found.SecurityContext, func() { found.SecurityContext = desired.SecurityContext }},
		{desired.ReadinessProbe, found.ReadinessProbe, func() { found.ReadinessProbe = desired.ReadinessProbe }},
		{desired.LivenessProbe, found.LivenessProbe, func() { found.LivenessProbe = desired.LivenessProbe }},
		{desired.StartupProbe, found.StartupProbe, func() { found.StartupProbe = desired.StartupProbe }},
		{desired.VolumeMounts, found.VolumeMounts, func() { found.VolumeMounts = desired.VolumeMounts }},
	}
	for _, field := range containerFields {
		if !apiequality.Semantic.DeepEqual(field.desired, field.found) {
			field.assign()
			changed = true
		}
	}

	return changed
}

// mergeStringMap converges a string map, treating nil and an empty map as
// equivalent. It returns whether the target changed.
func mergeStringMap(desired map[string]string, found *map[string]string) bool {
	if len(desired) == 0 {
		if len(*found) == 0 {
			return false
		}
		*found = nil
		return true
	}
	equal := len(*found) == len(desired)
	if equal {
		for k, v := range desired {
			if (*found)[k] != v {
				equal = false
				break
			}
		}
	}
	if equal {
		return false
	}
	merged := make(map[string]string, len(desired))
	for k, v := range desired {
		merged[k] = v
	}
	*found = merged
	return true
}

// podManagedFieldsEqual compares the controller-managed pod spec fields between
// a desired and a found Pod.
func podManagedFieldsEqual(desired, found *corev1.Pod) bool {
	if !containersManagedEqual(desired.Spec.Containers, found.Spec.Containers) {
		return false
	}
	if !apiequality.Semantic.DeepEqual(desired.Spec.Volumes, found.Spec.Volumes) {
		return false
	}
	if desired.Spec.ServiceAccountName != found.Spec.ServiceAccountName {
		return false
	}
	return true
}

// podOnlyImageDrifts reports whether the only managed-field difference between
// desired and found is the container image, which is mutable on an existing Pod.
func podOnlyImageDrifts(desired, found *corev1.Pod) bool {
	return !podContainerImagesEqual(desired, found) &&
		podNonImageContainerFieldsEqual(desired.Spec.Containers, found.Spec.Containers) &&
		apiequality.Semantic.DeepEqual(desired.Spec.Volumes, found.Spec.Volumes) &&
		desired.Spec.ServiceAccountName == found.Spec.ServiceAccountName
}

func podContainerImagesEqual(desired, found *corev1.Pod) bool {
	if len(desired.Spec.Containers) != len(found.Spec.Containers) {
		return false
	}
	for i := range desired.Spec.Containers {
		if desired.Spec.Containers[i].Image != found.Spec.Containers[i].Image {
			return false
		}
	}
	return true
}

func applyContainerImages(desired, found *corev1.Pod) {
	for i := range desired.Spec.Containers {
		if i < len(found.Spec.Containers) {
			found.Spec.Containers[i].Image = desired.Spec.Containers[i].Image
		}
	}
}

// containersManagedEqual compares the full set of managed container fields.
func containersManagedEqual(desired, found []corev1.Container) bool {
	if len(desired) != len(found) {
		return false
	}
	for i := range desired {
		d := desired[i].DeepCopy()
		f := found[i].DeepCopy()
		normalizeContainerDefaults(d)
		normalizeContainerDefaults(f)
		if !apiequality.Semantic.DeepEqual(*d, *f) {
			return false
		}
	}
	return true
}

// podNonImageContainerFieldsEqual compares every managed container field except
// Image.
func podNonImageContainerFieldsEqual(desired, found []corev1.Container) bool {
	if len(desired) != len(found) {
		return false
	}
	for i := range desired {
		d := desired[i].DeepCopy()
		f := found[i].DeepCopy()
		d.Image = ""
		f.Image = ""
		normalizeContainerDefaults(d)
		normalizeContainerDefaults(f)
		if !apiequality.Semantic.DeepEqual(*d, *f) {
			return false
		}
	}
	return true
}

// normalizePodSpecDefaults fills the defaults that the API server assigns to the
// controller-managed pod spec fields, making drift detection idempotent.
func normalizePodSpecDefaults(spec *corev1.PodSpec) {
	for i := range spec.Containers {
		normalizeContainerDefaults(&spec.Containers[i])
	}
	for i := range spec.Volumes {
		normalizeVolumeDefaults(&spec.Volumes[i])
	}
}

func normalizeContainerDefaults(c *corev1.Container) {
	for i := range c.Ports {
		if c.Ports[i].Protocol == "" {
			c.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	if c.TerminationMessagePath == "" {
		c.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if c.TerminationMessagePolicy == "" {
		c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	normalizeEnvDefaults(c.Env)
	normalizeProbeDefaults(c.ReadinessProbe)
	normalizeProbeDefaults(c.LivenessProbe)
	normalizeProbeDefaults(c.StartupProbe)
}

// normalizeEnvDefaults mirrors the API server defaulting of downward-API field
// selectors inside env vars.
func normalizeEnvDefaults(envs []corev1.EnvVar) {
	for i := range envs {
		if envs[i].ValueFrom == nil {
			continue
		}
		if envs[i].ValueFrom.FieldRef != nil && envs[i].ValueFrom.FieldRef.APIVersion == "" {
			envs[i].ValueFrom.FieldRef.APIVersion = "v1"
		}
	}
}

// normalizeProbeDefaults mirrors the probe defaults applied by the API server
// (see k8s.io/api/core/v1 defaults).
func normalizeProbeDefaults(p *corev1.Probe) {
	if p == nil {
		return
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 1
	}
	if p.PeriodSeconds == 0 {
		p.PeriodSeconds = 10
	}
	if p.SuccessThreshold == 0 {
		p.SuccessThreshold = 1
	}
	if p.FailureThreshold == 0 {
		p.FailureThreshold = 3
	}
	if p.HTTPGet != nil && p.HTTPGet.Scheme == "" {
		p.HTTPGet.Scheme = corev1.URISchemeHTTP
	}
}

// normalizeVolumeDefaults mirrors the defaults applied by the API server for
// the volume sources the KVCache templates may carry.
func normalizeVolumeDefaults(v *corev1.Volume) {
	var defaultMode int32 = 0644
	switch {
	case v.Secret != nil:
		if v.Secret.DefaultMode == nil {
			v.Secret.DefaultMode = &defaultMode
		}
	case v.ConfigMap != nil:
		if v.ConfigMap.DefaultMode == nil {
			v.ConfigMap.DefaultMode = &defaultMode
		}
	case v.Projected != nil:
		if v.Projected.DefaultMode == nil {
			v.Projected.DefaultMode = &defaultMode
		}
	case v.DownwardAPI != nil:
		if v.DownwardAPI.DefaultMode == nil {
			v.DownwardAPI.DefaultMode = &defaultMode
		}
		for i := range v.DownwardAPI.Items {
			if v.DownwardAPI.Items[i].FieldRef != nil && v.DownwardAPI.Items[i].FieldRef.APIVersion == "" {
				v.DownwardAPI.Items[i].FieldRef.APIVersion = "v1"
			}
		}
	}
}

type KVCacheBackend interface {
	Name() string
	ValidateObject(*orchestrationv1alpha1.KVCache) error
	BuildMetadataPod(*orchestrationv1alpha1.KVCache) *corev1.Pod
	BuildMetadataService(*orchestrationv1alpha1.KVCache) *corev1.Service
	BuildWatcherPodServiceAccount(*orchestrationv1alpha1.KVCache) *corev1.ServiceAccount
	BuildWatcherPodRole(*orchestrationv1alpha1.KVCache) *rbacv1.Role
	BuildWatcherPodRoleBinding(*orchestrationv1alpha1.KVCache) *rbacv1.RoleBinding
	BuildWatcherPod(*orchestrationv1alpha1.KVCache) *corev1.Pod
	BuildCacheStatefulSet(*orchestrationv1alpha1.KVCache) *appsv1.StatefulSet
	BuildService(*orchestrationv1alpha1.KVCache) *corev1.Service
}
