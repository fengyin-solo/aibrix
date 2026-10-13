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

package webhook

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupDeploymentWebhookWithManager registers the webhook for Deployment in the manager.
func SetupDeploymentWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&appsv1.Deployment{}).
		WithValidator(&DeploymentCustomDefaulter{}).
		WithDefaulter(&DeploymentCustomDefaulter{}).
		Complete()
}

type DeploymentCustomDefaulter struct {
}

//+kubebuilder:webhook:path=/mutate-apps-v1-deployment,mutating=true,failurePolicy=ignore,sideEffects=None,groups=apps,resources=deployments,verbs=create;update,versions=v1,name=medeployment.aibrix.ai,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &DeploymentCustomDefaulter{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *DeploymentCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	deployment, ok := obj.(*appsv1.Deployment)
	if !ok {
		return fmt.Errorf("expected a Deployment object but got %T", obj)
	}

	// Only proceed if the sidecar injection annotation is present and set to "true".
	annotations := deployment.GetAnnotations()
	if !sidecarInjectionRequested(annotations) {
		return nil
	}

	settings := runtimeSidecarSettingsFromAnnotations(annotations, annotations)
	for _, warning := range injectRuntimeSidecar(&deployment.Spec.Template.Spec, settings) {
		// The mutating admission response cannot carry warnings, so expose non-fatal
		// problems through logs; the validating webhook surfaces them as admission warnings.
		sidecarInjectionLog.Info(warning)
	}

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// +kubebuilder:webhook:path=/validate-apps-v1-deployment,mutating=false,failurePolicy=ignore,sideEffects=None,groups=apps,resources=deployments,verbs=create;update,versions=v1,name=vedeployment.aibrix.ai,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &DeploymentCustomDefaulter{}

// validate checks the sidecar injection annotations and the pod template that injection
// is enabled on. It is shared by ValidateCreate and ValidateUpdate.
func (r *DeploymentCustomDefaulter) validate(deployment *appsv1.Deployment) (admission.Warnings, error) {
	annotations := deployment.GetAnnotations()
	warnings, err := validateSidecarInjectionSettings(annotations, annotations)
	if err != nil {
		return warnings, err
	}
	if !sidecarInjectionRequested(annotations) {
		// Workloads that do not opt in are completely unaffected.
		return warnings, nil
	}

	settings := runtimeSidecarSettingsFromAnnotations(annotations, annotations)
	warnings = append(warnings, sidecarPodSpecWarnings(&deployment.Spec.Template.Spec, settings.engineType)...)
	return warnings, nil
}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *DeploymentCustomDefaulter) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	deployment, ok := obj.(*appsv1.Deployment)
	if !ok {
		return nil, fmt.Errorf("expected a Deployment object but got %T", obj)
	}
	return r.validate(deployment)
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *DeploymentCustomDefaulter) ValidateUpdate(_ context.Context, _ runtime.Object, newObj runtime.Object) (admission.Warnings, error) {
	deployment, ok := newObj.(*appsv1.Deployment)
	if !ok {
		return nil, fmt.Errorf("expected a Deployment object but got %T", newObj)
	}
	return r.validate(deployment)
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *DeploymentCustomDefaulter) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}
