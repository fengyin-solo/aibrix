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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
)

func tcpPort(name string, port int32) corev1.ContainerPort {
	return corev1.ContainerPort{Name: name, ContainerPort: port, Protocol: corev1.ProtocolTCP}
}

func TestInferEngineEndpoint(t *testing.T) {
	tests := map[string]struct {
		containers []corev1.Container
		want       string
	}{
		"no containers falls back to default endpoint": {
			containers: nil,
			want:       DefaultEngineEndpoint,
		},
		"container without declared ports falls back to 8000": {
			containers: []corev1.Container{{Name: "engine", Image: "vllm/vllm-openai:latest"}},
			want:       DefaultEngineEndpoint,
		},
		"single unnamed port 8080 is selected": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}},
			want:       "http://localhost:8080",
		},
		"single unnamed port 8001 is selected": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{{ContainerPort: 8001}}}},
			want:       "http://localhost:8001",
		},
		"multiple unnamed ports select the smallest": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("", 8080), tcpPort("", 8001),
			}}},
			want: "http://localhost:8001",
		},
		"named inference port wins over a smaller unnamed port": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("inference", 9000), tcpPort("", 8000),
			}}},
			want: "http://localhost:9000",
		},
		"inference port name is case-insensitive": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("Inference", 9000), tcpPort("", 8000),
			}}},
			want: "http://localhost:9000",
		},
		"http-named port is recognised as inference port": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("http", 9000), tcpPort("nixl", 5557),
			}}},
			want: "http://localhost:9000",
		},
		"multiple named inference ports select the smallest": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("inference", 9000), tcpPort("api", 8001),
			}}},
			want: "http://localhost:8001",
		},
		"unrecognised port names do not win and smallest declared port is used": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				tcpPort("metrics", 9090), tcpPort("nixl", 5557), tcpPort("", 8001),
			}}},
			want: "http://localhost:5557",
		},
		"udp ports are ignored": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				{Name: "dns", ContainerPort: 500, Protocol: corev1.ProtocolUDP},
				tcpPort("", 8001),
			}}},
			want: "http://localhost:8001",
		},
		"sidecar container ports are ignored": {
			containers: []corev1.Container{
				{Name: SidecarName, Ports: []corev1.ContainerPort{tcpPort("metrics", SidecarPort)}},
				{Name: "engine", Ports: []corev1.ContainerPort{tcpPort("", 8001)}},
			},
			want: "http://localhost:8001",
		},
		"non-positive ports are ignored": {
			containers: []corev1.Container{{Name: "engine", Ports: []corev1.ContainerPort{
				{ContainerPort: 0}, tcpPort("", 8001),
			}}},
			want: "http://localhost:8001",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, inferEngineEndpoint(tc.containers))
		})
	}
}

func sidecarContainer(t *testing.T, podSpec *corev1.PodSpec) *corev1.Container {
	t.Helper()
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == SidecarName {
			return &podSpec.Containers[i]
		}
	}
	t.Fatalf("container %q not found", SidecarName)
	return nil
}

func requireEnv(t *testing.T, c *corev1.Container, name, value string) {
	t.Helper()
	for _, env := range c.Env {
		if env.Name == name {
			assert.Equal(t, value, env.Value, "env %s", name)
			return
		}
	}
	t.Fatalf("env %s not found on container %s", name, c.Name)
}

func TestInjectRuntimeSidecar_DerivesEndpointFromPorts(t *testing.T) {
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "engine",
			Image: "vllm/vllm-openai:latest",
			Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
		}},
	}

	warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
	require.Empty(t, warnings)
	require.Len(t, podSpec.Containers, 2)

	sidecar := sidecarContainer(t, podSpec)
	requireEnv(t, sidecar, "INFERENCE_ENGINE", "vllm")
	requireEnv(t, sidecar, "INFERENCE_ENGINE_ENDPOINT", "http://localhost:8080")
}

func TestInjectRuntimeSidecar_EndpointAnnotationOverridesDerivation(t *testing.T) {
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "engine",
			Image: "vllm/vllm-openai:latest",
			Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
		}},
	}

	settings := runtimeSidecarSettings{
		image:          SidecarImage,
		engineEndpoint: "http://engine.example.com:9000",
	}
	warnings := injectRuntimeSidecar(podSpec, settings)
	require.Empty(t, warnings)
	requireEnv(t, sidecarContainer(t, podSpec), "INFERENCE_ENGINE_ENDPOINT", "http://engine.example.com:9000")
}

func TestInjectRuntimeSidecar_PreservesExistingSameNameVolume(t *testing.T) {
	tests := map[string]corev1.VolumeSource{
		"persistentVolumeClaim": {
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "my-adapters"},
		},
		"hostPath": {
			HostPath: &corev1.HostPathVolumeSource{Path: "/data/adapters"},
		},
		"configMap": {
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "adapters-cm"},
			},
		},
	}

	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			podSpec := &corev1.PodSpec{
				Volumes: []corev1.Volume{{Name: DefaultAdapterVolumeName, VolumeSource: source}},
				Containers: []corev1.Container{{
					Name:  "engine",
					Image: "vllm/vllm-openai:latest",
					Ports: []corev1.ContainerPort{{ContainerPort: 8000, Protocol: corev1.ProtocolTCP}},
				}},
			}

			warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
			require.Len(t, warnings, 1)
			assert.Contains(t, warnings[0], DefaultAdapterVolumeName)

			// The original volume source must be untouched and no extra volume added.
			require.Len(t, podSpec.Volumes, 1)
			assert.Equal(t, source, podSpec.Volumes[0].VolumeSource)

			// Both containers still mount the preserved volume by name.
			for i := range podSpec.Containers {
				assert.True(t, hasAdapterMount(&podSpec.Containers[i]))
			}
		})
	}
}

func TestInjectRuntimeSidecar_CreatesEmptyDirOnlyWhenVolumeMissing(t *testing.T) {
	t.Run("missing volume creates an emptyDir without warning", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "engine",
				Image: "vllm/vllm-openai:latest",
			}},
		}
		warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
		require.Empty(t, warnings)
		require.Len(t, podSpec.Volumes, 1)
		assert.Equal(t, DefaultAdapterVolumeName, podSpec.Volumes[0].Name)
		assert.NotNil(t, podSpec.Volumes[0].EmptyDir)
	})

	t.Run("existing emptyDir is left untouched without warning", func(t *testing.T) {
		medium := corev1.StorageMediumMemory
		podSpec := &corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: DefaultAdapterVolumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{Medium: medium},
				},
			}},
			Containers: []corev1.Container{{
				Name:  "engine",
				Image: "vllm/vllm-openai:latest",
			}},
		}
		warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
		require.Empty(t, warnings)
		require.Len(t, podSpec.Volumes, 1)
		require.NotNil(t, podSpec.Volumes[0].EmptyDir)
		assert.Equal(t, medium, podSpec.Volumes[0].EmptyDir.Medium)
	})
}

func hasAdapterMount(c *corev1.Container) bool {
	for _, m := range c.VolumeMounts {
		if m.Name == DefaultAdapterVolumeName && m.MountPath == DefaultAdapterMountPath {
			return true
		}
	}
	return false
}

func TestInjectRuntimeSidecar_IsIdempotent(t *testing.T) {
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:      "engine",
			Image:     "vllm/vllm-openai:latest",
			Env:       []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}},
			Ports:     []corev1.ContainerPort{{ContainerPort: 8000, Protocol: corev1.ProtocolTCP}},
		}},
	}

	require.Empty(t, injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage}))
	afterFirst := podSpec.DeepCopy()

	// Repeated defaulting must neither add another sidecar nor mutate anything else.
	warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
	require.Empty(t, warnings)
	assert.Equal(t, afterFirst, podSpec)
}

func TestInjectRuntimeSidecar_SkipsTemplateThatAlreadyHasSidecar(t *testing.T) {
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: SidecarName, Image: SidecarImage,
				VolumeMounts: []corev1.VolumeMount{{Name: DefaultAdapterVolumeName, MountPath: DefaultAdapterMountPath}},
			},
			{
				// User-added later; its missing adapter mount must not be backfilled when
				// the template already carries the runtime.
				Name:  "extra-engine",
				Image: "vllm/vllm-openai:latest",
			},
		},
	}
	snapshot := podSpec.DeepCopy()
	warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
	require.Empty(t, warnings)
	assert.Equal(t, snapshot, podSpec)
}

func TestInjectRuntimeSidecar_DoesNotChangeUserEnvOrResources(t *testing.T) {
	engine := corev1.Container{
		Name:  "engine",
		Image: "vllm/vllm-openai:latest",
		Env: []corev1.EnvVar{
			{Name: "VLLM_ALLOW_RUNTIME_LORA_UPDATING", Value: "true"},
			{Name: "INFERENCE_ENGINE_ENDPOINT", Value: "http://should-not-be-touched:1"},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{engine}}

	require.Empty(t, injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage}))

	var engineContainer *corev1.Container
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == "engine" {
			engineContainer = &podSpec.Containers[i]
		}
	}
	require.NotNil(t, engineContainer)
	assert.Equal(t, engine.Env, engineContainer.Env)
	assert.Equal(t, engine.Resources, engineContainer.Resources)
	assert.True(t, hasAdapterMount(engineContainer))
}

func TestInjectRuntimeSidecar_UnknownEngineIsObservable(t *testing.T) {
	t.Run("inferred unknown engine warns", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "nginx:latest"}},
		}
		warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage})
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], engineTypeUnknown)
		requireEnv(t, sidecarContainer(t, podSpec), "INFERENCE_ENGINE", engineTypeUnknown)
	})

	t.Run("explicit engine annotation is honoured without warning", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "custom/internal-engine:1"}},
		}
		warnings := injectRuntimeSidecar(podSpec, runtimeSidecarSettings{image: SidecarImage, engineType: "sglang"})
		require.Empty(t, warnings)
		requireEnv(t, sidecarContainer(t, podSpec), "INFERENCE_ENGINE", "sglang")
	})
}

func TestValidateSidecarInjectionSettings(t *testing.T) {
	tests := map[string]struct {
		settingsAnnotations map[string]string
		engineAnnotations   map[string]string
		wantWarning         bool
		wantError           bool
	}{
		"no switch annotation is a no-op": {
			settingsAnnotations: nil,
		},
		"switch false is a no-op": {
			settingsAnnotations: map[string]string{SidecarInjectionAnnotation: "false"},
		},
		"switch typo warns and does not error": {
			settingsAnnotations: map[string]string{SidecarInjectionAnnotation: "yes"},
			wantWarning:         true,
		},
		"empty image annotation warns": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:             "true",
				SidecarInjectionRuntimeImageAnnotation: "   ",
			},
			wantWarning: true,
		},
		"image with embedded spaces warns": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:             "true",
				SidecarInjectionRuntimeImageAnnotation: "aibrix /runtime:v0.5.0",
			},
			wantWarning: true,
		},
		"valid image does not warn": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:             "true",
				SidecarInjectionRuntimeImageAnnotation: "registry.example.com/aibrix/runtime:v0.5.0",
			},
		},
		"unknown explicit engine warns": {
			settingsAnnotations: map[string]string{SidecarInjectionAnnotation: "true"},
			engineAnnotations:   map[string]string{constants.ModelLabelEngine: "tensorrt-llm"},
			wantWarning:         true,
		},
		"known engine does not warn": {
			settingsAnnotations: map[string]string{SidecarInjectionAnnotation: "true"},
			engineAnnotations:   map[string]string{constants.ModelLabelEngine: "vllm"},
		},
		"endpoint without scheme is rejected": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "localhost:8000",
			},
			wantError: true,
		},
		"endpoint with unsupported scheme is rejected": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "ftp://localhost:8000",
			},
			wantError: true,
		},
		"valid endpoint is accepted": {
			settingsAnnotations: map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "http://localhost:8001",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			warnings, err := validateSidecarInjectionSettings(tc.settingsAnnotations, tc.engineAnnotations)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.wantWarning {
				require.NotEmpty(t, warnings)
			} else if !tc.wantError {
				require.Empty(t, warnings)
			}
		})
	}
}

func TestDeploymentDefaulter_Default(t *testing.T) {
	defaulter := &DeploymentCustomDefaulter{}
	ctx := context.Background()

	t.Run("no annotation leaves the deployment untouched", func(t *testing.T) {
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "engine"},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "engine", Image: "vllm/vllm-openai:latest"}},
					},
				},
			},
		}
		snapshot := deployment.DeepCopy()
		require.NoError(t, defaulter.Default(ctx, deployment))
		assert.Equal(t, snapshot, deployment)
	})

	t.Run("annotation false leaves the deployment untouched", func(t *testing.T) {
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "engine",
				Annotations: map[string]string{SidecarInjectionAnnotation: "false"},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "engine", Image: "vllm/vllm-openai:latest"}},
					},
				},
			},
		}
		snapshot := deployment.DeepCopy()
		require.NoError(t, defaulter.Default(ctx, deployment))
		assert.Equal(t, snapshot, deployment)
	})

	t.Run("annotation true derives the endpoint from the engine port", func(t *testing.T) {
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "engine",
				Annotations: map[string]string{SidecarInjectionAnnotation: "true"},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "engine",
							Image: "vllm/vllm-openai:latest",
							Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
						}},
					},
				},
			},
		}
		require.NoError(t, defaulter.Default(ctx, deployment))
		requireEnv(t, sidecarContainer(t, &deployment.Spec.Template.Spec),
			"INFERENCE_ENGINE_ENDPOINT", "http://localhost:8080")
	})
}

func TestDeploymentDefaulter_Validate(t *testing.T) {
	defaulter := &DeploymentCustomDefaulter{}
	ctx := context.Background()

	baseDeployment := func(annotations map[string]string, podSpec corev1.PodSpec) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "engine", Annotations: annotations},
			Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: podSpec}},
		}
	}

	t.Run("workload without annotation validates without warnings", func(t *testing.T) {
		dep := baseDeployment(nil, corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "nginx:latest"}},
		})
		warnings, err := defaulter.ValidateCreate(ctx, dep)
		require.NoError(t, err)
		assert.Empty(t, warnings)
	})

	t.Run("unknown inferred engine is warned", func(t *testing.T) {
		dep := baseDeployment(
			map[string]string{SidecarInjectionAnnotation: "true"},
			corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx:latest"}}},
		)
		warnings, err := defaulter.ValidateCreate(ctx, dep)
		require.NoError(t, err)
		require.NotEmpty(t, warnings)
		assert.Contains(t, warnings[0], engineTypeUnknown)
	})

	t.Run("preserved non-emptydir volume is warned", func(t *testing.T) {
		dep := baseDeployment(
			map[string]string{SidecarInjectionAnnotation: "true"},
			corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name:         DefaultAdapterVolumeName,
					VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc"}},
				}},
				Containers: []corev1.Container{{Name: "engine", Image: "vllm/vllm-openai:latest"}},
			},
		)
		warnings, err := defaulter.ValidateCreate(ctx, dep)
		require.NoError(t, err)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "persistentVolumeClaim")
	})

	t.Run("malformed endpoint annotation is rejected on update", func(t *testing.T) {
		dep := baseDeployment(
			map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "not-a-url",
			},
			corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "vllm/vllm-openai:latest"}}},
		)
		warnings, err := defaulter.ValidateUpdate(ctx, dep.DeepCopy(), dep)
		require.Error(t, err)
		assert.Empty(t, warnings)
	})
}

func stormService(annotations map[string]string, roles ...orchestrationv1alpha1.RoleSpec) *orchestrationv1alpha1.StormService {
	return &orchestrationv1alpha1.StormService{
		ObjectMeta: metav1.ObjectMeta{Name: "storm", Annotations: annotations},
		Spec: orchestrationv1alpha1.StormServiceSpec{
			Template: orchestrationv1alpha1.RoleSetTemplateSpec{
				Spec: &orchestrationv1alpha1.RoleSetSpec{Roles: roles},
			},
		},
	}
}

func roleWithEngine(name, image string, port int32) orchestrationv1alpha1.RoleSpec {
	return orchestrationv1alpha1.RoleSpec{
		Name: name,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  name + "-engine",
					Image: image,
					Ports: []corev1.ContainerPort{{ContainerPort: port, Protocol: corev1.ProtocolTCP}},
				}},
			},
		},
	}
}

func TestStormServiceDefaulter_DefaultInjectsEveryRole(t *testing.T) {
	defaulter := &StormServiceCustomDefaulter{}

	// One role already ships the runtime sidecar: it must be skipped completely,
	// while the other roles are injected.
	skippedRole := orchestrationv1alpha1.RoleSpec{
		Name: "skipped",
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: SidecarName, Image: SidecarImage},
					{Name: "skipped-engine", Image: "vllm/vllm-openai:latest"},
				},
			},
		},
	}
	ss := stormService(
		map[string]string{SidecarInjectionAnnotation: "true"},
		roleWithEngine("worker", "vllm/vllm-openai:latest", 8000),
		roleWithEngine("prefill", "sglang/sglang:latest", 8001),
		skippedRole,
	)

	require.NoError(t, defaulter.Default(context.Background(), ss))
	roles := ss.Spec.Template.Spec.Roles
	require.Len(t, roles, 3)

	// Every non-skipped role gets the sidecar, an adapter volume and engine mounts.
	for i := 0; i < 2; i++ {
		podSpec := &roles[i].Template.Spec
		sidecar := sidecarContainer(t, podSpec)
		assert.Equal(t, SidecarImage, sidecar.Image)
		require.Len(t, podSpec.Volumes, 1)
		assert.Equal(t, DefaultAdapterVolumeName, podSpec.Volumes[0].Name)
		assert.NotNil(t, podSpec.Volumes[0].EmptyDir)
	}
	requireEnv(t, sidecarContainer(t, &roles[0].Template.Spec), "INFERENCE_ENGINE", "vllm")
	requireEnv(t, sidecarContainer(t, &roles[0].Template.Spec),
		"INFERENCE_ENGINE_ENDPOINT", "http://localhost:8000")
	requireEnv(t, sidecarContainer(t, &roles[1].Template.Spec), "INFERENCE_ENGINE", "sglang")
	requireEnv(t, sidecarContainer(t, &roles[1].Template.Spec),
		"INFERENCE_ENGINE_ENDPOINT", "http://localhost:8001")

	// The pre-existing sidecar template is untouched: no volume, no mount backfill.
	skipped := &roles[2].Template.Spec
	assert.Empty(t, skipped.Volumes)
	for i := range skipped.Containers {
		if skipped.Containers[i].Name == "skipped-engine" {
			assert.False(t, hasAdapterMount(&skipped.Containers[i]))
		}
	}

	// Idempotency across roles: defaulting again changes nothing.
	snapshot := ss.DeepCopy()
	require.NoError(t, defaulter.Default(context.Background(), ss))
	assert.Equal(t, snapshot, ss)
}

func TestStormServiceDefaulter_DefaultSwitchAnnotation(t *testing.T) {
	defaulter := &StormServiceCustomDefaulter{}

	for _, value := range []string{"false", "yes"} {
		t.Run(value, func(t *testing.T) {
			ss := stormService(
				map[string]string{SidecarInjectionAnnotation: value},
				roleWithEngine("worker", "vllm/vllm-openai:latest", 8000),
			)
			snapshot := ss.DeepCopy()
			require.NoError(t, defaulter.Default(context.Background(), ss))
			assert.Equal(t, snapshot, ss)
		})
	}
}

func TestStormServiceDefaulter_ValidateSidecarInjection(t *testing.T) {
	defaulter := &StormServiceCustomDefaulter{}
	ctx := context.Background()

	t.Run("no injection annotation validates cleanly", func(t *testing.T) {
		ss := stormService(nil, roleWithEngine("worker", "nginx:latest", 0))
		warnings, err := defaulter.ValidateCreate(ctx, ss)
		require.NoError(t, err)
		assert.Empty(t, warnings)
	})

	t.Run("per-role volume conflict and unknown engine are warned with role prefix", func(t *testing.T) {
		role := roleWithEngine("worker", "nginx:latest", 0)
		role.Template.Spec.Volumes = []corev1.Volume{{
			Name:         DefaultAdapterVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data"}},
		}}
		ss := stormService(map[string]string{SidecarInjectionAnnotation: "true"}, role)

		warnings, err := defaulter.ValidateCreate(ctx, ss)
		require.NoError(t, err)
		require.Len(t, warnings, 2)
		assert.Contains(t, warnings[0], `"worker"`)
		assert.Contains(t, warnings[0], "hostPath")
		assert.Contains(t, warnings[1], engineTypeUnknown)
	})

	t.Run("typoed switch warns", func(t *testing.T) {
		ss := stormService(
			map[string]string{SidecarInjectionAnnotation: "enabled"},
			roleWithEngine("worker", "vllm/vllm-openai:latest", 8000),
		)
		warnings, err := defaulter.ValidateCreate(ctx, ss)
		require.NoError(t, err)
		require.NotEmpty(t, warnings)
	})

	t.Run("malformed endpoint annotation is rejected", func(t *testing.T) {
		ss := stormService(
			map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "not-a-url",
			},
			roleWithEngine("worker", "vllm/vllm-openai:latest", 8000),
		)
		warnings, err := defaulter.ValidateCreate(ctx, ss)
		require.Error(t, err)
		assert.Empty(t, warnings)
	})

	t.Run("valid injection configuration has no warnings", func(t *testing.T) {
		ss := stormService(
			map[string]string{
				SidecarInjectionAnnotation:               "true",
				SidecarInjectionEngineEndpointAnnotation: "http://localhost:8000",
			},
			roleWithEngine("worker", "vllm/vllm-openai:latest", 8000),
		)
		warnings, err := defaulter.ValidateCreate(ctx, ss)
		require.NoError(t, err)
		assert.Empty(t, warnings)
	})
}
