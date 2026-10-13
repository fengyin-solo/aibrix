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
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/utils"
)

var sidecarInjectionLog = logf.Log.WithName("sidecar-injection")

// Sidecar injection constants
const (
	// SidecarInjectionAnnotation Annotation used to enable or disable sidecar injection
	SidecarInjectionAnnotation = "model.aibrix.ai/sidecar-injection"
	// SidecarInjectionRuntimeImageAnnotation Annotation used to specify a custom image for the sidecar container
	SidecarInjectionRuntimeImageAnnotation = "model.aibrix.ai/sidecar-runtime-image"
	// SidecarInjectionEngineEndpointAnnotation overrides the inference engine endpoint the
	// runtime talks to. When unset the endpoint is derived from the engine container ports.
	SidecarInjectionEngineEndpointAnnotation = "model.aibrix.ai/sidecar-engine-endpoint"
	SidecarName                              = "aibrix-runtime"
	SidecarImage                             = "aibrix/runtime:v0.5.0"
	SidecarCommand                           = "aibrix_runtime"
	SidecarPort                              = 8080
	SidecarHealthPath                        = "/healthz"
	SidecarReadyPath                         = "/ready"
	// DefaultEnginePort is the engine port used when the engine containers declare no ports.
	DefaultEnginePort = 8000
	// DefaultEngineEndpoint is the fallback engine endpoint and can be overridden through the
	// SidecarInjectionEngineEndpointAnnotation annotation.
	DefaultEngineEndpoint    = "http://localhost:8000"
	DefaultAdapterVolumeName = "adapter-storage"
	DefaultAdapterMountPath  = "/tmp/aibrix/adapters"
	sidecarInjectionEnabled  = "true"
	sidecarInjectionDisabled = "false"
	engineTypeUnknown        = "unknown"
)

// inferencePortNames lists container port names (matched case-insensitively) that
// conventionally identify the inference engine's HTTP API. A declared port carrying
// one of these names is preferred over unnamed ports when deriving the engine endpoint.
var inferencePortNames = map[string]struct{}{
	"inference": {},
	"http":      {},
	"https":     {},
	"api":       {},
}

// knownEngineTypes are the engine identifiers the injection logic recognises. It must
// stay in sync with the keywords handled by inferEngineType.
var knownEngineTypes = map[string]struct{}{
	"vllm":     {},
	"sglang":   {},
	"tgi":      {},
	"triton":   {},
	"llamacpp": {},
}

// runtimeSidecarSettings holds the user-declared settings for the injected runtime
// sidecar. Empty fields fall back to image keyword inference, container port
// derivation and the built-in defaults.
type runtimeSidecarSettings struct {
	// engineType is the explicit model.aibrix.ai/engine value, empty when unset.
	engineType string
	// image is the sidecar image to inject, defaulting to SidecarImage.
	image string
	// engineEndpoint is the explicit engine endpoint override, empty when unset.
	engineEndpoint string
}

// runtimeSidecarSettingsFromAnnotations reads the sidecar settings from annotations.
// settingsAnnotations carries the sidecar-specific annotations (read from the workload
// metadata), while engineAnnotations is the metadata location where
// model.aibrix.ai/engine is declared (workload metadata for Deployment, the RoleSet
// template metadata for StormService).
func runtimeSidecarSettingsFromAnnotations(settingsAnnotations, engineAnnotations map[string]string) runtimeSidecarSettings {
	settings := runtimeSidecarSettings{
		image: SidecarImage,
	}
	if engine := strings.TrimSpace(engineAnnotations[constants.ModelLabelEngine]); engine != "" {
		settings.engineType = engine
	}
	if image := strings.TrimSpace(settingsAnnotations[SidecarInjectionRuntimeImageAnnotation]); image != "" {
		settings.image = image
	}
	// A user-provided endpoint wins over port derivation.
	if endpoint := strings.TrimSpace(settingsAnnotations[SidecarInjectionEngineEndpointAnnotation]); endpoint != "" {
		settings.engineEndpoint = endpoint
	}
	return settings
}

// sidecarInjectionRequested reports whether the injection switch is explicitly enabled.
// Any value other than "true" (including typos and "false") leaves the workload alone;
// validateSidecarInjectionSettings makes the typo visible.
func sidecarInjectionRequested(annotations map[string]string) bool {
	return annotations[SidecarInjectionAnnotation] == sidecarInjectionEnabled
}

// buildRuntimeSidecarContainer creates a runtime sidecar container with the given image,
// engine type and engine endpoint.
func buildRuntimeSidecarContainer(sidecarImage, engineType, engineEndpoint string) corev1.Container {
	if engineEndpoint == "" {
		engineEndpoint = DefaultEngineEndpoint
	}
	return corev1.Container{
		Name:  SidecarName,
		Image: sidecarImage,
		Command: []string{
			SidecarCommand,
			"--port", strconv.Itoa(SidecarPort),
		},
		Env: []corev1.EnvVar{
			{
				Name:  "INFERENCE_ENGINE",
				Value: engineType,
			},
			{
				Name:  "INFERENCE_ENGINE_ENDPOINT",
				Value: engineEndpoint,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      DefaultAdapterVolumeName,
				MountPath: DefaultAdapterMountPath,
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "metrics",
				ContainerPort: SidecarPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   SidecarHealthPath,
					Port:   intstr.FromInt(SidecarPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 3,
			PeriodSeconds:       2,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   SidecarReadyPath,
					Port:   intstr.FromInt(SidecarPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}
}

// inferEngineType infers the inference engine based on container image names
func inferEngineType(containers []corev1.Container) string {
	for _, c := range containers {
		if c.Name == SidecarName {
			continue
		}
		img := strings.ToLower(c.Image)
		if strings.Contains(img, "vllm") {
			return "vllm"
		}
		if strings.Contains(img, "sglang") {
			return "sglang"
		}
		if strings.Contains(img, "text-generation-inference") || strings.Contains(img, "tgi") {
			return "tgi"
		}
		if strings.Contains(img, "triton") {
			return "triton"
		}
		if strings.Contains(img, "llama") && strings.Contains(img, "cpp") {
			return "llamacpp"
		}
	}
	return engineTypeUnknown
}

// isInferencePortName reports whether a container port name identifies the inference
// API by convention. The empty name never matches so that unnamed ports only win when
// no explicitly named inference port exists.
func isInferencePortName(name string) bool {
	if name == "" {
		return false
	}
	_, ok := inferencePortNames[strings.ToLower(name)]
	return ok
}

// inferEngineEndpoint derives the engine base URL from the ports declared on the engine
// containers, using a deterministic rule:
//
//  1. TCP ports whose name identifies the inference API (see inferencePortNames) are
//     preferred; when several match, the smallest port number wins.
//  2. Otherwise the smallest declared TCP port wins.
//  3. With no declared TCP port, DefaultEngineEndpoint is returned.
//
// Ports declared on the runtime sidecar itself are ignored.
func inferEngineEndpoint(containers []corev1.Container) string {
	var namedPorts, allPorts []int32
	for _, c := range containers {
		if c.Name == SidecarName {
			continue
		}
		for _, p := range c.Ports {
			if p.Protocol != "" && p.Protocol != corev1.ProtocolTCP {
				continue
			}
			if p.ContainerPort <= 0 {
				continue
			}
			allPorts = append(allPorts, p.ContainerPort)
			if isInferencePortName(p.Name) {
				namedPorts = append(namedPorts, p.ContainerPort)
			}
		}
	}

	candidates := namedPorts
	if len(candidates) == 0 {
		candidates = allPorts
	}
	if len(candidates) == 0 {
		return DefaultEngineEndpoint
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	return fmt.Sprintf("http://localhost:%d", candidates[0])
}

// engineContainers returns the pod containers other than the runtime sidecar.
func engineContainers(podSpec *corev1.PodSpec) []corev1.Container {
	if podSpec == nil {
		return nil
	}
	containers := make([]corev1.Container, 0, len(podSpec.Containers))
	for _, c := range podSpec.Containers {
		if c.Name == SidecarName {
			continue
		}
		containers = append(containers, c)
	}
	return containers
}

// findVolume returns the volume with the given name, or nil when absent.
func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

// ensureAdapterVolume makes sure the shared adapter volume exists. A pre-existing
// volume is always kept untouched, including when it uses a non-EmptyDir source such
// as a PVC or hostPath: silently replacing its source would discard the user's mount.
// The type conflict is reported separately through sidecarPodSpecWarnings.
func ensureAdapterVolume(podSpec *corev1.PodSpec) {
	if findVolume(podSpec.Volumes, DefaultAdapterVolumeName) != nil {
		return
	}
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: DefaultAdapterVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
}

// describeVolumeSource returns a short, human-readable name for the set source of a
// corev1.VolumeSource for warning messages.
func describeVolumeSource(source corev1.VolumeSource) string {
	switch {
	case source.HostPath != nil:
		return "hostPath"
	case source.EmptyDir != nil:
		return "emptyDir"
	case source.GCEPersistentDisk != nil:
		return "gcePersistentDisk"
	case source.AWSElasticBlockStore != nil:
		return "awsElasticBlockStore"
	case source.Secret != nil:
		return "secret"
	case source.NFS != nil:
		return "nfs"
	case source.ISCSI != nil:
		return "iscsi"
	case source.Glusterfs != nil:
		return "glusterfs"
	case source.PersistentVolumeClaim != nil:
		return "persistentVolumeClaim"
	case source.RBD != nil:
		return "rbd"
	case source.ConfigMap != nil:
		return "configMap"
	case source.Cinder != nil:
		return "cinder"
	case source.CephFS != nil:
		return "cephfs"
	case source.DownwardAPI != nil:
		return "downwardAPI"
	case source.FC != nil:
		return "fc"
	case source.Flocker != nil:
		return "flocker"
	case source.AzureFile != nil:
		return "azureFile"
	case source.VsphereVolume != nil:
		return "vsphereVolume"
	case source.Quobyte != nil:
		return "quobyte"
	case source.AzureDisk != nil:
		return "azureDisk"
	case source.PhotonPersistentDisk != nil:
		return "photonPersistentDisk"
	case source.Projected != nil:
		return "projected"
	case source.PortworxVolume != nil:
		return "portworxVolume"
	case source.ScaleIO != nil:
		return "scaleIO"
	case source.StorageOS != nil:
		return "storageos"
	case source.CSI != nil:
		return "csi"
	case source.Ephemeral != nil:
		return "ephemeral"
	case source.Image != nil:
		return "image"
	default:
		return "non-emptyDir"
	}
}

// sidecarPodSpecWarnings inspects a pod spec for non-fatal injection problems:
// an adapter-storage volume declared with a non-EmptyDir source, and an engine type
// that cannot be inferred from container images. It works both before and after the
// sidecar has been injected because the volume is preserved and the sidecar's own
// image is never considered for engine inference.
func sidecarPodSpecWarnings(podSpec *corev1.PodSpec, explicitEngine string) []string {
	var warnings []string
	if volume := findVolume(podSpec.Volumes, DefaultAdapterVolumeName); volume != nil && volume.EmptyDir == nil {
		warnings = append(warnings, fmt.Sprintf(
			"volume %q uses a %s source instead of an emptyDir; the runtime keeps it unchanged, downloaded adapters may not be shared as expected",
			DefaultAdapterVolumeName, describeVolumeSource(volume.VolumeSource)))
	}
	if explicitEngine == "" && inferEngineType(engineContainers(podSpec)) == engineTypeUnknown {
		warnings = append(warnings, fmt.Sprintf(
			"could not infer the inference engine type from container images; the runtime sidecar is injected with INFERENCE_ENGINE=%q, set the %s annotation to silence this warning",
			engineTypeUnknown, constants.ModelLabelEngine))
	}
	return warnings
}

// injectRuntimeSidecar injects the aibrix-runtime sidecar into podSpec using settings.
// It is shared by the Deployment and StormService defaulters and is idempotent: a pod
// spec that already contains the runtime sidecar is returned completely unchanged.
// Non-fatal problems are returned as human-readable warnings so callers can log them
// or surface them as admission warnings. Existing volumes, environment variables and
// resources of the engine containers are never modified.
func injectRuntimeSidecar(podSpec *corev1.PodSpec, settings runtimeSidecarSettings) []string {
	if podSpec == nil {
		return nil
	}
	// Idempotency: skip templates that already carry the runtime, including any other
	// containers in that template. This guarantees repeated defaulting never mutates
	// existing volumes, environment variables or resources.
	if containsContainer(podSpec.Containers, SidecarName) {
		return nil
	}

	warnings := sidecarPodSpecWarnings(podSpec, settings.engineType)

	engineType := settings.engineType
	if engineType == "" {
		engineType = inferEngineType(podSpec.Containers)
	}

	engineEndpoint := settings.engineEndpoint
	if engineEndpoint == "" {
		engineEndpoint = inferEngineEndpoint(podSpec.Containers)
	}

	// Ensure the artifacts download path is shared with the sidecar container.
	ensureAdapterVolume(podSpec)
	for ci := range podSpec.Containers {
		container := &podSpec.Containers[ci]
		if container.Name == SidecarName {
			continue
		}
		if !utils.HasVolumeMount(container.VolumeMounts, DefaultAdapterVolumeName, DefaultAdapterMountPath) {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      DefaultAdapterVolumeName,
				MountPath: DefaultAdapterMountPath,
			})
		}
	}

	// Build the sidecar container and inject it at the beginning.
	runtimeContainer := buildRuntimeSidecarContainer(settings.image, engineType, engineEndpoint)
	podSpec.Containers = append([]corev1.Container{runtimeContainer}, podSpec.Containers...)
	return warnings
}

// validateEngineEndpoint checks that an engine endpoint override is an absolute
// http(s) URL. A malformed endpoint guarantees a non-functional runtime, so it is
// rejected outright rather than warned about.
func validateEngineEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("annotation %s must be an absolute HTTP(S) URL (for example %q), got %q",
			SidecarInjectionEngineEndpointAnnotation, DefaultEngineEndpoint, endpoint)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("annotation %s must use the http or https scheme, got %q",
			SidecarInjectionEngineEndpointAnnotation, endpoint)
	}
	return nil
}

// validateSidecarInjectionSettings validates sidecar-injection annotation values. It
// returns admission warnings for suspicious-but-usable values (a typo in the switch,
// an unusable image value, an unknown engine type) and an error for values that cannot
// be honoured (a malformed engine endpoint). It is a no-op for workloads without the
// injection switch annotation, so workloads that opt out are completely unaffected.
//
// engineAnnotations is the metadata location where model.aibrix.ai/engine is read.
func validateSidecarInjectionSettings(settingsAnnotations, engineAnnotations map[string]string) ([]string, error) {
	switchValue, exists := settingsAnnotations[SidecarInjectionAnnotation]
	if !exists {
		return nil, nil
	}
	if switchValue == sidecarInjectionDisabled {
		return nil, nil
	}

	var warnings []string
	if switchValue != sidecarInjectionEnabled {
		warnings = append(warnings, fmt.Sprintf(
			"unrecognized value %q for annotation %s: supported values are %q and %q, no runtime sidecar is injected",
			switchValue, SidecarInjectionAnnotation, sidecarInjectionEnabled, sidecarInjectionDisabled))
	}

	if rawImage, ok := settingsAnnotations[SidecarInjectionRuntimeImageAnnotation]; ok {
		image := strings.TrimSpace(rawImage)
		if image == "" {
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s is empty; falling back to the default image %q",
				SidecarInjectionRuntimeImageAnnotation, SidecarImage))
		} else if image != rawImage || strings.ContainsAny(image, " \t\n\r") {
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s contains surrounding whitespace or embedded spaces: %q",
				SidecarInjectionRuntimeImageAnnotation, rawImage))
		}
	}

	if engine, ok := engineAnnotations[constants.ModelLabelEngine]; ok && strings.TrimSpace(engine) != "" {
		if _, known := knownEngineTypes[strings.ToLower(strings.TrimSpace(engine))]; !known {
			knownNames := make([]string, 0, len(knownEngineTypes))
			for name := range knownEngineTypes {
				knownNames = append(knownNames, name)
			}
			sort.Strings(knownNames)
			warnings = append(warnings, fmt.Sprintf(
				"unknown inference engine %q in annotation %s; known values are %s and the runtime may fail to start",
				engine, constants.ModelLabelEngine, strings.Join(knownNames, ", ")))
		}
	}

	if rawEndpoint, ok := settingsAnnotations[SidecarInjectionEngineEndpointAnnotation]; ok {
		endpoint := strings.TrimSpace(rawEndpoint)
		if err := validateEngineEndpoint(endpoint); err != nil {
			return warnings, err
		}
	}

	return warnings, nil
}

// containsContainer checks if a container with the given name exists
func containsContainer(containers []corev1.Container, name string) bool {
	for _, c := range containers {
		if c.Name == name {
			return true
		}
	}
	return false
}
