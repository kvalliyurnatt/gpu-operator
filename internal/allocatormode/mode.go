/**
# Copyright (c) NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
**/

package allocatormode

import (
	"os"

	corev1 "k8s.io/api/core/v1"
)

const (
	RequireExplicitEnv = "GPU_OPERATOR_REQUIRE_EXPLICIT_ALLOCATOR_MODE_FOR_NEW_NODES"
	Label              = "nvidia.com/gpu-operator.resource-allocation.mode"
	DevicePlugin       = "device-plugin"
	DRA                = "dra"
	DevicePluginLabel  = "nvidia.com/gpu.deploy.device-plugin"
	DRALabel           = "nvidia.com/gpu.deploy.dra-driver"
)

func RequireExplicit() bool { return os.Getenv(RequireExplicitEnv) == "true" }

// Select uses label presence, including false values, as configuration history.
// A terminating CR counts as present until the cache observes its removal.
func Select(labels map[string]string, cp, gc, hold bool) (string, string) {
	if mode, explicit := labels[Label]; explicit {
		if mode != DevicePlugin && mode != DRA {
			return "", "InvalidAllocatorMode"
		}
		if mode == DevicePlugin && cp || mode == DRA && gc {
			return mode, ""
		}
		return "", "SelectedStackAbsent"
	}
	_, dp := labels[DevicePluginLabel]
	_, dra := labels[DRALabel]
	if dp && dra {
		return "", "AmbiguousAllocatorHistory"
	}
	if dp && cp {
		return DevicePlugin, ""
	}
	if dra && gc {
		return DRA, ""
	}
	if !dp && !dra && hold {
		return "", "WaitingForExplicitMode"
	}
	if cp {
		return DevicePlugin, ""
	}
	if gc {
		return DRA, ""
	}
	return "", "SelectedStackAbsent"
}

// Constrain intersects both placement branches with every existing required term.
// Appending a separate OR term would allow hardware/workload constraints to be bypassed.
func Constrain(spec *corev1.PodSpec, mode string) {
	own, other := DevicePluginLabel, DRALabel
	if mode == DRA {
		own, other = DRALabel, DevicePluginLabel
	}
	branches := [][]corev1.NodeSelectorRequirement{
		{{Key: Label, Operator: corev1.NodeSelectorOpIn, Values: []string{mode}}},
		{{Key: Label, Operator: corev1.NodeSelectorOpDoesNotExist}, {Key: own, Operator: corev1.NodeSelectorOpExists}, {Key: other, Operator: corev1.NodeSelectorOpDoesNotExist}},
	}
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	affinity := spec.Affinity.NodeAffinity
	base := []corev1.NodeSelectorTerm{{}}
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		base = nil
		for _, term := range affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			// Kubernetes treats an empty required term as matching no nodes.
			if len(term.MatchExpressions) != 0 || len(term.MatchFields) != 0 {
				base = append(base, term)
			}
		}
	}
	terms := []corev1.NodeSelectorTerm{}
	for _, term := range base {
		for _, branch := range branches {
			copy := term.DeepCopy()
			copy.MatchExpressions = append(copy.MatchExpressions, branch...)
			terms = append(terms, *copy)
		}
	}
	affinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: terms}
}
