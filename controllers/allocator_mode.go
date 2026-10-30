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

package controllers

import (
	"context"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NVIDIA/gpu-operator/internal/allocatormode"
)

func (nlc *nodeLabelingController) updateAllocatorModeLabels(ctx context.Context, labels, annotations map[string]string, nodeName string) bool {
	// Preserve discovery-loss cleanup: returning GPUs are initialized from the
	// explicit mode or the normal default/hold rules, without stale deployment state.
	if !hasCommonGPULabel(labels) {
		return removeAllGPUStateLabels(labels)
	}
	before := maps.Clone(labels)
	mode, reason := allocatormode.Select(labels, nlc.clusterPolicy != nil, nlc.gpuCluster != nil, allocatormode.RequireExplicit(), annotations[allocatormode.InitializedAnnotation] == "true")
	if reason != "" {
		nlc.logger.Info("Allocator placement", "node", nodeName, "reason", reason)
	}
	// Keep the initialization marker across component cleanup.
	if mode != "" {
		annotations[allocatormode.InitializedAnnotation] = "true"
	}
	// Preserve the ClusterPolicy opt-out. GPUCluster has never used this switch to disable operands.
	if mode == allocatormode.DevicePlugin && hasOperandsDisabled(labels) {
		return removeAllGPUStateLabels(labels)
	}
	// Independent driver ownership is not gated by allocator selection.
	if (!hasOperandsDisabled(labels) || mode == allocatormode.DRA) && (nlc.clusterPolicy != nil || nlc.gpuCluster != nil) {
		if value, present := labels[driverDeployLabelKey]; !present || value == "" {
			labels[driverDeployLabelKey] = "true"
		}
	}
	if mode == "" {
		// Remove obsolete stack labels, retaining independent driver and unrelated labels.
		keys := devicePluginOnlyStateLabelKeys()
		for key := range gpuClusterStateLabels {
			if key != driverDeployLabelKey {
				keys[key] = true
			}
		}
		nlc.removeLabelsFromNode(labels, keys, nodeName)
		return !maps.Equal(before, labels)
	}
	if mode == allocatormode.DRA {
		nlc.removeLabelsFromNode(labels, devicePluginOnlyStateLabelKeys(), nodeName)
		updateGPUClusterStateLabels(labels)
	} else {
		cp := nlc.clusterPolicy
		sandbox := cp.Spec.SandboxWorkloads.IsEnabled()
		config, _ := getWorkloadConfig(labels, sandbox)
		w := &gpuWorkloadConfiguration{config: config, sandboxMode: cp.Spec.SandboxWorkloads.Mode, node: nodeName, log: nlc.logger}
		w.updateGPUStateLabels(labels)
		if cp.Spec.MIGManager.IsEnabled() && hasMIGCapableGPU(labels) && !hasMIGConfigLabel(labels) && cp.Spec.MIGManager.Config != nil && cp.Spec.MIGManager.Config.Default == migConfigDisabledValue {
			labels[migConfigLabelKey] = migConfigDisabledValue
		}
	}
	return !maps.Equal(before, labels)
}

// allocatorInitializationPending makes normal source readiness cover first initialization.
// All reads use the manager cache; a pending node is retried until its atomic patch is observed.
func allocatorInitializationPending(ctx context.Context, c client.Reader, sourceMode string) (bool, error) {
	cp, gc, err := resolveActiveConfig(ctx, c)
	if err != nil {
		return false, err
	}
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return false, fmt.Errorf("list allocator initialization: %w", err)
	}
	for _, node := range nodes.Items {
		if !node.DeletionTimestamp.IsZero() || !hasGPULabels(node.Labels) && !hasCommonGPULabel(node.Labels) {
			continue
		}
		initialized := node.Annotations[allocatormode.InitializedAnnotation] == "true"
		mode, _ := allocatormode.Select(node.Labels, cp != nil, gc != nil, allocatormode.RequireExplicit(), initialized)
		if mode == sourceMode && !initialized {
			return true, nil
		}
	}
	return false, nil
}
