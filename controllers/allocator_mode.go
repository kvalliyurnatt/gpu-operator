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
	"maps"

	"github.com/NVIDIA/gpu-operator/internal/allocatormode"
)

func (nlc *nodeLabelingController) updateAllocatorModeLabels(ctx context.Context, labels map[string]string, nodeName string) bool {
	// Preserve discovery-loss cleanup: returning GPUs are initialized from the
	// explicit mode or the normal default/hold rules, without stale deployment state.
	if !hasCommonGPULabel(labels) {
		return removeAllGPUStateLabels(labels)
	}
	before := maps.Clone(labels)
	mode, reason := allocatormode.Select(labels, nlc.clusterPolicy != nil, nlc.gpuCluster != nil, allocatormode.RequireExplicit())
	if reason != "" {
		nlc.logger.Info("Allocator placement", "node", nodeName, "reason", reason)
	}
	// Preserve the ClusterPolicy opt-out: clearing deployment labels also resets
	// allocator history. GPUCluster has never used this switch to disable operands.
	if mode == allocatormode.DevicePlugin && hasOperandsDisabled(labels) {
		return removeAllGPUStateLabels(labels)
	}
	// Independent driver ownership is not gated by allocator selection.
	if (!hasOperandsDisabled(labels) || mode == allocatormode.DRA) && (nlc.clusterPolicy != nil || nlc.gpuCluster != nil) {
		if value, present := labels[driverDeployLabelKey]; !present || value == "" {
			labels[driverDeployLabelKey] = "true"
		}
	}
	dp, hadDP := labels[allocatormode.DevicePluginLabel]
	dra, hadDRA := labels[allocatormode.DRALabel]
	if mode == "" {
		// Keep only allocator history, independent driver labels and unrelated labels.
		keys := devicePluginOnlyStateLabelKeys()
		for key := range gpuClusterStateLabels {
			if key != driverDeployLabelKey {
				keys[key] = true
			}
		}
		nlc.removeLabelsFromNode(labels, keys, nodeName)
		if hadDP {
			labels[allocatormode.DevicePluginLabel] = dp
		}
		if hadDRA {
			labels[allocatormode.DRALabel] = dra
		}
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
