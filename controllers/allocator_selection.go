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

	"github.com/NVIDIA/gpu-operator/internal/coexistence"
)

func (nlc *nodeLabelingController) selectNode(ctx context.Context, node *corev1.Node) (bool, error) {
	labels := node.Labels
	if !hasCommonGPULabel(labels) {
		return removeAllGPUStateLabels(labels), nil
	}
	mode, explicit := labels[coexistence.ModeLabel]
	gc := nlc.gpuCluster
	selected, err := coexistence.Select(mode, explicit, node.Annotations[coexistence.SelectionAnnotation], nlc.clusterPolicy != nil, gc != nil, nlc.preserveSelections, nlc.defaultStack)
	if value, exists := node.Annotations[coexistence.SelectionAnnotation]; exists && value == "" && !explicit {
		err = fmt.Errorf("empty allocator selection annotation")
	}
	// Preserve the existing stack's labels while its source finalizer drains.
	// Do not recreate labels or enable the stack on additional nodes. In particular,
	// removing DRA here would bypass GPUCluster's claim-consumer-first teardown.
	recorded := node.Annotations[coexistence.SelectionAnnotation]
	if err == nil && recorded != "" && (!explicit || mode == recorded) &&
		((recorded == coexistence.DP && nlc.terminatingDP) || (recorded == coexistence.DRA && nlc.terminatingDRA)) {
		return false, nil
	}
	modified := false
	if err == nil && selected != "" {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		if node.Annotations[coexistence.SelectionAnnotation] != selected {
			node.Annotations[coexistence.SelectionAnnotation] = selected
			modified = true
		}
	}
	if err != nil || selected == "" || (selected == coexistence.DP && nlc.clusterPolicy == nil) || (selected == coexistence.DRA && gc == nil) {
		// Driver ownership is independent of allocator placement.
		before := maps.Clone(labels)
		driver, driverOK := labels[driverDeployLabelKey]
		removeAllGPUStateLabels(labels)
		if driverOK {
			labels[driverDeployLabelKey] = driver
		}
		return modified || !maps.Equal(before, labels), err
	}
	if selected == coexistence.DP {
		saved := nlc.gpuCluster
		nlc.gpuCluster = nil
		modified = nlc.updateGPUStateLabels(ctx, labels, node.Name) || modified
		nlc.gpuCluster = saved
	} else {
		modified = nlc.updateGPUStateLabels(ctx, labels, node.Name) || modified
	}
	return modified, nil
}

// allocatorInitializationPending makes upgrade completion observable via source readiness.
func allocatorInitializationPending(ctx context.Context, c client.Client) (bool, error) {
	cp, gc, err := resolveActiveConfig(ctx, c)
	if err != nil {
		return false, err
	}
	preserve, defaultStack, err := allocationPolicy(cp, gc)
	if err != nil {
		return false, err
	}
	gc = gpuClusterForAllocation(cp, gc)
	cp = clusterPolicyForAllocation(cp)
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return false, err
	}
	for _, node := range nodes.Items {
		if !hasGPULabels(node.Labels) {
			continue
		}
		mode, explicit := node.Labels[coexistence.ModeLabel]
		selected, err := coexistence.Select(mode, explicit, node.Annotations[coexistence.SelectionAnnotation], cp != nil, gc != nil, preserve, defaultStack)
		if value, exists := node.Annotations[coexistence.SelectionAnnotation]; exists && value == "" && !explicit {
			err = fmt.Errorf("empty allocator selection annotation")
		}
		if err != nil {
			return false, fmt.Errorf("node %s: %w", node.Name, err)
		}
		// Explicit nodes for an unavailable stack can wait without blocking adoption.
		if explicit && ((selected == coexistence.DP && cp == nil) || (selected == coexistence.DRA && gc == nil)) {
			continue
		}
		// A retained annotation alone does not exclude the other allocator's
		// DaemonSet. Observe removal of conflicting scheduling labels as well.
		if (selected == coexistence.DP && cp != nil && node.Labels[draDriverDeployLabelKey] == "true") ||
			(selected == coexistence.DRA && gc != nil && node.Labels["nvidia.com/gpu.deploy.device-plugin"] == "true") {
			return true, nil
		}
		if selected != "" && node.Annotations[coexistence.SelectionAnnotation] != selected {
			return true, nil
		}
	}
	return false, nil
}
