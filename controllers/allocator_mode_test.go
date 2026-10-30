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
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	nvidiav1alpha1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
	"github.com/NVIDIA/gpu-operator/internal/allocatormode"
)

func TestAllocatorHistoryLifecycle(t *testing.T) {
	t.Setenv(allocatormode.RequireExplicitEnv, "true")
	cp := &gpuv1.ClusterPolicy{}
	gc := &nvidiav1alpha1.GPUCluster{}
	nlc := &nodeLabelingController{clusterPolicy: cp, logger: logr.Discard()}
	labels := map[string]string{commonGPULabelKey: commonGPULabelValue}
	update := func() { nlc.updateGPUStateLabels(context.Background(), labels, "node") }
	update()
	require.NotContains(t, labels, allocatormode.DevicePluginLabel)
	t.Setenv(allocatormode.RequireExplicitEnv, "false")
	update()
	require.Equal(t, "true", labels[allocatormode.DevicePluginLabel])
	t.Setenv(allocatormode.RequireExplicitEnv, "true")
	nlc.gpuCluster = gc
	update()
	require.Equal(t, "true", labels[allocatormode.DevicePluginLabel])
	require.NotContains(t, labels, allocatormode.DRALabel)
	// Disabled values remain history and must survive a restart and no-CR gap.
	labels[allocatormode.DevicePluginLabel] = "false"
	nlc.clusterPolicy = nil
	nlc.gpuCluster = nil
	update()
	require.Equal(t, "false", labels[allocatormode.DevicePluginLabel])
	nlc = &nodeLabelingController{gpuCluster: gc, logger: logr.Discard()}
	update()
	require.NotContains(t, labels, allocatormode.DevicePluginLabel)
	require.Equal(t, "true", labels[allocatormode.DRALabel])
	before := maps.Clone(labels)
	update()
	require.Equal(t, before, labels)
	// Explicit mode blocks fallback to the available stack.
	labels[allocatormode.Label] = allocatormode.DevicePlugin
	update()
	require.NotContains(t, labels, draValidatorDeployLabelKey)
	require.Equal(t, "true", labels[allocatormode.DRALabel])
	labels[allocatormode.Label] = allocatormode.DRA
	labels[allocatormode.DRALabel] = "false"
	update()
	require.Equal(t, "false", labels[allocatormode.DRALabel])
	// User labels and independent driver opt-outs are preserved.
	labels[driverDeployLabelKey] = "false"
	labels["example.com/keep"] = "yes"
	update()
	require.Equal(t, "false", labels[driverDeployLabelKey])
	require.Equal(t, "yes", labels["example.com/keep"])
}

func TestAllocatorOperandsDisableCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     string
		explicit bool
		bothCRs  bool
		hold     bool
		paused   bool
	}{
		{name: "ClusterPolicy default resumes", mode: allocatormode.DevicePlugin},
		{name: "ClusterPolicy explicit mode resumes with hold", mode: allocatormode.DevicePlugin, explicit: true, hold: true},
		{name: "coexistence ClusterPolicy default resumes", mode: allocatormode.DevicePlugin, bothCRs: true},
		{name: "coexistence ClusterPolicy history reset waits with hold", mode: allocatormode.DevicePlugin, bothCRs: true, hold: true},
		{name: "ClusterPolicy reset clears component pause", mode: allocatormode.DevicePlugin, explicit: true, bothCRs: true, hold: true, paused: true},
		{name: "GPUCluster history unaffected", mode: allocatormode.DRA},
		{name: "GPUCluster paused value unaffected", mode: allocatormode.DRA, paused: true},
		{name: "GPUCluster explicit mode unaffected with hold", mode: allocatormode.DRA, explicit: true, hold: true},
		{name: "coexistence GPUCluster history unaffected with hold", mode: allocatormode.DRA, bothCRs: true, hold: true},
		{name: "coexistence GPUCluster explicit mode unaffected", mode: allocatormode.DRA, explicit: true, bothCRs: true, hold: true, paused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(allocatormode.RequireExplicitEnv, "false")
			nlc := &nodeLabelingController{logger: logr.Discard()}
			if tc.mode == allocatormode.DevicePlugin || tc.bothCRs {
				nlc.clusterPolicy = &gpuv1.ClusterPolicy{}
			}
			if tc.mode == allocatormode.DRA || tc.bothCRs {
				nlc.gpuCluster = &nvidiav1alpha1.GPUCluster{}
			}
			// Start with a configured node before enabling the new-node hold.
			base := map[string]string{commonGPULabelKey: commonGPULabelValue, "example.com/keep": "yes"}
			if tc.explicit {
				base[allocatormode.Label] = tc.mode
			}
			labels := maps.Clone(base)
			allocator := allocatormode.DevicePluginLabel
			if tc.mode == allocatormode.DRA {
				allocator = allocatormode.DRALabel
			}
			labels[allocator] = "true"
			if tc.paused {
				labels[allocator] = "false"
			}
			update := func() bool { return nlc.updateGPUStateLabels(context.Background(), labels, "node") }
			update()
			before := maps.Clone(labels)
			if tc.hold {
				t.Setenv(allocatormode.RequireExplicitEnv, "true")
			}
			labels[commonOperandsLabelKey] = "false"
			update()
			if tc.mode == allocatormode.DevicePlugin {
				want := maps.Clone(base)
				want[commonOperandsLabelKey] = "false"
				require.Equal(t, want, labels, "ClusterPolicy disable removes deployment labels, including history")
			} else {
				want := maps.Clone(before)
				want[commonOperandsLabelKey] = "false"
				require.Equal(t, want, labels, "GPUCluster ignores the ClusterPolicy disable switch")
			}
			require.False(t, update(), "disabled-state reconciliation must be idempotent")
			delete(labels, commonOperandsLabelKey)
			update()
			if tc.mode == allocatormode.DRA {
				require.Equal(t, before, labels)
			} else if tc.hold && !tc.explicit {
				require.NotContains(t, labels, allocatormode.DevicePluginLabel)
				require.NotContains(t, labels, allocatormode.DRALabel)
			} else {
				require.Equal(t, "true", labels[allocatormode.DevicePluginLabel])
				require.NotContains(t, labels, allocatormode.DRALabel)
			}
			require.False(t, update(), "recovery reconciliation must be idempotent")
		})
	}
}

func TestGPUClusterInitializesWithOperandsDisabled(t *testing.T) {
	for _, bothCRs := range []bool{false, true} {
		t.Run(fmt.Sprintf("coexistence=%t", bothCRs), func(t *testing.T) {
			t.Setenv(allocatormode.RequireExplicitEnv, "false")
			nlc := &nodeLabelingController{gpuCluster: &nvidiav1alpha1.GPUCluster{}, logger: logr.Discard()}
			labels := map[string]string{commonGPULabelKey: commonGPULabelValue, commonOperandsLabelKey: "false"}
			if bothCRs {
				nlc.clusterPolicy = &gpuv1.ClusterPolicy{}
				labels[allocatormode.Label] = allocatormode.DRA
			}
			nlc.updateGPUStateLabels(context.Background(), labels, "node")
			for key, value := range gpuClusterStateLabels {
				require.Equal(t, value, labels[key], key)
			}
			require.Equal(t, "false", labels[commonOperandsLabelKey])
			require.NotContains(t, labels, allocatormode.DevicePluginLabel)
		})
	}
}

func TestAllocatorDiscoveryLossRecovery(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cp, gc       bool
		initialMode  string
		explicitMode string
		initialValue string
		hold         bool
		wantMode     string
	}{
		{name: "ClusterPolicy default recovers", cp: true, initialMode: allocatormode.DevicePlugin, wantMode: allocatormode.DevicePlugin},
		{name: "GPUCluster default recovers", gc: true, initialMode: allocatormode.DRA, wantMode: allocatormode.DRA},
		{name: "explicit device plugin survives hold", cp: true, gc: true, initialMode: allocatormode.DevicePlugin, explicitMode: allocatormode.DevicePlugin, hold: true, wantMode: allocatormode.DevicePlugin},
		{name: "explicit DRA survives hold", cp: true, gc: true, initialMode: allocatormode.DRA, explicitMode: allocatormode.DRA, hold: true, wantMode: allocatormode.DRA},
		{name: "coexistence without explicit mode uses device plugin default", cp: true, gc: true, initialMode: allocatormode.DRA, wantMode: allocatormode.DevicePlugin},
		{name: "coexistence without explicit mode waits with hold", cp: true, gc: true, initialMode: allocatormode.DRA, hold: true},
		{name: "ClusterPolicy without explicit mode waits with hold", cp: true, initialMode: allocatormode.DevicePlugin, hold: true},
		{name: "GPUCluster without explicit mode waits with hold", gc: true, initialMode: allocatormode.DRA, hold: true},
		{name: "ClusterPolicy component disable resets", cp: true, initialMode: allocatormode.DevicePlugin, initialValue: "false", wantMode: allocatormode.DevicePlugin},
		{name: "GPUCluster component disable resets", gc: true, initialMode: allocatormode.DRA, initialValue: "false", wantMode: allocatormode.DRA},
		{name: "driver manager pause resets", cp: true, gc: true, initialMode: allocatormode.DRA, explicitMode: allocatormode.DRA, initialValue: "paused-for-driver-upgrade", wantMode: allocatormode.DRA},
		{name: "explicit absent stack does not fall back", cp: true, initialMode: allocatormode.DRA, explicitMode: allocatormode.DRA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(allocatormode.RequireExplicitEnv, fmt.Sprint(tc.hold))
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			base := map[string]string{gpuPCILabelKey: "true", commonGPULabelKey: commonGPULabelValue, "example.com/keep": "yes"}
			if tc.explicitMode != "" {
				base[allocatormode.Label] = tc.explicitMode
			}
			labels := maps.Clone(base)
			allocator := allocatormode.DevicePluginLabel
			if tc.initialMode == allocatormode.DRA {
				allocator = allocatormode.DRALabel
			}
			labels[allocator] = "true"
			if tc.initialValue != "" {
				labels[allocator] = tc.initialValue
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: labels}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
			nlc := &nodeLabelingController{client: c, logger: logr.Discard()}
			if tc.cp {
				nlc.clusterPolicy = &gpuv1.ClusterPolicy{}
			}
			if tc.gc {
				nlc.gpuCluster = &nvidiav1alpha1.GPUCluster{}
			}
			update := func() {
				_, err := nlc.labelGPUNodes(t.Context())
				require.NoError(t, err)
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			}
			update()
			// Simulate NFD withdrawing discovery, then publishing it again on the same Node.
			delete(node.Labels, gpuPCILabelKey)
			require.NoError(t, c.Update(t.Context(), node))
			update()
			want := maps.Clone(base)
			delete(want, gpuPCILabelKey)
			want[commonGPULabelKey] = "false"
			require.Equal(t, want, node.Labels, "discovery loss removes all deployment labels and preserves explicit mode")
			result, err := nlc.labelGPUNodes(t.Context())
			require.NoError(t, err)
			require.Zero(t, result.totalPatchedNodeCount, "discovery-loss cleanup must be idempotent")
			node.Labels[gpuPCILabelKey] = "true"
			require.NoError(t, c.Update(t.Context(), node))
			update()
			require.Equal(t, commonGPULabelValue, node.Labels[commonGPULabelKey])
			require.Equal(t, "yes", node.Labels["example.com/keep"])
			require.Equal(t, tc.explicitMode, node.Labels[allocatormode.Label])
			for key, mode := range map[string]string{allocatormode.DevicePluginLabel: allocatormode.DevicePlugin, allocatormode.DRALabel: allocatormode.DRA} {
				if tc.wantMode == mode {
					require.Equal(t, "true", node.Labels[key])
				} else {
					require.NotContains(t, node.Labels, key)
				}
			}
			result, err = nlc.labelGPUNodes(t.Context())
			require.NoError(t, err)
			require.Zero(t, result.totalPatchedNodeCount, "discovery recovery must be idempotent")
		})
	}
}

func TestAllocatorModeNodeWatch(t *testing.T) {
	r := &NodeLabelingReconciler{}
	for _, key := range []string{allocatormode.Label, allocatormode.DevicePluginLabel, allocatormode.DRALabel} {
		reasons, err := r.getNodeLabelUpdateReasons(context.Background(), "node", map[string]string{}, map[string]string{key: "false"})
		require.NoError(t, err)
		require.True(t, reasons.needsUpdate())
	}
}

func TestAllocatorCreationOrderPlacement(t *testing.T) {
	t.Setenv(allocatormode.RequireExplicitEnv, "false")
	for _, cpSecond := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("ClusterPolicy timestamp %d", cpSecond), func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, gpuv1.AddToScheme(scheme))
			require.NoError(t, nvidiav1alpha1.AddToScheme(scheme))
			cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp", CreationTimestamp: metav1.Unix(cpSecond, 0)}}
			cp.Spec.Driver.UseNvidiaDriverCRD = new(cpSecond < 2)
			gc := &nvidiav1alpha1.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster", CreationTimestamp: metav1.Unix(2, 0)}}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{gpuPCILabelKey: "true"}}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, gc, node).Build()
			r := &NodeLabelingReconciler{Client: c, Scheme: scheme, Log: logr.Discard()}
			_, err := r.Reconcile(t.Context(), ctrl.Request{})
			require.NoError(t, err, "rejected legacy ClusterPolicy must not block GPUCluster labeling")
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			if cpSecond < 2 {
				require.Equal(t, "true", node.Labels[allocatormode.DevicePluginLabel])
				require.NotContains(t, node.Labels, allocatormode.DRALabel)
			} else {
				require.Equal(t, "true", node.Labels[allocatormode.DRALabel])
				require.NotContains(t, node.Labels, allocatormode.DevicePluginLabel)
			}
		})
	}
}

func TestAllocatorReconcileBothCRsAndDeletion(t *testing.T) {
	t.Setenv(allocatormode.RequireExplicitEnv, "true")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, gpuv1.AddToScheme(scheme))
	require.NoError(t, nvidiav1alpha1.AddToScheme(scheme))
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	delegate := true
	cp.Spec.Driver.UseNvidiaDriverCRD = &delegate
	gc := &nvidiav1alpha1.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"feature.node.kubernetes.io/pci-10de.present": "true", commonGPULabelKey: commonGPULabelValue, allocatormode.Label: allocatormode.DRA}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, gc, node).Build()
	r := &NodeLabelingReconciler{Client: c, Scheme: scheme, Log: logr.Discard()}
	_, err := r.Reconcile(context.Background(), ctrl.Request{})
	require.NoError(t, err)
	actual := &corev1.Node{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), actual))
	require.Equal(t, "true", actual.Labels[allocatormode.DRALabel])
	require.NotContains(t, actual.Labels, allocatormode.DevicePluginLabel)
	require.NoError(t, c.Delete(context.Background(), cp))
	require.NoError(t, c.Delete(context.Background(), gc))
	_, err = r.Reconcile(context.Background(), ctrl.Request{})
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), actual))
	require.Equal(t, "true", actual.Labels[allocatormode.DRALabel])
	require.NotContains(t, actual.Labels, draValidatorDeployLabelKey)
}

// A concurrent user mode edit must reject the whole label patch, including history replacement.
type allocatorModeRacingClient struct {
	client.Client
	race bool
}

func (c *allocatorModeRacingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if node, ok := obj.(*corev1.Node); ok && c.race {
		c.race = false
		current := &corev1.Node{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(node), current); err != nil {
			return err
		}
		current.Labels[allocatormode.Label] = allocatormode.DRA
		if err := c.Client.Update(ctx, current); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestAllocatorPatchConflictRecomputesSelection(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: commonGPULabelValue, "feature.node.kubernetes.io/pci-10de.present": "true"}}}
	c := &allocatorModeRacingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), race: true}
	nlc := &nodeLabelingController{client: c, clusterPolicy: &gpuv1.ClusterPolicy{}, gpuCluster: &nvidiav1alpha1.GPUCluster{}, logger: logr.Discard()}
	_, err := nlc.labelGPUNodes(context.Background())
	require.True(t, apierrors.IsConflict(err), "expected optimistic-lock conflict: %v", err)
	actual := &corev1.Node{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), actual))
	require.NotContains(t, actual.Labels, allocatormode.DevicePluginLabel)
	_, err = nlc.labelGPUNodes(context.Background())
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), actual))
	require.Equal(t, "true", actual.Labels[allocatormode.DRALabel])
	require.NotContains(t, actual.Labels, allocatormode.DevicePluginLabel)
}
