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
	"encoding/json"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	gpu "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
	"github.com/NVIDIA/gpu-operator/internal/coexistence"
)

func TestAllocatorSelectionLifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, gpuv1.AddToScheme(scheme))
	require.NoError(t, gpu.AddToScheme(scheme))
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	gc := &gpu.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gc", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, gc).Build()
	nlc := &nodeLabelingController{client: c, clusterPolicy: cp, gpuCluster: gc, preserveSelections: true, defaultStack: coexistence.DRA, logger: logr.Discard()}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true"}, Annotations: map[string]string{coexistence.SelectionAnnotation: coexistence.DP}}}
	_, err := nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.Equal(t, "true", node.Labels["nvidia.com/gpu.deploy.device-plugin"])
	require.NotContains(t, node.Labels, coexistence.ModeLabel)
	// CP deletion preserves the DP reservation in the surviving coexistence installation.
	nlc.clusterPolicy = nil
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.NotContains(t, node.Labels, "nvidia.com/gpu.deploy.device-plugin")
	require.NotContains(t, node.Labels, draDriverDeployLabelKey)
	require.Equal(t, coexistence.DP, node.Annotations[coexistence.SelectionAnnotation])
	// A normal replacement deliberately supersedes automatic bookkeeping.
	nlc.preserveSelections = false
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.Equal(t, coexistence.DRA, node.Annotations[coexistence.SelectionAnnotation])
	require.Equal(t, "true", node.Labels[draDriverDeployLabelKey])
	// Customer intent remains authoritative even in normal replacement.
	node.Labels[coexistence.ModeLabel] = coexistence.DP
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.NotContains(t, node.Labels, draDriverDeployLabelKey)
	require.Equal(t, coexistence.DP, node.Labels[coexistence.ModeLabel])
	// Discovery cleanup must not remove the record.
	node.Labels[commonGPULabelKey] = "false"
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.Equal(t, coexistence.DP, node.Annotations[coexistence.SelectionAnnotation])
}

func TestAllocatorInitializationPending(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, gpuv1.AddToScheme(scheme))
	require.NoError(t, gpu.AddToScheme(scheme))
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"feature.node.kubernetes.io/pci-10de.present": "true", commonGPULabelKey: "true"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, node).Build()
	pending, err := allocatorInitializationPending(context.Background(), c)
	require.NoError(t, err)
	require.True(t, pending)
	node.Annotations = map[string]string{coexistence.SelectionAnnotation: coexistence.DP}
	require.NoError(t, c.Update(context.Background(), node))
	pending, err = allocatorInitializationPending(context.Background(), c)
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), node))
}

func TestDefaultChangeAndDiscoveryRecovery(t *testing.T) {
	gc := &gpu.GPUCluster{}
	cp := &gpuv1.ClusterPolicy{}
	cp.Spec.Driver.Enabled = new(false)
	nlc := &nodeLabelingController{clusterPolicy: cp, gpuCluster: gc, preserveSelections: true, defaultStack: coexistence.DRA, logger: logr.Discard()}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true"}}}
	_, err := nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.Equal(t, coexistence.DRA, node.Annotations[coexistence.SelectionAnnotation])
	nlc.defaultStack = coexistence.DP
	node.Labels[commonGPULabelKey] = "false"
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.NotContains(t, node.Labels, draDriverDeployLabelKey)
	node.Labels[commonGPULabelKey] = "true"
	_, err = nlc.selectNode(context.Background(), node)
	require.NoError(t, err)
	require.Equal(t, "true", node.Labels[draDriverDeployLabelKey])
	// A later new node observes the new default.
	newNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "new", Labels: map[string]string{commonGPULabelKey: "true"}}}
	_, err = nlc.selectNode(context.Background(), newNode)
	require.NoError(t, err)
	require.Equal(t, coexistence.DP, newNode.Annotations[coexistence.SelectionAnnotation])
	// Repeated reconciliation is a no-op, including waiting nodes with driver labels.
	nlc.clusterPolicy = nil
	newNode.Labels[driverDeployLabelKey] = "true"
	_, err = nlc.selectNode(context.Background(), newNode)
	require.NoError(t, err)
	changed, err := nlc.selectNode(context.Background(), newNode)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestSelectionPatchRejectsConcurrentModeChange(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true"}}}
	cp := &gpuv1.ClusterPolicy{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).WithInterceptorFuncs(interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		data, err := patch.Data(obj)
		require.NoError(t, err)
		var body map[string]any
		require.NoError(t, json.Unmarshal(data, &body))
		require.NotEmpty(t, body["metadata"].(map[string]any)["resourceVersion"])
		latest := &corev1.Node{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), latest))
		latest.Labels[coexistence.ModeLabel] = coexistence.DRA
		require.NoError(t, c.Update(ctx, latest))
		return c.Patch(ctx, obj, patch, opts...)
	}}).Build()
	nlc := &nodeLabelingController{client: c, clusterPolicy: cp, logger: logr.Discard()}
	_, err := nlc.labelGPUNodes(context.Background())
	require.True(t, apierrors.IsConflict(err))
	actual := &corev1.Node{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(node), actual))
	require.Equal(t, coexistence.DRA, actual.Labels[coexistence.ModeLabel])
	require.NotContains(t, actual.Annotations, coexistence.SelectionAnnotation)
	require.NotContains(t, actual.Labels, "nvidia.com/gpu.deploy.device-plugin")
}

func TestCoexistenceRejectsLegacyDriverBeforeActivation(t *testing.T) {
	gc := &gpu.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))}}
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cluster-policy"}}
	cp.Spec.Driver.Enabled = new(true)
	cp.Spec.Driver.UseNvidiaDriverCRD = new(false)
	r, _ := newGPUClusterReconciler(t, gc, cp)
	_, message, err := r.validatePrerequisites(t.Context(), gc)
	require.NoError(t, err)
	require.Contains(t, message, "independent NVIDIADriver ownership")
}

func TestBlockedGPUClusterCannotSelectNodes(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, gpuv1.AddToScheme(scheme))
	require.NoError(t, gpu.AddToScheme(scheme))
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cluster-policy"}}
	cp.Spec.Driver.Enabled = new(false)
	gc := &gpu.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp, gc, node).Build()
	r := &NodeLabelingReconciler{Client: c, Scheme: scheme, Namespace: "test", Log: logr.Discard()}
	_, err := r.Reconcile(t.Context(), ctrl.Request{})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.Equal(t, coexistence.DP, node.Annotations[coexistence.SelectionAnnotation])
	require.Equal(t, "true", node.Labels["nvidia.com/gpu.deploy.device-plugin"])
	require.NotContains(t, node.Labels, draDriverDeployLabelKey)
}

func TestSurvivingParticipationControlsNodeSelection(t *testing.T) {
	for name, tc := range map[string]struct {
		cp, gc, participate              bool
		record, mode, defaultStack, want string
		dp, dra                          bool
	}{
		"GC deleted: DRA node waits":            {cp: true, participate: true, record: coexistence.DRA, defaultStack: coexistence.DP, want: coexistence.DRA},
		"CP deleted: DP node waits":             {gc: true, participate: true, record: coexistence.DP, defaultStack: coexistence.DRA, want: coexistence.DP},
		"new node records missing DRA":          {cp: true, participate: true, defaultStack: coexistence.DRA, want: coexistence.DRA},
		"new node records missing DP":           {gc: true, participate: true, defaultStack: coexistence.DP, want: coexistence.DP},
		"new node selects surviving DP":         {cp: true, participate: true, defaultStack: coexistence.DP, want: coexistence.DP, dp: true},
		"new node selects surviving DRA":        {gc: true, participate: true, defaultStack: coexistence.DRA, want: coexistence.DRA, dra: true},
		"fresh GC replaces stale DP":            {gc: true, record: coexistence.DP, defaultStack: coexistence.DP, want: coexistence.DRA, dra: true},
		"fresh CP replaces stale DRA":           {cp: true, record: coexistence.DRA, defaultStack: coexistence.DRA, want: coexistence.DP, dp: true},
		"explicit absent DP overrides fresh GC": {gc: true, record: coexistence.DRA, mode: coexistence.DP, defaultStack: coexistence.DRA, want: coexistence.DP},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(coexistence.DefaultStackEnv, tc.defaultStack)
			objects := []client.Object{}
			annotations := map[string]string{}
			if tc.participate {
				annotations[coexistence.ParticipationAnnotation] = "true"
			}
			if tc.cp {
				cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp", Annotations: annotations}}
				cp.Spec.Driver.Enabled = new(false)
				objects = append(objects, cp)
			}
			if tc.gc {
				gc := coexistenceConfig()
				gc.Annotations = annotations
				if tc.participate {
					gc.Status.Conditions = []metav1.Condition{{Type: "CoexistenceInitialized", Status: metav1.ConditionTrue, Reason: "PrerequisitesMet"}}
				}
				objects = append(objects, gc)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true", driverDeployLabelKey: "true"}}}
			if tc.record != "" {
				node.Annotations = map[string]string{coexistence.SelectionAnnotation: tc.record}
			}
			if tc.mode != "" {
				node.Labels[coexistence.ModeLabel] = tc.mode
			}
			objects = append(objects, node)
			r, c := newGPUClusterReconciler(t, objects...)
			nlc := &NodeLabelingReconciler{Client: c, Scheme: r.Scheme, Namespace: r.Namespace}
			_, err := nlc.Reconcile(t.Context(), ctrl.Request{})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			require.Equal(t, tc.want, node.Annotations[coexistence.SelectionAnnotation])
			require.Equal(t, tc.dp, node.Labels["nvidia.com/gpu.deploy.device-plugin"] == "true")
			require.Equal(t, tc.dra, node.Labels[draDriverDeployLabelKey] == "true")
			if !tc.dp && !tc.dra {
				require.Equal(t, "true", node.Labels[driverDeployLabelKey])
			}
		})
	}
}

func TestAdoptionWaitsForConflictingSchedulingLabelRemoval(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Annotations: map[string]string{coexistence.SelectionAnnotation: coexistence.DP}, Labels: map[string]string{
		commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true", draDriverDeployLabelKey: "true",
	}}}
	r, c := newGPUClusterReconciler(t, cp, gc, node)
	pending, err := allocatorInitializationPending(t.Context(), c)
	require.NoError(t, err)
	require.True(t, pending)
	// This is placement reconciliation, not a claim-drain or clean-node check.
	delete(node.Labels, draDriverDeployLabelKey)
	require.NoError(t, c.Update(t.Context(), node))
	pending, err = allocatorInitializationPending(t.Context(), c)
	require.NoError(t, err)
	require.False(t, pending)
	initializeForTest(t, r, gc)
}

func TestAbsentStackSelectionSurvivesDefaultChange(t *testing.T) {
	nlc := &nodeLabelingController{clusterPolicy: &gpuv1.ClusterPolicy{}, preserveSelections: true, defaultStack: coexistence.DRA, logger: logr.Discard()}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "waiting", Labels: map[string]string{commonGPULabelKey: "true"}}}
	_, err := nlc.selectNode(t.Context(), node)
	require.NoError(t, err)
	require.Equal(t, coexistence.DRA, node.Annotations[coexistence.SelectionAnnotation])
	nlc.defaultStack = coexistence.DP
	_, err = nlc.selectNode(t.Context(), node)
	require.NoError(t, err)
	require.Equal(t, coexistence.DRA, node.Annotations[coexistence.SelectionAnnotation])
	require.NotContains(t, node.Labels, "nvidia.com/gpu.deploy.device-plugin")
	nlc.gpuCluster = &gpu.GPUCluster{}
	_, err = nlc.selectNode(t.Context(), node)
	require.NoError(t, err)
	require.Equal(t, "true", node.Labels[draDriverDeployLabelKey])
}

func TestStandaloneTerminationDoesNotRebuildOrRemoveExistingLabels(t *testing.T) {
	for _, mode := range []string{coexistence.DP, coexistence.DRA} {
		t.Run(mode, func(t *testing.T) {
			nlc := &nodeLabelingController{terminatingDP: mode == coexistence.DP, terminatingDRA: mode == coexistence.DRA}
			key := "nvidia.com/gpu.deploy.device-plugin"
			if mode == coexistence.DRA {
				key = draDriverDeployLabelKey
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "existing", Labels: map[string]string{commonGPULabelKey: "true", key: "true"}, Annotations: map[string]string{coexistence.SelectionAnnotation: mode}}}
			changed, err := nlc.selectNode(t.Context(), node)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, "true", node.Labels[key])
			delete(node.Labels, key)
			changed, err = nlc.selectNode(t.Context(), node)
			require.NoError(t, err)
			require.False(t, changed)
			require.NotContains(t, node.Labels, key)
		})
	}
}
