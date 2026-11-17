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
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	"github.com/NVIDIA/gpu-operator/internal/coexistence"
	"github.com/NVIDIA/gpu-operator/internal/consts"
)

func ownedMeta(name, kind string, owner client.Object) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "test", UID: types.UID(name + "-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "nvidia.com/v1alpha1", Kind: kind, Name: owner.GetName(), UID: owner.GetUID(), Controller: new(true)}}}
}
func deleteSource(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	require.NoError(t, c.Delete(t.Context(), obj))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
}

func TestDeletionWaitsForOperandsAndMetadataObservation(t *testing.T) {
	cp := testSourceCP()
	node := selectedNode(coexistence.DP)
	node.Labels[devicePluginDeployLabelKey] = "false"
	node.Labels[coexistence.ModeLabel] = coexistence.DRA
	node.Labels[consts.NVIDIADriverOwnerLabel] = "driver"
	node.Labels[driverDeployLabelKey] = "false"
	node.Annotations[driverAutoUpgradeAnnotationKey] = "true"
	ds := &appsv1.DaemonSet{ObjectMeta: ownedMeta("device-plugin", "ClusterPolicy", cp)}
	ds.Finalizers = []string{"test/keep"}
	pod := &corev1.Pod{ObjectMeta: ownedMeta("dp-pod", "DaemonSet", ds)}
	c := lifecycleClient(t, cp, node, ds, pod)
	deleteSource(t, c, cp)
	_, err := teardownSourceOperands(t.Context(), c, "test", cp)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.False(t, sourceConditionTrue(cp, operandsRemoved))
	r := &NodeLabelingReconciler{Client: c}
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.Equal(t, coexistence.DP, node.Annotations[coexistence.SelectionAnnotation])
	require.Contains(t, node.Labels, devicePluginDeployLabelKey)
	// Even an explicit mode change must not bypass the reservation.
	nlc := &nodeLabelingController{client: c, clusterPolicy: cp, gpuCluster: testSourceGC(), terminatingDP: true}
	changed, err := nlc.selectNode(t.Context(), node)
	require.NoError(t, err)
	require.False(t, changed)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ds), ds))
	require.NotNil(t, ds.DeletionTimestamp)
	// Fake clients have no GC: simulate observed foreground completion.
	require.NoError(t, client.IgnoreNotFound(c.Delete(t.Context(), pod)))
	ds.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), ds))
	_, err = teardownSourceOperands(t.Context(), c, "test", cp)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.True(t, sourceConditionTrue(cp, operandsRemoved))
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.Contains(t, cp.Finalizers, nodeCleanupFinalizer, "a successful patch is not observation")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.NotContains(t, node.Annotations, coexistence.SelectionAnnotation)
	require.NotContains(t, node.Labels, devicePluginDeployLabelKey)
	require.Equal(t, coexistence.DRA, node.Labels[coexistence.ModeLabel])
	require.Equal(t, "false", node.Labels[driverDeployLabelKey])
	require.Equal(t, "true", node.Annotations[driverAutoUpgradeAnnotationKey])
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp)))
}

func TestTeardownOwnershipAndDRAOrdering(t *testing.T) {
	gc := testSourceGC()
	deleteTime := metav1.Now()
	gc.DeletionTimestamp = &deleteTime
	consumer := &appsv1.DaemonSet{ObjectMeta: ownedMeta("consumer", "GPUCluster", gc), Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu"}}}}}}
	consumer.Finalizers = []string{"test/keep"}
	plugin := &appsv1.DaemonSet{ObjectMeta: ownedMeta("plugin", "GPUCluster", gc)}
	deployment := &appsv1.Deployment{ObjectMeta: ownedMeta("dra-controller", "GPUCluster", gc)}
	rs := &appsv1.ReplicaSet{ObjectMeta: ownedMeta("controller-rs", "Deployment", deployment)}
	pod := &corev1.Pod{ObjectMeta: ownedMeta("controller-pod", "ReplicaSet", rs)}
	independent := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "test", UID: "driver-uid", OwnerReferences: []metav1.OwnerReference{{Kind: "NVIDIADriver", Name: "driver", UID: "independent-uid"}}}}
	passive := &corev1.ConfigMap{ObjectMeta: ownedMeta("config", "GPUCluster", gc)}
	c := lifecycleClient(t, gc, consumer, plugin, deployment, rs, pod, independent, passive)
	_, err := teardownSourceOperands(t.Context(), c, "test", gc)
	require.NoError(t, err)
	for _, obj := range []client.Object{plugin, deployment, rs, pod, independent, passive} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
		require.Nil(t, obj.GetDeletionTimestamp())
	}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(consumer), consumer))
	consumer.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), consumer))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	_, err = teardownSourceOperands(t.Context(), c, "test", gc)
	require.NoError(t, err)
	require.False(t, sourceConditionTrue(gc, operandsRemoved), "Delete success is not completion")
	for _, obj := range []client.Object{plugin, deployment, rs, pod} {
		require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj)))
	}
	_, err = teardownSourceOperands(t.Context(), c, "test", gc)
	require.NoError(t, err)
	require.True(t, sourceConditionTrue(gc, operandsRemoved))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(independent), independent))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(passive), passive))
}

func TestCleanupRetriesPreserveSurvivingSelections(t *testing.T) {
	for _, defaultStack := range []string{"", coexistence.DP, coexistence.DRA} {
		t.Run("default="+defaultStack, func(t *testing.T) {
			cp := testSourceCP()
			gc := testSourceGC()
			node := selectedNode(coexistence.DP)
			node.Labels[devicePluginDeployLabelKey] = ""
			c := lifecycleClient(t, cp, gc, node)
			deleteSource(t, c, cp)
			require.NoError(t, setSourceCondition(t.Context(), c, cp, operandsRemoved, true, "TeardownComplete", "done"))
			r := &NodeLabelingReconciler{Client: c}
			_, err := r.cleanupDeletedSources(t.Context())
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			// Surviving DRA may activate before the terminating CP disappears.
			nlc := &nodeLabelingController{client: c, clusterPolicy: cp, gpuCluster: gc, terminatingDP: true, defaultStack: defaultStack}
			_, err = nlc.selectNode(t.Context(), node)
			require.NoError(t, err)
			want := coexistence.DRA
			if defaultStack == coexistence.DP {
				want = ""
			}
			require.Equal(t, want, node.Annotations[coexistence.SelectionAnnotation])
			require.NoError(t, c.Update(t.Context(), node))
			_, err = r.cleanupDeletedSources(t.Context())
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			require.Equal(t, want, node.Annotations[coexistence.SelectionAnnotation])
			require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp)))
		})
	}
}

func TestCleanupFailureAndRestart(t *testing.T) {
	cp := testSourceCP()
	node := selectedNode(coexistence.DP)
	// Cleanup includes nodes that no longer have discovery labels.
	node.Labels = map[string]string{devicePluginDeployLabelKey: "false"}
	base := lifecycleClient(t, cp, node)
	deleteSource(t, base, cp)
	require.NoError(t, setSourceCondition(t.Context(), base, cp, operandsRemoved, true, "TeardownComplete", "done"))
	failing := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		if _, ok := obj.(*corev1.Node); ok {
			return fmt.Errorf("node write unavailable")
		}
		return c.Patch(ctx, obj, p, opts...)
	}})
	r := &NodeLabelingReconciler{Client: failing}
	_, err := r.cleanupDeletedSources(t.Context())
	require.ErrorContains(t, err, "unavailable")
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.Contains(t, cp.Finalizers, nodeCleanupFinalizer)
	// A replacement controller resumes only the unfinished metadata phase.
	r = &NodeLabelingReconciler{Client: base}
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.Empty(t, node.Annotations)
	require.Empty(t, node.Labels)
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(base.Get(t.Context(), client.ObjectKeyFromObject(cp), cp)))
}

func TestSourceReconcileDeletionPrecedesPrerequisites(t *testing.T) {
	cp := testSourceCP()
	gc := testSourceGC()
	cp.CreationTimestamp = gc.CreationTimestamp
	c := lifecycleClient(t, cp, gc)
	deleteSource(t, c, cp)
	r := &ClusterPolicyReconciler{Client: c, Namespace: "test"}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.True(t, sourceConditionTrue(cp, operandsRemoved))
}

func TestGPUClusterContinuesAfterOldFinalizerRemoved(t *testing.T) {
	gc := testSourceGC()
	plugin := &appsv1.DaemonSet{ObjectMeta: ownedMeta("plugin", "GPUCluster", gc)}
	c := lifecycleClient(t, gc, plugin)
	deleteSource(t, c, gc)
	r := &GPUClusterReconciler{Client: c, Namespace: "test"}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gc)})
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(plugin), plugin)))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.False(t, sourceConditionTrue(gc, operandsRemoved))
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gc)})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, sourceConditionTrue(gc, operandsRemoved))
}

func TestAdoptionConditionIsObservedAndLatched(t *testing.T) {
	cp := testSourceCP()
	cp.Status.Conditions = nil
	c := lifecycleClient(t, cp)
	ready, err := observeSourceAdoption(t.Context(), c, cp)
	require.NoError(t, err)
	require.False(t, ready)
	actual := &gpuv1.ClusterPolicy{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), actual))
	require.True(t, sourceConditionTrue(actual, nodeSelectionInitialized))
	t.Setenv(coexistence.DefaultStackEnv, coexistence.DRA)
	ready, err = observeSourceAdoption(t.Context(), c, actual)
	require.NoError(t, err)
	require.True(t, ready)
}

func TestModeCannotBypassTerminatingSource(t *testing.T) {
	for _, stack := range []string{coexistence.DP, coexistence.DRA} {
		n := selectedNode(stack)
		n.Labels[coexistence.ModeLabel] = coexistence.DP
		if stack == coexistence.DP {
			n.Labels[coexistence.ModeLabel] = coexistence.DRA
		}
		n.Labels[commonGPULabelKey] = "false"
		nlc := &nodeLabelingController{terminatingDP: stack == coexistence.DP, terminatingDRA: stack == coexistence.DRA}
		changed, err := nlc.selectNode(t.Context(), n)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, stack, n.Annotations[coexistence.SelectionAnnotation])
	}
}

func TestCleanupSameNameNewUIDIsNotOwned(t *testing.T) {
	gc := testSourceGC()
	old := gc.DeepCopy()
	old.UID = "old-uid"
	ds := &appsv1.DaemonSet{ObjectMeta: ownedMeta("old-plugin", "GPUCluster", old)}
	c := lifecycleClient(t, gc, ds)
	workloads, err := ownedWorkloads(t.Context(), c, "test", gc)
	require.NoError(t, err)
	require.Empty(t, workloads)
}

func TestCleanupPatchConflictPreservesConcurrentMode(t *testing.T) {
	cp := testSourceCP()
	node := selectedNode(coexistence.DP)
	base := lifecycleClient(t, cp, node)
	deleteSource(t, base, cp)
	require.NoError(t, setSourceCondition(t.Context(), base, cp, operandsRemoved, true, "TeardownComplete", "done"))
	racing := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		if _, ok := obj.(*corev1.Node); ok {
			latest := &corev1.Node{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), latest))
			latest.Labels[coexistence.ModeLabel] = coexistence.DRA
			require.NoError(t, c.Update(ctx, latest))
		}
		return c.Patch(ctx, obj, p, opts...)
	}})
	r := &NodeLabelingReconciler{Client: racing}
	_, err := r.cleanupDeletedSources(t.Context())
	require.True(t, apierrors.IsConflict(err))
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.Contains(t, cp.Finalizers, nodeCleanupFinalizer)
	r.Client = base
	_, err = r.cleanupDeletedSources(t.Context())
	require.NoError(t, err)
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.Equal(t, coexistence.DRA, node.Labels[coexistence.ModeLabel])
	require.NotContains(t, node.Annotations, coexistence.SelectionAnnotation)
}

func TestSimultaneousSourceDeletionConverges(t *testing.T) {
	cp := testSourceCP()
	gc := testSourceGC()
	node := selectedNode(coexistence.DRA)
	node.Labels[devicePluginDeployLabelKey] = "true"
	node.Labels[draDriverDeployLabelKey] = "false"
	c := lifecycleClient(t, cp, gc, node)
	deleteSource(t, c, cp)
	deleteSource(t, c, gc)
	require.NoError(t, setSourceCondition(t.Context(), c, cp, operandsRemoved, true, "TeardownComplete", "done"))
	require.NoError(t, setSourceCondition(t.Context(), c, gc, operandsRemoved, true, "TeardownComplete", "done"))
	r := &NodeLabelingReconciler{Client: c}
	for range 4 {
		_, err := r.cleanupDeletedSources(t.Context())
		if err != nil {
			require.True(t, apierrors.IsConflict(err))
		}
	}
	require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp)))
	require.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc)))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
	require.NotContains(t, node.Labels, devicePluginDeployLabelKey)
	require.NotContains(t, node.Labels, draDriverDeployLabelKey)
	require.NotContains(t, node.Annotations, coexistence.SelectionAnnotation)
}

func TestTeardownAPIFailureKeepsReservation(t *testing.T) {
	cp := testSourceCP()
	ds := &appsv1.DaemonSet{ObjectMeta: ownedMeta("dp", "ClusterPolicy", cp)}
	base := lifecycleClient(t, cp, ds)
	deleteSource(t, base, cp)
	failing := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
		return fmt.Errorf("delete denied")
	}})
	_, err := teardownSourceOperands(t.Context(), failing, "test", cp)
	require.ErrorContains(t, err, "delete denied")
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.False(t, sourceConditionTrue(cp, operandsRemoved))
	require.Contains(t, cp.Finalizers, nodeCleanupFinalizer)
}

func TestCleanupErrorDoesNotFreezeUnrelatedNodeAssignment(t *testing.T) {
	cp := testSourceCP()
	gc := testSourceGC()
	old := selectedNode(coexistence.DP)
	old.Labels[devicePluginDeployLabelKey] = "true"
	fresh := selectedNode("")
	fresh.Name = "new-node"
	fresh.Labels[coexistence.ModeLabel] = coexistence.DRA
	base := lifecycleClient(t, cp, gc, old, fresh)
	deleteSource(t, base, cp)
	require.NoError(t, setSourceCondition(t.Context(), base, cp, operandsRemoved, true, "TeardownComplete", "done"))
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		if _, ok := obj.(*corev1.Node); ok && obj.GetName() == old.Name {
			return fmt.Errorf("old node cleanup denied")
		}
		return c.Patch(ctx, obj, p, opts...)
	}})
	r := &NodeLabelingReconciler{Client: c, Namespace: "test"}
	_, err := r.Reconcile(t.Context(), ctrl.Request{})
	require.ErrorContains(t, err, "old node cleanup denied")
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(fresh), fresh))
	require.Equal(t, "true", fresh.Labels[draDriverDeployLabelKey])
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(old), old))
	require.Equal(t, coexistence.DP, old.Annotations[coexistence.SelectionAnnotation])
}
