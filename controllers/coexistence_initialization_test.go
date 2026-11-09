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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	gpu "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
	"github.com/NVIDIA/gpu-operator/internal/coexistence"
	"github.com/NVIDIA/gpu-operator/internal/conditions"
	"github.com/NVIDIA/gpu-operator/internal/state"
)

func coexistenceConfig() *gpu.GPUCluster {
	return &gpu.GPUCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster", Generation: 1, CreationTimestamp: metav1.NewTime(time.Unix(2, 0))},
	}
}

func TestCoexistenceInitialPrerequisites(t *testing.T) {
	for name, tc := range map[string]struct {
		present, terminating, legacy bool
		created                      int64
		wantReason                   string
	}{

		"CP terminating": {present: true, terminating: true, created: 1, wantReason: conditions.ClusterPolicyRequired},
		"CP newer":       {present: true, created: 3, wantReason: conditions.PrerequisiteNotMet},
		"CP same time":   {present: true, created: 2, wantReason: conditions.PrerequisiteNotMet},
		"legacy driver":  {present: true, legacy: true, created: 1, wantReason: conditions.PrerequisiteNotMet},
		"active CP":      {present: true, created: 1},
	} {
		t.Run(name, func(t *testing.T) {
			gc := coexistenceConfig()
			objects := []client.Object{gc}
			if tc.present {
				cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp", CreationTimestamp: metav1.NewTime(time.Unix(tc.created, 0))}}
				cp.Spec.Driver.Enabled = new(tc.legacy)
				if tc.terminating {
					now := metav1.Now()
					cp.DeletionTimestamp = &now
					cp.Finalizers = []string{"test/hold"}
				}
				objects = append(objects, cp)
			}
			r, c := newGPUClusterReconciler(t, objects...)
			r.conditionUpdater = conditions.NewGPUClusterUpdater(c)
			gccReconcile(t, r, gc.Name)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
			require.Equal(t, gpu.NotReady, gc.Status.State)
			require.Zero(t, r.stateManager.(*fakeStateManager).syncCalls)
			if tc.wantReason != "" {
				require.False(t, coexistenceInitialized(gc))
				require.True(t, meta.IsStatusConditionFalse(gc.Status.Conditions, conditions.Ready))
				errorCondition := meta.FindStatusCondition(gc.Status.Conditions, conditions.Error)
				require.NotNil(t, errorCondition)
				require.Equal(t, tc.wantReason, errorCondition.Reason)
			} else {
				initializeForTest(t, r, gc)
				require.True(t, coexistenceInitialized(gc))
				gccReconcile(t, r, gc.Name)
				require.Equal(t, gpu.Ready, gccState(t, c, gc.Name))
				require.Equal(t, 1, r.stateManager.(*fakeStateManager).syncCalls)
			}
		})
	}
}

func TestCoexistenceInitializationSurvivesCPDeletionAndRestart(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	r, c := newGPUClusterReconciler(t, gc, cp)
	r.conditionUpdater = conditions.NewGPUClusterUpdater(c)
	initializeForTest(t, r, gc)
	require.NoError(t, c.Delete(t.Context(), cp))
	r.stateManager = &fakeStateManager{results: state.Results{Status: state.SyncStateNotReady}}
	gccReconcile(t, r, gc.Name)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, coexistenceInitialized(gc))
	require.Equal(t, gpu.NotReady, gc.Status.State)

	// Restart with the same durable CR annotations.
	gc.Generation++
	require.NoError(t, c.Update(t.Context(), gc))
	// A new reconciler has no process-local history of the earlier CP.
	restarted := &GPUClusterReconciler{Client: c, Namespace: r.Namespace, stateManager: &fakeStateManager{results: state.Results{Status: state.SyncStateReady}}, conditionUpdater: conditions.NewGPUClusterUpdater(c)}
	gccReconcile(t, restarted, gc.Name)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.Equal(t, gpu.Ready, gc.Status.State)
	require.True(t, coexistenceInitialized(gc))

	// A replacement object does not inherit the previous CR's initialization.
	require.NoError(t, c.Delete(t.Context(), gc))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	gc.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), gc))
	replacement := coexistenceConfig()
	require.NoError(t, c.Create(t.Context(), replacement))
	gccReconcile(t, restarted, replacement.Name)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(replacement), replacement))
	require.Equal(t, gpu.Ready, replacement.Status.State)
	require.False(t, coexistenceInitialized(replacement))
}

func TestCoexistenceInitializationConflictPreventsActivation(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	r, c := newGPUClusterReconciler(t, gc, cp)
	// Reach the status-write phase after both metadata patches have been observed.
	gccReconcile(t, r, gc.Name)
	gccReconcile(t, r, gc.Name)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			return apierrors.NewConflict(schema.GroupResource{Group: "nvidia.com", Resource: "gpuclusters"}, obj.GetName(), nil)
		},
	})
	_, err := r.Reconcile(t.Context(), gccRequest(gc.Name))
	require.True(t, apierrors.IsConflict(err))
	require.Zero(t, r.stateManager.(*fakeStateManager).syncCalls)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.False(t, coexistenceInitialized(gc))
	require.Nil(t, gpuClusterForAllocation(cp, gc))

	r.Client = c
	initializeForTest(t, r, gc)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, coexistenceInitialized(gc))
}

func TestNodeLabelingWaitsForCoexistenceInitialization(t *testing.T) {
	for _, withCP := range []bool{true} {
		t.Run(map[bool]string{false: "CP absent", true: "CP present"}[withCP], func(t *testing.T) {
			gc := coexistenceConfig()
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true"}}}
			objects := []client.Object{gc, node}
			if withCP {
				cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
				cp.Spec.Driver.Enabled = new(false)
				objects = append(objects, cp)
			}
			r, c := newGPUClusterReconciler(t, objects...)
			nlc := &NodeLabelingReconciler{Client: c, Scheme: r.Scheme, Namespace: r.Namespace}
			_, err := nlc.Reconcile(t.Context(), ctrl.Request{})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			require.NotContains(t, node.Labels, draDriverDeployLabelKey)
			if withCP {
				require.Equal(t, coexistence.DP, node.Annotations[coexistence.SelectionAnnotation])
			} else {
				require.NotContains(t, node.Annotations, coexistence.SelectionAnnotation)
				return
			}
			initializeForTest(t, r, gc)
			// A fresh explicit-DRA node becomes eligible after observing the status decision.
			node.Labels[coexistence.ModeLabel] = coexistence.DRA
			require.NoError(t, c.Update(t.Context(), node))
			_, err = nlc.Reconcile(t.Context(), ctrl.Request{})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(node), node))
			require.Equal(t, "true", node.Labels[draDriverDeployLabelKey])
			require.Equal(t, coexistence.DRA, node.Annotations[coexistence.SelectionAnnotation])
		})
	}
}

func TestCoexistenceInitializationTriggersNodeLabeling(t *testing.T) {
	gc := coexistenceConfig()
	p := gpuClusterAllocationPredicate()
	ready := gc.DeepCopy()
	meta.SetStatusCondition(&ready.Status.Conditions, metav1.Condition{Type: conditions.CoexistenceInitialized, Status: metav1.ConditionTrue, Reason: "PrerequisitesMet"})
	require.True(t, p.Update(event.TypedUpdateEvent[*gpu.GPUCluster]{ObjectOld: gc, ObjectNew: ready}))
	changed := ready.DeepCopy()
	meta.SetStatusCondition(&changed.Status.Conditions, metav1.Condition{Type: conditions.Ready, Status: metav1.ConditionFalse, Reason: conditions.OperandNotReady})
	require.False(t, p.Update(event.TypedUpdateEvent[*gpu.GPUCluster]{ObjectOld: ready, ObjectNew: changed}))
	changed.Generation++
	require.True(t, p.Update(event.TypedUpdateEvent[*gpu.GPUCluster]{ObjectOld: ready, ObjectNew: changed}))
}

func TestCoexistenceWaitsForNodeAdoption(t *testing.T) {
	t.Setenv(coexistence.DefaultStackEnv, coexistence.DRA)
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	// An old Ready status must not allow adoption to be skipped during upgrade.
	cp.Status.State = gpuv1.Ready
	node := func(name string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true"}}}
	}
	adopted, existing, explicit := node("adopted"), node("existing"), node("explicit")
	adopted.Annotations = map[string]string{coexistence.SelectionAnnotation: coexistence.DP}
	existing.Labels["nvidia.com/gpu.deploy.device-plugin"] = "true"
	explicit.Labels[coexistence.ModeLabel] = coexistence.DRA
	r, c := newGPUClusterReconciler(t, gc, cp, adopted, existing, explicit)
	r.conditionUpdater = conditions.NewGPUClusterUpdater(c)
	manager := r.stateManager.(*fakeStateManager)
	assertPending := func() {
		t.Helper()
		result, err := r.Reconcile(t.Context(), gccRequest(gc.Name))
		require.NoError(t, err)
		require.Positive(t, result.RequeueAfter)
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
		require.False(t, coexistenceInitialized(gc))
		require.Equal(t, gpu.NotReady, gc.Status.State)
		require.True(t, meta.IsStatusConditionFalse(gc.Status.Conditions, conditions.Ready))
		require.Equal(t, conditions.NodeAdoptionPending, meta.FindStatusCondition(gc.Status.Conditions, conditions.Error).Reason)
		require.Zero(t, manager.syncCalls)
	}
	assertPending()
	nlc := &NodeLabelingReconciler{Client: c, Scheme: r.Scheme, Namespace: r.Namespace}
	labelNodes := func() {
		t.Helper()
		_, err := nlc.Reconcile(t.Context(), ctrl.Request{})
		require.NoError(t, err)
	}
	labelNodes()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(existing), existing))
	require.Equal(t, coexistence.DP, existing.Annotations[coexistence.SelectionAnnotation])
	require.NotContains(t, existing.Labels, draDriverDeployLabelKey)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(explicit), explicit))
	require.Equal(t, coexistence.DRA, explicit.Annotations[coexistence.SelectionAnnotation])
	require.NotContains(t, explicit.Labels, draDriverDeployLabelKey)

	// A visible node joining before initialization is adopted under CP as well.
	before := node("before-initialization")
	require.NoError(t, c.Create(t.Context(), before))
	assertPending()
	labelNodes()
	initializeForTest(t, r, gc)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, coexistenceInitialized(gc))
	require.Zero(t, manager.syncCalls)

	// The DRA preference starts at initialization; recorded DP selections survive.
	after := node("after-initialization")
	require.NoError(t, c.Create(t.Context(), after))
	labelNodes()
	for _, n := range []*corev1.Node{adopted, existing, before} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(n), n))
		require.Equal(t, coexistence.DP, n.Annotations[coexistence.SelectionAnnotation])
		require.Equal(t, "true", n.Labels["nvidia.com/gpu.deploy.device-plugin"])
		require.NotContains(t, n.Labels, draDriverDeployLabelKey)
	}
	for _, n := range []*corev1.Node{explicit, after} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(n), n))
		require.Equal(t, coexistence.DRA, n.Annotations[coexistence.SelectionAnnotation])
		require.Equal(t, "true", n.Labels[draDriverDeployLabelKey])
	}
	gccReconcile(t, r, gc.Name)
	require.Equal(t, 1, manager.syncCalls)
	require.Equal(t, gpu.Ready, gccState(t, c, gc.Name))
}

func TestCoexistenceAdoptionListFailurePreventsInitialization(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	r, c := newGPUClusterReconciler(t, gc, cp)
	r.conditionUpdater = conditions.NewGPUClusterUpdater(c)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				return apierrors.NewServiceUnavailable("node cache unavailable")
			}
			return c.List(ctx, list, opts...)
		},
	})
	_, err := r.Reconcile(t.Context(), gccRequest(gc.Name))
	require.ErrorContains(t, err, "node cache unavailable")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.False(t, coexistenceInitialized(gc))
	require.Equal(t, gpu.NotReady, gc.Status.State)
	require.Zero(t, r.stateManager.(*fakeStateManager).syncCalls)
}

// Each pass observes the previous persisted write, like a refreshed informer cache.
func initializeForTest(t *testing.T, r *GPUClusterReconciler, gc *gpu.GPUCluster) {
	t.Helper()
	for range 4 {
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
		if coexistenceInitialized(gc) {
			return
		}
		gccReconcile(t, r, gc.Name)
	}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, coexistenceInitialized(gc))
}

func TestParticipationWritesMustBeObservedBeforeActivation(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	r, c := newGPUClusterReconciler(t, cp, gc)
	for step := range 3 {
		gccReconcile(t, r, gc.Name)
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
		require.True(t, participates(gc))
		require.Equal(t, step >= 1, participates(cp))
		require.Equal(t, step == 2, coexistenceInitialized(gc))
		require.Zero(t, r.stateManager.(*fakeStateManager).syncCalls)
		if step < 2 {
			require.Nil(t, gpuClusterForAllocation(cp, gc))
		}
	}
	gccReconcile(t, r, gc.Name)
	require.Equal(t, 1, r.stateManager.(*fakeStateManager).syncCalls)
}

func TestPartialParticipationWriteConflictResumesAfterRestart(t *testing.T) {
	gc := coexistenceConfig()
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp"}}
	cp.Spec.Driver.Enabled = new(false)
	r, c := newGPUClusterReconciler(t, cp, gc)
	gccReconcile(t, r, gc.Name) // Persist GPUCluster participation first.
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*gpuv1.ClusterPolicy); ok {
				return apierrors.NewConflict(schema.GroupResource{Group: "nvidia.com", Resource: "clusterpolicies"}, obj.GetName(), nil)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	_, err := r.Reconcile(t.Context(), gccRequest(gc.Name))
	require.True(t, apierrors.IsConflict(err))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.True(t, participates(gc))
	require.False(t, coexistenceInitialized(gc))
	require.Nil(t, gpuClusterForAllocation(cp, gc))
	require.Zero(t, r.stateManager.(*fakeStateManager).syncCalls)
	restarted := &GPUClusterReconciler{Client: c, Namespace: r.Namespace, stateManager: &fakeStateManager{results: state.Results{Status: state.SyncStateReady}}, conditionUpdater: conditions.NewGPUClusterUpdater(c)}
	initializeForTest(t, restarted, gc)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cp), cp))
	require.True(t, participates(cp))
}

func TestStandaloneGPUClusterNeedsNoCoexistenceInitialization(t *testing.T) {
	gc := coexistenceConfig()
	r, c := newGPUClusterReconciler(t, gc)
	gccReconcile(t, r, gc.Name)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(gc), gc))
	require.Equal(t, gpu.Ready, gc.Status.State)
	require.False(t, participates(gc))
	require.False(t, coexistenceInitialized(gc))
	require.NotNil(t, gpuClusterForAllocation(nil, gc))
}

func TestParticipationAndTerminationTriggerNodeLabeling(t *testing.T) {
	cp := &gpuv1.ClusterPolicy{}
	updated := cp.DeepCopy()
	updated.Annotations = map[string]string{coexistence.ParticipationAnnotation: "true"}
	p := singletonCRPredicate[*gpuv1.ClusterPolicy]()
	require.True(t, p.Update(event.TypedUpdateEvent[*gpuv1.ClusterPolicy]{ObjectOld: cp, ObjectNew: updated}))
	now := metav1.Now()
	updated.DeletionTimestamp = &now
	require.True(t, p.Update(event.TypedUpdateEvent[*gpuv1.ClusterPolicy]{ObjectOld: cp, ObjectNew: updated}))
	require.Nil(t, clusterPolicyForAllocation(updated))
}

func TestTerminatingGPUClusterPreservesExistingPluginButDoesNotActivateNewNodes(t *testing.T) {
	gc := coexistenceConfig()
	now := metav1.Now()
	gc.DeletionTimestamp = &now
	gc.Finalizers = []string{gpuClusterFinalizer}
	gc.Annotations = map[string]string{coexistence.ParticipationAnnotation: "true"}
	gc.Status.Conditions = []metav1.Condition{{Type: conditions.CoexistenceInitialized, Status: metav1.ConditionTrue, Reason: "PrerequisitesMet"}}
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cp", Annotations: map[string]string{coexistence.ParticipationAnnotation: "true"}}}
	cp.Spec.Driver.Enabled = new(false)
	t.Setenv(coexistence.DefaultStackEnv, coexistence.DRA)
	existing := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "existing", Labels: map[string]string{
		commonGPULabelKey: "true", "feature.node.kubernetes.io/pci-10de.present": "true", draDriverDeployLabelKey: "true",
	}, Annotations: map[string]string{coexistence.SelectionAnnotation: coexistence.DRA}}}
	newNode := existing.DeepCopy()
	newNode.Name = "new"
	newNode.Annotations = nil
	delete(newNode.Labels, draDriverDeployLabelKey)
	r, c := newGPUClusterReconciler(t, cp, gc, existing, newNode)
	nlc := &NodeLabelingReconciler{Client: c, Scheme: r.Scheme, Namespace: r.Namespace}
	_, err := nlc.Reconcile(t.Context(), ctrl.Request{})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(existing), existing))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(newNode), newNode))
	require.Equal(t, "true", existing.Labels[draDriverDeployLabelKey])
	require.NotContains(t, newNode.Labels, draDriverDeployLabelKey)
	require.Equal(t, coexistence.DRA, newNode.Annotations[coexistence.SelectionAnnotation])
	// A second pass must not turn a waiting record into a deployment label.
	_, err = nlc.Reconcile(t.Context(), ctrl.Request{})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(newNode), newNode))
	require.NotContains(t, newNode.Labels, draDriverDeployLabelKey)
	gc.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), gc))
	_, err = nlc.Reconcile(t.Context(), ctrl.Request{})
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(existing), existing))
	require.NotContains(t, existing.Labels, draDriverDeployLabelKey)
	require.NotContains(t, existing.Labels, "nvidia.com/gpu.deploy.device-plugin")
	require.Equal(t, coexistence.DRA, existing.Annotations[coexistence.SelectionAnnotation])
}
