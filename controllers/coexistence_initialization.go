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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	nvidiav1alpha1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
	"github.com/NVIDIA/gpu-operator/internal/coexistence"
	"github.com/NVIDIA/gpu-operator/internal/conditions"
)

func coexistenceInitialized(gc *nvidiav1alpha1.GPUCluster) bool {
	return meta.IsStatusConditionTrue(gc.Status.Conditions, conditions.CoexistenceInitialized)
}

// initializeCoexistence persists the decision independently of operand readiness so
// CP deletion or an operator restart cannot invalidate an initialized installation.
func (r *GPUClusterReconciler) initializeCoexistence(ctx context.Context, gc *nvidiav1alpha1.GPUCluster) error {
	patch := client.MergeFromWithOptions(gc.DeepCopy(), client.MergeFromWithOptimisticLock{})
	meta.SetStatusCondition(&gc.Status.Conditions, metav1.Condition{
		Type:               conditions.CoexistenceInitialized,
		Status:             metav1.ConditionTrue,
		Reason:             "PrerequisitesMet",
		Message:            "Coexistence prerequisites, existing GPU node selections, and participation annotations on both CRs observed",
		ObservedGeneration: gc.Generation,
	})
	gc.Status.State = nvidiav1alpha1.NotReady
	gc.Status.Namespace = r.Namespace
	if err := r.Status().Patch(ctx, gc, patch); err != nil {
		return fmt.Errorf("failed to initialize GPUCluster coexistence: %w", err)
	}
	return nil
}

// participates is durable across deletion of the peer and operator restarts.
func participates(obj client.Object) bool {
	return obj.GetAnnotations()[coexistence.ParticipationAnnotation] == "true"
}

func allocationPolicy(cp *gpuv1.ClusterPolicy, gc *nvidiav1alpha1.GPUCluster) (bool, string, error) {
	defaultStack, err := coexistence.DefaultStack()
	preserve := (cp != nil && participates(cp)) || (gc != nil && participates(gc))
	// Before participation is recorded, ordinary CP selection adopts existing nodes.
	// After adoption, initialization gates activation, not the default for new nodes.
	// A replacement GPUCluster must not change that default while it initializes.
	return preserve, defaultStack, err
}

func clusterPolicyForAllocation(cp *gpuv1.ClusterPolicy) *gpuv1.ClusterPolicy {
	if cp != nil && !cp.DeletionTimestamp.IsZero() {
		return nil
	}
	return cp
}

// gpuClusterForAllocation uses observed metadata and status, never a local write
// result, to gate activation. A participation annotation alone is not readiness.
func gpuClusterForAllocation(cp *gpuv1.ClusterPolicy, gc *nvidiav1alpha1.GPUCluster) *nvidiav1alpha1.GPUCluster {
	if gc == nil || !gc.DeletionTimestamp.IsZero() {
		return nil
	}
	if cp != nil {
		if !coexistenceInitialized(gc) || !participates(gc) || !participates(cp) ||
			(cp.Spec.Driver.IsEnabled() && !cp.Spec.Driver.UseNvidiaDriverCRDType()) {
			return nil
		}
	} else if participates(gc) && !coexistenceInitialized(gc) {
		return nil
	}
	return gc
}

// observeParticipation writes at most one CR per pass. Later cache observations
// must confirm both writes before status permits the second stack to start.
func (r *GPUClusterReconciler) observeParticipation(ctx context.Context, cp *gpuv1.ClusterPolicy, gc *nvidiav1alpha1.GPUCluster) (bool, error) {
	for _, obj := range []client.Object{gc, cp} {
		if participates(obj) {
			continue
		}
		patch := client.MergeFromWithOptions(obj.DeepCopyObject().(client.Object), client.MergeFromWithOptimisticLock{})
		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[coexistence.ParticipationAnnotation] = "true"
		obj.SetAnnotations(annotations)
		if err := r.Patch(ctx, obj, patch); err != nil {
			return false, fmt.Errorf("recording coexistence on %T %s: %w", obj, obj.GetName(), err)
		}
		return false, nil
	}
	return true, nil
}

func gpuClusterAllocationPredicate() predicate.TypedFuncs[*nvidiav1alpha1.GPUCluster] {
	p := singletonCRPredicate[*nvidiav1alpha1.GPUCluster]()
	baseUpdate := p.UpdateFunc
	p.UpdateFunc = func(e event.TypedUpdateEvent[*nvidiav1alpha1.GPUCluster]) bool {
		return baseUpdate(e) || coexistenceInitialized(e.ObjectNew) != coexistenceInitialized(e.ObjectOld)
	}
	return p
}
