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
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	gpu "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
	"github.com/NVIDIA/gpu-operator/internal/coexistence"
	"github.com/NVIDIA/gpu-operator/internal/consts"
)

const (
	nodeCleanupFinalizer     = "nvidia.com/gpu-operator-node-cleanup"
	operandsRemoved          = "OperandsRemoved"
	nodeSelectionInitialized = "NodeSelectionInitialized"
	lifecycleRetry           = 5 * time.Second
	workloadOwnerUIDIndex    = "gpu-operator.workload-owner-uid"
)

func sourceStack(obj client.Object) string {
	if _, ok := obj.(*gpuv1.ClusterPolicy); ok {
		return coexistence.DP
	}
	return coexistence.DRA
}
func sourceConditions(obj client.Object) *[]metav1.Condition {
	switch obj := obj.(type) {
	case *gpuv1.ClusterPolicy:
		return &obj.Status.Conditions
	case *gpu.GPUCluster:
		return &obj.Status.Conditions
	default:
		return nil
	}
}
func sourceConditionTrue(obj client.Object, name string) bool {
	cs := sourceConditions(obj)
	return cs != nil && meta.IsStatusConditionTrue(*cs, name)
}
func sourceConditionsChanged(a, b client.Object) bool {
	// Ignore ordinary operand-readiness churn; only lifecycle conditions gate
	// node selection and cleanup.
	for _, name := range []string{nodeSelectionInitialized, operandsRemoved} {
		if sourceConditionTrue(a, name) != sourceConditionTrue(b, name) {
			return true
		}
	}
	return false
}

// Source controllers alone call this helper; the node-label controller only reads status.
func setSourceCondition(ctx context.Context, c client.Client, obj client.Object, name string, value bool, reason, message string) error {
	original := obj.DeepCopyObject().(client.Object)
	// Adoption can be the first status write; the GPUCluster schema requires state.
	switch o := obj.(type) {
	case *gpuv1.ClusterPolicy:
		if o.Status.State == "" {
			o.Status.State = gpuv1.NotReady
		}
	case *gpu.GPUCluster:
		if o.Status.State == "" {
			o.Status.State = gpu.NotReady
		}
	}
	status := metav1.ConditionFalse
	if value {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(sourceConditions(obj), metav1.Condition{Type: name, Status: status, Reason: reason, Message: message, ObservedGeneration: obj.GetGeneration()})
	if !value || name == operandsRemoved {
		meta.SetStatusCondition(sourceConditions(obj), metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: obj.GetGeneration()})
		switch o := obj.(type) {
		case *gpuv1.ClusterPolicy:
			o.Status.State = gpuv1.NotReady
		case *gpu.GPUCluster:
			o.Status.State = gpu.NotReady
		}
	}
	if reflect.DeepEqual(original, obj) {
		return nil
	}
	return c.Status().Patch(ctx, obj, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{}))
}

// Observe the persisted finalizer in a subsequent pass before rendering operands.
func registerNodeCleanup(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	if controllerutil.ContainsFinalizer(obj, nodeCleanupFinalizer) {
		return true, nil
	}
	original := obj.DeepCopyObject().(client.Object)
	controllerutil.AddFinalizer(obj, nodeCleanupFinalizer)
	return false, c.Patch(ctx, obj, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{}))
}

func observeSourceAdoption(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	if sourceConditionTrue(obj, nodeSelectionInitialized) {
		return true, nil
	}
	pending, err := sourceAdoptionPending(ctx, c, obj)
	reason, message := "AdoptionComplete", "Observed selection records and compatible placement on eligible discovered nodes"
	if pending || err != nil {
		reason, message = "NodeAdoptionPending", "Waiting to observe selection records and compatible placement"
	}
	if err != nil {
		message = err.Error()
	}
	if statusErr := setSourceCondition(ctx, c, obj, nodeSelectionInitialized, !pending && err == nil, reason, message); statusErr != nil {
		return false, statusErr
	}
	// Even a successful status patch is observed before activation in another pass.
	return false, err
}

func ownerUIDs(obj client.Object) []string {
	var keys []string
	for _, ref := range obj.GetOwnerReferences() {
		keys = append(keys, string(ref.UID))
	}
	return keys
}

func registerWorkloadIndexes(ctx context.Context, mgr ctrl.Manager) error {
	for _, obj := range []client.Object{&appsv1.DaemonSet{}, &appsv1.Deployment{}, &appsv1.ReplicaSet{}, &appsv1.StatefulSet{}, &corev1.Pod{}} {
		if err := mgr.GetFieldIndexer().IndexField(ctx, obj, workloadOwnerUIDIndex, ownerUIDs); err != nil {
			return err
		}
	}
	return nil
}

// Follow ownership UIDs through Deployment -> ReplicaSet -> Pod and DS -> Pod.
// Cached indexed lists avoid scanning unrelated workloads or using an APIReader.
func ownedWorkloads(ctx context.Context, c client.Client, namespace string, owner client.Object) ([]client.Object, error) {
	queue := []types.UID{owner.GetUID()}
	seen := map[types.UID]bool{}
	var result []client.Object
	for len(queue) > 0 {
		uid := queue[0]
		queue = queue[1:]
		if uid == "" || seen[uid] {
			continue
		}
		seen[uid] = true
		for _, list := range []client.ObjectList{&appsv1.DaemonSetList{}, &appsv1.DeploymentList{}, &appsv1.ReplicaSetList{}, &appsv1.StatefulSetList{}, &corev1.PodList{}} {
			if err := c.List(ctx, list, client.InNamespace(namespace), client.MatchingFields{workloadOwnerUIDIndex: string(uid)}); err != nil {
				return nil, err
			}
			if err := meta.EachListItem(list, func(item runtime.Object) error {
				obj := item.(client.Object)
				result = append(result, obj)
				if _, pod := obj.(*corev1.Pod); !pod {
					queue = append(queue, obj.GetUID())
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// The first phase removes only claim-consuming workload controllers. Foreground
// deletion keeps their owner chain available until the kubelet has removed Pods.
func teardownSourceOperands(ctx context.Context, c client.Client, namespace string, obj client.Object) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, nodeCleanupFinalizer) {
		return ctrl.Result{}, nil
	}
	if sourceConditionTrue(obj, operandsRemoved) {
		return ctrl.Result{RequeueAfter: lifecycleRetry}, nil
	}
	workloads, err := ownedWorkloads(ctx, c, namespace, obj)
	if err != nil {
		_ = setSourceCondition(ctx, c, obj, operandsRemoved, false, "OperandTeardownPending", err.Error())
		return ctrl.Result{}, err
	}
	var consumers []client.Object
	if sourceStack(obj) == coexistence.DRA {
		for _, workload := range workloads {
			switch w := workload.(type) {
			case *appsv1.DaemonSet:
				if len(w.Spec.Template.Spec.ResourceClaims) > 0 {
					consumers = append(consumers, w)
				}
			case *appsv1.Deployment:
				if len(w.Spec.Template.Spec.ResourceClaims) > 0 {
					consumers = append(consumers, w)
				}
			case *appsv1.ReplicaSet:
				if len(w.Spec.Template.Spec.ResourceClaims) > 0 {
					consumers = append(consumers, w)
				}
			case *appsv1.StatefulSet:
				if len(w.Spec.Template.Spec.ResourceClaims) > 0 {
					consumers = append(consumers, w)
				}
			case *corev1.Pod:
				if len(w.Spec.ResourceClaims) > 0 {
					consumers = append(consumers, w)
				}
			}
		}
	}
	if len(workloads) == 0 {
		if err := setSourceCondition(ctx, c, obj, operandsRemoved, true, "TeardownComplete", "Observed all source-owned operand workloads and Pods removed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: lifecycleRetry}, nil
	}
	if err := setSourceCondition(ctx, c, obj, operandsRemoved, false, "OperandTeardownPending", "Waiting for source-owned operand workloads and Pods to be removed"); err != nil {
		return ctrl.Result{}, err
	}
	targets := workloads
	if len(consumers) > 0 {
		targets = consumers
	}
	for _, target := range targets {
		if !target.GetDeletionTimestamp().IsZero() {
			continue
		}
		uid := target.GetUID()
		if err := c.Delete(ctx, target, client.PropagationPolicy(metav1.DeletePropagationForeground), client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			_ = setSourceCondition(ctx, c, obj, operandsRemoved, false, "OperandTeardownPending", err.Error())
			return ctrl.Result{}, fmt.Errorf("delete %T %s: %w", target, target.GetName(), err)
		}
	}
	return ctrl.Result{RequeueAfter: lifecycleRetry}, nil
}

func stackExclusiveKeys(stack string) map[string]bool {
	if stack == coexistence.DP {
		return devicePluginOnlyStateLabelKeys()
	}
	keys := map[string]bool{}
	for key := range gpuClusterStateLabels {
		if key != driverDeployLabelKey {
			keys[key] = true
		}
	}
	return keys
}
func hasStackMetadata(node *corev1.Node, stack string) bool {
	if node.Annotations[coexistence.SelectionAnnotation] == stack {
		return true
	}
	for key := range stackExclusiveKeys(stack) {
		if _, ok := node.Labels[key]; ok {
			return true
		}
	}
	return false
}

// Metadata is removed value-blind, but only for the deleted stack. A retry must
// preserve a surviving stack's record, customer intent, and independent drivers.
func cleanSourceNode(node *corev1.Node, source client.Object, keepDriver bool) bool {
	changed := false
	for key := range stackExclusiveKeys(sourceStack(source)) {
		if _, ok := node.Labels[key]; ok {
			delete(node.Labels, key)
			changed = true
		}
	}
	if node.Annotations[coexistence.SelectionAnnotation] == sourceStack(source) {
		delete(node.Annotations, coexistence.SelectionAnnotation)
		changed = true
	}
	if !keepDriver && node.Labels[consts.NVIDIADriverOwnerLabel] == "" {
		for _, key := range []string{driverDeployLabelKey, vgpuManagerDeployLabelKey} {
			if _, ok := node.Labels[key]; ok {
				delete(node.Labels, key)
				changed = true
			}
		}
	}
	return changed
}

func (r *NodeLabelingReconciler) cleanupDeletedSources(ctx context.Context) (bool, error) {
	cps := &gpuv1.ClusterPolicyList{}
	gcs := &gpu.GPUClusterList{}
	if err := r.List(ctx, cps); err != nil {
		return false, err
	}
	if err := r.List(ctx, gcs); err != nil {
		return false, err
	}
	sources := []client.Object{}
	for i := range cps.Items {
		sources = append(sources, &cps.Items[i])
	}
	for i := range gcs.Items {
		sources = append(sources, &gcs.Items[i])
	}
	var deleting []client.Object
	keepDriver := false
	for _, obj := range sources {
		if obj.GetDeletionTimestamp().IsZero() {
			keepDriver = true
		}
		if cp, ok := obj.(*gpuv1.ClusterPolicy); !ok || cp.Spec.Driver.UseNvidiaDriverCRDType() {
			keepDriver = true
		}
		if !obj.GetDeletionTimestamp().IsZero() && controllerutil.ContainsFinalizer(obj, nodeCleanupFinalizer) {
			deleting = append(deleting, obj)
		}
	}
	if len(deleting) == 0 {
		return false, nil
	}
	if !keepDriver {
		drivers := &gpu.NVIDIADriverList{}
		if err := r.List(ctx, drivers); err != nil {
			return true, err
		}
		keepDriver = len(drivers.Items) > 0
	}
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return true, err
	}
	for _, obj := range deleting {
		if !sourceConditionTrue(obj, operandsRemoved) {
			continue
		}
		patched := false
		for i := range nodes.Items {
			// Do not mutate the shared snapshot: a later source's patch must also be
			// conditional on the resourceVersion actually observed in the cache.
			node := nodes.Items[i].DeepCopy()
			original := node.DeepCopy()
			if !cleanSourceNode(node, obj, keepDriver) {
				continue
			}
			if err := r.Patch(ctx, node, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return true, err
			}
			patched = true
		}
		if patched {
			continue
		}
		original := obj.DeepCopyObject().(client.Object)
		controllerutil.RemoveFinalizer(obj, nodeCleanupFinalizer)
		if err := r.Patch(ctx, obj, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil && !apierrors.IsNotFound(err) {
			return true, err
		}
	}
	return true, nil
}
