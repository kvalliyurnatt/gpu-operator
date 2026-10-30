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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvidiav1alpha1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1alpha1"
)

func TestCoexistenceScopesGPUClusterButNotIndependentDriver(t *testing.T) {
	for name, owner := range map[string]client.Object{
		"gpu-cluster": &nvidiav1alpha1.GPUCluster{ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster", UID: "gc-uid"}},
		"driver":      &nvidiav1alpha1.NVIDIADriver{ObjectMeta: metav1.ObjectMeta{Name: "driver", UID: "driver-uid"}},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, appsv1.AddToScheme(scheme))
			require.NoError(t, nvidiav1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			s := &stateSkel{name: "test-state", client: c, scheme: scheme}
			ds := &appsv1.DaemonSet{
				TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
				ObjectMeta: metav1.ObjectMeta{Name: "operand", Namespace: "test"},
				Spec: appsv1.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "operand"}},
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "operand"}},
						Spec: corev1.PodSpec{NodeSelector: map[string]string{"component": "true"}}},
				},
			}
			object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ds)
			require.NoError(t, err)
			_, err = s.syncObjects(t.Context(), owner, []*unstructured.Unstructured{{Object: object}})
			require.NoError(t, err)
			actual := &appsv1.DaemonSet{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ds), actual))
			require.Equal(t, "true", actual.Spec.Template.Spec.NodeSelector["component"])
			if name == "gpu-cluster" {
				require.NotNil(t, actual.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
			} else {
				require.Nil(t, actual.Spec.Template.Spec.Affinity)
			}
		})
	}
}
