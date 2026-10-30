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

package allocatormode

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestSelection(t *testing.T) {
	for _, cp := range []bool{false, true} {
		for _, gc := range []bool{false, true} {
			for _, hold := range []bool{false, true} {
				for _, dp := range []string{"absent", "true", "false", ""} {
					for _, dra := range []string{"absent", "true", "false", ""} {
						nodeLabels := map[string]string{}
						if dp != "absent" {
							nodeLabels[DevicePluginLabel] = dp
						}
						if dra != "absent" {
							nodeLabels[DRALabel] = dra
						}
						mode, reason := Select(nodeLabels, cp, gc, hold)
						switch {
						case dp != "absent" && dra != "absent":
							require.Empty(t, mode)
							require.Equal(t, "AmbiguousAllocatorHistory", reason)
						case dp != "absent" && cp:
							require.Equal(t, DevicePlugin, mode)
						case dra != "absent" && gc:
							require.Equal(t, DRA, mode)
						case dp == "absent" && dra == "absent" && hold:
							require.Empty(t, mode)
							require.Equal(t, "WaitingForExplicitMode", reason)
						case cp:
							require.Equal(t, DevicePlugin, mode)
						case gc:
							require.Equal(t, DRA, mode)
						default:
							require.Empty(t, mode)
						}
						for _, explicit := range []string{DevicePlugin, DRA, "", "invalid"} {
							nodeLabels[Label] = explicit
							mode, _ = Select(nodeLabels, cp, gc, hold)
							if explicit == DevicePlugin && cp || explicit == DRA && gc {
								require.Equal(t, explicit, mode)
							} else {
								require.Empty(t, mode)
							}
						}
					}
				}
			}
		}
	}
}

func TestPlacementWithStaleLabels(t *testing.T) {
	for _, mode := range []string{DevicePlugin, DRA} {
		spec := corev1.PodSpec{NodeSelector: map[string]string{"component": "true"}, Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "hardware", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"}}}}}}}}}
		Constrain(&spec, mode)
		for _, explicit := range []string{"absent", DevicePlugin, DRA, "", "invalid"} {
			for _, dp := range []bool{false, true} {
				for _, dra := range []bool{false, true} {
					for _, hw := range []bool{false, true} {
						nodeLabels := map[string]string{"component": "true"}
						if hw {
							nodeLabels["hardware"] = "gpu"
						}
						if explicit != "absent" {
							nodeLabels[Label] = explicit
						}
						if dp {
							nodeLabels[DevicePluginLabel] = "false"
						}
						if dra {
							nodeLabels[DRALabel] = "false"
						}
						match := false
						for _, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
							ls := &metav1.LabelSelector{}
							for _, expr := range term.MatchExpressions {
								ls.MatchExpressions = append(ls.MatchExpressions, metav1.LabelSelectorRequirement{Key: expr.Key, Operator: metav1.LabelSelectorOperator(expr.Operator), Values: expr.Values})
							}
							selector, err := metav1.LabelSelectorAsSelector(ls)
							require.NoError(t, err)
							if selector.Matches(labels.Set(nodeLabels)) {
								match = true
							}
						}
						expected := hw && (explicit == mode || explicit == "absent" && ((mode == DevicePlugin && dp && !dra) || (mode == DRA && dra && !dp)))
						require.Equal(t, expected, match, "mode=%s explicit=%s dp=%v dra=%v hw=%v", mode, explicit, dp, dra, hw)
					}
				}
			}
		}
	}
}

func TestEmptyRequiredAffinityRemainsUnschedulable(t *testing.T) {
	spec := corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{}}}}
	Constrain(&spec, DRA)
	require.Empty(t, spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
}

func TestEmptyRequiredTermRemainsUnschedulable(t *testing.T) {
	spec := corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{}}}}}}
	Constrain(&spec, DRA)
	require.Empty(t, spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
}
