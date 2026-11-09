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

package coexistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestSelect(t *testing.T) {
	for name, tc := range map[string]struct {
		mode, record                       string
		explicit, cp, gc, preserve, newDRA bool
		want                               string
		invalid                            bool
	}{
		"fresh CP":                            {cp: true, want: DP},
		"fresh GC":                            {gc: true, want: DRA},
		"no sources":                          {},
		"normal replacement ignores old DP":   {gc: true, record: DP, want: DRA},
		"normal CP replaces old DRA":          {cp: true, record: DRA, want: DP},
		"existing DP ignores new default":     {cp: true, gc: true, preserve: true, newDRA: true, record: DP, want: DP},
		"existing DRA ignores new default":    {cp: true, gc: true, preserve: true, record: DRA, want: DRA},
		"new DRA":                             {cp: true, gc: true, preserve: true, newDRA: true, want: DRA},
		"missing GC retains DRA":              {cp: true, preserve: true, record: DRA, want: DRA},
		"new DRA without GC":                  {cp: true, preserve: true, newDRA: true, want: DRA},
		"single GC ignores default DP":        {gc: true, want: DRA},
		"single CP ignores default DRA":       {cp: true, newDRA: true, want: DP},
		"new DP":                              {cp: true, gc: true, preserve: true, want: DP},
		"missing CP waits":                    {gc: true, preserve: true, record: DP, want: DP},
		"new node after CP deletion":          {gc: true, preserve: true, want: DP},
		"explicit DP absent":                  {gc: true, mode: DP, explicit: true, want: DP},
		"explicit DRA absent":                 {cp: true, mode: DRA, explicit: true, want: DRA},
		"explicit overrides annotation":       {cp: true, gc: true, preserve: true, mode: DRA, explicit: true, record: DP, want: DRA},
		"removed explicit retains annotation": {cp: true, gc: true, preserve: true, record: DRA, want: DRA},
		"explicit repairs invalid annotation": {gc: true, mode: DRA, explicit: true, record: "bad", want: DRA},
		"invalid annotation":                  {gc: true, record: "bad", invalid: true},
		"invalid mode":                        {gc: true, mode: "bad", explicit: true, invalid: true},
		"empty explicit mode":                 {cp: true, explicit: true, invalid: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Select(tc.mode, tc.explicit, tc.record, tc.cp, tc.gc, tc.preserve, map[bool]string{false: DP, true: DRA}[tc.newDRA])
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestModeAffinityPreservesEveryAlternative(t *testing.T) {
	original := []corev1.NodeSelectorTerm{
		{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "hardware", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"}}}},
		{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"special"}}}},
	}
	spec := &corev1.PodSpec{NodeSelector: map[string]string{"os": "linux"}, Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: original}}}}
	ConstrainMode(spec, DRA)
	terms := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	require.Len(t, terms, 4)
	for i, term := range terms {
		require.Equal(t, original[i/2].MatchFields, term.MatchFields)
		require.ElementsMatch(t, original[i/2].MatchExpressions, term.MatchExpressions[:len(term.MatchExpressions)-1])
		requirement := term.MatchExpressions[len(term.MatchExpressions)-1]
		require.Equal(t, ModeLabel, requirement.Key)
		if i%2 == 0 {
			require.Equal(t, corev1.NodeSelectorOpDoesNotExist, requirement.Operator)
		} else {
			require.Equal(t, []string{DRA}, requirement.Values)
		}
	}
	require.Equal(t, map[string]string{"os": "linux"}, spec.NodeSelector)
	// An explicitly empty required selector matches no nodes and must stay empty.
	spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = nil
	ConstrainMode(spec, DP)
	require.Empty(t, spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
}

func TestDefaultStackConfiguration(t *testing.T) {
	for _, value := range []string{DP, DRA, "", "DRA", "none"} {
		t.Run("value="+value, func(t *testing.T) {
			t.Setenv(DefaultStackEnv, value)
			got, err := DefaultStack()
			if value == DP || value == DRA {
				require.NoError(t, err)
				require.Equal(t, value, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}
