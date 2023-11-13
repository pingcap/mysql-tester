// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseReplaceRegex(t *testing.T) {
	testCases := []struct {
		succ      bool
		regexpStr string
		input     string
		output    string
	}{
		{
			succ:      true,
			regexpStr: `/dll/so/`,
			input:     "a.dll.dll",
			output:    "a.so.so",
		},
		{
			succ:      true,
			regexpStr: `/\.dll/.so/`,
			input:     "a.dlldll",
			output:    "a.sodll",
		},
		{
			succ:      true,
			regexpStr: `/\.\.dll/..so/`,
			input:     "a.dll..dlldll",
			output:    "a.dll..sodll",
		},
		{
			succ:      true,
			regexpStr: `/conn=[0-9]+/conn=<num>/`,
			input:     "Some infos [conn=2097154]",
			output:    "Some infos [conn=<num>]",
		},
		{
			succ:      true,
			regexpStr: `/conn=[0-9]+/conn=<num>/`,
			input:     "Some infos [conn=xxx]",
			output:    "Some infos [conn=xxx]",
		},
		{
			succ:      true,
			regexpStr: `/\/a//b//`,
			input:     "/a",
			output:    "/b/",
		},
		{
			succ:      false,
			regexpStr: `/conn=[0-9]+/conn=<num>`,
			input:     "",
			output:    "",
		},
		{
			succ:      false,
			regexpStr: `conn=[0-9]+/conn=<num>/`,
			input:     "",
			output:    "",
		},
		{
			succ:      false,
			regexpStr: `/*/conn=<num>/`,
			input:     "",
			output:    "",
		},
		{
			succ:      false,
			regexpStr: `/abc\/conn=<num>\/`,
			input:     "",
			output:    "",
		},
	}

	for _, testCase := range testCases {
		reg, err := ParseReplaceRegex(testCase.regexpStr)
		if !testCase.succ {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		require.NotNil(t, reg)

		require.Equal(t, testCase.output, reg.regex.ReplaceAllString(testCase.input, reg.replace))
	}
}
