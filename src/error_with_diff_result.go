// Copyright 2025 PingCAP, Inc.
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
	"fmt"

	"github.com/sergi/go-diff/diffmatchpatch"
)

type WrongResultError struct {
	query    string
	line     int
	expected string
	actual   string
}

func NewWrongResultError(line int, query, expected, actual string) *WrongResultError {
	return &WrongResultError{
		line:     line,
		query:    query,
		expected: expected,
		actual:   actual,
	}
}

func (e *WrongResultError) Error() string {
	diff := diffmatchpatch.New()
	diffText := diff.DiffPrettyText(diff.DiffMain(e.actual, e.expected, false))
	return fmt.Sprintf("failed to run query \n\"%v\" \n around line %d, \n we need(%v):\n%s\nbut got(%v):\n%s\ndiff:\n%s\n",
		e.query, e.line, len(e.expected), e.expected, len(e.actual), e.actual, diffText)
}
