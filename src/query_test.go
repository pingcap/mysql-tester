// Copyright 2020 PingCAP, Inc.
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

func TestParseQueryies(t *testing.T) {
	sql := "select * from t;"

	if q, err := ParseQuery(query{Query: sql, Line: 1, Delimiter: ";"}); err == nil {
		assertEqual(t, q.tp, Q_QUERY, fmt.Sprintf("Expected: %d, got: %d", Q_QUERY, q.tp))
		assertEqual(t, q.Query, sql, fmt.Sprintf("Expected: %s, got: %s", sql, q.Query))
	} else {
		t.Fatalf("error is not nil. %v", err)
	}

	sql = "--sorted_result select * from t;"
	if q, err := ParseQuery(query{Query: sql, Line: 1, Delimiter: ";"}); err == nil {
		assertEqual(t, q.tp, Q_SORTED_RESULT, "sorted_result")
		assertEqual(t, q.Query, "select * from t;", fmt.Sprintf("Expected: '%s', got '%s'", "select * from t;", q.Query))
	} else {
		t.Fatalf("error is not nil. %s", err)
	}

	// invalid comment command style
	sql = "--abc select * from t;"
	_, err := ParseQuery(query{Query: sql, Line: 1, Delimiter: ";"})
	assertEqual(t, err, ErrInvalidCommand, fmt.Sprintf("Expected: %v, got %v", ErrInvalidCommand, err))

	sql = "--let $foo=`SELECT 1`"
	if q, err := ParseQuery(query{Query: sql, Line: 1, Delimiter: ";"}); err == nil {
		assertEqual(t, q.tp, Q_LET, fmt.Sprintf("Expected: %d, got: %d", Q_LET, q.tp))
	}
}

func TestLoadQueries(t *testing.T) {
	dir := t.TempDir()
	err := os.Chdir(dir)
	assert.NoError(t, err)

	err = os.Mkdir("t", 0755)
	assert.NoError(t, err)

	testCases := []struct {
		input   string
		queries []query
	}{
		{
			input: "delimiter |\n do something; select something; |\n delimiter ; \nselect 1;",
			queries: []query{
				{Query: "do something; select something; |", tp: Q_QUERY, Delimiter: "|"},
				{Query: "select 1;", tp: Q_QUERY, Delimiter: ";"},
			},
		},
		{
			input: "delimiter |\ndrop procedure if exists scopel\ncreate procedure scope(a int, b float)\nbegin\ndeclare b int;\ndeclare c float;\nbegin\ndeclare c int;\nend;\nend |\ndrop procedure scope|\ndelimiter ;\n",
			queries: []query{
				{Query: "drop procedure if exists scopel\ncreate procedure scope(a int, b float)\nbegin\ndeclare b int;\ndeclare c float;\nbegin\ndeclare c int;\nend;\nend |", tp: Q_QUERY, Delimiter: "|"},
				{Query: "drop procedure scope|", tp: Q_QUERY, Delimiter: "|"},
			},
		},
		{
			input: "--error 1054\nselect 1;",
			queries: []query{
				{Query: " 1054", tp: Q_ERROR, Delimiter: ";"},
				{Query: "select 1;", tp: Q_QUERY, Delimiter: ";"},
			},
		},
	}

	for _, testCase := range testCases {
		fileName := filepath.Join("t", "test.test")
		f, err := os.Create(fileName)
		assert.NoError(t, err)

		f.WriteString(testCase.input)
		f.Close()

		test := newTester("test")
		queries, err := test.loadQueries()
		assert.NoError(t, err)
		assert.Len(t, queries, len(testCase.queries))
		for i, query := range testCase.queries {
			assert.Equal(t, queries[i].Query, query.Query)
			assert.Equal(t, queries[i].tp, query.tp)
			assert.Equal(t, queries[i].Delimiter, query.Delimiter)
		}
	}
}
