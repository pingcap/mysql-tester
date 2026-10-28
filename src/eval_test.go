// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/defined2014/mysql"
	"github.com/stretchr/testify/require"
)

func TestExpandEvalQuery(t *testing.T) {
	t.Setenv("query", "WITH q AS (\n  SELECT 1\n);\n")
	t.Setenv("query2", "SELECT 2")

	got, err := expandEvalQuery(query{
		Query:     " $query;",
		delimiter: ";",
		Line:      17,
		tp:        Q_EVAL,
	})
	require.NoError(t, err)
	require.Equal(t, Q_QUERY, got.tp)
	require.Equal(t, "WITH q AS (\n  SELECT 1\n);", got.Query)
	require.Equal(t, 17, got.Line)

	got, err = expandEvalQuery(query{
		Query:     " explain $query2|",
		delimiter: "|",
		tp:        Q_EVAL,
	})
	require.NoError(t, err)
	require.Equal(t, "explain SELECT 2|", got.Query)

	got, err = expandEvalQuery(query{
		Query:     ` SELECT JSON_EXTRACT('{"a": 1}', '$.a');`,
		delimiter: ";",
		tp:        Q_EVAL,
	})
	require.NoError(t, err)
	require.Equal(t, `SELECT JSON_EXTRACT('{"a": 1}', '$.a');`, got.Query)

	_, err = expandEvalQuery(query{Query: "SELECT $missing;", delimiter: ";", tp: Q_EVAL})
	require.EqualError(t, err, "variable $missing is not set")
}

func TestParseLetAssignmentRemovesCommandDelimiter(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		raw       string
		delimiter string
		wantName  string
		wantValue string
	}{
		{
			name:      "numeric value",
			prefix:    "let",
			raw:       " $nines=999999999;",
			delimiter: ";",
			wantName:  "nines",
			wantValue: "999999999",
		},
		{
			name:      "string value",
			prefix:    "--let",
			raw:       " $block_mode=aes-128-ecb;",
			delimiter: ";",
			wantName:  "block_mode",
			wantValue: "aes-128-ecb",
		},
		{
			name:      "custom delimiter and multiline query",
			prefix:    "--let",
			raw:       " $query=`SELECT 1;\nSELECT 2`|",
			delimiter: "|",
			wantName:  "query",
			wantValue: "`SELECT 1;\nSELECT 2`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseQuery(query{Query: tt.prefix + tt.raw, delimiter: tt.delimiter})
			require.NoError(t, err)
			require.NotNil(t, parsed)
			require.Equal(t, Q_LET, parsed.tp)

			name, value, ok := parseLetAssignment(parsed.Query, parsed.delimiter)
			require.True(t, ok)
			require.Equal(t, tt.wantName, name)
			require.Equal(t, tt.wantValue, value)
			require.NotEqual(t, tt.delimiter, value[len(value)-1:])
		})
	}
}

func TestEvalExecutesAndConsumesExpectedError(t *testing.T) {
	registerEvalDriver()

	oldRecord, oldCheckErr := record, checkErr
	record, checkErr = true, true
	t.Cleanup(func() {
		record, checkErr = oldRecord, oldCheckErr
	})

	db, err := sql.Open(evalDriverName, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	tester := newTester("eval")
	tester.mdb = db
	tester.curr = &Conn{mdb: db, conn: conn}
	tester.expectedErrs = []string{"ER_NON_UPDATABLE_TABLE"}
	t.Setenv("query", "UPDATE t1 SET a = a + 1;\n")

	eval, err := ParseQuery(query{Query: "eval $query;", Line: 10, delimiter: ";"})
	require.NoError(t, err)
	require.Equal(t, Q_EVAL, eval.tp)
	evalValue, err := expandEvalQuery(*eval)
	require.NoError(t, err)
	require.NoError(t, tester.execute(evalValue))
	require.Empty(t, tester.expectedErrs, "eval must consume --error for exactly one statement")

	require.NoError(t, tester.execute(query{Query: "SELECT 42;", Line: 11, delimiter: ";"}))
	require.Contains(t, tester.buf.String(), "UPDATE t1 SET a = a + 1;")
	require.Contains(t, tester.buf.String(), "SELECT 42;\nanswer\n42\n")
}

const evalDriverName = "mysql-tester-issue-1701"

var evalDriverOnce sync.Once

func registerEvalDriver() {
	evalDriverOnce.Do(func() { sql.Register(evalDriverName, evalDriver{}) })
}

type evalDriver struct{}

func (evalDriver) Open(string) (driver.Conn, error) { return evalConn{}, nil }

type evalConn struct{}

func (evalConn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("prepare is not used") }
func (evalConn) Close() error                        { return nil }
func (evalConn) Begin() (driver.Tx, error)           { return nil, fmt.Errorf("transactions are not used") }

func (evalConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch strings.TrimSuffix(strings.TrimSpace(query), ";") {
	case "UPDATE t1 SET a = a + 1":
		return nil, &mysql.MySQLError{Number: 1288, Message: "The target table t1 is not updatable"}
	case "SELECT 42":
		return &evalRows{columns: []string{"answer"}, values: [][]driver.Value{{int64(42)}}}, nil
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
}

type evalRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *evalRows) Columns() []string { return r.columns }
func (r *evalRows) Close() error      { return nil }

func (r *evalRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
