/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type tester struct {
	name string

	curr utils.MySQLCompare

	// check expected error, use --error before the statement
	// we only care if an error is returned, not the exact error message.
	expectedErrs bool

	// replace output column through --replace_column 1 <static data> 3 #
	replaceColumn []ReplaceColumn

	// replace output result through --replace_regex /\.dll/.so/
	replaceRegex []*ReplaceRegex
}

func newTester(name string) *tester {
	t := new(tester)

	t.name = name

	return t
}

func (t *tester) preProcess() {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	if err != nil {
		panic(err.Error())
	}
	t.curr = mcmp
}

func (t *tester) postProcess() {
	r, err := t.curr.MySQLConn.ExecuteFetch("show tables", 1000, true)
	if err != nil {
		panic(err)
	}
	for _, row := range r.Rows {
		t.curr.Exec(fmt.Sprintf("drop table %s", row[0].ToString()))
	}
	t.curr.Close()
}

func (t *tester) addFailure(err error) {
	fmt.Println(err.Error())
}

func (t *tester) addSuccess() {

}

func (t *tester) Run() error {
	t.preProcess()
	defer t.postProcess()
	queries, err := t.loadQueries()
	if err != nil {
		t.addFailure(err)
		return err
	}

	testCnt := 0
	startTime := time.Now()
	for _, q := range queries {
		switch q.tp {
		// no-ops
		case Q_ENABLE_QUERY_LOG,
			Q_DISABLE_QUERY_LOG,
			Q_ECHO,
			Q_DISABLE_WARNINGS,
			Q_ENABLE_WARNINGS,
			Q_ENABLE_INFO,
			Q_DISABLE_INFO,
			Q_ENABLE_RESULT_LOG,
			Q_DISABLE_RESULT_LOG,
			Q_SORTED_RESULT:
			// do nothing
		case Q_BEGIN_CONCURRENT, Q_END_CONCURRENT, Q_CONNECT, Q_CONNECTION, Q_DISCONNECT, Q_LET, Q_REPLACE_COLUMN:
			t.addFailure(fmt.Errorf("%s not supported", String(q.tp)))
		case Q_ERROR:
			t.expectedErrs = true
		case Q_QUERY:
			if err = t.execute(q); err != nil {
				err = errors.Annotate(err, fmt.Sprintf("sql:%v", q.Query))
				t.addFailure(err)
				return err
			}

			testCnt++

			t.replaceColumn = nil
			t.replaceRegex = nil
		case Q_REMOVE_FILE:
			err = os.Remove(strings.TrimSpace(q.Query))
			if err != nil {
				return errors.Annotate(err, "failed to remove file")
			}
		case Q_REPLACE_REGEX:
			t.replaceRegex = nil
			regex, err := ParseReplaceRegex(q.Query)
			if err != nil {
				return errors.Annotate(err, fmt.Sprintf("Could not parse regex in --replace_regex: line: %d sql:%v", q.Line, q.Query))
			}
			t.replaceRegex = regex
		default:
			log.WithFields(log.Fields{"command": q.firstWord, "arguments": q.Query, "line": q.Line}).Warn("command not implemented")
		}
	}

	fmt.Printf("%s: ok! %d test cases passed, take time %v s\n", t.testFileName(), testCnt, time.Since(startTime).Seconds())

	return nil
}

func (t *tester) loadQueries() ([]query, error) {
	data, err := os.ReadFile(t.testFileName())
	if err != nil {
		return nil, err
	}

	seps := bytes.Split(data, []byte("\n"))
	queries := make([]query, 0, len(seps))
	newStmt := true
	for i, v := range seps {
		v := bytes.TrimSpace(v)
		s := string(v)
		// we will skip # comment here
		if strings.HasPrefix(s, "#") {
			newStmt = true
			continue
		} else if strings.HasPrefix(s, "--") {
			queries = append(queries, query{Query: s, Line: i + 1})
			newStmt = true
			continue
		} else if len(s) == 0 {
			continue
		}

		if newStmt {
			queries = append(queries, query{Query: s, Line: i + 1})
		} else {
			lastQuery := queries[len(queries)-1]
			lastQuery = query{Query: fmt.Sprintf("%s\n%s", lastQuery.Query, s), Line: lastQuery.Line}
			queries[len(queries)-1] = lastQuery
		}

		// if the line has a ; in the end, we will treat new line as the new statement.
		newStmt = strings.HasSuffix(s, ";")
	}

	return ParseQueries(queries...)
}

func (t *tester) execute(query query) error {
	if len(query.Query) == 0 {
		return nil
	}

	err := t.executeStmt(query.Query)

	if err != nil {
		return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", query.Query, query.Line, err))
	}
	// clear expected errors after we execute the first query
	t.expectedErrs = false

	if err != nil {
		return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", query.Query, query.Line, err))
	}

	return errors.Trace(err)
}

func newPrimaryKeyIndexDefinitionSingleColumn(name sqlparser.IdentifierCI) *sqlparser.IndexDefinition {
	index := &sqlparser.IndexDefinition{
		Info: &sqlparser.IndexInfo{
			Name: sqlparser.NewIdentifierCI("PRIMARY"),
			Type: sqlparser.IndexTypePrimary,
		},
		Columns: []*sqlparser.IndexColumn{{Column: name}},
	}
	return index
}

func (t *tester) executeStmt(query string) error {
	parser := sqlparser.NewTestParser()
	ast, err := parser.Parse(query)
	if err != nil {
		return err
	}
	_, commentOnly := ast.(*sqlparser.CommentOnly)
	if commentOnly {
		return nil
	}

	log.Debugf("executeStmt: %s", query)

	switch {
	case t.expectedErrs:
		_, err := t.curr.ExecAllowAndCompareError(query)
		if err == nil {
			// If we expected an error, but didn't get one, return an error
			return fmt.Errorf("expected error, but got none")
		}
	default:
		_ = t.curr.Exec(query)
	}

	create, ok := ast.(*sqlparser.CreateTable)
	if ok {
		t.handleCreateTable(create)
	}
	return nil
}

func getShardingKeysForTable(create *sqlparser.CreateTable) (sks []sqlparser.IdentifierCI, allCols []vindexes.Column) {
	var allIdCI []sqlparser.IdentifierCI
	// first we normalize the primary keys
	for _, col := range create.TableSpec.Columns {
		if col.Type.Options.KeyOpt == sqlparser.ColKeyPrimary {
			create.TableSpec.Indexes = append(create.TableSpec.Indexes, newPrimaryKeyIndexDefinitionSingleColumn(col.Name))
			col.Type.Options.KeyOpt = sqlparser.ColKeyNone
		}
		allIdCI = append(allIdCI, col.Name)
		allCols = append(allCols, vindexes.Column{
			Name:  col.Name,
			Type:  col.Type.SQLType(),
			Scale: getInt32FromPointer(col.Type.Scale),
			Size:  getInt32FromPointer(col.Type.Length),
		})
	}

	// and now we can fetch the primary keys
	for _, index := range create.TableSpec.Indexes {
		if index.Info.Type == sqlparser.IndexTypePrimary {
			for _, column := range index.Columns {
				sks = append(sks, column.Column)
			}
		}
	}

	// if we have no primary keys, we'll use all columns as the sharding keys
	if len(sks) == 0 {
		sks = allIdCI
	}
	return
}

func getInt32FromPointer(val *int) int32 {
	if val == nil {
		return 0
	}
	return int32(*val)
}

func (t *tester) handleCreateTable(create *sqlparser.CreateTable) {
	sks, allCols := getShardingKeysForTable(create)

	shardingKeys := &vindexes.ColumnVindex{
		Columns: sks,
		Name:    "hash",
		Type:    "hash",
	}

	ks := vschema.Keyspaces[keyspaceName]
	tableName := create.Table.Name
	ks.Tables[tableName.String()] = &vindexes.Table{
		Name:                    tableName,
		Keyspace:                ks.Keyspace,
		ColumnVindexes:          []*vindexes.ColumnVindex{shardingKeys},
		Columns:                 allCols,
		ColumnListAuthoritative: true,
	}

	ksJson, err := json.Marshal(ks)
	if err != nil {
		panic(err)
	}

	err = clusterInstance.VtctldClientProcess.ApplyVSchema(keyspaceName, string(ksJson))
	if err != nil {
		panic(err)
	}
}

func (t *tester) testFileName() string {
	// test and result must be in current ./t the same as MySQL
	return fmt.Sprintf("./t/%s.test", t.name)
}

func (t *tester) Errorf(format string, args ...interface{}) {
	t.addFailure(errors.Errorf(format, args...))
}

func (t *tester) FailNow() {
	// we don't need to do anything here
}

func (t *tester) Helper() {}
