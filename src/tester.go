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
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
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

	currentQuery string

	skipBinary  string
	skipVersion int

	// check expected error, use --error before the statement
	// we only care if an error is returned, not the exact error message.
	expectedErrs bool

	failureCount       int
	queryCount         int
	successCount       int
	errorFile          *os.File
	currentQueryFailed bool
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
	if olap {
		_, err := t.curr.VtConn.ExecuteFetch("set workload = 'olap'", 0, false)
		if err != nil {
			panic(err)
		}
	}
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

var PERM os.FileMode = 0755

func (t *tester) addFailure(err error) {
	t.failureCount++
	t.currentQueryFailed = true
	currentQuery := t.currentQuery
	if currentQuery == "" {
		currentQuery = "GENERAL"
	}
	if t.errorFile == nil {
		t.errorFile = t.createErrorFileFor(currentQuery)
	}

	_, err = t.errorFile.WriteString(err.Error())
	if err != nil {
		panic("failed to write error file\n" + err.Error())
	}

	t.createVSchemaDump()
}

func (t *tester) createVSchemaDump() {
	errorDir := t.errorDir()
	err := os.MkdirAll(errorDir, PERM)
	if err != nil {
		panic("failed to create vschema directory\n" + err.Error())
	}

	vschemaBytes, err := json.MarshalIndent(vschema, "", "\t")
	if err != nil {
		panic("failed to marshal vschema\n" + err.Error())
	}

	err = os.WriteFile(path.Join(errorDir, "vschema.json"), vschemaBytes, PERM)
	if err != nil {
		panic("failed to write vschema\n" + err.Error())
	}
}

func (t *tester) createErrorFileFor(query string) *os.File {
	qc := fmt.Sprintf("%d", t.queryCount)
	err := os.MkdirAll(t.errorDir(), PERM)
	if err != nil {
		panic("failed to create error directory\n" + err.Error())
	}
	errorPath := path.Join(t.errorDir(), qc)
	file, err := os.Create(errorPath)
	if err != nil {
		panic("failed to create error file\n" + err.Error())
	}
	_, err = file.WriteString(fmt.Sprintf("Error log for query:\n%s\n\n", query))
	if err != nil {
		panic("failed to write to error file\n" + err.Error())
	}

	return file
}

func (t *tester) errorDir() string {
	errFileName := t.name
	if strings.HasPrefix(t.name, "http") {
		u, err := url.Parse(t.name)
		if err == nil {
			errFileName = path.Base(u.Path)
			if errFileName == "" || errFileName == "/" {
				errFileName = url.QueryEscape(t.name)
			}
		}
	}
	return path.Join("errors", errFileName)
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
			Q_SORTED_RESULT,
			Q_REPLACE_REGEX:
			// do nothing
		case Q_BEGIN_CONCURRENT, Q_END_CONCURRENT, Q_CONNECT, Q_CONNECTION, Q_DISCONNECT, Q_LET, Q_REPLACE_COLUMN:
			t.addFailure(fmt.Errorf("%s not supported", String(q.tp)))
		case Q_SKIP_IF_BELOW_VERSION:
			strs := strings.Split(q.Query, " ")
			if len(strs) != 3 {
				t.addFailure(fmt.Errorf("incorrect syntax for Q_SKIP_IF_BELOW_VERSION in: %v", q.Query))
				continue
			}
			t.skipBinary = strs[1]
			var err error
			t.skipVersion, err = strconv.Atoi(strs[2])
			if err != nil {
				t.addFailure(err)
				continue
			}
		case Q_ERROR:
			t.expectedErrs = true
		case Q_QUERY:
			if t.skipBinary != "" {
				okayToRun := utils.BinaryIsAtLeastAtVersion(t.skipVersion, t.skipBinary)
				t.skipBinary = ""
				if !okayToRun {
					continue
				}
			}
			t.queryCount++
			t.currentQuery = q.Query
			t.currentQueryFailed = false
			if err = t.execute(q); err != nil && !t.expectedErrs {
				t.addFailure(err)
			} else if !t.currentQueryFailed {
				t.successCount++
			}
			// clear expected errors and current query after we execute any query
			t.expectedErrs = false
			t.currentQuery = ""
			if t.errorFile != nil {
				err := t.errorFile.Close()
				if err != nil {
					panic("failed to close error file\n" + err.Error())
				}
				t.errorFile = nil
			}
		case Q_REMOVE_FILE:
			err = os.Remove(strings.TrimSpace(q.Query))
			if err != nil {
				return errors.Annotate(err, "failed to remove file")
			}
		default:
			t.addFailure(fmt.Errorf("%s not supported", String(q.tp)))
		}
	}

	fmt.Printf(
		"%s: ok! Ran %d queries, %d successfully and %d failures take time %v s\n",
		t.name,
		t.queryCount,
		t.successCount,
		t.queryCount-t.successCount,
		time.Since(startTime).Seconds(),
	)

	return nil
}

func (t *tester) loadQueries() ([]query, error) {
	data, err := t.readData()
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

func (t *tester) readData() ([]byte, error) {
	if strings.HasPrefix(t.name, "http") {
		client := http.Client{}
		res, err := client.Get(t.name)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, errors.Errorf("failed to get data from %s, status code %d", t.name, res.StatusCode)
		}
		defer res.Body.Close()
		return io.ReadAll(res.Body)
	}
	return os.ReadFile(t.testFileName())
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
	create, isCreateStatement := ast.(*sqlparser.CreateTable)
	autoVschema := isCreateStatement && !t.expectedErrs && vschemaFile == ""
	if autoVschema {
		t.handleCreateTable(create)
	}

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

	if autoVschema {
		err = utils.WaitForAuthoritative(t, keyspaceName, create.Table.Name.String(), clusterInstance.VtgateProcess.ReadVSchema)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func getShardingKeysForTable(create *sqlparser.CreateTable) (sks []sqlparser.IdentifierCI) {
	var allIdCI []sqlparser.IdentifierCI
	// first we normalize the primary keys
	for _, col := range create.TableSpec.Columns {
		if col.Type.Options.KeyOpt == sqlparser.ColKeyPrimary {
			create.TableSpec.Indexes = append(create.TableSpec.Indexes, newPrimaryKeyIndexDefinitionSingleColumn(col.Name))
			col.Type.Options.KeyOpt = sqlparser.ColKeyNone
		}
		allIdCI = append(allIdCI, col.Name)
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

func (t *tester) handleCreateTable(create *sqlparser.CreateTable) {
	sks := getShardingKeysForTable(create)

	shardingKeys := &vindexes.ColumnVindex{
		Columns: sks,
		Name:    "xxhash",
		Type:    "xxhash",
	}

	ks := vschema.Keyspaces[keyspaceName]
	tableName := create.Table.Name
	ks.Tables[tableName.String()] = &vindexes.Table{
		Name:           tableName,
		Keyspace:       ks.Keyspace,
		ColumnVindexes: []*vindexes.ColumnVindex{shardingKeys},
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
