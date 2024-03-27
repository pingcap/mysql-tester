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
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"github.com/defined2014/mysql"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	vtMySQL "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	mysqlSocket      string
	mysqlUser        string
	mysqlPasswd      string
	vtHost           string
	vtPort           string
	vtUser           string
	vtPasswd         string
	logLevel         string
	all              bool
	collationDisable bool
)

func init() {
	flag.StringVar(&mysqlSocket, "mysql-socket", "127.0.0.1", "The host of the MySQL server.")
	flag.StringVar(&mysqlUser, "mysql-user", "root", "The user for connecting to the MySQL database.")
	flag.StringVar(&mysqlPasswd, "mysql-passwd", "", "The password for the MySQL user.")
	flag.StringVar(&vtHost, "vt-host", "127.0.0.1", "The host of the vtgate server.")
	flag.StringVar(&vtPort, "vt-port", "4000", "The listen port of vtgate server.")
	flag.StringVar(&vtUser, "vt-user", "root", "The user for connecting to the vtgate")
	flag.StringVar(&vtPasswd, "vt-passwd", "", "The password for the vtgate")
	flag.StringVar(&logLevel, "log-level", "error", "The log level of mysql-tester: info, warn, error, debug.")
	flag.BoolVar(&all, "all", false, "run all tests")
	flag.BoolVar(&collationDisable, "collation-disable", false, "run collation related-test with new-collation disabled")
}

type query struct {
	firstWord string
	Query     string
	Line      int
	tp        CmdType
}

type Conn struct {
	// DB might be a shared one by multiple Conn, if the connection information are the same.
	mdb *sql.DB
	// connection information.
	hostName string
	userName string
	password string
	db       string

	conn *sql.Conn
}

type ReplaceColumn struct {
	col     int
	replace []byte
}

type ReplaceRegex struct {
	regex   *regexp.Regexp
	replace string
}

type tester struct {
	name string

	curr utils.MySQLCompare

	buf bytes.Buffer

	// enable query log will output origin statement into result file too
	// use --disable_query_log or --enable_query_log to control it
	enableQueryLog bool

	// enable result log will output to result file or not.
	// use --enable_result_log or --disable_result_log to control it
	enableResultLog bool

	// sortedResult make the output or the current query sorted.
	sortedResult bool

	enableConcurrent bool

	// Disable or enable warnings. This setting is enabled by default.
	// With this setting enabled, mysqltest uses SHOW WARNINGS to display
	// any warnings produced by SQL statements.
	enableWarning bool

	// enable query info, like rowsAffected, lastMessage etc.
	enableInfo bool

	// check expected error, use --error before the statement
	// see http://dev.mysql.com/doc/mysqltest/2.0/en/writing-tests-expecting-errors.html
	expectedErrs []string

	// replace output column through --replace_column 1 <static data> 3 #
	replaceColumn []ReplaceColumn

	// replace output result through --replace_regex /\.dll/.so/
	replaceRegex []*ReplaceRegex
}

func newTester(name string) *tester {
	t := new(tester)

	t.name = name
	t.enableQueryLog = true
	t.enableResultLog = true
	// disable warning by default since our a lot of test cases
	// are ported wihtout explictly "disablewarning"
	t.enableWarning = false
	t.enableConcurrent = false
	t.enableInfo = false

	return t
}

func (t *tester) preProcess() {
	mysqlConn := vtMySQL.ConnParams{
		Uname:      mysqlUser,
		Pass:       mysqlPasswd,
		DbName:     "ks",
		UnixSocket: mysqlSocket,
	}
	p, err := strconv.Atoi(vtPort)
	if err != nil {
		panic(err.Error())
	}
	vtConn := vtMySQL.ConnParams{
		Uname:  vtUser,
		Pass:   vtPasswd,
		DbName: "ks",
		Host:   vtHost,
		Port:   p,
	}

	mcmp, err := utils.NewMySQLCompare(nil, vtConn, mysqlConn)
	if err != nil {
		panic(err.Error())
	}
	t.curr = mcmp
}

func (t *tester) postProcess() {
	t.curr.Close()
}

func (t *tester) addFailure(testSuite *XUnitTestSuite, err *error, cnt int) {
	testSuite.TestCases = append(testSuite.TestCases, XUnitTestCase{
		Classname:  "",
		Name:       t.testFileName(),
		Time:       "",
		QueryCount: cnt,
		Failure:    (*err).Error(),
	})
	testSuite.Failures++
}

func (t *tester) addSuccess(testSuite *XUnitTestSuite, startTime *time.Time, cnt int) {
	testSuite.TestCases = append(testSuite.TestCases, XUnitTestCase{
		Classname:  "",
		Name:       t.testFileName(),
		Time:       fmt.Sprintf("%fs", time.Since(*startTime).Seconds()),
		QueryCount: cnt,
	})
}

func (t *tester) Run() error {

	t.preProcess()
	defer t.postProcess()
	queries, err := t.loadQueries()
	if err != nil {
		err = errors.Trace(err)
		t.addFailure(&testSuite, &err, 0)
		return err
	}

	testCnt := 0
	startTime := time.Now()
	var concurrentQueue []query
	for _, q := range queries {
		switch q.tp {
		case Q_ENABLE_QUERY_LOG:
			t.enableQueryLog = true
		case Q_DISABLE_QUERY_LOG:
			t.enableQueryLog = false
		case Q_ENABLE_RESULT_LOG:
			t.enableResultLog = true
		case Q_DISABLE_RESULT_LOG:
			t.enableResultLog = false
		case Q_DISABLE_WARNINGS:
			t.enableWarning = false
		case Q_ENABLE_WARNINGS:
			t.enableWarning = true
		case Q_ENABLE_INFO:
			t.enableInfo = true
		case Q_DISABLE_INFO:
			t.enableInfo = false
		case Q_BEGIN_CONCURRENT, Q_END_CONCURRENT, Q_CONNECT, Q_CONNECTION, Q_DISCONNECT, Q_LET:
			return fmt.Errorf("%s not supported", String(q.tp))
		case Q_ERROR:
			t.expectedErrs = strings.Split(strings.TrimSpace(q.Query), ",")
		case Q_ECHO:
			varSearch := regexp.MustCompile(`\$([A-Za-z0-9_]+)( |$)`)
			s := varSearch.ReplaceAllStringFunc(q.Query, func(s string) string {
				return os.Getenv(varSearch.FindStringSubmatch(s)[1])
			})

			t.buf.WriteString(s)
			t.buf.WriteString("\n")
		case Q_QUERY:
			if t.enableConcurrent {
				concurrentQueue = append(concurrentQueue, q)
			} else if err = t.execute(q); err != nil {
				err = errors.Annotate(err, fmt.Sprintf("sql:%v", q.Query))
				t.addFailure(&testSuite, &err, testCnt)
				return err
			}

			testCnt++

			t.sortedResult = false
			t.replaceColumn = nil
			t.replaceRegex = nil
		case Q_SORTED_RESULT:
			t.sortedResult = true
		case Q_REPLACE_COLUMN:
			// TODO: Use CSV module or so to handle quoted replacements
			t.replaceColumn = nil // Only use the latest one!
			cols := strings.Fields(q.Query)
			// Require that col + replacement comes in pairs otherwise skip the last column number
			for i := 0; i < len(cols)-1; i = i + 2 {
				colNr, err := strconv.Atoi(cols[i])
				if err != nil {
					err = errors.Annotate(err, fmt.Sprintf("Could not parse column in --replace_column: sql:%v", q.Query))
					t.addFailure(&testSuite, &err, testCnt)
					return err
				}

				t.replaceColumn = append(t.replaceColumn, ReplaceColumn{col: colNr, replace: []byte(cols[i+1])})
			}
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

func (t *tester) stmtExecute(query string) (err error) {
	if t.enableQueryLog {
		t.buf.WriteString(query)
		t.buf.WriteString("\n")
	}
	return t.executeStmt(query)
}

// checkExpectedError check if error was expected
// If so, it will handle Buf and return nil
func (t *tester) checkExpectedError(q query, err error) error {
	if err == nil {
		if len(t.expectedErrs) == 0 {
			return nil
		}
		for _, s := range t.expectedErrs {
			s = strings.TrimSpace(s)
			if s == "0" {
				// 0 means accept any error!
				return nil
			}
		}
		return errors.Errorf("Statement succeeded, expected error(s) '%s'", strings.Join(t.expectedErrs, ","))
	}
	if err != nil && len(t.expectedErrs) == 0 {
		return err
	}
	// Parse the error to get the mysql error code
	errNo := 0
	switch innerErr := errors.Cause(err).(type) {
	case *mysql.MySQLError:
		errNo = int(innerErr.Number)
	}
	if errNo == 0 {
		log.Warnf("%s:%d Could not parse mysql error: %s", t.name, q.Line, err.Error())
		return err
	}
	for _, s := range t.expectedErrs {
		s = strings.TrimSpace(s)
		checkErrNo, err1 := strconv.Atoi(s)
		if err1 != nil {
			i, ok := MysqlErrNameToNum[s]
			if ok {
				checkErrNo = i
			} else {
				if len(t.expectedErrs) > 1 {
					log.Warnf("%s:%d Unknown named error %s in --error %s", t.name, q.Line, s, strings.Join(t.expectedErrs, ","))
				} else {
					log.Warnf("%s:%d Unknown named --error %s", t.name, q.Line, s)
				}
				continue
			}
		}
		if errNo == checkErrNo {
			if len(t.expectedErrs) == 1 {
				// !checkErr - Also keep old behavior, i.e. not use "Got one of the listed errors"
				errStr := err.Error()
				for _, reg := range t.replaceRegex {
					errStr = reg.regex.ReplaceAllString(errStr, reg.replace)
				}
				fmt.Fprintf(&t.buf, "%s\n", strings.ReplaceAll(errStr, "\r", ""))
			} else if strings.TrimSpace(t.expectedErrs[0]) != "0" {
				fmt.Fprintf(&t.buf, "Got one of the listed errors\n")
			}
			return nil
		}
	}

	gotErrCode := strconv.Itoa(errNo)
	for k, v := range MysqlErrNameToNum {
		if v == errNo {
			gotErrCode = k
			break
		}
	}
	if len(t.expectedErrs) > 1 {
		log.Warnf("%s:%d query failed with non expected error(s)! (%s not in %s) (err: %s) (query: %s)",
			t.name, q.Line, gotErrCode, strings.Join(t.expectedErrs, ","), err.Error(), q.Query)
	} else {
		log.Warnf("%s:%d query failed with non expected error(s)! (%s != %s) (err: %s) (query: %s)",
			t.name, q.Line, gotErrCode, t.expectedErrs[0], err.Error(), q.Query)
	}
	errStr := err.Error()
	for _, reg := range t.replaceRegex {
		errStr = reg.regex.ReplaceAllString(errStr, reg.replace)
	}
	fmt.Fprintf(&t.buf, "%s\n", strings.ReplaceAll(errStr, "\r", ""))
	return err
}

func (t *tester) execute(query query) error {
	if len(query.Query) == 0 {
		return nil
	}

	err := t.stmtExecute(query.Query)

	err = t.checkExpectedError(query, err)
	if err != nil {
		return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", query.Query, query.Line, err))
	}
	// clear expected errors after we execute the first query
	t.expectedErrs = nil

	if err != nil {
		return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", query.Query, query.Line, err))
	}

	return errors.Trace(err)
}

type byteRow struct {
	data [][]byte
}

type byteRows struct {
	cols []string
	data []byteRow
}

func (rows *byteRows) Len() int {
	return len(rows.data)
}

func (rows *byteRows) Less(i, j int) bool {
	r1 := rows.data[i]
	r2 := rows.data[j]
	for i := 0; i < len(r1.data); i++ {
		res := bytes.Compare(r1.data[i], r2.data[i])
		switch res {
		case -1:
			return true
		case 1:
			return false
		case 0:
			// bytes.Compare(nil, []byte{}) returns 0
			// But in sql row representation, they are NULL and empty string "" respectively, and thus not equal.
			// So we need special logic to handle here: make NULL < ""
			if r1.data[i] == nil && r2.data[i] != nil {
				return true
			}
			if r1.data[i] != nil && r2.data[i] == nil {
				return false
			}
		}
	}
	return false
}

func (rows *byteRows) Swap(i, j int) {
	rows.data[i], rows.data[j] = rows.data[j], rows.data[i]
}

func dumpToByteRows(rows *sql.Rows) (*byteRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	data := make([]byteRow, 0, 8)
	args := make([]interface{}, len(cols))
	for {
		for rows.Next() {
			tmp := make([][]byte, len(cols))
			for i := 0; i < len(args); i++ {
				args[i] = &tmp[i]
			}
			err := rows.Scan(args...)
			if err != nil {
				return nil, errors.Trace(err)
			}

			data = append(data, byteRow{tmp})
		}
		if !rows.NextResultSet() {
			break
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &byteRows{cols: cols, data: data}, nil
}

func (t *tester) executeStmt(query string) error {
	log.Debugf("executeStmt: %s", query)
	_ = t.curr.Exec(query)

	return nil
}

func (t *tester) testFileName() string {
	// test and result must be in current ./t the same as MySQL
	return fmt.Sprintf("./t/%s.test", t.name)
}

func hasCollationPrefix(name string) bool {
	names := strings.Split(name, "/")
	caseName := names[len(names)-1]
	return strings.HasPrefix(caseName, "collation")
}

func (t *tester) resultFileName() string {
	// test and result must be in current ./r, the same as MySQL
	name := t.name
	if hasCollationPrefix(name) {
		if collationDisable {
			name = name + "_disabled"
		} else {
			name = name + "_enabled"
		}
	}
	return fmt.Sprintf("./r/%s.result", name)
}

func loadAllTests() ([]string, error) {
	tests := make([]string, 0)
	// tests must be in t folder or subdir in t folder
	err := filepath.Walk("./t/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".test") {
			name := strings.TrimPrefix(strings.TrimSuffix(path, ".test"), "t/")
			if !collationDisable || hasCollationPrefix(name) {
				tests = append(tests, name)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return tests, nil
}

// convertTestsToTestTasks convert all test cases into several testBatches.
// If we have 11 cases and batchSize is 5, then we will have 4 testBatches.
func convertTestsToTestTasks(tests []string) (tTasks []testBatch, have_show, have_is bool) {
	batchSize := 30
	total := (len(tests) / batchSize) + 2
	// the extra 1 is for sub_query_more test
	tTasks = make([]testBatch, total+1)
	testIdx := 0
	have_subqmore, have_role := false, false
	for i := 0; i < total; i++ {
		tTasks[i] = make(testBatch, 0, batchSize)
		for j := 0; j <= batchSize && testIdx < len(tests); j++ {
			// skip sub_query_more test, since it consumes the most time
			// we better use a separate goroutine to run it
			// role test has many connection/disconnection operation.
			// we better use a separate goroutine to run it
			switch tests[testIdx] {
			case "sub_query_more":
				have_subqmore = true
			case "show":
				have_show = true
			case "infoschema":
				have_is = true
			case "role":
				have_role = true
			case "role2":
				have_role = true
			default:
				tTasks[i] = append(tTasks[i], tests[testIdx])
			}
			testIdx++
		}
	}

	if have_subqmore {
		tTasks[total-1] = testBatch{"sub_query_more"}
	}

	if have_role {
		tTasks[total] = testBatch{"role", "role2"}
	}
	return
}

var msgs = make(chan testTask)
var xmlFile *os.File
var testSuite XUnitTestSuite

type testTask struct {
	err  error
	test string
}

type testBatch []string

func (t testBatch) Run() {
	for _, test := range t {
		tr := newTester(test)
		msgs <- testTask{
			test: test,
			err:  tr.Run(),
		}
	}
}

func (t testBatch) String() string {
	return strings.Join([]string(t), ", ")
}

func executeTests(tasks []testBatch, have_show, have_is bool) {
	// show and infoschema have to be executed first, since the following
	// tests will create database using their own name.
	if have_show {
		show := newTester("show")
		msgs <- testTask{
			test: "show",
			err:  show.Run(),
		}
	}

	if have_is {
		infoschema := newTester("infoschema")
		msgs <- testTask{
			test: "infoschema",
			err:  infoschema.Run(),
		}
	}

	for _, t := range tasks {
		t.Run()
	}
}

func consumeError() []error {
	var es []error
	for {
		if t, more := <-msgs; more {
			if t.err != nil {
				e := fmt.Errorf("run test [%s] err: %v", t.test, t.err)
				log.Errorln(e)
				es = append(es, e)
			} else {
				log.Infof("run test [%s] ok", t.test)
			}
		} else {
			return es
		}
	}
}

func main() {
	flag.Parse()
	tests := flag.Args()

	if ll := os.Getenv("LOG_LEVEL"); ll != "" {
		logLevel = ll
	}
	if logLevel != "" {
		ll, err := log.ParseLevel(logLevel)
		if err != nil {
			log.Errorf("error parsing log level %s: %v", logLevel, err)
		}
		log.SetLevel(ll)
	}

	// we will run all tests if no tests assigned
	if len(tests) == 0 {
		var err error
		if tests, err = loadAllTests(); err != nil {
			log.Fatalf("load all tests err %v", err)
		}
	}

	log.Infof("running tests: %v", tests)

	go func() {
		executeTests(convertTestsToTestTasks(tests))
		close(msgs)
	}()

	es := consumeError()
	println()
	if len(es) != 0 {
		log.Errorf("%d tests failed\n", len(es))
		for _, item := range es {
			log.Errorln(item)
		}
		// Can't delete this statement.
		os.Exit(1)
	} else {
		println("Great, All tests passed")
	}
}
