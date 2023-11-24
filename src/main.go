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
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/defined2014/mysql"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

var (
	host             string
	port             string
	user             string
	passwd           string
	logLevel         string
	record           bool
	params           string
	all              bool
	reserveSchema    bool
	xmlPath          string
	retryConnCount   int
	collationDisable bool
	checkErr         bool
)

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "The host of the TiDB/MySQL server.")
	flag.StringVar(&port, "port", "4000", "The listen port of TiDB/MySQL server.")
	flag.StringVar(&user, "user", "root", "The user for connecting to the database.")
	flag.StringVar(&passwd, "passwd", "", "The password for the user.")
	flag.StringVar(&logLevel, "log-level", "error", "The log level of mysql-tester: info, warn, error, debug.")
	flag.BoolVar(&record, "record", false, "Whether to record the test output to the result file.")
	flag.StringVar(&params, "params", "", "Additional params pass as DSN(e.g. session variable)")
	flag.BoolVar(&all, "all", false, "run all tests")
	flag.BoolVar(&reserveSchema, "reserve-schema", false, "Reserve schema after each test")
	flag.StringVar(&xmlPath, "xunitfile", "", "The xml file path to record testing results.")
	flag.IntVar(&retryConnCount, "retry-connection-count", 120, "The max number to retry to connect to the database.")
	flag.BoolVar(&checkErr, "check-error", false, "if --error ERR does not match, return error instead of just warn")
	flag.BoolVar(&collationDisable, "collation-disable", false, "run collation related-test with new-collation disabled")
}

const (
	default_connection = "default"
)

type query struct {
	firstWord string
	Query     string
	Line      int
	tp        int
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
	mdb  *sql.DB
	name string

	curr *Conn

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

	// only for test, not record, every time we execute a statement, we should read the result
	// data to check correction.
	resultFD *os.File

	// conns record connection created by test.
	conn map[string]*Conn

	// currConnName record current connection name.
	currConnName string

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

func setSessionVariable(db *Conn) {
	ctx := context.Background()
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_init_chunk_size=1"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_init_chunk_size=1\" err[%v]", err)
	}
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_max_chunk_size=32"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_max_chunk_size=32\" err[%v]", err)
	}
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_multi_statement_mode=1"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_multi_statement_mode=1\" err[%v]", err)
	}
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_hash_join_concurrency=1"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_hash_join_concurrency=1\" err[%v]", err)
	}
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_enable_pseudo_for_outdated_stats=false"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_enable_pseudo_for_outdated_stats=false\" err[%v]", err)
	}
	// enable tidb_enable_analyze_snapshot in order to let analyze request with SI isolation level to get accurate response
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_enable_analyze_snapshot=1"); err != nil {
		log.Warnf("Executing \"SET @@tidb_enable_analyze_snapshot=1 failed\" err[%v]", err)
	} else {
		log.Debugf("enable tidb_enable_analyze_snapshot")
	}
	if _, err := db.conn.ExecContext(ctx, "SET @@tidb_enable_clustered_index='int_only'"); err != nil {
		log.Fatalf("Executing \"SET @@tidb_enable_clustered_index='int_only'\" err[%v]", err)
	}
}

// isTiDB returns true if the DB is confirmed to be TiDB
func isTiDB(db *sql.DB) bool {
	if _, err := db.Exec("SELECT tidb_version()"); err != nil {
		log.Infof("This doesn't look like a TiDB server, err[%v]", err)
		return false
	}
	return true
}

func (t *tester) addConnection(connName, hostName, userName, password, db string) {
	var (
		mdb *sql.DB
		err error
	)

	if t.expectedErrs == nil {
		if t.curr != nil &&
			t.curr.hostName == hostName &&
			t.curr.userName == userName &&
			t.curr.password == password &&
			t.expectedErrs == nil {
			// Reuse mdb
			mdb = t.curr.mdb
		} else {
			mdb, err = OpenDBWithRetry("mysql", userName+":"+password+"@tcp("+hostName+":"+port+")/"+db+"?time_zone=%27Asia%2FShanghai%27&allowAllFiles=true"+params, retryConnCount)
		}
	} else {
		mdb, err = OpenDBWithRetry("mysql", userName+":"+password+"@tcp("+hostName+":"+port+")/"+db+"?time_zone=%27Asia%2FShanghai%27&allowAllFiles=true"+params, 1)
	}
	if err != nil {
		if t.expectedErrs == nil {
			log.Fatalf("Open db err %v", err)
		}
		t.expectedErrs = nil
		return
	}
	conn, err := initConn(mdb, userName, passwd, hostName, db)
	if err != nil {
		if t.expectedErrs == nil {
			log.Fatalf("Open db err %v", err)
		}
		t.expectedErrs = nil
		return
	}
	t.conn[connName] = conn
	t.switchConnection(connName)
}

func (t *tester) switchConnection(connName string) {
	conn, ok := t.conn[connName]
	if !ok {
		log.Fatalf("Connection %v doesn't exist.", connName)
	}
	// switch connection.
	t.mdb = conn.mdb
	t.curr = conn
	t.currConnName = connName
}

func (t *tester) disconnect(connName string) {
	conn, ok := t.conn[connName]
	if !ok {
		log.Fatalf("Connection %v doesn't exist.", connName)
	}
	err := conn.conn.Close()
	if err != nil {
		log.Fatal(err)
	}
	delete(t.conn, connName)
	conn = t.conn[default_connection]
	t.curr = conn
	t.mdb = conn.mdb
	t.currConnName = default_connection
}

func (t *tester) preProcess() {
	dbName := "test"
	mdb, err := OpenDBWithRetry("mysql", user+":"+passwd+"@tcp("+host+":"+port+")/"+dbName+"?time_zone=%27Asia%2FShanghai%27&allowAllFiles=true"+params, retryConnCount)
	t.conn = make(map[string]*Conn)
	if err != nil {
		log.Fatalf("Open db err %v", err)
	}

	dbName = strings.ReplaceAll(t.name, "/", "__")
	log.Debugf("Create new db `%s`", dbName)
	if _, err = mdb.Exec(fmt.Sprintf("create database `%s`", dbName)); err != nil {
		log.Fatalf("Executing create db %s err[%v]", dbName, err)
	}
	t.mdb = mdb
	conn, err := initConn(mdb, user, passwd, host, dbName)
	if err != nil {
		log.Fatalf("Open db err %v", err)
	}
	t.conn[default_connection] = conn
	t.curr = conn
	t.currConnName = default_connection
}

func (t *tester) postProcess() {
	if !reserveSchema {
		t.mdb.Exec(fmt.Sprintf("drop database `%s`", strings.ReplaceAll(t.name, "/", "__")))
	}
	for _, v := range t.conn {
		v.conn.Close()
	}
	t.mdb.Close()
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

	if err = t.openResult(); err != nil {
		err = errors.Trace(err)
		t.addFailure(&testSuite, &err, 0)
		return err
	}

	var s string
	defer func() {
		if t.resultFD != nil {
			t.resultFD.Close()
		}
	}()

	testCnt := 0
	startTime := time.Now()
	var concurrentQueue []query
	var concurrentSize int
	for _, q := range queries {
		s = q.Query
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
		case Q_BEGIN_CONCURRENT:
			// mysql-tester enhancement
			concurrentQueue = make([]query, 0)
			t.enableConcurrent = true
			if s == "" {
				concurrentSize = 8
			} else {
				concurrentSize, err = strconv.Atoi(strings.TrimSpace(s))
				if err != nil {
					err = errors.Annotate(err, "Atoi failed")
					t.addFailure(&testSuite, &err, testCnt)
					return err
				}
			}
		case Q_END_CONCURRENT:
			t.enableConcurrent = false
			if err = t.concurrentRun(concurrentQueue, concurrentSize); err != nil {
				err = errors.Annotate(err, fmt.Sprintf("concurrent test failed in %v", t.name))
				t.addFailure(&testSuite, &err, testCnt)
				return err
			}
			t.expectedErrs = nil
		case Q_ERROR:
			t.expectedErrs = strings.Split(strings.TrimSpace(s), ",")
		case Q_ECHO:
			varSearch := regexp.MustCompile(`\\$([A-Za-z0-9_]+)( |$)`)
			s := varSearch.ReplaceAllStringFunc(s, func(s string) string {
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
		case Q_CONNECT:
			q.Query = strings.TrimSpace(q.Query)
			if q.Query[len(q.Query)-1] == ';' {
				q.Query = q.Query[:len(q.Query)-1]
			}
			q.Query = q.Query[1 : len(q.Query)-1]
			args := strings.Split(q.Query, ",")
			for i := range args {
				args[i] = strings.TrimSpace(args[i])
			}
			for i := 0; i < 4; i++ {
				args = append(args, "")
			}
			t.addConnection(args[0], args[1], args[2], args[3], args[4])
		case Q_CONNECTION:
			q.Query = strings.TrimSpace(q.Query)
			if q.Query[len(q.Query)-1] == ';' {
				q.Query = q.Query[:len(q.Query)-1]
			}
			t.switchConnection(q.Query)
		case Q_DISCONNECT:
			q.Query = strings.TrimSpace(q.Query)
			if q.Query[len(q.Query)-1] == ';' {
				q.Query = q.Query[:len(q.Query)-1]
			}
			t.disconnect(q.Query)
		case Q_LET:
			q.Query = strings.TrimSpace(q.Query)
			eqIdx := strings.Index(q.Query, "=")
			if eqIdx > 1 {
				start := 0
				if q.Query[0] == '$' {
					start = 1
				}
				varName := strings.TrimSpace(q.Query[start:eqIdx])
				varValue := strings.TrimSpace(q.Query[eqIdx+1:])
				varSearch := regexp.MustCompile("`(.*)`")
				varValue = varSearch.ReplaceAllStringFunc(varValue, func(s string) string {
					s = strings.Trim(s, "`")
					r, err := t.executeStmtString(s)
					if err != nil {
						log.WithFields(log.Fields{
							"query": s, "line": q.Line},
						).Error("failed to perform let query")
						return ""
					}
					return r
				})
				os.Setenv(varName, varValue)
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

	if xmlPath != "" {
		t.addSuccess(&testSuite, &startTime, testCnt)
	}

	return t.flushResult()
}

func (t *tester) concurrentRun(concurrentQueue []query, concurrentSize int) error {
	if len(concurrentQueue) == 0 {
		return nil
	}
	offset := t.buf.Len()

	if concurrentSize <= 0 {
		return errors.Errorf("concurrentSize must be positive")
	}
	if concurrentSize > len(concurrentQueue) {
		concurrentSize = len(concurrentQueue)
	}
	batchQuery := make([][]query, concurrentSize)
	for i, query := range concurrentQueue {
		j := i % concurrentSize
		batchQuery[j] = append(batchQuery[j], query)
	}
	errOccured := make(chan struct{}, len(concurrentQueue))
	var wg sync.WaitGroup
	wg.Add(len(batchQuery))
	for _, q := range batchQuery {
		go t.concurrentExecute(q, &wg, errOccured)
	}
	wg.Wait()
	close(errOccured)
	if _, ok := <-errOccured; ok {
		return errors.Errorf("Run failed")
	}
	buf := t.buf.Bytes()[:offset]
	t.buf = *(bytes.NewBuffer(buf))
	return nil
}

func initConn(mdb *sql.DB, host, user, passwd, dbName string) (*Conn, error) {
	mdb.SetMaxIdleConns(-1) // Disable the underlying connection pool.
	sqlConn, err := mdb.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	conn := &Conn{
		mdb:      mdb,
		hostName: host,
		userName: user,
		password: passwd,
		db:       dbName,
		conn:     sqlConn,
	}
	if isTiDB(mdb) {
		setSessionVariable(conn)
	}
	if dbName != "" {
		if _, err = sqlConn.ExecContext(context.Background(), fmt.Sprintf("use `%s`", dbName)); err != nil {
			log.Fatalf("Executing Use test err[%v]", err)
		}
	}
	return conn, nil
}

func (t *tester) concurrentExecute(querys []query, wg *sync.WaitGroup, errOccured chan struct{}) {
	defer wg.Done()
	tt := newTester(t.name)
	dbName := "test"
	mdb, err := OpenDBWithRetry("mysql", user+":"+passwd+"@tcp("+host+":"+port+")/"+dbName+"?time_zone=%27Asia%2FShanghai%27&allowAllFiles=true"+params, retryConnCount)
	if err != nil {
		log.Fatalf("Open db err %v", err)
	}
	conn, err := initConn(mdb, user, passwd, host, t.name)
	if err != nil {
		log.Fatalf("Open db err %v", err)
	}
	tt.curr = conn
	tt.mdb = mdb
	defer tt.mdb.Close()

	for _, query := range querys {
		if len(query.Query) == 0 {
			return
		}

		err := tt.stmtExecute(query.Query)
		if err != nil && len(t.expectedErrs) > 0 {
			for _, tStr := range t.expectedErrs {
				if strings.Contains(err.Error(), tStr) {
					err = nil
					break
				}
			}
		}
		if err != nil {
			msgs <- testTask{
				test: t.name,
				err:  errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", query.Query, query.Line, err)),
			}
			errOccured <- struct{}{}
			return
		}
	}
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
		if !checkErr {
			log.Warnf("%s:%d query succeeded, but expected error(s)! (expected errors: %s) (query: %s)",
				t.name, q.Line, strings.Join(t.expectedErrs, ","), q.Query)
			return nil
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
			if len(t.expectedErrs) == 1 || !checkErr {
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
	if !checkErr {
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
		return nil
	}
	return err
}

func (t *tester) execute(query query) error {
	if len(query.Query) == 0 {
		return nil
	}

	offset := t.buf.Len()
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

	if !record {
		// check test result now
		gotBuf := t.buf.Bytes()[offset:]

		buf := make([]byte, t.buf.Len()-offset)
		if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", query.Query, query.Line, gotBuf, err))
		}

		if !bytes.Equal(gotBuf, buf) {
			return errors.Trace(errors.Errorf("failed to run query \n\"%v\" \n around line %d, \nwe need(%v):\n%s\nbut got(%v):\n%s\n", query.Query, query.Line, len(buf), buf, len(gotBuf), gotBuf))
		}
	}

	return errors.Trace(err)
}

func (t *tester) writeQueryResult(rows *byteRows) error {
	if t.sortedResult {
		sort.Sort(rows)
	}

	if len(t.replaceColumn) > 0 {
		for _, row := range rows.data {
			for _, r := range t.replaceColumn {
				if len(row.data) < r.col {
					continue
				}
				row.data[r.col-1] = r.replace
			}
		}
	}

	cols := rows.cols
	for i, c := range cols {
		t.buf.WriteString(c)
		if i != len(cols)-1 {
			t.buf.WriteString("\t")
		}
	}
	t.buf.WriteString("\n")

	for _, row := range rows.data {
		var value string
		for i, col := range row.data {
			// replace result by regex
			for _, reg := range t.replaceRegex {
				col = reg.regex.ReplaceAll(col, []byte(reg.replace))
			}

			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			t.buf.WriteString(value)
			if i < len(row.data)-1 {
				t.buf.WriteString("\t")
			}
		}
		t.buf.WriteString("\n")
	}
	return nil
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
	raw, err := t.curr.conn.QueryContext(context.Background(), query)
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := dumpToByteRows(raw)
	if err != nil {
		return errors.Trace(err)
	}

	if t.enableResultLog && (len(rows.cols) > 0 || len(rows.data) > 0) {
		if err = t.writeQueryResult(rows); err != nil {
			return errors.Trace(err)
		}
	}

	if t.enableInfo {
		t.curr.conn.Raw(func(driverConn any) error {
			rowsAffected := driverConn.(*mysql.MysqlConn).RowsAffected()
			lastMessage := driverConn.(*mysql.MysqlConn).LastMessage()
			t.buf.WriteString(fmt.Sprintf("affected rows: %d\n", rowsAffected))
			t.buf.WriteString(fmt.Sprintf("info: %s\n", lastMessage))
			return nil
		})
	}

	if t.enableWarning {
		raw, err := t.curr.conn.QueryContext(context.Background(), "show warnings;")
		if err != nil {
			return errors.Trace(err)
		}

		rows, err := dumpToByteRows(raw)
		if err != nil {
			return errors.Trace(err)
		}

		if len(rows.data) > 0 {
			sort.Sort(rows)
			return t.writeQueryResult(rows)
		}
	}
	return nil
}

func (t *tester) executeStmtString(query string) (string, error) {
	var result string
	err := t.mdb.QueryRow(query).Scan(&result)
	if err != nil {
		return "", err
	}
	return result, nil
}

func (t *tester) openResult() error {
	if record {
		return nil
	}

	var err error
	t.resultFD, err = os.Open(t.resultFileName())
	return err
}

func (t *tester) flushResult() error {
	if !record {
		return nil
	}
	path := t.resultFileName()
	// Create all directories in the file path
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %v", err)
	}
	return os.WriteFile(path, t.buf.Bytes(), 0644)
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
	startTime := time.Now()
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

	if xmlPath != "" {
		_, err := os.Stat(xmlPath)
		if err == nil {
			err = os.Remove(xmlPath)
			if err != nil {
				log.Error("drop previous xunit file fail: ", err)
				os.Exit(1)
			}
		}

		xmlFile, err = os.Create(xmlPath)
		if err != nil {
			log.Error("create xunit file fail:", err)
			os.Exit(1)
		}
		xmlFile, err = os.OpenFile(xmlPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			log.Error("open xunit file fail:", err)
			os.Exit(1)
		}

		testSuite = XUnitTestSuite{
			Name:       "",
			Tests:      0,
			Failures:   0,
			Properties: make([]XUnitProperty, 0),
			TestCases:  make([]XUnitTestCase, 0),
		}

		defer func() {
			if xmlFile != nil {
				testSuite.Tests = len(tests)
				testSuite.Time = fmt.Sprintf("%fs", time.Since(startTime).Seconds())
				testSuite.Properties = append(testSuite.Properties, XUnitProperty{
					Name:  "go.version",
					Value: goVersion(),
				})
				err := Write(xmlFile, testSuite)
				if err != nil {
					log.Error("Write xunit file fail:", err)
				}
			}
		}()
	}

	// we will run all tests if no tests assigned
	if len(tests) == 0 {
		var err error
		if tests, err = loadAllTests(); err != nil {
			log.Fatalf("load all tests err %v", err)
		}
	}

	if !record {
		log.Infof("running tests: %v", tests)
	} else {
		log.Infof("recording tests: %v", tests)
	}

	if !checkErr {
		log.Warn("--check-error is not set! --error in .test file will simply accept zero or more errors! (i.e. not even check for errors!)")
	}
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
