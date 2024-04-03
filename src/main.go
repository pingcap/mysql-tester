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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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
	sharded          bool
	collationDisable bool
)

func init() {
	flag.StringVar(&mysqlSocket, "mysql-socket", "127.0.0.1", "The host of the MySQL server.")
	flag.StringVar(&mysqlUser, "mysql-user", "root", "The user for connecting to the MySQL database.")
	flag.StringVar(&mysqlPasswd, "mysql-passwd", "", "The password for the MySQL user.")
	flag.StringVar(&vtHost, "vt-host", "127.0.0.1", "The host of the vtgate server.")
	flag.StringVar(&vtPort, "vt-port", "", "The listen port of vtgate server.")
	flag.StringVar(&vtUser, "vt-user", "root", "The user for connecting to the vtgate")
	flag.StringVar(&vtPasswd, "vt-passwd", "", "The password for the vtgate")
	flag.StringVar(&logLevel, "log-level", "error", "The log level of vitess-tester: info, warn, error, debug.")
	flag.BoolVar(&all, "all", false, "run all tests")
	flag.BoolVar(&sharded, "sharded", false, "run all tests on a sharded keyspace")
	flag.BoolVar(&collationDisable, "collation-disable", false, "run collation related-test with new-collation disabled")
}

type (
	query struct {
		firstWord string
		Query     string
		Line      int
		tp        CmdType
	}
	ReplaceColumn struct {
		col     int
		replace []byte
	}
	ReplaceRegex struct {
		regex   *regexp.Regexp
		replace string
	}
	tester struct {
		name string

		curr utils.MySQLCompare

		// enable query log will output origin statement into result file too
		// use --disable_query_log or --enable_query_log to control it
		enableQueryLog bool

		// enable result log will output to result file or not.
		// use --enable_result_log or --disable_result_log to control it
		enableResultLog bool

		// sortedResult make the output or the current query sorted.
		sortedResult bool

		// Disable or enable warnings. This setting is enabled by default.
		// With this setting enabled, mysqltest uses SHOW WARNINGS to display
		// any warnings produced by SQL statements.
		enableWarning bool

		// enable query info, like rowsAffected, lastMessage etc.
		enableInfo bool

		// check expected error, use --error before the statement
		// we only care if an error is returned, not the exact error message.
		expectedErrs bool

		// replace output column through --replace_column 1 <static data> 3 #
		replaceColumn []ReplaceColumn

		// replace output result through --replace_regex /\.dll/.so/
		replaceRegex []*ReplaceRegex
	}

	TestingCtx struct{}
)

func (t TestingCtx) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func (t TestingCtx) FailNow() {
	panic("test failed")
}

func (t TestingCtx) Helper() {}

func newTester(name string) *tester {
	t := new(tester)

	t.name = name
	t.enableQueryLog = true
	t.enableResultLog = true
	// disable warning by default since our a lot of test cases
	// are ported wihtout explictly "disablewarning"
	t.enableWarning = false
	t.enableInfo = false

	return t
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
	return strings.Join(t, ", ")
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

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspaceName    = "mysqltest"
	cell            = "mysqltest"

	vtParams    mysql.ConnParams
	mysqlParams mysql.ConnParams
)

type hashVindex struct {
	vindexes.Hash
	Type string `json:"type"`
}

func (hv hashVindex) String() string {
	return "hash"
}

var vschema = vindexes.VSchema{
	Keyspaces: map[string]*vindexes.KeyspaceSchema{
		keyspaceName: {
			Keyspace: &vindexes.Keyspace{Sharded: true},
			Tables:   map[string]*vindexes.Table{},
			Vindexes: map[string]vindexes.Vindex{
				"hash": &hashVindex{Type: "hash"},
			},
			Views: map[string]sqlparser.SelectStatement{},
		},
	},
}

func setupCluster(sharded bool) func() {
	clusterInstance = cluster.NewCluster(cell, "localhost")

	// Start topo server
	err := clusterInstance.StartTopo()
	if err != nil {
		clusterInstance.Teardown()
		panic(err)
	}

	if sharded {
		ksSchema, err := json.Marshal(vschema.Keyspaces[keyspaceName])
		if err != nil {
			panic(err)
		}
		keyspace := &cluster.Keyspace{
			Name:    keyspaceName,
			VSchema: string(ksSchema),
		}

		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			panic(err.Error())
		}
	} else {
		// Start Unsharded keyspace
		ukeyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}
		err = clusterInstance.StartUnshardedKeyspace(*ukeyspace, 0, false)
		if err != nil {
			clusterInstance.Teardown()
			panic(err)
		}
	}

	// Start vtgate
	err = clusterInstance.StartVtgate()
	if err != nil {
		clusterInstance.Teardown()
		panic(err)
	}

	vtParams = clusterInstance.GetVTParams(keyspaceName)

	// create mysql instance and connection parameters
	conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, "")
	if err != nil {
		clusterInstance.Teardown()
		panic(err)
	}
	mysqlParams = conn

	return func() {
		clusterInstance.Teardown()
		closer()
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

	closer := setupCluster(sharded)
	defer closer()

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
