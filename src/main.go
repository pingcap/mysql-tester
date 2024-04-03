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
	"os"
	"path/filepath"
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
	olap             bool
)

func init() {
	flag.BoolVar(&olap, "olap", false, "Use OLAP run the queries.")
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

type query struct {
	firstWord string
	Query     string
	Line      int
	tp        CmdType
}

func loadAllTests() (tests []string, err error) {
	// tests must be in t folder or subdir in t folder
	err = filepath.Walk("./t/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".test") {
			name := strings.TrimPrefix(strings.TrimSuffix(path, ".test"), "t/")
			if !collationDisable {
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

func executeTests(fileNames []string) (failed bool) {
	for _, name := range fileNames {
		show := newTester(name)
		err := show.Run()
		if err != nil {
			failed = true
			continue
		}
		if show.failureCount > 0 {
			failed = true
		}
	}
	return
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
			Keyspace: &vindexes.Keyspace{},
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
		vschema.Keyspaces[keyspaceName].Keyspace.Sharded = true
		ksSchema, err := json.Marshal(vschema.Keyspaces[keyspaceName])
		if err != nil {
			panic(err)
		}
		keyspace := &cluster.Keyspace{
			Name:    keyspaceName,
			VSchema: string(ksSchema),
		}

		println("starting sharded keyspace")
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			panic(err.Error())
		}
	} else {
		// Start Unsharded keyspace
		ukeyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}
		println("starting unsharded keyspace")
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

	// remove errors folder if exists
	err := os.RemoveAll("errors")
	if err != nil {
		panic(err.Error())
	}

	if failed := executeTests(tests); failed {
		log.Errorf("some tests failed 😭\nsee errors in errors folder")
		os.Exit(1)
	}
	println("Great, All tests passed")
}
