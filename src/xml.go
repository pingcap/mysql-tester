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
	"encoding/xml"
	log "github.com/sirupsen/logrus"
	"io"
)

// JUnitTestSuite is a single mysqltest suite which may contain many
// testcases.
type JUnitTestSuite struct {
	XMLName           xml.Name       `xml:"testsuite"`
	Name              string         `xml:"name,attr"`
	TotalCasesCount   int            `xml:"totalCasesCount,attr"`
	SuccessCasesCount int            `xml:"failCasesCount,attr"`
	Time              string         `xml:"time,attr"`
	Successes         []JUnitSuccess `xml:"testcase"`
	Failures          []JUnitFailure `xml:"failures"`
}

// JUnitSuccess is a single success test case with its result.
type JUnitSuccess struct {
	XMLName    xml.Name `xml:"testcase"`
	Name       string   `xml:"name,attr"`
	Classname  string   `xml:"classname,attr"`
	Time       string   `xml:"time,attr"`
	QueryCount int      `xml:"query-count,attr"`
	Message    string   `xml:"message,omitempty"`
}

// JUnitFailure contains data related to a failed test.
type JUnitFailure struct {
	XMLName   xml.Name `xml:"testcase"`
	Name      string   `xml:"name,attr"`
	Classname string   `xml:"classname,attr"`
	Message   string   `xml:"message,omitempty"`
}

func Write(out io.Writer, testSuite JUnitTestSuite) error {
	_, err := xmlFile.Write([]byte(xml.Header))
	if err != nil {
		log.Errorf("write junit file fail:", err)
		return err
	}
	doc, err := xml.MarshalIndent(testSuite, "", "\t")
	if err != nil {
		return err
	}
	_, err = out.Write(doc)
	return err
}
