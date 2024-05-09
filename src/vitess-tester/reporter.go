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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

type Reporter interface {
	AddTestCase(query string, lineNo int)
	EndTestCase()
	AddFailure(err error)
	Report() string
	Failed() bool
}

type FileReporter struct {
	name      string
	errorFile *os.File
	startTime time.Time

	currentQuery        string
	currentQueryLineNum int
	currentStartTime    time.Time
	currentQueryFailed  bool

	failureCount int
	queryCount   int
	successCount int
}

func newFileReporter(name string) *FileReporter {
	return &FileReporter{
		name:      name,
		startTime: time.Now(),
	}

}

func (e *FileReporter) Failed() bool {
	return e.failureCount > 0
}

func (e *FileReporter) Report() string {
	return fmt.Sprintf(
		"%s: ok! Ran %d queries, %d successfully and %d failures take time %v s\n",
		e.name,
		e.queryCount,
		e.successCount,
		e.queryCount-e.successCount,
		time.Since(e.startTime).Seconds(),
	)
}

func (e *FileReporter) AddTestCase(query string, lineNum int) {
	e.currentQuery = query
	e.currentQueryLineNum = lineNum
	e.currentStartTime = time.Now()
	e.queryCount++
	e.currentQueryFailed = false
}

func (e *FileReporter) EndTestCase() {
	if !e.currentQueryFailed {
		e.successCount++
	}
	if e.errorFile != nil {
		err := e.errorFile.Close()
		if err != nil {
			panic("failed to close error file\n" + err.Error())
		}
		e.errorFile = nil
	}
}

func (e *FileReporter) AddFailure(err error) {
	e.failureCount++
	e.currentQueryFailed = true
	if e.currentQuery == "" {
		e.currentQuery = "GENERAL"
	}
	if e.errorFile == nil {
		e.errorFile = e.createErrorFileFor()
	}

	_, err = e.errorFile.WriteString(err.Error())
	if err != nil {
		panic("failed to write error file\n" + err.Error())
	}

	e.createVSchemaDump()
}

func (e *FileReporter) createErrorFileFor() *os.File {
	qc := fmt.Sprintf("%d", e.currentQueryLineNum)
	err := os.MkdirAll(e.errorDir(), PERM)
	if err != nil {
		panic("failed to create error directory\n" + err.Error())
	}
	errorPath := path.Join(e.errorDir(), qc)
	file, err := os.Create(errorPath)
	if err != nil {
		panic("failed to create error file\n" + err.Error())
	}
	_, err = file.WriteString(fmt.Sprintf("Error log for query on line %d:\n%s\n\n", e.currentQueryLineNum, e.currentQuery))
	if err != nil {
		panic("failed to write to error file\n" + err.Error())
	}

	return file
}

func (e *FileReporter) createVSchemaDump() {
	errorDir := e.errorDir()
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

func (e *FileReporter) errorDir() string {
	errFileName := e.name
	if strings.HasPrefix(e.name, "http") {
		u, err := url.Parse(e.name)
		if err == nil {
			errFileName = path.Base(u.Path)
			if errFileName == "" || errFileName == "/" {
				errFileName = url.QueryEscape(e.name)
			}
		}
	}
	return path.Join("errors", errFileName)
}

var _ Reporter = (*FileReporter)(nil)
