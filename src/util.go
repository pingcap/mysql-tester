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
	"database/sql"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

// OpenDBWithRetry opens a database specified by its database driver name and a
// driver-specific data source name. And it will do some retries if the connection fails.
func OpenDBWithRetry(driverName, dataSourceName string, retryCount int) (mdb *sql.DB, err error) {
	startTime := time.Now()
	sleepTime := time.Millisecond * 500
	// The max retry interval is 60 s.
	for i := 0; i < retryCount; i++ {
		mdb, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			log.Warnf("open db failed, retry count %d (remain %d) err %v", i, retryCount-i, err)
			time.Sleep(sleepTime)
			continue
		}
		err = mdb.Ping()
		if err == nil {
			break
		}
		log.Warnf("ping db failed, retry count %d (remain %d) err %v", i, retryCount-i, err)
		mdb.Close()
		time.Sleep(sleepTime)
	}
	if err != nil {
		log.Errorf("open db failed %v, take time %v", err, time.Since(startTime))
		return nil, errors.Trace(err)
	}

	return
}

func processEscapes(str string) string {
	escapeMap := map[string]string{
		`\n`: "\n",
		`\t`: "\t",
		`\r`: "\r",
		`\/`: "/",
		`\\`: "\\", // better be the last one
	}

	for escape, replacement := range escapeMap {
		str = strings.ReplaceAll(str, escape, replacement)
	}

	return str
}

func ParseReplaceRegex(originalString string) ([]*ReplaceRegex, error) {
	var begin, middle, end, cnt int
	ret := make([]*ReplaceRegex, 0)
	for i, c := range originalString {
		if c != '/' {
			continue
		}
		if i != 0 && originalString[i-1] == '\\' {
			continue
		}
		cnt++
		switch cnt % 3 {
		// The first '/'
		case 1:
			begin = i
		// The second '/'
		case 2:
			middle = i
		// The last '/', we could compile regex and process replace string
		case 0:
			end = i
			reg, err := regexp.Compile(originalString[begin+1 : middle])
			if err != nil {
				return nil, err
			}
			ret = append(ret, &ReplaceRegex{
				regex:   reg,
				replace: processEscapes(originalString[middle+1 : end]),
			})
		}
	}
	if cnt%3 != 0 {
		return nil, errors.Errorf("Could not parse regex in --replace_regex: sql:%v", originalString)
	}
	return ret, nil
}

func parseSourceAndTarget(s string) (*SourceAndTarget, error) {
	s = strings.ToLower(strings.TrimSpace(s))

	parts := strings.Split(s, "as")
	if len(parts) != 2 {
		return nil, errors.Errorf("Could not parse source table and target table name: %v", s)
	}

	st := &SourceAndTarget{
		sourceTable: strings.TrimSpace(parts[0]),
		targetTable: strings.TrimSpace(parts[1]),
	}
	return st, nil
}
