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
