#!/bin/bash
set -e
GOBIN=$PWD go get github.com/pingcap/mysql-tester/src@9099f3cc66f69519fe1fd3be65c83207099ea670
mv src mysql_test
echo -e "build mysql_test successfully"
