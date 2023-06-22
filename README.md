## Add Testcase Best Practice

1. Write a test file under the t directory, using example.test as a reference.
2. Set up a MySQL database, then run the mysql-tester with the `-record` parameter to generate an example.result file.
3. Set up a Test environment, and run the mysql-tester to test the functionality. It will compare the result with the example.result file.


## Usage

```bash
# build the mysql-tester binary
make build

# record the test output to the result file
./bin/mysql-tester -port 3306 -user root -passwd 123123 -path testcase -record

# run all the testcases, and record the result to result.xml
./bin/mysql-tester -port 3306 -user root -passwd 123123 -path testcase -xunitfile result.xml

# run the testcases test1 and test2, and record the result to result.xml
./bin/mysql-tester -port 3306 -user root -passwd 123123 -path testcase -xunitfile result.xml test1 test2

```

## Program Arguments
```bash
./mysql-tester -h
Usage of ./bin/mysql-tester:
  -all
        run all tests
  -dbName string
        The database name that firstly connect to. (default "mysql")
  -host string
        The host of the TiDB/MySQL server. (default "127.0.0.1")
  -log-level string
        The log level of mysql-tester: info, warn, error, debug. (default "error")
  -params string
        Additional params pass as DSN(e.g. session variable)
  -passwd string
        The password for the user.
  -path string
        The Base Path of testcase. (default "testcase")
  -port string
        The listen port of TiDB/MySQL server. (default "3306")
  -record
        Whether to record the test output to the result file.
  -reserve-schema
        Reserve schema after each test
  -retry-connection-count int
        The max number to retry to connect to the database. (default 120)
  -user string
        The user for connecting to the database. (default "root")
  -xunitfile string
        The xml file path to record testing results.

```
