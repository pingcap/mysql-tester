# MySQL Tester

This is a golang implementation of [MySQL Test Framework](https://github.com/mysql/mysql-server/tree/8.0/mysql-test).

## Requirements

- All the tests should be put in [`t`](./t), take [t/example.test](./t/example.test) as an example.
- All the expected test results should be put in [`r`](./r). Result file has the same file name with the corresponding test file, but with a `.result` file suffix, take [r/example.result](./r/example.result) as an examle.

## How to use

Build the `mysql-tester` binary:
```sh
make
```

Basic usage:
```
Usage of ./mysql-tester:
  -host string
        The host of the TiDB/MySQL server. (default "127.0.0.1")
  -log-level string
        The log level of mysql-tester: info, warn, error, debug. (default "error")
  -passwd string
        The password for the user.
  -port string
        The listen port of TiDB/MySQL server. (default "4000")
  -record
        Whether to record the test output to the result file.
  -user string
        The user for connecting to the database. (default "root")
```

By default, it connects to the TiDB/MySQL server at `127.0.0.1:4000` with `root` and no passward:
```sh
./mysql-tester # run all the tests
./mysql-tester example # run a specified test
./mysql-tester example example   example # seperate different tests with spaces
```
