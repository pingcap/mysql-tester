module github.com/pingcap/mysql-tester

go 1.19

require (
	github.com/go-sql-driver/mysql v1.7.1
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/go-sql-driver/mysql v1.7.1 => github.com/defined2014/mysql v0.0.0-20231117092812-9bc18244ae3e
