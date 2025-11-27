module github.com/pingcap/mysql-tester

go 1.25

require (
	// It forks from github.com/go-sql-driver/mysql v1.7.1
	// We use it to get LastMessage() and RowsAffected()
	github.com/go-sql-driver/mysql v1.9.3
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.8.4
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
