module github.com/pingcap/mysql-tester

go 1.12

require (
	github.com/go-sql-driver/mysql v1.3.0
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8 // indirect
	github.com/pingcap/parser v0.0.0-20210314080929-ed8900c94180
	github.com/sirupsen/logrus v1.4.2
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0 // indirect
	golang.org/x/sys v0.0.0-20190910064555-bbd175535a8b // indirect
	golang.org/x/text v0.3.5 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

exclude github.com/go-sql-driver/mysql v1.5.0
