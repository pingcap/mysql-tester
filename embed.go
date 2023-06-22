package mysql_tester

import (
	"embed"
	_ "embed"
)

//go:embed testcase/*
var Testcase embed.FS
