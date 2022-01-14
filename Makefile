.PHONY: all build test tidy clean

default: mysql-tester

mysql-tester:
	go build -o mysql-tester ./src

debug:
	go build -gcflags="all=-N -l" -o mysql-tester ./src

test: mysql-tester
	# waiting on pr/46
	#go test -cover ./...
	./mysql-tester

tidy:
	go mod tidy

clean:
	go clean -i ./...
	rm -rf mysql-tester

