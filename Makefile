.PHONY: all build test tidy clean

default: build

build:
	go build -o ./bin ./...
	mv bin/src bin/mysql-tester

test:
	go test -cover ./...

tidy:
	go mod tidy

clean:
	go clean -i ./...
	rm -rf mysql-tester

run:
	@if [ -z "${args}" ]; then \
    	./bin/mysql-tester -port 15306 -path .; \
	else \
		./bin/mysql-tester ${args}; \
	fi
