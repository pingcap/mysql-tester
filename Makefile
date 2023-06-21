.PHONY: all build test tidy clean

default: build

build:
	go build -o mysql-tester ./src

test:
	go test -cover ./...

tidy:
	go mod tidy

clean:
	go clean -i ./...
	rm -rf mysql-tester

run:
	@if [ -z "${args}" ]; then \
    	./mysql-tester -port 15306 -path .; \
	else \
		./mysql-tester ${args}; \
	fi
