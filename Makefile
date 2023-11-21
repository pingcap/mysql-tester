.PHONY: all build test tidy clean

default: build

build:
	go build -o mysql-tester ./src

debug:
	go build -gcflags="all=-N -l" -o mysql-tester ./src

test: build
	# waiting on pr/46
	go test -cover ./...
	./mysql-tester -check-error

tidy:
	go mod tidy

clean:
	go clean -i ./...
	rm -rf mysql-tester

gen_perror: generate_perror/main.go
	go build -o gen_perror ./generate_perror
