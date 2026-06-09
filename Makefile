.PHONY: all build test tidy clean generate

GO := go

default: build

build: generate
	$(GO) build -o mysql-tester ./src

generate: gen_perror
	$(GO) generate ./src

debug:
	$(GO) build -gcflags="all=-N -l" -o mysql-tester ./src

test: build
	$(GO) test -cover ./...
	#./mysql-tester -check-error

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -i ./...
	rm -rf mysql-tester

gen_perror: generate_perror/main.go
	$(GO) build -o gen_perror ./generate_perror
