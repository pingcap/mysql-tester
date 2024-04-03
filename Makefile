.PHONY: all build test tidy clean

GO := go

default: build

build:
	$(GO) build -o vitess-tester ./src

debug:
	$(GO) build -gcflags="all=-N -l" -o vitess-tester ./src

test: build
	$(GO) test -cover ./...
	#./vitess-tester -check-error

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -i ./...
	rm -rf vitess-tester

gen_perror: generate_perror/main.go
	$(GO) build -o gen_perror ./generate_perror
