.PHONY: check vet test
check: vet test

vet:
	go vet ./...

test:
	go test -race -v ./...
