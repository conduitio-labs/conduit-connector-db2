GOLINT := golangci-lint

.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-db2.version=${VERSION}'" -o conduit-connector-db2 cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race -gcflags=all=-d=checkptr=0 ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml

mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
	mockgen -package mock -source source/interface.go -destination source/mock/iterator.go

