DB2_STARTUP_TIMEOUT ?= 50

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-db2.version=${VERSION}'" -o conduit-connector-db2 cmd/connector/main.go

.PHONY: test
test:
	go install github.com/ibmdb/go_ibm_db/installer@v0.4.2
	go run $(shell go env GOMODCACHE)/github.com/ibmdb/go_ibm_db@v0.4.2/installer/setup.go
	docker run -itd --name mydb2 --privileged=true -p 50000:50000 -e LICENSE=accept -e DB2INST1_PASSWORD=pwd -e DBNAME=testdb -v /db2/vol:/database ibmcom/db2
	sleep $(DB2_STARTUP_TIMEOUT)
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
			docker stop mydb2; \
			exit $$ret

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: mockgen
mockgen:
	mockgen -package mock -source destination/interface.go -destination destination/mock/destination.go
	mockgen -package mock -source source/interface.go -destination source/mock/iterator.go

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
