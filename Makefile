DB2_STARTUP_TIMEOUT ?= 50

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-db2.version=${VERSION}'" -o conduit-connector-db2 cmd/connector/main.go

.PHONY: test
test:
	go list -f "{{.Module.Version}}" github.com/ibmdb/go_ibm_db/installer | xargs -tI % go run github.com/ibmdb/go_ibm_db/installer@%
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down --volumes; \
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
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
