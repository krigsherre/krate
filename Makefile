.PHONY: build test test-race test-integration bench bench-peer clean \
       redis-docker redis-docker-stop proto-gen lint help

REDIS_ADDR ?= localhost:6379
DURATION   ?= 60s
INSTANCES  ?= 4
KEYS       ?= 100000
LIMIT      ?= 10000

build:
	go build ./...

build-example:
	go build -o bin/krate-example ./cmd/krate-example/

build-bench:
	go build -o bin/krate-bench ./cmd/krate-bench/

test:
	go test ./... -v -count=1

test-race:
	go test ./... -v -race -count=1

test-integration:
	@echo "Requires Redis at $(REDIS_ADDR)"
	KRATE_TEST_REDIS=$(REDIS_ADDR) go test -v -race -count=1 -timeout 120s -run Integration ./...

test-short:
	go test ./... -v -count=1 -short

test-cover:
	go test ./... -race -count=1 -coverprofile=coverage.out
	go tool cover -func=coverage.out
	@echo ""
	@echo "To view HTML coverage: go tool cover -html=coverage.out"

bench:
	go test -bench=. -benchmem -count=1 -timeout 60s ./...

bench-race:
	go test -bench=. -benchmem -race -count=1 -timeout 60s ./...

bench-run: build-bench
	@echo "Requires Redis at $(REDIS_ADDR)"
	REDIS_ADDR=$(REDIS_ADDR) ./bin/krate-bench -scenario=all -duration=5s

proto-gen:
	@echo "Generating proto..."
	cd proto && buf generate
	@echo "Done. Generated files in peer/peerpb/"

lint:
	@which golangci-lint > /dev/null 2>&1 || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run ./...

vet:
	go vet ./...

redis-docker:
	docker run -d --name krate-redis -p 6379:6379 redis:7-alpine
	@sleep 1
	@docker exec krate-redis redis-cli ping

redis-docker-stop:
	docker stop krate-redis 2>/dev/null || true
	docker rm krate-redis 2>/dev/null || true

bench-docker: redis-docker build-bench
	@echo ""
	@echo "Running benchmark suite..."
	@echo ""
	REDIS_ADDR=$(REDIS_ADDR) ./bin/krate-bench-peer; \
	EXIT=$$?; \
	(MAKE) redis-docker-stop; \
	exit $$$$EXIT

clean:
	go clean -testcache
	rm -f coverage.out
	rm -rf bin/

help:
	@echo ""
	@echo "  krate — build, test, and benchmark"
	@echo ""
	@echo "  Build"
	@echo "    make build              Build all packages"
	@echo "    make build-example      Build example HTTP server"
	@echo "    make build-bench        Build benchmark tool"
	@echo ""
	@echo "  Test"
	@echo "    make test               Run all tests"
	@echo "    make test-race          Run all tests with race detector"
	@echo "    make test-integration   Run integration tests (needs Redis)"
	@echo "    make test-short         Run tests in short mode"
	@echo "    make test-cover         Run tests with coverage report"
	@echo ""
	@echo "  Benchmark"
	@echo "    make bench              Run Go benchmarks (sketch, bucket, etc.)"
	@echo "    make bench-peer         Run peer benchmark (needs Redis)"
	@echo "    make bench-docker       Start Redis, run benchmark, stop Redis"
	@echo ""
	@echo "  Docker"
	@echo "    make redis-docker       Start Redis container"
	@echo "    make redis-docker-stop  Stop Redis container"
	@echo ""
	@echo "  Other"
	@echo "    make proto-gen          Regenerate protobuf code"
	@echo "    make lint               Run golangci-lint"
	@echo "    make vet                Run go vet"
	@echo "    make clean              Remove build artifacts and test cache"
	@echo ""
	@echo "  Variables (override with make VAR=value)"
	@echo "    REDIS_ADDR=$(REDIS_ADDR)"
	@echo "    DURATION=$(DURATION)"
	@echo "    INSTANCES=$(INSTANCES)"
	@echo "    KEYS=$(KEYS)"
	@echo "    LIMIT=$(LIMIT)"
	@echo ""