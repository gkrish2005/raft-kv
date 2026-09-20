.PHONY: all build test test-race proto clean vet

all: proto build test

proto:
	@mkdir -p proto/client/v1 proto/raft/v1
	protoc --proto_path=. \
		--go_out=. --go_opt=module=raftkv \
		--go-grpc_out=. --go-grpc_opt=module=raftkv \
		proto/client.proto proto/raft.proto

build: proto
	@mkdir -p bin
	go build -o bin/raftkv-node ./cmd/raftkv-node
	go build -o bin/raftkv-cli ./cmd/raftkv-cli
	go build -o bin/raftkv-chaos ./cmd/raftkv-chaos
	go build -o bin/ai-eval ./cmd/ai-eval

ai-eval: build
	@./bin/ai-eval --mode=$(or $(MODE),recorded) --out=docs/eval_report.md

ai-eval-fixtures: build
	@./bin/ai-eval --generate

test:
	go test ./...

test-race:
	go test -race ./...

test-chaos:
	go test -v -race -timeout=10m ./internal/chaos/...

soak-chaos: build
	./bin/raftkv-chaos -duration=30m -seed=1337

vet:
	go vet ./...

clean:
	rm -rf bin/ proto/client/
