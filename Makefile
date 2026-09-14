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

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin/ proto/client/
