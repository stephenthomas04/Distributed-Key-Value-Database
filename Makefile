.PHONY: build test start stop benchmark generate fmt vet
ifeq ($(OS),Windows_NT)
EXE := .exe
endif
build:
	go build -o bin/server$(EXE) ./cmd/server
	go build -o bin/kvctl$(EXE) ./cmd/kvctl
	go build -o bin/benchmark$(EXE) ./cmd/benchmark
test:
	go test ./...
fmt:
	gofmt -w cmd internal api
vet:
	go vet ./...
start:
	docker compose up --build -d
stop:
	docker compose down
benchmark: build
	python scripts/benchmark.py
generate:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/kv.proto
