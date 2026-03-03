.PHONY: build test test-unit test-integration run clean

build:
	go build -o bin/processor ./cmd/processor
	go build -o bin/generator ./cmd/generator

test: test-unit test-integration

test-unit:
	go test -short -race ./...

test-integration:
	go test -v -race ./tests/integration/ -timeout 180s

run:
	cd deployments && docker compose up --build

clean:
	cd deployments && docker compose down -v
	rm -rf bin/