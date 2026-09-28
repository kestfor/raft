.PHONY: format test vet build proto tools up down logs

format:
	go tool golangci-lint fmt

lint:
	go tool golangci-lint run ./...

test:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -o bin/raftnode ./cmd/raftnode

proto:
	buf lint
	buf generate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f
