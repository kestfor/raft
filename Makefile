.DEFAULT_GOAL := help

GO ?= go
PYTHON ?= python3
VENV ?= .venv
CLUSTER_STATE ?= $(or $(TEST_CLUSTER_STATE),.artifacts/test-cluster.json)
CONFIG ?= configs/raftnode.yaml
ENDPOINT ?=
NODE_ARGS ?=
CTL_ARGS ?= status
BENCH_ARGS ?=
PYTEST_ARGS ?=
GO_TEST_ARGS ?=
LOG_LINES ?= 50
LOG_ARGS ?=

.PHONY: help setup build build-node build-ctl build-bench node format lint vet test check \
        python-setup cluster-up cluster-status cluster-ps cluster-endpoint cluster-logs cluster-down cluster-restart \
        acceptance acceptance-all ctl bench docker-up docker-status docker-logs docker-down \
        proto proto-lint clean up down logs test-cluster-up test-cluster-down

help: ## Show commands and common options
	@awk 'BEGIN {FS = ":.*## "} /^##@/ {printf "\n%s\n", substr($$0, 5)} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-21s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nOptions: CONFIG=... CLUSTER_STATE=... ENDPOINT=...\n         CTL_ARGS="..." BENCH_ARGS="..." PYTEST_ARGS="..." GO_TEST_ARGS="..."\n'

##@ Setup and build
setup: build python-setup ## Build all binaries and prepare Python tests

build: build-node build-ctl build-bench ## Build raftnode, raftctl and raftbench

build-node: ## Build bin/raftnode
	@$(GO) build -o bin/raftnode ./cmd/raftnode

build-ctl: ## Build bin/raftctl
	@$(GO) build -o bin/raftctl ./cmd/raftctl

build-bench: ## Build bin/raftbench
	@$(GO) build -o bin/raftbench ./cmd/raftbench

python-setup: $(VENV)/.requirements-installed ## Create venv and install pinned pytest dependencies

$(VENV)/.requirements-installed: tests/requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install -r tests/requirements.txt
	touch $@

##@ Local nodes and test cluster
node: build-node ## Run one node in foreground (CONFIG=..., NODE_ARGS=...)
	bin/raftnode --config "$(CONFIG)" $(NODE_ARGS)

cluster-up: build-node build-ctl ## Start 3 managed test nodes with separate WALs
	$(PYTHON) -m tests.acceptance.cluster up --state "$(CLUSTER_STATE)"

cluster-status: ## Show managed node addresses, roles, PIDs and log progress
	@$(PYTHON) -m tests.acceptance.cluster status --state "$(CLUSTER_STATE)"

cluster-ps: ## Find this user's running Raft nodes and cluster controllers
	@$(PYTHON) -m tests.acceptance.cluster ps

cluster-endpoint: ## Print the current managed leader's REST URL
	@$(PYTHON) -m tests.acceptance.cluster endpoint --state "$(CLUSTER_STATE)"

cluster-logs: ## Show node logs (LOG_LINES=50, LOG_ARGS=--follow)
	@$(PYTHON) -m tests.acceptance.cluster logs --state "$(CLUSTER_STATE)" --lines $(LOG_LINES) $(LOG_ARGS)

cluster-down: ## Stop the selected cluster; retain WALs and logs
	$(PYTHON) -m tests.acceptance.cluster down --state "$(CLUSTER_STATE)"

cluster-restart: build-node ## Restart managed nodes with the same WALs and configs
	$(PYTHON) -m tests.acceptance.cluster restart --state "$(CLUSTER_STATE)"

##@ Clients and performance
ctl: build-ctl ## Run raftctl (CTL_ARGS="get key"); defaults to managed leader
	@set -e; \
	endpoint="$(ENDPOINT)"; \
	if [ -z "$$endpoint" ]; then endpoint="$$($(PYTHON) -m tests.acceptance.cluster endpoint --state "$(CLUSTER_STATE)")"; fi; \
	bin/raftctl --endpoint "$$endpoint" $(CTL_ARGS)

bench: build-bench ## Run raftbench (BENCH_ARGS="-t put -n 100000 -c 512")
	@set -e; \
	endpoint="$(ENDPOINT)"; \
	if [ -z "$$endpoint" ]; then endpoint="$$($(PYTHON) -m tests.acceptance.cluster endpoint --state "$(CLUSTER_STATE)")"; fi; \
	bin/raftbench --endpoint "$$endpoint" $(BENCH_ARGS)

##@ Validation
format: ## Format Go sources
	$(GO) tool golangci-lint fmt

lint: ## Run Go linters
	$(GO) tool golangci-lint run ./...

vet: ## Run go vet
	$(GO) vet ./...

test: ## Run Go tests with race detector (GO_TEST_ARGS="-run ...")
	$(GO) test -race $(GO_TEST_ARGS) ./...

check: lint vet test ## Run all Go checks

acceptance: build-ctl python-setup ## Run pytest against the managed cluster; leave it running
	mkdir -p .artifacts
	$(VENV)/bin/python -m pytest tests/acceptance --cluster-manifest "$(CLUSTER_STATE)" --html=.artifacts/acceptance.html --self-contained-html --junitxml=.artifacts/acceptance.xml $(PYTEST_ARGS)

acceptance-all: build-node build-ctl python-setup ## Start a fresh cluster, run pytest, always stop it
	mkdir -p .artifacts
	$(VENV)/bin/python -m pytest tests/acceptance --html=.artifacts/acceptance.html --self-contained-html --junitxml=.artifacts/acceptance.xml $(PYTEST_ARGS)

##@ Docker Compose

docker-up: ## Build and start the Compose cluster
	docker compose up --build -d

docker-status: ## Show Compose containers
	docker compose ps

docker-logs: ## Follow Compose logs
	docker compose logs -f

docker-down: ## Stop Compose cluster, retaining data volumes
	docker compose down

##@ Contracts and generated files
proto-lint: ## Validate protobuf with buf
	buf lint

proto: proto-lint ## Regenerate protobuf Go code
	buf generate

clean: ## Remove built binaries and pytest cache (keep data, reports and venv)
	rm -rf bin .pytest_cache

# Compatibility aliases. Prefer explicit cluster-* or docker-* names.
up: docker-up
down: docker-down
logs: docker-logs
test-cluster-up: cluster-up
test-cluster-down: cluster-down
