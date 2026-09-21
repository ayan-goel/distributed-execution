GO := $(if $(wildcard .tools/go/bin/go),$(CURDIR)/.tools/go/bin/go,go)
GOFMT := $(if $(wildcard .tools/go/bin/gofmt),$(CURDIR)/.tools/go/bin/gofmt,gofmt)
export GOCACHE := $(CURDIR)/.local/go-build
export GOPATH := $(CURDIR)/.local/go
export GOTOOLCHAIN := local

.PHONY: build test lint smoke integration store-test
build:
	$(GO) build -trimpath -o bin/dispatch ./cmd/dispatch
	$(GO) build -trimpath -o bin/dispatch-server ./cmd/dispatch-server
	cargo build --workspace --locked

test:
	$(GO) test -race ./...
	cargo test --workspace --locked

lint:
	test -z "$$($(GOFMT) -l $$(find cmd internal migrations -name '*.go' 2>/dev/null))"
	$(GO) vet ./...
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings

smoke:
	sh scripts/smoke.sh

integration:
	sh scripts/test-schema.sh
	sh scripts/test-store.sh

store-test:
	$(GO) test -race -tags integration -count=1 ./internal/store
