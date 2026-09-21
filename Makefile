GO := $(if $(wildcard .tools/go/bin/go),$(CURDIR)/.tools/go/bin/go,go)
GOFMT := $(if $(wildcard .tools/go/bin/gofmt),$(CURDIR)/.tools/go/bin/gofmt,gofmt)
export GOCACHE := $(CURDIR)/.local/go-build
export GOPATH := $(CURDIR)/.local/go
export GOTOOLCHAIN := local
export GO

.PHONY: build test lint smoke integration store-test tools generate protocol-test generate-check
build:
	$(GO) build -trimpath -o bin/dispatch ./cmd/dispatch
	$(GO) build -trimpath -o bin/dispatch-server ./cmd/dispatch-server
	cargo build --workspace --locked

test:
	$(GO) test -race ./...
	cargo test --workspace --locked
	sh scripts/test-protocol.sh

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
	cargo build -p dispatch-worker --example session_probe --locked
	$(GO) test -race -tags integration -count=1 ./internal/store ./internal/admission ./internal/api ./internal/workerapi ./cmd/dispatch-server

tools:
	@if test "$$($(CURDIR)/.tools/bin/protoc-gen-go --version 2>/dev/null)" != 'protoc-gen-go v1.36.11'; then GOBIN=$(CURDIR)/.tools/bin $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11; fi
	@if test "$$($(CURDIR)/.tools/bin/protoc-gen-go-grpc --version 2>/dev/null)" != 'protoc-gen-go-grpc 1.6.2'; then GOBIN=$(CURDIR)/.tools/bin $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2; fi

generate: tools
	sh scripts/generate-protocol.sh

protocol-test:
	sh scripts/test-protocol.sh

generate-check: tools
	sh scripts/check-generated.sh
