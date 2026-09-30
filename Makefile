CARGO ?= cargo
MSRV := $(shell grep -m1 '^rust-version' Cargo.toml | cut -d '"' -f2)

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check lint test doc features msrv deny bench ci tools clean

help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

fmt: ## Format the code
	$(CARGO) fmt --all

fmt-check: ## Check formatting
	$(CARGO) fmt --all --check

lint: ## Run clippy on all targets with all features
	$(CARGO) clippy --all-targets --all-features -- -D warnings

test: ## Run the tests with all features
	$(CARGO) test --all-features

doc: ## Build the documentation, failing on warnings
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --no-deps --all-features

features: ## Run clippy on every feature combination (requires cargo-hack)
	$(CARGO) hack clippy --feature-powerset --all-targets -- -D warnings

msrv: ## Check the build on the minimum supported Rust version
	$(CARGO) +$(MSRV) check --all-features

deny: ## Check dependencies for advisories, licenses, and sources (requires cargo-deny)
	$(CARGO) deny check

bench: ## Run the benchmarks; reports are written to target/criterion
	$(CARGO) bench

ci: fmt-check lint test doc features deny ## Run the CI checks, except msrv

tools: ## Install cargo-hack and cargo-deny
	$(CARGO) install --locked cargo-hack cargo-deny

clean: ## Remove build artifacts
	$(CARGO) clean
