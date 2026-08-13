.PHONY: contract-check contract-generate python-test rust-test

BUF_IMAGE ?= bufbuild/buf:1.50.0
RUST_IMAGE ?= rust:1.82-slim

contract-generate:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) generate

contract-check:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) format --diff --exit-code
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) lint
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) breaking --against baseline/qdl-v2-phase1.binpb
	$(MAKE) contract-generate
	git diff --exit-code -- generated

python-test:
	python -m unittest discover -s tests

rust-test:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace $(RUST_IMAGE) cargo test --workspace --locked

