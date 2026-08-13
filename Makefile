.PHONY: contract-check contract-generate phase2-benchmark phase2-redis-smoke phase2-test phase3-lease-smoke phase3-load-smoke phase3-real-provider-smoke phase3-rust-smoke phase3-test python-test rust-test

BUF_IMAGE ?= bufbuild/buf:1.50.0
RUST_IMAGE ?= rust:1.82-slim@sha256:1111c28d995d06a7863ba6cea3b3dcb87bebe65af8ec5517caaf2c8c26f38010

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
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace $(RUST_IMAGE) sh -c 'cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked'

phase2-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python -m unittest -v tests.test_fund_phase2_transport tests.test_fund_phase2_pipeline tests.test_fund_phase2_simulator tests.test_fund_phase2_shadow_smoke

phase2-redis-smoke:
	QDL_TEST_IMAGE=data-layer:v0.1.0 scripts/phase2_redis_rebuild_smoke.sh

phase2-benchmark:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python scripts/phase2_benchmark.py --events 10000 --partitions 10 --payload-bytes 512 --batch-size 100 --consumer-groups 8 --min-throughput 500 --max-p99-ms 250 --max-disk-amplification 4

phase3-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:phase3-test python -m unittest -v tests.test_fund_phase3_control tests.test_fund_phase3_binance tests.test_fund_phase3_okx tests.test_fund_phase3_projection tests.test_fund_phase3_provenance tests.test_fund_phase3_extension

phase3-lease-smoke:
	scripts/phase3_lease_smoke.sh

phase3-rust-smoke:
	scripts/phase3_rust_binance_smoke.sh

phase3-real-provider-smoke:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:phase3-test python scripts/phase3_real_provider_smoke.py --output upgrade/evidence/phase3-real-provider-smoke.json --timeout-seconds 45

phase3-load-smoke:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:phase3-test python scripts/phase3_load_recovery.py --events 20000 --partitions 80 --output upgrade/evidence/phase3-load-recovery.json
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:phase3-test python scripts/phase3_sustained_load.py --events 5000 --partitions 80 --target-rate 500 --output upgrade/evidence/phase3-sustained-load.json
