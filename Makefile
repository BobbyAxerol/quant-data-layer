.PHONY: contract-check contract-generate phase2-benchmark phase2-redis-smoke phase2-test phase3-lease-smoke phase3-load-smoke phase3-real-provider-smoke phase3-rust-smoke phase3-test phase4-dnse-real-smoke phase4-history-test phase4-migration-smoke phase4-okx-real-smoke phase4-okx-test phase4-replay-test phase4-test phase4-vn-shadow-smoke phase45-build phase45-clean phase45-dependency-audit phase45-provider-smoke phase45-test phase5-api-test phase5-migration-smoke python-test rust-test

BUF_IMAGE ?= bufbuild/buf:1.50.0
RUST_IMAGE ?= rust:1.82-slim@sha256:1111c28d995d06a7863ba6cea3b3dcb87bebe65af8ec5517caaf2c8c26f38010
PHASE45_TEST_IMAGE ?= data-layer:phase45-test

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

phase4-migration-smoke:
	scripts/phase4_migration_smoke.sh

phase4-history-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python -m unittest -v tests.test_fund_phase4_history

phase4-replay-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python -m unittest -v tests.test_fund_phase4_replay

phase4-okx-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python -m unittest -v tests.test_fund_phase4_okx_history

phase4-okx-real-smoke:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python scripts/phase4_okx_real_smoke.py --output /app/upgrade/evidence/phase4-okx-real-history.json

phase4-dnse-real-smoke:
	docker compose exec -T data_layer python scripts/phase4_dnse_provider_smoke.py --date "$${QDL_DNSE_SMOKE_DATE:?set QDL_DNSE_SMOKE_DATE to a completed trading date}" --output upgrade/evidence/phase4-dnse-provider-coverage.json

phase4-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:phase4-test python -m unittest -v tests.test_fund_phase4_quality tests.test_fund_phase4_history tests.test_fund_phase4_replay tests.test_fund_phase4_okx_history

phase4-vn-shadow-smoke:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python scripts/phase4_vn_shadow_smoke.py --preload-root /app/data/preload --output /app/upgrade/evidence/phase4-vn-shadow-migration.json

phase45-build:
	docker build --provenance=false -t $(PHASE45_TEST_IMAGE) .

phase45-test: phase45-build
	docker run --rm -v "$(CURDIR):/app" -w /app $(PHASE45_TEST_IMAGE) python -m unittest -v tests.test_fund_phase45_readiness tests.test_fund_phase4_replay

phase45-dependency-audit: phase45-build
	docker run --rm $(PHASE45_TEST_IMAGE) sh -c 'python -m pip freeze --local > /tmp/qdl-runtime-requirements.txt && test ! -e /opt/venv/bin/poetry && python -m pip install --disable-pip-version-check --no-cache-dir "pip-audit>=2.9,<3" && pip-audit -r /tmp/qdl-runtime-requirements.txt --progress-spinner=off'

phase45-provider-smoke: phase45-build
	docker run --rm -v "$(CURDIR):/app" -w /app $(PHASE45_TEST_IMAGE) python scripts/phase45_websocket_smoke.py

phase45-clean:
	docker image rm $(PHASE45_TEST_IMAGE) 2>/dev/null || true

phase5-api-test:
	docker run --rm -v "$(CURDIR):/app" -w /app data-layer:v0.1.0 python -m unittest -v tests.test_fund_phase5_api tests.test_fund_phase5_consumer

phase5-migration-smoke:
	bash scripts/phase5_migration_smoke.sh
