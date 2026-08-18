.PHONY: contract-check contract-generate phase2-benchmark phase2-redis-smoke phase2-test phase3-lease-smoke phase3-load-smoke phase3-real-provider-smoke phase3-rust-smoke phase3-test phase4-dnse-real-smoke phase4-history-test phase4-migration-smoke phase4-okx-real-smoke phase4-okx-test phase4-replay-test phase4-test phase4-vn-shadow-smoke phase45-build phase45-clean phase45-dependency-audit phase45-provider-smoke phase45-test phase5-api-test phase5-build phase5-clean phase5-contract-check phase5-dependency-audit phase5-load phase5-migration-smoke phase5-real-provider-smoke phase5-test phase7-build phase7-clean phase7-contract-check phase7-migration-smoke phase7-test phase71-topology-test phase71-test phase72-test phase72-topology-test phase73-test phase73-certify phase80-test phase80-certify phase81-test phase81-certify phase82-test phase82-dnse-acquire phase82-certify phase83-test phase83-build phase83-authority phase83-release-capacity phase83-freeze phase90b-build phase90b-test phase90b-certify phase90b-clean phase90c-build phase90c-test phase90c-migration phase90c-certify phase90c-clean python-test rust-test

BUF_IMAGE ?= bufbuild/buf:1.50.0
RUST_IMAGE ?= rust:1.82-slim@sha256:1111c28d995d06a7863ba6cea3b3dcb87bebe65af8ec5517caaf2c8c26f38010
PHASE45_TEST_IMAGE ?= data-layer:phase45-test
PHASE5_TEST_IMAGE ?= data-layer:phase5-test
PHASE7_TEST_IMAGE ?= data-layer:phase7-test
PHASE8_RUST_IMAGE ?= qdl-phase8-rust:phase8-candidate
PHASE8_RELEASE ?= phase8-rust-realtime-core-v0.1.0-beta
PHASE8_GIT_SHA ?= $(shell git rev-parse HEAD)
PHASE90B_IMAGE ?= data-layer:phase90b-candidate
PHASE90B_RELEASE ?= phase90b-isolated-v2-beta
PHASE90B_GIT_SHA ?= $(shell git rev-parse HEAD)
PHASE90C_IMAGE ?= data-layer:phase90c-test
PHASE90C_RELEASE ?= phase90c-production-prerequisites
PHASE90C_GIT_SHA ?= $(shell git rev-parse HEAD)

contract-generate:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) generate

contract-check:
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) format --diff --exit-code
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) lint
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) breaking --against baseline/qdl-v2-phase1.binpb
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace/contracts $(BUF_IMAGE) breaking --against baseline/qdl-v2-phase7-beta.binpb
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

phase5-build:
	docker build --provenance=false -t $(PHASE5_TEST_IMAGE) .

phase5-contract-check:
	$(MAKE) contract-check
	docker run --rm -v "$(CURDIR):/app" -w /app $(PHASE5_TEST_IMAGE) python scripts/generate_phase5_openapi.py
	git diff --exit-code -- contracts/v2/openapi.snapshot.json

phase5-test: phase5-build
	docker run --rm -v "$(CURDIR):/app" -w /app $(PHASE5_TEST_IMAGE) python -m unittest -v tests.test_fund_phase5_api tests.test_fund_phase5_contracts tests.test_fund_phase5_consumer tests.test_fund_phase5_stream_sdk tests.test_fund_phase5_e2e tests.test_fund_phase5_load tests.test_fund_phase5_real_provider

phase5-load: phase5-build
	docker run --rm -v "$(CURDIR):/app" -w /app $(PHASE5_TEST_IMAGE) python scripts/phase5_api_replica_load.py --replicas 8 --requests 2000 --concurrency 100 --min-rps 250 --max-p99-ms 500

phase5-dependency-audit: phase5-build
	docker run --rm $(PHASE5_TEST_IMAGE) sh -c 'python -m pip freeze --local > /tmp/qdl-runtime-requirements.txt && python -m pip install --disable-pip-version-check --no-cache-dir "pip-audit>=2.9,<3" && pip-audit -r /tmp/qdl-runtime-requirements.txt --progress-spinner=off'

phase5-real-provider-smoke:
	docker run --rm -v "$(CURDIR):/app" -w /app --network host $(PHASE5_TEST_IMAGE) python scripts/phase5_real_provider_smoke.py --output upgrade/evidence/phase5-real-provider-smoke.json

phase5-clean:
	docker image rm $(PHASE5_TEST_IMAGE) 2>/dev/null || true

phase7-build:
	docker build --provenance=false -t $(PHASE7_TEST_IMAGE) .

phase7-contract-check:
	$(MAKE) contract-check
	docker run --rm --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m -v "$(CURDIR):/app:ro" -w /app $(PHASE7_TEST_IMAGE) python -m unittest -v tests.test_phase0_contract_golden tests.test_fund_phase5_contracts tests.test_fund_phase7_contract_security

phase7-migration-smoke:
	bash scripts/phase5_migration_smoke.sh

phase7-test: phase7-build
	docker run --rm --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m -v "$(CURDIR):/app:ro" -w /app $(PHASE7_TEST_IMAGE) python -m unittest -v tests.test_fund_phase7_contract_security tests.test_fund_phase5_api tests.test_fund_phase5_contracts tests.test_fund_phase5_consumer tests.test_fund_phase5_stream_sdk tests.test_fund_phase5_e2e tests.test_fund_phase5_load

phase71-test: phase7-build
	docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m $(PHASE7_TEST_IMAGE) python -m unittest -v tests.test_fund_phase71_beta_runtime tests.test_fund_phase7_contract_security

phase71-topology-test: phase7-build
	QDL_BETA_IMAGE="$$(docker image inspect $(PHASE7_TEST_IMAGE) --format '{{.Id}}')" \
	QDL_BETA_REDIS_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_INIT_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_CURSOR_KEYS_JSON='{"beta-k1":"phase71-ci-cursor-key-material-32-bytes"}' \
	QDL_BETA_JWT_KEYS_JSON='{"phase7-test":"phase7-test-secret-material-32bytes"}' \
	scripts/phase71_beta_topology_smoke.sh

phase72-test: phase7-build
	docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m $(PHASE7_TEST_IMAGE) python -m unittest -v tests.test_fund_phase72_consumer_canary tests.test_fund_phase71_beta_runtime tests.test_fund_phase7_contract_security

phase72-topology-test: phase7-build
	QDL_BETA_IMAGE="$$(docker image inspect $(PHASE7_TEST_IMAGE) --format '{{.Id}}')" \
	QDL_BETA_REDIS_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_INIT_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_CURSOR_KEYS_JSON='{"beta-k1":"phase72-ci-cursor-key-material-32-bytes"}' \
	QDL_BETA_JWT_KEYS_JSON='{"beta-jwt-k1":"phase72-first-jwt-key-material-32bytes","beta-jwt-k2":"phase72-second-jwt-key-material-32bytes"}' \
	QDL_BETA_INTERNAL_INGEST_SECRET='phase72-internal-ingest-secret-32bytes' \
	QDL_PHASE72_EVIDENCE_OUTPUT="$(CURDIR)/upgrade/evidence/phase72-topology-canary.json" \
	scripts/phase72_consumer_canary_smoke.sh

phase73-test: phase7-build
	docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m $(PHASE7_TEST_IMAGE) python -m unittest -v tests.test_fund_phase73_beta_decision tests.test_fund_phase72_consumer_canary tests.test_fund_phase71_beta_runtime tests.test_fund_phase7_contract_security

phase73-certify: phase7-build
	docker image inspect redis:7.2-alpine >/dev/null 2>&1 || docker pull redis:7.2-alpine
	QDL_BETA_IMAGE="$$(docker image inspect $(PHASE7_TEST_IMAGE) --format '{{.Id}}')" \
	QDL_BETA_REDIS_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_INIT_IMAGE="$$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
	QDL_BETA_CURSOR_KEYS_JSON='{"beta-k1":"phase73-ci-cursor-key-material-32-bytes"}' \
	QDL_BETA_JWT_KEYS_JSON='{"beta-jwt-k1":"phase73-first-jwt-key-material-32bytes","beta-jwt-k2":"phase73-second-jwt-key-material-32bytes"}' \
	QDL_BETA_INTERNAL_INGEST_SECRET='phase73-internal-ingest-secret-32bytes' \
	scripts/phase73_public_beta_certification.sh

phase80-test:
	python3 -m unittest -v tests.test_fund_phase80_broker_substrate
	QDL_PHASE8_CERT_DIR=/tmp docker compose -f docker-compose.phase8-kafka.yml config --quiet
	docker build --provenance=false -f Dockerfile.phase8-rust -t qdl-phase8-rust:test .

phase80-certify:
	scripts/phase80_broker_certification.py

phase81-test:
	docker run --rm --read-only --tmpfs /tmp:rw,nosuid,nodev,size=64m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m -v "$(CURDIR):/app:ro" -w /app data-layer:phase8-test python -m unittest -v tests.test_fund_phase81_raw_core tests.test_fund_phase2_pipeline tests.test_fund_phase3_binance

phase81-certify:
	scripts/phase81_core_certification.py

phase82-test:
	docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m -v "$(CURDIR):/app:ro" -w /app data-layer:phase8-test python -m unittest -v tests.test_fund_phase82_conformance tests.test_fund_phase81_raw_core tests.test_fund_phase2_pipeline tests.test_fund_phase3_binance tests.test_fund_phase3_okx

phase82-dnse-acquire:
	docker compose exec -T data_layer python scripts/phase82_dnse_acquire.py --date "$${QDL_DNSE_SMOKE_DATE:?set a completed trading date}" --output /app/target/phase82-dnse-authentic.json

phase82-certify:
	docker run --rm --user 0:0 -v "$(CURDIR):/app" -w /app data-layer:phase8-test python scripts/phase82_exact_frame_certification.py --live-seconds 180 --retain-per-venue 128 --repeat 200 --dnse-date "$${QDL_DNSE_SMOKE_DATE:?set a completed trading date}" --dnse-input /app/target/phase82-dnse-authentic.json

phase83-test:
	docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m --tmpfs /app/logs:rw,uid=10001,gid=10001,size=16m -v "$(CURDIR):/app:ro" -w /app data-layer:phase8-test python -m unittest -v tests.test_fund_phase83_release tests.test_fund_phase82_conformance tests.test_fund_phase6_release
	docker run --rm -v "$(CURDIR):/workspace" -w /workspace qdl-phase8-rust-builder:phase8 bash -c 'cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'

phase83-build:
	docker build --provenance=false -f Dockerfile.phase8-rust --build-arg QDL_GIT_SHA=$(PHASE8_GIT_SHA) --build-arg QDL_RELEASE=$(PHASE8_RELEASE) -t $(PHASE8_RUST_IMAGE) .

phase83-authority:
	python3 scripts/phase83_authority_certification.py --image $(PHASE8_RUST_IMAGE) --image-digest "$$(docker image inspect $(PHASE8_RUST_IMAGE) --format '{{.Id}}')"

phase83-release-capacity:
	python3 scripts/phase83_release_capacity.py --rust-replay target/qdl-parity-replay-release --candidate-image-digest "$$(docker image inspect $(PHASE8_RUST_IMAGE) --format '{{.Id}}')" --dnse-input target/phase82-dnse-authentic.json --dnse-date 2026-08-14

phase83-freeze:
	python3 scripts/phase83_freeze_candidate.py --release $(PHASE8_RELEASE) --git-sha $(PHASE8_GIT_SHA) --image $(PHASE8_RUST_IMAGE) --image-ref "qdl-phase8-rust@sha256:$$(docker image inspect $(PHASE8_RUST_IMAGE) --format '{{.Id}}' | sed 's/^sha256://')"

phase7-clean:
	docker image rm $(PHASE7_TEST_IMAGE) 2>/dev/null || true

phase90b-build:
	docker build --provenance=false --build-arg QDL_GIT_SHA=$(PHASE90B_GIT_SHA) --build-arg QDL_RELEASE=$(PHASE90B_RELEASE) -t $(PHASE90B_IMAGE) .

phase90b-test: phase90b-build
	docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges:true --pids-limit 256 --memory 768m --cpus 1.5 --user 10001:10001 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,uid=10001,gid=10001 --tmpfs /app/logs:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001 $(PHASE90B_IMAGE) python -m unittest -v tests.test_phase90b_isolated_beta tests.test_phase90a_runtime_correctness tests.test_fund_phase73_beta_decision tests.test_fund_phase72_consumer_canary tests.test_fund_phase71_beta_runtime tests.test_fund_phase7_contract_security tests.test_phase0_contract_golden tests.test_fund_phase5_contracts
	docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges:true --pids-limit 256 --memory 768m --cpus 1.5 --user 10001:10001 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,uid=10001,gid=10001 --tmpfs /app/logs:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001 $(PHASE90B_IMAGE) python -m unittest discover -s tests

phase90b-certify: phase90b-test
	docker image inspect redis:7.2-alpine >/dev/null 2>&1 || docker pull redis:7.2-alpine
	QDL_PHASE90B_IMAGE=$(PHASE90B_IMAGE) scripts/phase90b_isolated_beta_certification.sh

phase90b-clean:
	docker image rm $(PHASE90B_IMAGE) 2>/dev/null || true

phase90c-build:
	docker build --provenance=false --build-arg QDL_GIT_SHA=$(PHASE90C_GIT_SHA) --build-arg QDL_RELEASE=$(PHASE90C_RELEASE) -t $(PHASE90C_IMAGE) .

phase90c-test: phase90c-build
	docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges:true --pids-limit 256 --memory 768m --cpus 1.5 --user 10001:10001 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,uid=10001,gid=10001 --tmpfs /app/logs:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001 $(PHASE90C_IMAGE) python -m unittest -v tests.test_phase90c_prerequisites tests.test_phase90c_migration_contract tests.test_phase90b_isolated_beta tests.test_fund_phase80_broker_substrate tests.test_fund_phase83_release

phase90c-migration:
	scripts/phase90c_migration_smoke.sh

phase90c-certify: phase90c-test phase90c-migration
	python3 scripts/phase90c_prerequisite_certification.py --expect NO_GO_EXTERNAL
	sha256sum upgrade/evidence/phase90c-production-prerequisites.json upgrade/evidence/PHASE90C_PRODUCTION_PREREQUISITES_REPORT.md upgrade/evidence/phase90c-authority-migration.json > upgrade/evidence/phase90c-evidence.sha256

phase90c-clean:
	docker image rm $(PHASE90C_IMAGE) 2>/dev/null || true
