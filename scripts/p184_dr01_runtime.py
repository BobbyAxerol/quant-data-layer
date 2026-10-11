"""CONTROLLED_SIM authentication regression; NEVER closes a DR01 runtime cell.

Use --source-sha (40 hex) --image-digest (sha256:64 hex) --output FILE.
Placeholder provenance requires --development and FILE.rehearsal.jsonl.
Real provenance requires a fresh run, never copying or relabelling a rehearsal.
Starts only an ephemeral loopback HTTP server, not containers or shared services.
The actual DL app authenticates ephemeral RS256 keys against isolated manifests.
Bindings use the existing DL compiler for ONE BAR-1m test projection per venue.
The positive HTTP read is instrument METADATA, not market data. This is NOT the
full deployed r22 roster or all-feed runtime qualification; TS binding consumption,
warmup/data reads and deployed request-quality parity are not demonstrated.
Consumer isolation is not account authorization: those two cells stay open unless
--account-packet supplies the isolated TS Gateway probe inputs documented below.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
import stat
from decimal import Decimal
from pathlib import Path
import secrets
import re
import tempfile
import socket
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VENUES = ("BINANCE", "OKX")
CASES = ("BINDING_COMPILE", "WRONG_REALM_DENIED", "WRONG_REVISION_DENIED",
         "WRONG_ACCOUNT_DENIED")
CELLS = tuple(f"DR01.{case}@{venue}" for case in CASES for venue in VENUES) + (
    "DR01.NO_GLOBAL_GRANT@ANY",)
DUMMY_SHA = "0" * 40
DUMMY_IMAGE = "sha256:" + "0" * 64
MAX_RESPONSE = 65536


def result(cell, ok, evidence, *, observed=True):
    if cell not in CELLS or type(ok) is not bool or type(observed) is not bool:
        raise ValueError("known cell and strict boolean results required")
    if ok and not observed:
        raise ValueError("unobserved evidence cannot pass")
    return {"cell": cell, "level": "CONTROLLED_SIM", "ok": ok,
            "outcome": "OBSERVED" if observed else "NOT_OBSERVED",
            "evidence": evidence, "at_ms": time.time_ns() // 1_000_000}


def provenance(development, source_sha, image_digest):
    if type(development) is not bool:
        raise ValueError("development must be a strict boolean")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
        raise ValueError("source SHA and image digest must be format-valid")
    if not development and (len(set(source_sha)) == 1 or len(set(image_digest[7:])) == 1):
        raise ValueError("placeholder provenance requires --development")
    return {"schema": "p184.surface-evidence-provenance.v1",
            "source_sha": source_sha, "image_digest": image_digest,
            "development": development, "final_qualification": False,
            "test_provenance": True, "runner": "p184_dr01_runtime",
            "scope": "isolated DL HTTP authentication and binding compilation",
            "shipped_image_verified": False, "mtls_verified": False,
            "binding_scope": "ONE_BAR_1M_TEST_PROJECTION_PER_VENUE",
            "full_r22_roster_verified": False, "all_feeds_runtime_verified": False,
            "positive_http_scope": "INSTRUMENT_METADATA_ONLY",
            "ts_provenance": "caller_supplied_not_attested", "broker_account_covered": False}


def denied(observation, status, code):
    return (type(observation.get("status")) is int
            and observation["status"] == status and observation.get("code") == code)


def assess(observations):
    """Evaluate wire observations, not an authentication model."""
    rows = []
    for venue in VENUES:
        obs = observations[venue]
        baseline = obs["baseline"].get("status") == 200 and obs["baseline"].get("identity_matches") is True
        rows.append(result(f"DR01.BINDING_COMPILE@{venue}",
            baseline and obs["binding"].get("compiled") is True,
            {"binding": obs["binding"], "dl_manifest_http": obs["baseline"],
             "ts_process_consumption_observed": False,
             "full_r22_roster_verified": False, "market_data_read_observed": False,
             "binding_scope": "ONE_BAR_1M_TEST_PROJECTION_PER_VENUE"}))
        for case, key in (("WRONG_REALM_DENIED", "realm"), ("WRONG_REVISION_DENIED", "revision")):
            rows.append(result(f"DR01.{case}@{venue}", baseline and denied(obs[key], 401, "UNAUTHENTICATED"),
                {"baseline": obs["baseline"], "negative": obs[key]}))
        rows.append(result(f"DR01.WRONG_ACCOUNT_DENIED@{venue}", False,
            {"blocker": "BROKER_ACCOUNT_AUTHORIZATION_NOT_PRESENT_IN_DL",
             "consumer_boundary_observed": baseline and denied(obs["consumer"], 403, "CONSUMER_MISMATCH"),
             "key_subject_boundary_observed": baseline and denied(obs["principal"], 401, "UNAUTHENTICATED"),
             "consumer_negative": obs["consumer"], "principal_negative": obs["principal"],
             "broker_account_covered": False}, observed=False))
    rows.append(result("DR01.NO_GLOBAL_GRANT@ANY", all(
        observations[v]["baseline"].get("identity_matches") is True
        and observations[v]["baseline"].get("status") == 200
        and denied(observations[v]["ungranted"], 403, "PERMISSION_DENIED") for v in VENUES),
        {v: observations[v]["ungranted"] for v in VENUES}))
    return sorted(rows, key=lambda row: CELLS.index(row["cell"]))


def build_fixture(directory):
    """Reuse the existing manifest/token harness with newly generated secrets."""
    from dataclasses import replace
    from qdl.api_v2 import create_v2_app
    from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
    from qdl.domain.instrument import InstrumentRegistry
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.query import EntitlementPolicy, InstrumentQuery, MemoryMarketDataBackend, V2QueryService
    from qdl.security import DataPlaneIdentityService, DataPlaneSecurityConfig
    from tests.phase7_support import TEST_AUDIENCE, TEST_ISSUER, manifest_mapping
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from qdl.consumer.universal_release import ConsumerRouteBinding
    from qdl.consumer.realtime_route import requirement_key
    from scripts.phase115_render_consumer_route_binding import binding_from_stable_release

    template = binding_from_stable_release(ROOT / "config/v2/stable-v2-release-routing.yaml",
                                          ROOT, "trading-system.paper.stable")
    # Validate the entire compiled artifact before deriving disposable identities.
    template = ConsumerRouteBinding.from_canonical_mapping(template.canonical_mapping())
    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")

    registry, manifests, credentials, records = InstrumentRegistry(), [], {}, {}
    nonce = secrets.token_hex(8)
    for venue in VENUES:
        symbol = "ETHUSDT" if venue == "BINANCE" else "ETH-USDT-SWAP"
        product = next(p for p in template.products if p.venue == venue and p.native_symbol == symbol
                       and p.product_type == "PERPETUAL" and p.feed == "BAR" and p.interval == "1m")
        item = catalog.instrument_for(product.instrument_uid)
        registry.register(item, [])
        cid = f"p184-dr01-test.{nonce}.{venue.lower()}"
        subject = f"spiffe://qdl/paper/{cid}"
        mapping = manifest_mapping(consumer_id=cid, subject=subject,
            instrument_uid=item.instrument_uid, source_policy_id=product.source_policy_id)
        requirement = mapping["spec"]["requirements"][0]
        requirement["max_freshness_ms"] = product.max_freshness_ms
        requirement["require_final_bars"] = product.require_final_bars
        manifest = ConsumerManifestLoader.from_mapping(mapping)
        manifests.append(manifest)
        rid = hashlib.sha256(f"{cid}:{requirement_key(manifest.requirements[0])}".encode()).hexdigest()
        policy = next(p for p in template.request_policies if p.requirement_id == product.requirement_id)
        product = replace(product, consumer_id=cid, requirement_id=rid, execution_grade=False)
        policy = replace(policy, requirement_id=rid, event_recency_policy=None, max_session_liveness_ms=None)
        binding = replace(template, consumer_id=cid, products=(product,), request_policies=(policy,),
                          consumer_manifest_revision=1, binding_sha256=None)
        sealed = ConsumerRouteBinding.from_canonical_mapping(binding.canonical_mapping())
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        key_path = Path(directory) / (venue + ".key")
        with key_path.open("xb") as key_file:
            key_path.chmod(0o600)
            key_file.write(key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        credentials[venue] = {"cid": cid, "subject": subject, "key_path": key_path,
            "public_key": key.public_key().public_bytes(serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo), "policy": product.source_policy_id,
            "binding": {"compiled": True, "binding_sha256": sealed.binding_sha256,
                "manifest_sha256": manifest.manifest_sha256,
                "compiler": "binding_from_stable_release + ConsumerRouteBinding",
                "source_roster_products": len(template.products),
                "source_roster_venue_products": sum(p.venue == venue for p in template.products),
                "source_roster_binding_sha256": template.binding_sha256,
                "test_scope": "isolated one-product ALPHA projection", "venue": venue}}
        records[venue] = item
    config = DataPlaneSecurityConfig(environment="paper", issuer=TEST_ISSUER, audience=TEST_AUDIENCE,
        algorithms=("RS256",), keys_by_id={v: c["public_key"] for v, c in credentials.items()},
        subjects_by_key_id={v: c["subject"] for v, c in credentials.items()},
        environments_by_key_id={v: "paper" for v in VENUES})
    service = V2QueryService(instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(),
        entitlements=EntitlementPolicy(()))
    app = create_v2_app(service, identity_service=DataPlaneIdentityService(
        config, ConsumerManifestRegistry(tuple(manifests))))
    return app, credentials, records


@contextmanager
def isolated_server(app):
    import uvicorn

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical", access_log=False,
            lifespan="off", timeout_graceful_shutdown=2))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started:
                if not thread.is_alive() or time.monotonic() >= deadline:
                    raise RuntimeError("isolated HTTP server startup failed")
                time.sleep(.01)
            yield f"http://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            server.should_exit = True
            thread.join(5)
            if thread.is_alive():
                server.force_exit = True
                thread.join(3)
            if thread.is_alive():
                raise RuntimeError("isolated HTTP server cleanup failed")


def request_json(base_url, route, headers):
    from urllib.request import Request

    # No environment proxies, redirects or retry loops; this endpoint is ours.
    from urllib.request import HTTPRedirectHandler

    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = build_opener(ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(Request(base_url + route, headers=headers), timeout=3)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise ValueError("HTTP response bound exceeded")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("HTTP response must be an object")
        return response.code, payload, hashlib.sha256(raw).hexdigest()


def request(base_url, route, headers):
    status, payload, digest = request_json(base_url, route, headers)
    return {"status": status, "code": payload.get("code"),
            "instrument_uid": payload.get("instrument_uid"), "response_sha256": digest}


def _funds(rows):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 32:
        raise ValueError("nonempty bounded expected balances required")
    values = {}
    for row in rows:
        currency = row["currency"]
        if not isinstance(currency, str) or not re.fullmatch(r"[A-Z0-9]{1,20}", currency) or currency in values:
            raise ValueError("invalid or duplicate balance currency")
        amounts = []
        for field in ("total", "locked", "free"):
            raw = row[field]
            if not isinstance(raw, str) or len(raw) > 80:
                raise ValueError("balance values must be bounded decimal strings")
            amount = Decimal(raw)
            if not amount.is_finite() or amount < 0:
                raise ValueError("invalid balance amount")
            amounts.append(amount)
        # Exact decimal comparison below never passes through a float.
        values[currency] = tuple(amounts)
    return values


def _private_key(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve().is_relative_to(ROOT):
        raise ValueError("account key must be external and absolute")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("account key must be a mode-0600 regular file")
        raw = handle.read(4097)
    if not 1 <= len(raw) <= 4096:
        raise ValueError("account key size invalid")
    key = raw.decode("ascii").strip()
    if not key or any(ord(c) < 33 or ord(c) > 126 for c in key):
        raise ValueError("account key content invalid")
    return key


def load_account_packet(path, *, source_sha, image_digest):
    """Validate everything before requests; packet and keys never enter evidence.

    JSON schema p184.dr01-account-packet.v1: isolated=true, mode="paper",
    source_sha/image_digest equal CLI, gateway_url="http://127.0.0.1:PORT",
    bindings={"BINANCE":{"A":binding,"B":binding},"OKX":{...}}.
    Each binding has alpha_id, account_id, api_key_file (external absolute 0600),
    balances=[{"currency":"USDT","total":"1000.125","locked":"25.125","free":"975"}].
    Provision these four accounts and two active alphas in the isolated Gateway
    before running. This runner performs no setup. API GETs can consume isolated
    auth/rate counters; they do not place orders or mutate business records.
    """
    with Path(path).open("rb") as handle:
        raw = handle.read(MAX_RESPONSE + 1)
    if len(raw) > MAX_RESPONSE:
        raise ValueError("account packet too large")
    packet = json.loads(raw)
    if (packet.get("schema") != "p184.dr01-account-packet.v1"
            or packet.get("isolated") is not True or packet.get("mode") != "paper"
            or packet.get("source_sha") != source_sha or packet.get("image_digest") != image_digest):
        raise ValueError("isolated Paper packet and matching provenance required")
    url = urlsplit(packet["gateway_url"])
    if (url.scheme != "http" or url.hostname != "127.0.0.1" or not url.port
            or url.username or url.password or url.path not in ("", "/") or url.query or url.fragment
            or packet["gateway_url"].rstrip("/") != f"http://127.0.0.1:{url.port}"):
        raise ValueError("literal loopback HTTP origin with explicit port required")
    if set(packet["bindings"]) != set(VENUES):
        raise ValueError("exact two-venue account packet required")
    accounts = set()
    for venue in VENUES:
        pair = packet["bindings"][venue]
        if set(pair) != {"A", "B"}:
            raise ValueError("two isolated alpha/account bindings per venue required")
        for binding in pair.values():
            if set(binding) != {"alpha_id", "account_id", "api_key_file", "balances"}:
                raise ValueError("account binding fields invalid")
            for field in ("alpha_id", "account_id"):
                if not isinstance(binding[field], str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", binding[field]):
                    raise ValueError("account identity invalid")
            if binding["account_id"] in accounts:
                raise ValueError("account ids must be distinct")
            accounts.add(binding["account_id"])
            _funds(binding["balances"])
            binding["key"] = _private_key(binding["api_key_file"])
        if pair["A"]["alpha_id"] == pair["B"]["alpha_id"] or pair["A"]["key"] == pair["B"]["key"]:
            raise ValueError("independent alpha credentials required")
    for label in ("A", "B"):
        first, second = (packet["bindings"][v][label] for v in VENUES)
        if first["alpha_id"] != second["alpha_id"] or first["key"] != second["key"]:
            raise ValueError("each alpha must use the same identity/key across venues")
    packet["gateway_url"] = packet["gateway_url"].rstrip("/")
    packet["packet_sha256"] = hashlib.sha256(raw).hexdigest()
    return packet


def account_probe(packet, venue):
    """GET the actual Gateway boundary; no model or account JWT claim."""
    pair = packet["bindings"][venue]
    a, b = pair["A"], pair["B"]
    detail = {"boundary": "TS_GATEWAY_PAPER_ACCOUNT_ALPHA_SCOPE", "test_scope": "ISOLATED_TEST_ONLY",
              "packet_sha256": packet["packet_sha256"], "broker_authoritative": False, "checks": {},
              "bindings": {label: {"alpha_id": b["alpha_id"], "account_id": b["account_id"],
                  "expected_balances_sha256": hashlib.sha256(json.dumps(b["balances"],
                      sort_keys=True, separators=(",", ":")).encode()).hexdigest()} for label, b in pair.items()}}
    cases = (("a_to_a", a, a, a["key"]), ("b_to_b", b, b, b["key"]),
             ("a_to_b", a, b, a["key"]), ("invalid_key", a, a, secrets.token_urlsafe(48)))
    for name, principal, target, key in cases:
        params = {"alpha_id": principal["alpha_id"], "account_id": target["account_id"],
                  "mode": "paper", "venue": venue}
        route = f"/v1/accounts/{target['account_id']}/balances?" + urlencode(params)
        try:
            status, payload, digest = request_json(packet["gateway_url"], route, {"X-API-Key": key})
            if name in {"a_to_a", "b_to_b"}:
                rows = payload.get("balances")
                passed = (status == 200 and payload.get("status") == "OK"
                    and isinstance(rows, list) and type(payload.get("count")) is int
                    and payload["count"] == len(rows) and 1 <= len(rows) <= 32
                    and all(r.get("strategy_id") == principal["alpha_id"]
                        and r.get("account_id") == target["account_id"] and r.get("mode") == "paper"
                        and r.get("venue") == venue for r in rows)
                    and _funds(rows) == _funds(target["balances"]))
            else:
                reason = "ACCOUNT_ALPHA_SCOPE_MISMATCH" if name == "a_to_b" else "INVALID_API_KEY"
                passed = status == 403 and payload.get("status") == "REJECTED" and payload.get("reason") == reason
            detail["checks"][name] = {"ok": passed, "http_status": status, "response_sha256": digest,
                                     "outcome": "OBSERVED"}
            if name not in {"a_to_a", "b_to_b"}:
                detail["checks"][name]["expected_reason"] = reason
                detail["checks"][name]["reason_matches"] = payload.get("reason") == reason
        except Exception as error:
            detail["checks"][name] = {"ok": False, "outcome": "NOT_OBSERVED",
                                     "error_type": type(error).__name__}
    return result(f"DR01.WRONG_ACCOUNT_DENIED@{venue}",
        all(check["ok"] is True for check in detail["checks"].values()), detail,
        observed=all(check["outcome"] == "OBSERVED" for check in detail["checks"].values()))


def observe():
    with tempfile.TemporaryDirectory(prefix="p184-dr01-credentials-") as directory:
        return _observe(directory)


def _observe(directory):
    from tests.phase7_support import TEST_AUDIENCE, TEST_ISSUER
    from qdl_sdk.credentials import RotatingJwtCredentialProvider

    app, credentials, records = build_fixture(directory)
    observations = {}
    with isolated_server(app) as base_url:
        for venue in VENUES:
            cred = credentials[venue]
            route = f"/v2/instruments/{records[venue].instrument_uid}"
            def headers(**overrides):
                settings = dict(private_key_file=cred["key_path"], key_id=venue, algorithm="RS256",
                    issuer=TEST_ISSUER, audience=TEST_AUDIENCE, subject=cred["subject"], environment="paper",
                    roles=("market_data_reader", "historical_reader", "stream_consumer"),
                    consumer_manifest_revision=1, lifetime_seconds=300, refresh_before_seconds=30)
                settings.update(overrides)
                token = asyncio.run(RotatingJwtCredentialProvider(**settings).get_token())
                return {"Authorization": "Bearer " + token, "X-QDL-Consumer-ID": cred["cid"],
                        "X-QDL-Purpose": "INTERNAL_ALPHA"}
            baseline = request(base_url, route, headers())
            baseline["identity_matches"] = baseline["instrument_uid"] == records[venue].instrument_uid
            other = credentials["OKX" if venue == "BINANCE" else "BINANCE"]
            forbidden = f"/v2/market-data/{records[venue].instrument_uid}/snapshot?" + urlencode({
                "feed": "QUOTE", "consumer_grade": "ALPHA", "source_policy_id": cred["policy"]})
            observations[venue] = {"baseline": baseline, "binding": cred["binding"],
                "realm": request(base_url, route, headers(environment="live")),
                "revision": request(base_url, route, headers(consumer_manifest_revision=2)),
                "consumer": request(base_url, route, headers() | {"X-QDL-Consumer-ID": other["cid"]}),
                "principal": request(base_url, route, headers(subject=other["subject"]) | {
                    "X-QDL-Consumer-ID": other["cid"]}),
                "ungranted": request(base_url, forbidden, headers())}
    return observations


def run(output, *, development, source_sha, image_digest, account_packet=None):
    header = provenance(development, source_sha, image_digest)
    packet = None
    if account_packet is not None:
        try:
            packet = load_account_packet(account_packet, source_sha=source_sha, image_digest=image_digest)
        except Exception:
            raise ValueError("invalid isolated account packet or external key file") from None
        header["account_packet_sha256"] = packet["packet_sha256"]
        header["account_boundary_requested"] = "TS_GATEWAY_PAPER_ACCOUNT_ALPHA_SCOPE"
    if development and not Path(output).name.endswith(".rehearsal.jsonl"):
        raise ValueError("development output must end with .rehearsal.jsonl; never relabel as final")
    # Exclusive creation occurs before service startup. Never relabel/reuse a receipt.
    with Path(output).open("x", encoding="utf-8") as handle:
        handle.write(json.dumps({"provenance": header}, sort_keys=True) + "\n")
        handle.flush()
        try:
            rows = assess(observe())
        except Exception as error:
            # Exception messages can contain credentials; retain only the class.
            rows = [result(cell, False, {"blocker": "HTTP_RUN_NOT_COMPLETED",
                "error_type": type(error).__name__}, observed=False) for cell in CELLS]
        if packet is not None:
            accounts = {f"DR01.WRONG_ACCOUNT_DENIED@{v}": account_probe(packet, v) for v in VENUES}
            rows = [accounts.get(row["cell"], row) for row in rows]
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development", action="store_true")
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--account-packet", type=Path, help="isolated Paper Gateway account probe packet")
    args = parser.parse_args(argv)
    rows = run(**vars(args))
    print(json.dumps({"development": args.development, "final_qualification": False,
        "passed": sum(r["ok"] is True for r in rows), "total": len(rows)}))
    return 0 if all(r["ok"] is True for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
