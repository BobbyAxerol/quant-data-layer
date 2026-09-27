#!/usr/bin/env python3
"""Manifest-complete V2 read benchmark, launched in disposable consumer containers.

Host requires only Python and Docker. Client dependencies come from the existing
release image. See docs/runbooks/consumer-endpoint-benchmark.md. No order calls,
runtime configuration writes, image builds, provider-direct reads or fallback.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
from urllib.parse import urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[1]
OPERATIONS = {
    "snapshot": ("GET", "/v2/market-data/{instrument_uid}/snapshot", "snapshot:read"),
    "warmup": ("GET", "/v2/market-data/{instrument_uid}/warmup", "history:read"),
    "history": ("GET", "/v2/market-data/{instrument_uid}/history", "history:read"),
    "warmup_batch": ("POST", "/v2/market-data/warmup:batch", "history:read"),
    "reference_batch": ("POST", "/v2/market-data/reference:batch", "history:read"),
    "feed_status": ("GET", "/v2/feeds/{instrument_uid}/status", "status:read"),
    "instruments": ("GET", "/v2/instruments", "instruments:read"),
    "instrument": ("GET", "/v2/instruments/{identity}", "instruments:read"),
    "readiness": ("GET", "/v2/system/readiness", "status:read"),
    "readiness_check": ("POST", "/v2/system/readiness:check", "status:read"),
    "gaps": ("GET", "/v2/data-quality/gaps", "quality:read"),
    "stream": ("gRPC", "qdl.query.v2.QueryStream/Subscribe", "stream:read"),
}
DEFAULTS = {"rounds": 3, "requests_per_second": 1, "batch_size": 8,
            "timeout_seconds": 30, "max_seconds": 1800, "stream_seconds": 0}
SAFETY_BLOCKED = {
    "gaps": "GLOBAL_SPOOL_SCAN_QUERY_OOM_20260922; repair/verify server before enabling this diagnostic",
}


def validate_profile(raw):
    p = dict(raw)
    if set(p) - {"image", "network", "queries", "stream_targets", "consumers", "limits", "operations"}:
        raise ValueError("unknown profile fields; credentials must be file references")
    if not p.get("image") or not re.fullmatch(r"[A-Za-z0-9_.-]+", p.get("network", "")):
        raise ValueError("existing image and Docker network required")
    if not 1 <= len(p.get("queries", [])) <= 4:
        raise ValueError("one to four query replicas required")
    for url in p["queries"]:
        u = urlsplit(url)
        if u.scheme != "https" or not u.hostname or u.username or u.password or u.query or u.fragment or u.path not in ("", "/"):
            raise ValueError("queries must be HTTPS origins without credentials")
    if not p.get("stream_targets") or any(not re.fullmatch(r"[A-Za-z0-9_.-]+:[0-9]+", x) for x in p["stream_targets"]):
        raise ValueError("explicit stream failover targets required")
    limits = {**DEFAULTS, **p.get("limits", {})}
    bounds = {"rounds": (1, 100), "requests_per_second": (.1, 5), "batch_size": (1, 50),
              "timeout_seconds": (1, 90), "max_seconds": (10, 14400), "stream_seconds": (0, 60)}
    if set(limits) != set(bounds):
        raise ValueError("unknown benchmark limit")
    for k, (lo, hi) in bounds.items():
        v = limits[k]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi:
            raise ValueError(f"invalid {k}")
    if any(not isinstance(limits[k], int) for k in ("rounds", "batch_size")):
        raise ValueError("rounds and batch_size must be integers")
    p["limits"] = limits
    if "operations" in p and (not p["operations"] or not isinstance(p["operations"], list) or set(p["operations"]) - set(OPERATIONS)):
        raise ValueError("operations must be an explicit non-empty subset; omitted means all")
    ids = set()
    if not p.get("consumers"):
        raise ValueError("select at least one consumer")
    for c in p["consumers"]:
        if set(c) != {"id", "tls", "jwt"} or c["id"] in ids:
            raise ValueError("consumer fields/identity invalid")
        ids.add(c["id"])
        if set(c["tls"]) != {"ca_file", "cert_file", "key_file"}:
            raise ValueError("exact TLS file paths required")
        if set(c["jwt"]) != {"private_key_file", "key_id", "issuer", "audience", "roles"}:
            raise ValueError("JWT file reference and public metadata required")
        if not c["jwt"]["roles"] or not isinstance(c["jwt"]["roles"], list):
            raise ValueError("JWT roles must be an explicit list")
        for path in [*c["tls"].values(), c["jwt"]["private_key_file"]]:
            if not isinstance(path, str) or not Path(path).is_absolute() or "," in path:
                raise ValueError("credential file paths must be absolute")
    return p


def endpoint_coverage(snapshot):
    public = {(m.upper(), path) for path, item in snapshot["paths"].items()
              for m in item if m in ("get", "post", "put", "patch", "delete")}
    covered = {(m, path) for m, path, _ in OPERATIONS.values() if m != "gRPC"}
    if public != covered:
        raise ValueError(f"OpenAPI benchmark coverage drift: {sorted(public ^ covered)}")
    return sorted(public)


def load_scope(consumer_id):
    from qdl.consumer import StableReleaseRoutePlan, requirement_key
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_deployment import StableAcquisitionPlan
    from qdl.certification.phase103_consumer_acceptance import build_manifest_acceptance_scope

    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition = StableAcquisitionPlan.load(ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog)
    release = StableReleaseRoutePlan.load(ROOT / "config/v2/stable-v2-release-routing.yaml", manifest_root=ROOT)
    selected = next(c for c in release.consumers if c.consumer_id == consumer_id)
    if not any(x.route == "V2_PRIMARY" for x in selected.products):
        raise ValueError("selected consumer has no released V2 products; legacy remains excluded")
    scope = build_manifest_acceptance_scope(
        (selected.manifest_path,), catalog=catalog, acquisition=acquisition,
        expected_consumer_ids=frozenset({consumer_id}), schema="qdl.phase105.consumer-acceptance-scope.v1")
    actual = {requirement_key(x.requirement) for x in scope.products}
    if actual != {x.requirement_key for x in selected.products if x.route == "V2_PRIMARY"}:
        raise ValueError("benchmark products differ from released V2 routes")
    return selected.manifest, scope


def cases_for(products, batch_size):
    """Every product has a singleton read; batches are additional wall timings."""
    cases = [(x, ()) for x in ("instruments", "readiness", "gaps")]
    ordinary, references, bars, instruments = [], [], [], {}
    for p in products:
        instruments.setdefault(p.instrument_uid, p)
        if p.delivery.value == "ON_DEMAND":
            references.append(p)
            cases.append(("reference_batch", (p,)))
        else:
            ordinary.append(p)
            cases.extend((x, (p,)) for x in ("snapshot", "feed_status"))
            if p.feed.value == "BAR" and p.requirement.warmup_limit:
                bars.append(p)
                cases.extend((x, (p,)) for x in ("warmup", "history"))
            if p.delivery.value == "DURABLE":
                cases.append(("stream", (p,)))
    cases.extend(("instrument", (p,)) for p in instruments.values())
    # Keep grades separate: public batch contracts reject mixed-grade requests.
    for name, values in (("warmup_batch", bars), ("reference_batch", references), ("readiness_check", ordinary)):
        for grade in sorted({p.requirement.consumer_grade.value for p in values}):
            selected = [p for p in values if p.requirement.consumer_grade.value == grade]
            for i in range(0, len(selected), batch_size):
                chunk = tuple(selected[i:i + batch_size])
                if name != "reference_batch" or len(chunk) > 1:
                    cases.append((name, chunk))
    return cases


def percentiles(samples):
    if not samples:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    ordered = sorted(samples)
    return {"n": len(samples), "p50_ms": round(statistics.median(ordered), 3),
            "p95_ms": round(ordered[math.ceil(.95 * len(ordered)) - 1], 3),
            "p99_ms": round(ordered[math.ceil(.99 * len(ordered)) - 1], 3) if len(ordered) >= 100 else None,
            "max_ms": round(ordered[-1], 3)}


def diagnostics(payload):
    """Only typed identity/quality; never retain payload, JWT or cursor bytes."""
    found = []
    if not isinstance(payload, dict):
        return found
    items = payload.get("results", [payload])
    for item in items[:100]:
        data = item.get("data")
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            data = data["data"][-1:]
        if isinstance(data, list):
            data = data[-1] if data else {}
        view = data if isinstance(data, dict) else item
        quality = view.get("quality", item.get("quality", {})) or {}
        q = {k: quality[k] for k in (
            "state", "freshness_ms", "event_recency_state", "provider_session_state",
            "provider_session_liveness_ms", "gap_open", "complete", "execution_eligible", "flags", "policy_id") if k in quality}
        problem = item.get("problem") or {}
        row = {"instrument_uid": view.get("instrument_uid", item.get("instrument_uid")),
               "feed": view.get("feed"), "interval": view.get("interval"),
               "problem_code": problem.get("code", item.get("code")), "quality": q}
        if q:
            row["quality_sha256"] = hashlib.sha256(json.dumps(q, sort_keys=True).encode()).hexdigest()
        if isinstance(view.get("observed_at_ns"), int):
            row["event_age_at_client_ms"] = round((time.time_ns() - view["observed_at_ns"]) / 1e6, 3)
        found.append(row)
    return found


class NoEvent(Exception):
    pass


async def measure(call, validate, timeout_seconds):
    started = time.perf_counter()
    result = {"status": "FAIL", "response_ms": None, "usable_ms": None}
    payload = None
    try:
        payload = await asyncio.wait_for(call(), timeout_seconds)
        result["response_ms"] = (time.perf_counter() - started) * 1000
        validate(payload)
        result.update(status="PASS", usable_ms=(time.perf_counter() - started) * 1000)
    except NoEvent:
        result.update(status="NO_EVENT", error_code="NO_EVENT")
    except Exception as exc:
        code = getattr(exc, "code", type(exc).__name__)
        # Exception text may contain URLs, credentials or raw provider payloads.
        result["error_code"] = str(code) if re.fullmatch(r"[A-Za-z0-9_]{1,80}", str(code)) else type(exc).__name__
    result["elapsed_ms"] = (time.perf_counter() - started) * 1000
    result["diagnostics"] = diagnostics(payload)
    if isinstance(payload, dict):
        result["availability"] = {k: payload[k] for k in ("status", "ready", "authority", "coverage", "count", "partial", "error_count") if k in payload}
        if "next_cursor" in payload:
            result["availability"]["catalog_complete_in_this_page"] = payload["next_cursor"] is None
        if payload.get("schema") == "qdl.data-quality.gaps.v2":
            result["availability"]["open_gap_count"] = len(payload["items"])
    return result


def reference_product(p):
    from qdl.certification.reference_l2_acceptance import ReferenceAcceptanceProduct, reference_request_for_requirement
    return ReferenceAcceptanceProduct(
        **{k: getattr(p, k) for k in ("consumer_id", "consumer_subject", "manifest_revision", "manifest_sha256",
                                     "instrument_uid", "instrument_id", "venue", "market", "native_symbol", "requirement")},
        sdk_requirement=reference_request_for_requirement(p.requirement, now_ns=time.time_ns()))


def validate_operation(op, products, payload, refs=()):
    from qdl_sdk import models as m
    from qdl_sdk.client import AsyncDataLayerClient, _validate_query_payload, _validate_feed_status_payload
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl.certification.reference_l2_acceptance import reference_evidence

    def read(p, raw, warmup):
        response = _validate_query_payload(sdk_requirement(p), raw, warmup=warmup)
        views = response.data if warmup else [response.data]
        if not views:
            raise ValueError("EMPTY_DATA")
        for i, view in enumerate(views):
            validate_product_view(p, view, require_current_quality=i == len(views) - 1)

    if op in ("snapshot", "warmup", "history"):
        read(products[0], payload, op != "snapshot")
    elif op == "feed_status":
        _validate_feed_status_payload(sdk_requirement(products[0]), payload)
    elif op == "warmup_batch":
        response = AsyncDataLayerClient._validate_batch_chunk(tuple(sdk_requirement(p) for p in products), payload)
        if response.partial:
            raise ValueError("PARTIAL_RESULT")
        for p, item in zip(products, response.results, strict=True):
            read(p, item.data.model_dump(mode="json", by_alias=True), True)
    elif op == "reference_batch":
        response = AsyncDataLayerClient._validate_reference_batch_chunk(tuple(p.sdk_requirement for p in refs), payload)
        if response.partial:
            raise ValueError("PARTIAL_RESULT")
        for p, item in zip(refs, response.results, strict=True):
            reference_evidence(p, item, observed_at_ns=time.time_ns())
    elif op == "instrument":
        value = m.InstrumentResponse.model_validate(payload)
        p = products[0]
        if (value.instrument_uid, value.instrument_id, value.venue, value.native_symbol) != (p.instrument_uid, p.instrument_id, p.venue, p.native_symbol):
            raise ValueError("IDENTITY_MISMATCH")
    elif op == "instruments":
        m.InstrumentPageResponse.model_validate(payload)
    elif op == "readiness":
        m.SystemReadinessSummary.model_validate(payload)
    elif op == "readiness_check":
        value = m.ReadinessResponse.model_validate(payload)
        if [x.instrument_uid for x in value.results] != [p.instrument_uid for p in products]:
            raise ValueError("IDENTITY_MISMATCH")
    elif op == "gaps":
        m.GapListResponse.model_validate(payload)
    elif op == "stream":
        validate_product_view(products[0], m.MarketDataView.model_validate(payload["data"]))
    else:
        raise ValueError("UNCOVERED_OPERATION")


async def invoke(op, products, query, client, manifest, stream_seconds):
    from qdl_sdk.models import Grade, StreamEvent
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    cid = manifest.consumer_id
    grade = Grade((products[0].requirement if products else manifest.requirements[0]).consumer_grade.value)
    reqs = tuple(sdk_requirement(p) for p in products) if op != "reference_batch" else ()
    if op in ("snapshot", "warmup", "feed_status"):
        return await getattr(query, op)(reqs[0], consumer_id=cid)
    if op == "warmup_batch":
        return await query.warmup_batch(reqs, consumer_id=cid, require_all=True)
    if op == "instrument":
        return await query.instrument(products[0].instrument_uid, consumer_id=cid, consumer_grade=grade)
    if op == "instruments":
        # One page latency, not a claim to have enumerated every exchange asset.
        return await query.instruments(consumer_id=cid, consumer_grade=grade, limit=500)
    if op == "stream":
        async with client.warmup_then_stream(reqs[0]) as session:
            end = time.monotonic() + stream_seconds
            while time.monotonic() < end:
                try:
                    event = await asyncio.wait_for(session.__anext__(), max(.001, end - time.monotonic()))
                except asyncio.TimeoutError as exc:
                    raise NoEvent() from exc
                if isinstance(event, StreamEvent):
                    from qdl_sdk.projection import market_data_view_from_stream
                    view = market_data_view_from_stream(event, template=session.warmup.data[-1], requirement=reqs[0])
                    from qdl.certification.phase103_consumer_acceptance import validate_product_view
                    validate_product_view(products[0], view)
                    session.acknowledge(event)
                    return {"data": view.model_dump(mode="json"), "signed_cursor_acknowledged": True}
            raise NoEvent()
    path = OPERATIONS[op][1]
    headers = await query._identity_headers(grade, cid)
    kwargs = {"headers": headers}
    if op == "history":
        path = path.replace("{instrument_uid}", reqs[0].instrument_uid)
        kwargs["params"] = reqs[0].query_params()
    elif op == "readiness_check":
        kwargs["json"] = {"consumer_id": cid, "require_all": True, "requirements": [r.to_mapping() for r in reqs]}
    response = await query._read_request(OPERATIONS[op][0].lower(), path, **kwargs)
    return query._decode(response)


async def run_client(profile, inventory=False):
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    from qdl_sdk.tls import WorkloadTlsConfig
    from qdl_sdk.transport import RestQueryTransport, GrpcStreamTransport
    consumer = profile["consumers"][0]
    manifest, scope = load_scope(consumer["id"])
    endpoint_coverage(json.loads((ROOT / "contracts/v2/openapi.snapshot.json").read_text()))
    limits = profile["limits"]
    cases = cases_for(scope.products, min(limits["batch_size"], manifest.quotas.max_batch_items))
    interval = 1 / min(limits["requests_per_second"], manifest.quotas.requests_per_minute / 600)
    report = {"schema": "qdl.consumer-endpoint-benchmark.v1", "consumer_id": manifest.consumer_id,
              "manifest_revision": manifest.manifest_revision, "scope": scope.evidence(),
              "limits": limits, "effective_request_spacing_seconds": interval,
              "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "public_operations": OPERATIONS, "results": [],
              "exclusions": ["V1/legacy endpoints", "internal producer/admin endpoints", "orders", "provider-direct", "fallback"],
              "network_model": "separate container on configured Docker network; not remote-region latency",
              "timing_contract": "response includes HTTP decode; usable includes SDK and product validation; no strategy computation",
              "percentile_warning": "small-sample p95 descriptive only; p99 withheld below 100 usable samples"}
    report["safety_blocked_operations"] = SAFETY_BLOCKED
    if inventory:
        report.update(status="INVENTORY_ONLY", planned_cases_per_replica=len(cases))
        return report
    tls = WorkloadTlsConfig(consumer["tls"]["ca_file"], consumer["tls"]["cert_file"], consumer["tls"]["key_file"])
    credential = RotatingJwtCredentialProvider(
        **consumer["jwt"], algorithm="RS256", subject=manifest.subject,
        environment=manifest.environment, consumer_manifest_revision=manifest.manifest_revision)
    deadline = time.monotonic() + limits["max_seconds"]
    next_request = 0.0
    for replica in profile["queries"]:
        query = RestQueryTransport(replica, tls=tls, credential_provider=credential, timeout_seconds=limits["timeout_seconds"])
        stream = GrpcStreamTransport(profile["stream_targets"], tls=tls, credential_provider=credential)
        client = AsyncDataLayerClient(query_transport=query, stream_transport=stream, consumer_id=manifest.consumer_id,
                                     max_reconnect_attempts=0, max_buffer_events=min(100, manifest.quotas.max_buffer_events))
        first_request = True
        try:
            for op, products in cases:
                row = {"replica": replica, "operation": op, "endpoint": OPERATIONS[op][1],
                       "products": [p.evidence() for p in products], "samples": [],
                       "latency_unit": "batch_wall" if len(products) > 1 else "request",
                       "meaning": "diagnostic_validated" if op in ("feed_status", "readiness", "readiness_check", "gaps") else "data_validated"}
                report["results"].append(row)
                if op not in profile.get("operations", OPERATIONS):
                    row["status"] = "NOT_MEASURED_OPERATION_FILTER"
                    continue
                if op in SAFETY_BLOCKED:
                    row.update(status="SAFETY_BLOCKED", reason=SAFETY_BLOCKED[op])
                    continue
                if OPERATIONS[op][2] not in manifest.allowed_permissions:
                    row["status"] = "PERMISSION_EXCLUDED"
                    continue
                if op == "stream" and not limits["stream_seconds"]:
                    row["status"] = "NOT_MEASURED_STREAM_DISABLED"
                    continue
                for round_index in range(limits["rounds"] if op != "stream" else 1):
                    wait = max(0, next_request - time.monotonic())
                    if time.monotonic() + wait >= deadline:
                        break
                    await asyncio.sleep(wait)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    refs = tuple(reference_product(p) for p in products) if op == "reference_batch" else ()
                    async def call():
                        if refs:
                            return await query.reference_batch(tuple(p.sdk_requirement for p in refs), consumer_id=manifest.consumer_id, require_all=True)
                        return await invoke(op, products, query, client, manifest, limits["stream_seconds"])
                    next_request = time.monotonic() + max(interval, 2.0 if op == "stream" else 0)
                    sample = await measure(call, lambda payload: validate_operation(op, products, payload, refs),
                                           min(remaining, limits["timeout_seconds"] + (limits["stream_seconds"] if op == "stream" else 0)))
                    sample.update(round=round_index, first_request_on_replica=first_request, pacing_wait_ms=round(wait * 1000, 3))
                    first_request = False
                    row["samples"].append(sample)
                expected = 1 if op == "stream" else limits["rounds"]
                samples = row["samples"]
                row["status"] = ("NOT_MEASURED_DEADLINE" if len(samples) < expected else
                                 "FAIL" if any(s["status"] == "FAIL" for s in samples) else
                                 "NO_EVENT" if any(s["status"] == "NO_EVENT" for s in samples) else "PASS")
                row["response_latency"] = percentiles([s["response_ms"] for s in samples if s["response_ms"] is not None])
                row["usable_latency"] = percentiles([s["usable_ms"] for s in samples if s["status"] == "PASS"])
                row["steady_usable_latency"] = percentiles([s["usable_ms"] for s in samples if s["status"] == "PASS" and s["round"] > 0])
        finally:
            await client.close()
    states = [r["status"] for r in report["results"]]
    report["status"] = "FAIL" if "FAIL" in states else "INCOMPLETE" if any(x in states for x in ("NOT_MEASURED_DEADLINE", "NO_EVENT", "SAFETY_BLOCKED")) else "PASS_SELECTED_READS"
    report["certification"] = "BENCHMARK_ONLY_NOT_A_RELEASE_CERTIFICATE"
    return report


def docker_command(profile, consumer, name, image_id, inventory):
    command = ["docker", "run", "--rm", "--name", name, "--network", "none" if inventory else profile["network"],
               "--read-only", "--cpus", "1", "--memory", "512m", "--pids-limit", "128", "--cap-drop", "ALL",
               "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,nosuid,size=64m", "-e", "PYTHONDONTWRITEBYTECODE=1",
               "-e", "QDL_ENDPOINT_BENCHMARK_PROFILE", "--entrypoint", "python"]
    for src, dst in ((ROOT / "scripts/benchmark_consumer_endpoints.py", "/app/scripts/benchmark_consumer_endpoints.py"),
                     (ROOT / "config/v2", "/app/config/v2"), (ROOT / "consumers", "/app/consumers"),
                     (ROOT / "contracts/v2/openapi.snapshot.json", "/app/contracts/v2/openapi.snapshot.json")):
        command += ["--mount", f"type=bind,src={src},dst={dst},readonly"]
    client = json.loads(json.dumps(profile))
    c = json.loads(json.dumps(consumer))
    if not inventory:
        for group, field in (("tls", "ca_file"), ("tls", "cert_file"), ("tls", "key_file"), ("jwt", "private_key_file")):
            source = Path(c[group][field]).resolve(strict=True)
            if not source.is_file():
                raise ValueError("credential must be a file")
            target = f"/bench-id/{group}-{field}"
            command += ["--mount", f"type=bind,src={source},dst={target},readonly"]
            c[group][field] = target
    client["consumers"] = [c]
    command += [image_id, "-B", "/app/scripts/benchmark_consumer_endpoints.py", "_client"]
    if inventory:
        command += ["--inventory"]
    return command, client


def cleanup_container(name):
    probe = subprocess.run(["docker", "container", "ls", "-aq", "--filter", f"name=^/{name}$"], capture_output=True, text=True, timeout=15, check=True)
    if probe.stdout.strip():
        subprocess.run(["docker", "stop", "-t", "5", name], capture_output=True, timeout=15, check=True)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
    remaining = subprocess.run(["docker", "container", "ls", "-aq", "--filter", f"name=^/{name}$"], capture_output=True, text=True, timeout=15, check=True)
    if remaining.stdout.strip():
        raise RuntimeError("benchmark container cleanup failed")


def write_reports(output, reports):
    (output / "report.json").write_text(json.dumps(reports, indent=2) + "\n")
    columns = ["consumer", "replica", "operation", "products", "meaning", "status", "n", "p50_ms", "p95_ms", "p99_ms", "max_ms"]
    lines = ["# Consumer Endpoint Benchmark", "", "Call to validated data/diagnostic, milliseconds. Batch timings are whole-batch wall time.",
             "Same-host external container; NOT provider-to-host latency or release certification.", "",
             "| Consumer | Replica | Operation | Product(s) | Meaning | Status | N | p50 | p95 | p99 | Max |",
             "|---|---|---|---|---|---|---:|---:|---:|---:|---:|"]
    with (output / "report.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for report in reports:
            for row in report.get("results", []):
                value = {"consumer": report["consumer_id"], "replica": row["replica"], "operation": row["operation"],
                         "products": ";".join(f"{p['venue']}:{p['native_symbol']}:{p['feed']}:{p['interval'] or '-'}" for p in row["products"]) or "consumer-scope",
                         "meaning": row.get("meaning", "data_validated"), "status": row["status"], **row.get("usable_latency", percentiles([]))}
                writer.writerow(value)
                lines.append("| " + " | ".join(str(value[k]) if value[k] is not None else "n/a" for k in columns) + " |")
    (output / "report.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inventory", "run", "_client"))
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--inventory", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.command == "_client":
        sys.path.insert(0, str(ROOT))
        profile = validate_profile(json.loads(os.environ["QDL_ENDPOINT_BENCHMARK_PROFILE"]))
        result = asyncio.run(run_client(profile, args.inventory))
        print(json.dumps(result))
        return 0
    if not args.profile or not args.output:
        parser.error("--profile and a new --output directory required")
    profile = validate_profile(json.loads(args.profile.read_text()))
    image_id = subprocess.run(["docker", "image", "inspect", profile["image"], "--format", "{{.Id}}"],
                              capture_output=True, text=True, timeout=15, check=True).stdout.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise ValueError("image must resolve locally to an immutable digest; no automatic build/pull")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    tool_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    source_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    reports = []
    for consumer in profile["consumers"]:
        name = "qdl-endpoint-bench-" + uuid.uuid4().hex[:12]
        cmd, client = docker_command(profile, consumer, name, image_id, args.command == "inventory")
        try:
            proc = subprocess.run(cmd, env={**os.environ, "QDL_ENDPOINT_BENCHMARK_PROFILE": json.dumps(client)},
                                  capture_output=True, text=True, timeout=profile["limits"]["max_seconds"] + 120)
            if proc.returncode:
                raise RuntimeError("client failed; diagnostic type: " + (proc.stderr.strip().splitlines()[-1].split(":")[0] if proc.stderr.strip() else "unknown"))
            report = json.loads(proc.stdout)
            report.update(image_id=image_id, source_sha=source_sha, tool_sha256=tool_hash)
            reports.append(report)
        finally:
            cleanup_container(name)
            if reports:
                write_reports(args.output, reports)
        print(json.dumps({"consumer": consumer["id"], "status": report["status"], "products": report["scope"]["product_count"], "cleanup": "REMOVED"}))
    return 0 if all(r["status"] in ("PASS_SELECTED_READS", "INVENTORY_ONLY") for r in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
