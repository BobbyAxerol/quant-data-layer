"""DR01 deployed alpha GET-only probe, delivered through python -B - probe.

prepare is offline DL compilation; probe runs only when Claude explicitly invokes
it in the alpha compose --rm container; record joins the two venue observations.
No account cells, orchestration, retries, entitlement edits or order calls.

The JWT manifest revision is never assumed. prepare takes the revision the live
readers serve (--manifest-revision) and the deployed realm directory that the
readers mount at /consumer-realms (--manifest-dir); it refuses unless exactly one
paper manifest for the venue's alpha consumer in that directory, and its entry in
the realm's bundle.json, both carry that revision (bundle sha = the reader's
canonical manifest sha). The wrong revision is --wrong-revision or, by default,
the declared revision minus one. Both are sealed in the packet; probe compares the
alpha env against the packet and signs with those values only.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

VENUES = ("BINANCE", "OKX")
CASES = ("BINDING_COMPILE", "WRONG_REALM_DENIED", "WRONG_REVISION_DENIED")
MAX_BYTES = 2_000_000
MAX_REALM_FILES = 64


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def read_json(path):
    raw = Path(path).read_bytes()
    if len(raw) > MAX_BYTES:
        raise ValueError("input exceeds bound")
    return json.loads(raw)


def identity(req):
    return tuple(req.get(k) for k in ("instrument_uid", "feed", "interval", "source_policy_id"))


def manifest_sha(payload):
    # Same canonical form as qdl.consumer.manifest.ConsumerManifestLoader (the reader's bundle sha).
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def revisions(declared, wrong=None):
    """(revision, wrong_revision) as positive ints; wrong defaults to declared-1 and never equals declared."""
    if type(declared) is not int or declared < 1:
        raise ValueError("manifest revision must be a positive integer")
    wrong = declared - 1 if wrong is None else wrong
    if type(wrong) is not int or wrong < 1 or wrong == declared:
        raise ValueError("wrong revision must be a positive integer other than the manifest revision")
    return declared, wrong


def realm_manifest(directory, venue, declared):
    """The deployed paper manifest of the venue's alpha consumer, checked against the realm bundle and revision."""
    import yaml
    directory = Path(directory).resolve()
    consumer_id = f"alpha.{venue.lower()}.paper.stable"
    files = sorted(p for p in directory.iterdir() if p.suffix in (".yaml", ".yml") and p.is_file())
    if len(files) > MAX_REALM_FILES:
        raise ValueError("realm directory exceeds bound")
    found = []
    for path in files:
        raw = path.read_bytes()
        if len(raw) > MAX_BYTES:
            raise ValueError("input exceeds bound")
        doc = yaml.safe_load(raw)
        meta = doc.get("metadata") if isinstance(doc, dict) else None
        if isinstance(meta, dict) and meta.get("id") == consumer_id:
            found.append((path, raw, doc))
    if len(found) != 1:
        raise ValueError("expected exactly one realm manifest for the alpha consumer")
    path, raw, doc = found[0]
    if doc["metadata"].get("environment") != "paper" or doc["metadata"].get("revision") != declared:
        raise ValueError("realm manifest revision differs from the declared revision")
    bundle_raw = (directory / "bundle.json").read_bytes()
    if len(bundle_raw) > MAX_BYTES:
        raise ValueError("input exceeds bound")
    bundle = json.loads(bundle_raw)
    served = [m for m in bundle.get("manifests", ()) if m.get("consumer_id") == consumer_id
              and m.get("environment") == "paper"]
    if (bundle.get("environment") != "paper" or len(served) != 1
            or served[0].get("manifest_revision") != declared or served[0].get("manifest_sha256") != manifest_sha(doc)):
        raise ValueError("realm bundle does not serve the declared manifest revision")
    return path, raw, doc, digest(bundle_raw)


def wildcard_paths(obj, path="$"):
    if isinstance(obj, dict):
        return [p for k, v in obj.items() for p in wildcard_paths(v, f"{path}.{k}")]
    if isinstance(obj, list):
        return [p for i, v in enumerate(obj) for p in wildcard_paths(v, f"{path}[{i}]")]
    return [path] if isinstance(obj, str) and ("*" in obj or "?" in obj) else []


def prepare(args):
    # Reuse the deployed alpha compiler, not the TS-only stable renderer.
    root = Path(args.root).resolve()
    sys.path.insert(0, str(root))
    import yaml
    from scripts.compile_alpha_deployment_bindings import compile_inventory
    from qdl.consumer.universal_release import ConsumerRouteBinding
    paths = {"catalog": Path(args.catalog) if args.catalog else root / "config/v2/stable-source-bindings.yaml",
             "reference_manifest": root / "consumers/stable/reference-l2-stable.yaml",
             "release_routing": root / "config/v2/stable-v2-release-routing.yaml",
             "policy": root / "config/v2/universal-release-policy.yaml"}
    raw = {k: p.read_bytes() for k, p in paths.items()}
    values = {k: yaml.safe_load(v) for k, v in raw.items()}
    report = compile_inventory(inventory=read_json(args.inventory), **values,
        **{k + "_sha256": digest(v) for k, v in raw.items()})
    entry = next(x for x in report["deployments"] if x["deployment_id"] == args.deployment)
    if entry["status"] != "ADMITTED":
        raise ValueError("deployment not admitted")
    binding = entry["binding"]
    ConsumerRouteBinding.from_canonical_mapping(binding)
    revision, wrong_revision = revisions(args.manifest_revision, args.wrong_revision)
    manifest_path, manifest_raw, manifest, bundle_sha = realm_manifest(args.manifest_dir, args.venue, revision)
    requirements = manifest["spec"]["requirements"]
    # Check concrete source entitlement selectors as well as consumer selectors.
    selectors = {"consumer": manifest["spec"], "bindings": [
        {k: b.get(k) for k in ("instrument_uid", "feed", "interval", "source")}
        for b in values["catalog"]["bindings"]]}
    wildcards = wildcard_paths(selectors)
    if wildcards:
        raise ValueError("wildcard entitlement")
    products = [p for p in binding["products"] if p["venue"] == args.venue]
    baseline = next(r for p in products if p["feed"] == "QUOTE"
                    for r in requirements if identity(r) == identity(p))
    # An absent deployment route alone is insufficient: deny against the actual
    # access manifest, using an otherwise valid instrument/feed in the catalog.
    own_uids = {p["instrument_uid"] for p in values["catalog"]["instruments"] if p["venue"] == args.venue}
    forbidden = next(b for b in values["catalog"]["bindings"]
                     if b["instrument_uid"] in own_uids
                     and not any(r["instrument_uid"] == b["instrument_uid"]
                                 and r["feed"] == b["feed"] and r.get("interval") == b.get("interval")
                                 for r in requirements))
    negative = dict(baseline, instrument_uid=forbidden["instrument_uid"], feed=forbidden["feed"],
                    interval=forbidden.get("interval"), source_policy_id=forbidden["source"]["source_policy_id"])
    evidence = {"manifest_path": str(manifest_path), "manifest_sha256": digest(manifest_raw),
                "manifest_canonical_sha256": manifest_sha(manifest), "realm_bundle_sha256": bundle_sha,
                "manifest_revision": revision, "wildcard_paths": wildcards,
                "selector_count": len(requirements),
                "grep_equivalent_command": f"rg -n 'instrument_uid:|feed:|source_policy_id:' {manifest_path}",
                "source_files": {str(paths[k]): digest(v) for k, v in raw.items()}}
    packet = {"schema": "p184.dr01.deployed-packet.v1", "venue": args.venue,
              "access_consumer_id": manifest["metadata"]["id"], "subject": manifest["metadata"]["subject"],
              "jwt_revision": revision, "wrong_revision": wrong_revision, "environment": "paper", "binding": binding,
              "baseline": baseline, "forbidden": negative, "entitlements": evidence}
    with Path(args.output).open("x") as out:
        json.dump(packet, out, sort_keys=True)


def validate(packet, binding, env):
    if packet.get("schema") != "p184.dr01.deployed-packet.v1" or packet.get("venue") not in VENUES:
        raise ValueError("invalid packet")
    if packet.get("wrong_revision") is None:
        raise ValueError("invalid packet")
    revision, _ = revisions(packet.get("jwt_revision"), packet["wrong_revision"])
    expected = packet["binding"]
    for obj in (binding, expected):
        claimed = obj["binding_sha256"]
        if claimed != digest(canonical({k: v for k, v in obj.items() if k != "binding_sha256"})):
            raise ValueError("binding checksum mismatch")
    if canonical(expected) != canonical(binding):
        raise ValueError("compiled and mounted binding differ")
    required = {"DATA_LAYER_V2_ACCESS_CONSUMER_ID": packet["access_consumer_id"],
                "DATA_LAYER_V2_CONSUMER_ID": binding["consumer_id"],
                "DATA_LAYER_V2_JWT_SUBJECT": packet["subject"],
                "DATA_LAYER_V2_JWT_ENVIRONMENT": "paper", "DATA_LAYER_V2_JWT_MANIFEST_REVISION": str(revision),
                "DATA_LAYER_V2_RELEASE_MANIFEST_SHA256": binding["universal_manifest_sha256"]}
    if any(env.get(k) != v for k, v in required.items()):
        raise ValueError("alpha identity or revision drift")
    if packet["entitlements"]["wildcard_paths"] or packet["entitlements"].get("manifest_revision") != revision:
        raise ValueError("entitlement drift")
    if identity(packet["baseline"]) not in {identity(p) for p in binding["products"]}:
        raise ValueError("baseline outside binding")
    url = urlsplit(env["DATA_LAYER_V2_QUERY_URL"])
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("authenticated TLS query origin required")
    if url.path not in ("", "/"):
        raise ValueError("query URL must be an origin")


def route(req):
    keys = ("feed", "source_policy_id", "consumer_grade", "interval", "max_freshness_ms",
            "event_recency_policy", "max_session_liveness_ms", "require_full_coverage", "require_final_bars",
            "stale_policy", "gap_policy", "recovery", "bar_revision_policy")
    params = {k: str(req[k]).lower() if isinstance(req[k], bool) else req[k]
              for k in keys if req.get(k) is not None}
    uid = req["instrument_uid"]
    if not re.fullmatch(r"[a-f0-9-]{36}", uid):
        raise ValueError("invalid instrument UID")
    return f"/v2/market-data/{uid}/snapshot?" + urlencode(params)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def get(opener, origin, path, headers):
    started = time.monotonic_ns()
    try:
        response = opener.open(Request(origin.rstrip("/") + path, headers=headers, method="GET"), timeout=10)
    except HTTPError as exc:
        response = exc
    with response:
        raw = response.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError("response exceeds bound")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("response must be an object")
        data = payload.get("data") or {}
        return {"status": response.code, "code": payload.get("code"),
                "instrument_uid": data.get("instrument_uid"), "feed": data.get("feed"),
                "body_sha256": digest(raw), "request_ms": (time.monotonic_ns() - started) / 1e6}


async def token(env, **overrides):
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    settings = dict(private_key_file=env["DATA_LAYER_V2_JWT_PRIVATE_KEY_FILE"],
        key_id=env["DATA_LAYER_V2_JWT_KEY_ID"], algorithm=env.get("DATA_LAYER_V2_JWT_ALGORITHM", "RS256"),
        issuer=env["DATA_LAYER_V2_JWT_ISSUER"], audience=env["DATA_LAYER_V2_JWT_AUDIENCE"],
        subject=env["DATA_LAYER_V2_JWT_SUBJECT"], environment="paper",
        consumer_manifest_revision=int(env["DATA_LAYER_V2_JWT_MANIFEST_REVISION"]),
        roles=tuple(env.get("DATA_LAYER_V2_JWT_ROLES", "market_data_reader,historical_reader,stream_consumer").split(",")),
        venues=tuple(env.get("DATA_LAYER_V2_JWT_VENUES", "BINANCE,OKX").split(",")),
        lifetime_seconds=120, refresh_before_seconds=30)
    settings.update(overrides)
    return await RotatingJwtCredentialProvider(**settings).get_token()


def evaluate(packet, observations):
    base = observations["baseline"]
    good = base["status"] == 200 and base["instrument_uid"] == packet["baseline"]["instrument_uid"] and base["feed"] == packet["baseline"]["feed"]
    checks = {"BINDING_COMPILE": good,
              "WRONG_REALM_DENIED": good and observations["realm"]["status"] == 401 and observations["realm"].get("code") == "UNAUTHENTICATED",
              "WRONG_REVISION_DENIED": good and observations["revision"]["status"] == 401 and observations["revision"].get("code") == "UNAUTHENTICATED",
              "NO_GLOBAL_GRANT": good and observations["forbidden"]["status"] == 403
                  and observations["forbidden"]["code"] == "PERMISSION_DENIED"}
    return [{"cell": f"DR01.{case}@{packet['venue'] if case != 'NO_GLOBAL_GRANT' else 'ANY'}",
             "level": "RUNTIME", "ok": bool(ok), "outcome": "OBSERVED", "venue": packet["venue"],
             "evidence": {"observations": observations, "binding_sha256": packet["binding"]["binding_sha256"],
                          "release_revision": packet["binding"]["release_revision"],
                          "jwt_revision": packet["jwt_revision"], "wrong_revision": packet["wrong_revision"],
                          "entitlements": packet["entitlements"], "mtls": True}}
            for case, ok in checks.items()]


def probe(args):
    packet = read_json(args.packet)
    binding = read_json(os.environ.get("DATA_LAYER_V2_ROUTE_MANIFEST_FILE", "/run/p184/binding.json"))
    validate(packet, binding, os.environ)
    context = ssl.create_default_context(cafile=os.environ["DATA_LAYER_V2_TLS_CA_FILE"])
    context.load_cert_chain(os.environ["DATA_LAYER_V2_TLS_CERT_FILE"], os.environ["DATA_LAYER_V2_TLS_KEY_FILE"])
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context), NoRedirect())
    observations = {}
    for name, changes, req in (("baseline", {}, packet["baseline"]),
        ("realm", {"environment": "live"}, packet["baseline"]),
        ("revision", {"consumer_manifest_revision": packet["wrong_revision"]}, packet["baseline"]),
        ("forbidden", {}, packet["forbidden"])):
        headers = {"Authorization": "Bearer " + asyncio.run(token(os.environ, **changes)),
                   "X-QDL-Consumer-ID": packet["access_consumer_id"], "X-QDL-Purpose": "INTERNAL_ALPHA"}
        observations[name] = get(opener, os.environ["DATA_LAYER_V2_QUERY_URL"], route(req), headers)
        time.sleep(.25)
    rows = evaluate(packet, observations)
    for row in rows:
        print(json.dumps(row, sort_keys=True), flush=True)
    return 0 if all(r["ok"] for r in rows) else 1


def record(args):
    if not args.ci_confirmed or len(set(args.source_sha)) == 1 or len(set(args.image_digest[7:])) == 1 or not re.fullmatch(r"[a-f0-9]{40}", args.source_sha) or not re.fullmatch(r"sha256:[a-f0-9]{64}", args.image_digest):
        raise ValueError("CI confirmation and full TS provenance required")
    rows = [json.loads(line) for path in args.rows for line in Path(path).read_text().splitlines() if line]
    expected = {(v, f"DR01.{c}@{v if c != 'NO_GLOBAL_GRANT' else 'ANY'}")
                for v in VENUES for c in (*CASES, "NO_GLOBAL_GRANT")}
    keys = [(r.get("venue"), r.get("cell")) for r in rows]
    if len(keys) != len(expected) or set(keys) != expected or any(
            r.get("level") != "RUNTIME" or type(r.get("ok")) is not bool or r.get("outcome") != "OBSERVED" for r in rows):
        raise ValueError("exact two-venue runtime rows required")
    combined = [r for r in rows if r["cell"] != "DR01.NO_GLOBAL_GRANT@ANY"]
    grants = [r for r in rows if r["cell"] == "DR01.NO_GLOBAL_GRANT@ANY"]
    combined.append({"cell": "DR01.NO_GLOBAL_GRANT@ANY", "level": "RUNTIME", "ok": all(r["ok"] for r in grants),
                     "outcome": "OBSERVED", "evidence": grants})
    with Path(args.output).open("x") as out:
        out.write(json.dumps({"provenance": {"schema": "p184.surface-evidence-provenance.v1",
            "source_sha": args.source_sha, "image_digest": args.image_digest}}) + "\n")
        for row in combined:
            out.write(json.dumps(row, sort_keys=True) + "\n")
    return 0 if all(r["ok"] for r in combined) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="mode", required=True)
    p = subs.add_parser("prepare")
    for name in ("root", "inventory", "deployment", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--venue", choices=VENUES, required=True)
    p.add_argument("--catalog")
    p.add_argument("--manifest-dir", required=True,
                   help="deployed reader realm directory (the host source of /consumer-realms)")
    p.add_argument("--manifest-revision", type=int, required=True, help="JWT manifest revision the readers serve")
    p.add_argument("--wrong-revision", type=int, help="revision for WRONG_REVISION_DENIED (default: revision-1)")
    p = subs.add_parser("probe")
    p.add_argument("--packet", required=True)
    p = subs.add_parser("record")
    p.add_argument("--rows", nargs=2, required=True)
    for name in ("source-sha", "image-digest", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--ci-confirmed", action="store_true")
    args = parser.parse_args(argv)
    return {"prepare": prepare, "probe": probe, "record": record}[args.mode](args) or 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Never echo credentials, HTTP response bodies or signed JWTs.
        print(json.dumps({"runner_failed": type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)
