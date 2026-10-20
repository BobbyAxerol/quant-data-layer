#!/usr/bin/env python3
"""Data Layer mesh mTLS generations: export the deployed one, generate a new one, verify old vs new.

Purpose
    The whole V2 mTLS mesh (Kafka KRaft peers kafka1-3, the Rust core / ingestors / bar edge Kafka clients, the KN
    projector and Stream Kafka clients, the Query and Stream TLS servers and the Trading System writer client) shares
    one trust anchor. ``scripts/phase80_generate_tls.sh`` creates a fresh CA on every run and deletes ``ca.key``, so a
    certificate renewal is always a CA rotation and every mutually authenticating peer has to move in one transaction
    (CERTIFICATION_LEDGER C.38). This tool prepares that transaction as two immutable *generations* with one layout,
    so the deployment switches between them only by mount source (``generation.env`` -> versioned compose):

    GEN/kafka/            phase80 output: kafka{1,2,3}.keystore.p12, phase8-admin.keystore.p12, truststore.jks,
                          truststore-kn-v220.jks (the brokers' dynamic listener truststore path, see below), the
                          three password files and the client .properties; mounted at /etc/kafka/secrets
    GEN/stable-certs/     <role>/{ca.crt,client.crt,client.key} for core, producer, projector, authority-dispatcher;
                          {query,stream}/{ca.crt,server.crt,server.key,client-ca-bundle.crt}; mounted at /stable-certs
    GEN/kn-tls/           broker-ca.crt, kn-projector.{crt,key}, kn-stream.{crt,key}; mounted at /kn/tls
    GEN/consumer-ca/      query-client-ca-bundle.crt, stream-client-ca-bundle.crt (the Query/Stream client trust)
    GEN/identities/trading-system/{ca.crt,client.crt,client.key}   the TS writer's /run/secrets/qdl-v2
    GEN/generation.env    the compose variables naming the five sources above
    GEN/manifest.json     public facts only: fingerprints, subjects, SANs, EKU, validity, file sha256

Modes (run with sudo: deployed material is owned by uid 10001/root and new material must be)
    export   describe the deployed generation without copying any file except the ``stable_tls`` Docker volume
             (no host path exists for it): writes GEN/stable-certs (byte copy, same owner/mode) and a
             generation.env that names the original host paths of every other source, so a rollback mounts exactly
             the bytes that run today.
    generate run phase80_generate_tls.sh (QDL_PHASE8_CERT_DAYS, default 365 here) into GEN/kafka and assemble the
             layout; the client CA bundles are the deployed bundles with the old mesh CA replaced by the new CA
             (every other CA kept in order); truststore-kn-v220.jks is a byte copy of the new truststore.jks.
    verify   old vs new, fail closed: per identity slot the same subject CN (Kafka ACL / super.users principal) and
             SAN set, EKU and key usage a superset, private key matches certificate, chain verifies against the new
             CA only, validity window; keystore leaf + chain; truststores hold exactly the expected CA set (keytool in
             the pinned Kafka image, --network none, read-only mount); password files unchanged (sha256 compare
             only, never printed); client CA bundles = old with only the mesh CA replaced.

Why 365 days (justification recorded in the plan): the 90-day default was chosen on 2026-08-22 only to replace the
hard-coded 2-day validity that expired the mesh every 48 h; no policy names 90. Each renewal is a full-mesh outage
window (the CA key is deleted by design and peers verify each other), there is no automated renewal, and expiry
alerting is only being added (F2). 365 days with an alert at < 21 days halves nothing in security (private keys stay
host-local, never leave the host) but cuts forced mesh outages from four to one per year.

Kafka dynamic truststore: on 2026-09-27 (KN-5 additive trust packet) every broker got the dynamic override
``listener.name.ssl.ssl.truststore.location=/etc/kafka/secrets/truststore-kn-v220.jks`` in KRaft metadata. The
override survives a recreate, so a new generation must provide that exact file name (same store password); this
tool writes it, keeping metadata untouched (no kafka-configs --alter in the window).

Boundary: no container is started except ``--rm --network none`` keytool/phase80 helpers on the pinned Kafka image;
no Kafka, Redis, Postgres or production container is touched. Private keys and passwords are never printed and
never written to the manifest. Exit 0 = OK, 2 = verification failed, 1 = usage/runtime error.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts/phase80_generate_tls.sh"
KAFKA_IMAGE = "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
QDL_UID = 10001
DEFAULT_DAYS = 365
STATE = Path("/home/bobby/.local/state/qdl-v2")
# The deployed generation (2026-08-22 C.38 rotation + 2026-09-27 KN client CA), as mounted on 2026-10-19.
DEPLOYED = {
    "QDL_TLS_GENERATION": "20260822T144323Z",
    "QDL_TLS_KAFKA_DIR": str(STATE / "655d2106d01f/cert-material-rotate-20260822T144323Z"),
    "stable_tls_volume": "/var/lib/docker/volumes/qdl_v2_stable_candidate_stable_tls/_data",
    "QDL_TLS_KN_DIR": str(STATE / "releases/v2.2.0-02cd827/tls"),
    "QDL_TLS_QUERY_CLIENT_CA_FILE": str(STATE / "p184-d3-capacity-20261009/reader-public-r16/query-client-ca-bundle.crt"),
    "QDL_TLS_STREAM_CLIENT_CA_FILE": str(STATE / "p184-d3-capacity-20261009/reader-public-r16/stream-client-ca-bundle.crt"),
    "QDL_TLS_TS_IDENTITY_DIR": str(STATE / "655d2106d01f/bundle/identities-rotate-20260822T144323Z/trading-system"),
}
ENV_KEYS = ("QDL_TLS_GENERATION", "QDL_TLS_KAFKA_DIR", "QDL_TLS_STABLE_CERTS_DIR", "QDL_TLS_KN_DIR",
            "QDL_TLS_QUERY_CLIENT_CA_FILE", "QDL_TLS_STREAM_CLIENT_CA_FILE", "QDL_TLS_TS_IDENTITY_DIR")
STABLE_CLIENT_ROLES = {"core": "phase8-core", "producer": "phase8-producer", "projector": "phase8-consumer",
                       "authority-dispatcher": "stable-authority-dispatcher"}
STABLE_SERVER_ROLES = {"query": "stable-query", "stream": "stable-stream"}
KN_PRINCIPALS = ("kn-projector", "kn-stream")
KEYSTORES = {"kafka1": "kafka1", "kafka2": "kafka2", "kafka3": "kafka3", "phase8-admin": "phase8-admin"}
PASSWORD_FILES = ("key.password", "store.password", "truststore.password")
MESH_CA_CN = "qdl-phase8-certification-ca"
KN_CLIENT_CA_CN = "qdl-kn-v220-client-ca"


# ----------------------------------------------------------------------------------------------- helpers
def run(argv, *, input_bytes=None, timeout=120, check=True):
    proc = subprocess.run(argv, input=input_bytes, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {argv[0]} {argv[1] if len(argv) > 1 else ''}: "
                           f"{proc.stderr.decode(errors='replace')[-400:]}")
    return proc


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def pem_blocks(data: bytes) -> list[bytes]:
    return [m.group(0) + b"\n" for m in re.finditer(
        rb"-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----", data, flags=re.S)]


def cert_facts(pem: bytes) -> dict:
    """Public facts of one PEM certificate via openssl (no private material)."""
    text = run(["openssl", "x509", "-noout", "-subject", "-issuer", "-startdate", "-enddate", "-fingerprint",
                "-sha256", "-nameopt", "RFC2253", "-ext",
                "subjectAltName,extendedKeyUsage,keyUsage,basicConstraints"], input_bytes=pem).stdout.decode()
    pub = run(["openssl", "x509", "-noout", "-pubkey"], input_bytes=pem).stdout
    facts = {"subject": "", "issuer": "", "san": [], "eku": [], "key_usage": [], "ca": False}
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("subject="):
            facts["subject"] = line.split("=", 1)[1].strip()
        elif line.startswith("issuer="):
            facts["issuer"] = line.split("=", 1)[1].strip()
        elif line.startswith("notBefore="):
            facts["not_before"] = _when(line.split("=", 1)[1])
        elif line.startswith("notAfter="):
            facts["not_after"] = _when(line.split("=", 1)[1])
        elif "Fingerprint=" in line:
            facts["sha256_fingerprint"] = line.split("=", 1)[1].replace(":", "").lower()
        elif "Subject Alternative Name" in line and i + 1 < len(lines):
            facts["san"] = sorted(x.strip() for x in lines[i + 1].split(","))
        elif "Extended Key Usage" in line and i + 1 < len(lines):
            facts["eku"] = sorted(x.strip() for x in lines[i + 1].split(","))
        elif re.search(r"X509v3 Key Usage", line) and i + 1 < len(lines):
            facts["key_usage"] = sorted(x.strip() for x in lines[i + 1].split(","))
        elif "Basic Constraints" in line and i + 1 < len(lines):
            facts["ca"] = "CA:TRUE" in lines[i + 1]
    cn = re.search(r"CN=([^,]+)", facts["subject"])
    facts["cn"] = cn.group(1) if cn else None
    facts["public_key_sha256"] = hashlib.sha256(pub).hexdigest()
    return facts


def _when(value: str) -> str:
    return dt.datetime.strptime(value.strip(), "%b %d %H:%M:%S %Y %Z").replace(
        tzinfo=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def key_public_sha256(key_path: Path) -> str:
    pub = run(["openssl", "pkey", "-in", str(key_path), "-pubout"]).stdout
    return hashlib.sha256(pub).hexdigest()


def p12_certs(path: Path, password_file: Path) -> list[bytes]:
    out = run(["openssl", "pkcs12", "-in", str(path), "-nokeys", "-passin", f"file:{password_file}"]).stdout
    return pem_blocks(out)


def jks_certs(store_dir: Path, store: str, password_file: str) -> list[bytes]:
    """Certificates of a JKS truststore via keytool in the pinned Kafka image (--network none, read-only)."""
    password = (store_dir / password_file).read_text().strip()
    # The store password reaches keytool through the container environment only (never argv, never printed).
    proc = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--user", "0:0", "--read-only", "-e", "QDL_TS_PW",
         "--mount", f"type=bind,source={store_dir},target=/certs,readonly",
         "--entrypoint", "/opt/java/openjdk/bin/keytool", KAFKA_IMAGE, "-list", "-rfc",
         "-keystore", f"/certs/{store}", "-storepass:env", "QDL_TS_PW"],
        capture_output=True, timeout=120, env={**os.environ, "QDL_TS_PW": password})
    if proc.returncode != 0:
        raise RuntimeError(f"keytool -list {store} failed: {proc.stderr.decode(errors='replace')[-300:]}")
    return pem_blocks(proc.stdout)


def read_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    missing = [k for k in ENV_KEYS if k not in env]
    if missing:
        raise ValueError(f"generation.env misses {missing}")
    return env


def write_env(path: Path, env: dict, note: str) -> None:
    lines = [f"# {note}", "# Data Layer mesh TLS generation: compose variables (scripts/rotate_mesh_tls.py)."]
    lines += [f"{k}={env[k]}" for k in ENV_KEYS]
    path.write_text("\n".join(lines) + "\n")
    os.chmod(path, 0o644)


def chown_tree(path: Path, uid: int, gid: int, dir_mode: int, file_mode: int) -> None:
    for dirpath, dirnames, filenames in os.walk(path):
        os.chown(dirpath, uid, gid)
        os.chmod(dirpath, dir_mode)
        for name in filenames:
            p = Path(dirpath) / name
            os.chown(p, uid, gid)
            os.chmod(p, file_mode)


def _drop_to_host_user() -> None:
    import pwd
    user = pwd.getpwuid(1001)
    os.setgroups(os.getgrouplist(user.pw_name, user.pw_gid))
    os.setgid(user.pw_gid)
    os.setuid(1001)
    os.environ["HOME"] = user.pw_dir


def require_root() -> None:
    if os.geteuid() != 0:
        raise SystemExit("run with sudo: deployed material is owned by uid 10001/root")


# ----------------------------------------------------------------------------------------------- permissions
# Consumer of each generation subtree (uid, gid) and the modes it gets (owner F2/F3 finding 2026-10-19: the 2026-08-22
# cert-material dir mounted into the brokers has world-readable private keys). Kafka runs as appuser uid 1000
# (docker inspect + /proc of the live broker); the DL roles as 10001:10001 with cap_drop ALL (no DAC override); the
# TS writer as root with default capabilities. Private material (keys, keystores, passwords, .properties carrying
# the store password, truststores) 0400/0440 owned by the consumer; public certificates 0444; directories 0750.
PERMS = {
    "kafka": (1000, 1000),
    "stable-certs": (QDL_UID, QDL_UID),
    "kn-tls": (QDL_UID, QDL_UID),
    "identities": (0, 0),
}
PUBLIC_SUFFIXES = (".crt", ".public.pem")


def _wanted_mode(path: Path, subtree: str) -> int:
    if path.is_dir():
        return 0o750
    if path.name.endswith(PUBLIC_SUFFIXES) and not path.name.endswith("client-ca-bundle.crt"):
        return 0o444
    if path.name.endswith("client-ca-bundle.crt"):
        return 0o444
    return 0o400 if subtree in ("kn-tls", "identities") else 0o440


def harden(gen_dir: Path) -> dict:
    """Apply PERMS to an existing generation; file bytes unchanged (sha256 before == after)."""
    require_root()
    before = {str(f): sha256_file(f) for f in gen_dir.rglob("*") if f.is_file()}
    for subtree, (uid, gid) in PERMS.items():
        root = gen_dir / subtree
        if not root.exists():
            continue
        for path in [root, *root.rglob("*")]:
            os.chown(path, uid, gid, follow_symlinks=False)
            os.chmod(path, _wanted_mode(path, subtree))
    after = {str(f): sha256_file(f) for f in gen_dir.rglob("*") if f.is_file()}
    return {"files": len(after), "bytes_unchanged": before == after, "problems": perms_check(gen_dir)}


def perms_check(gen_dir: Path) -> list[str]:
    """Every private file of the generation is owner-only/group-only for its consumer; nothing world-readable
    except public certificates; directories 0750."""
    problems = []
    for subtree, (uid, gid) in PERMS.items():
        root = gen_dir / subtree
        if not root.exists():
            continue
        for path in [root, *root.rglob("*")]:
            st = path.lstat()
            mode = st.st_mode & 0o777
            if (st.st_uid, st.st_gid) != (uid, gid):
                problems.append(f"{path}: owner {st.st_uid}:{st.st_gid} != {uid}:{gid}")
            if mode != _wanted_mode(path, subtree):
                problems.append(f"{path}: mode {oct(mode)} != {oct(_wanted_mode(path, subtree))}")
            if path.is_file() and mode & 0o004 and _wanted_mode(path, subtree) != 0o444:
                problems.append(f"{path}: private file is world-readable")
    return problems


# ----------------------------------------------------------------------------------------------- slots
def slots(env: dict) -> list[dict]:
    """Every identity the mesh mounts: (consumer, cert, key, expected CN, purpose)."""
    stable, kn, ts = Path(env["QDL_TLS_STABLE_CERTS_DIR"]), Path(env["QDL_TLS_KN_DIR"]), Path(env["QDL_TLS_TS_IDENTITY_DIR"])
    out = []
    for role, cn in STABLE_CLIENT_ROLES.items():
        out.append({"slot": f"stable-certs/{role}", "cert": stable / role / "client.crt",
                    "key": stable / role / "client.key", "ca": stable / role / "ca.crt", "cn": cn, "purpose": "sslclient"})
    for role, cn in STABLE_SERVER_ROLES.items():
        out.append({"slot": f"stable-certs/{role}", "cert": stable / role / "server.crt",
                    "key": stable / role / "server.key", "ca": stable / role / "ca.crt", "cn": cn, "purpose": "sslserver"})
    for cn in KN_PRINCIPALS:
        out.append({"slot": f"kn-tls/{cn}", "cert": kn / f"{cn}.crt", "key": kn / f"{cn}.key",
                    "ca": None, "cn": cn, "purpose": "sslclient"})
    out.append({"slot": "identities/trading-system", "cert": ts / "client.crt", "key": ts / "client.key",
                "ca": ts / "ca.crt", "cn": "stable-trading-system", "purpose": "sslclient"})
    return out


def ca_files(env: dict) -> list[Path]:
    stable = Path(env["QDL_TLS_STABLE_CERTS_DIR"])
    files = [stable / r / "ca.crt" for r in (*STABLE_CLIENT_ROLES, *STABLE_SERVER_ROLES)]
    files += [Path(env["QDL_TLS_KN_DIR"]) / "broker-ca.crt", Path(env["QDL_TLS_TS_IDENTITY_DIR"]) / "ca.crt",
              Path(env["QDL_TLS_KAFKA_DIR"]) / "ca.crt"]
    return files


def describe(env: dict, *, with_stores: bool = True) -> dict:
    """Public facts of one generation (fails on unreadable material)."""
    doc = {"generation": env["QDL_TLS_GENERATION"], "sources": {k: env[k] for k in ENV_KEYS}, "slots": {},
           "ca_files": {}, "keystores": {}, "truststores": {}, "client_ca_bundles": {}, "password_sha256": {}}
    for s in slots(env):
        facts = cert_facts(Path(s["cert"]).read_bytes())
        facts["key_matches_cert"] = key_public_sha256(Path(s["key"])) == facts["public_key_sha256"]
        facts["cert_file_sha256"] = sha256_file(s["cert"])
        doc["slots"][s["slot"]] = facts
    for path in ca_files(env):
        doc["ca_files"][str(path)] = cert_facts(path.read_bytes())["sha256_fingerprint"]
    kafka = Path(env["QDL_TLS_KAFKA_DIR"])
    for name, cn in KEYSTORES.items():
        certs = p12_certs(kafka / f"{name}.keystore.p12", kafka / "store.password")
        doc["keystores"][name] = [cert_facts(c) for c in certs]
    for name in PASSWORD_FILES:
        doc["password_sha256"][name] = sha256_file(kafka / name)
    if with_stores:
        for store in ("truststore.jks", "truststore-kn-v220.jks"):
            if (kafka / store).exists():
                doc["truststores"][store] = sorted(cert_facts(c)["sha256_fingerprint"]
                                                   for c in jks_certs(kafka, store, "truststore.password"))
    for key in ("QDL_TLS_QUERY_CLIENT_CA_FILE", "QDL_TLS_STREAM_CLIENT_CA_FILE"):
        doc["client_ca_bundles"][key] = [cert_facts(c)["sha256_fingerprint"] for c in pem_blocks(Path(env[key]).read_bytes())]
    for role in STABLE_SERVER_ROLES:
        path = Path(env["QDL_TLS_STABLE_CERTS_DIR"]) / role / "client-ca-bundle.crt"
        doc["client_ca_bundles"][f"stable-certs/{role}"] = [cert_facts(c)["sha256_fingerprint"] for c in pem_blocks(path.read_bytes())]
    return doc


# ----------------------------------------------------------------------------------------------- export
def export(gen_dir: Path) -> dict:
    require_root()
    if gen_dir.exists():
        raise SystemExit(f"{gen_dir} exists; generations are immutable")
    gen_dir.mkdir(parents=True, mode=0o700)
    os.chown(gen_dir, 1001, 1001)
    stable = gen_dir / "stable-certs"
    shutil.copytree(DEPLOYED["stable_tls_volume"], stable, copy_function=shutil.copy2)
    for dirpath, _, filenames in os.walk(DEPLOYED["stable_tls_volume"]):  # copytree keeps mode; keep owner too
        rel = Path(dirpath).relative_to(DEPLOYED["stable_tls_volume"])
        st = os.stat(dirpath)
        os.chown(stable / rel, st.st_uid, st.st_gid)
        for name in filenames:
            st = os.stat(Path(dirpath) / name)
            os.chown(stable / rel / name, st.st_uid, st.st_gid)
    env = {k: DEPLOYED[k] for k in ENV_KEYS if k in DEPLOYED}
    env["QDL_TLS_STABLE_CERTS_DIR"] = str(stable)
    write_env(gen_dir / "generation.env", env, "DEPLOYED generation exported read-only (rollback target)")
    copy_ok = all(sha256_file(Path(DEPLOYED["stable_tls_volume"]) / p) == sha256_file(stable / p)
                  for p in [str(f.relative_to(stable)) for f in stable.rglob("*") if f.is_file()])
    doc = describe(env)
    doc["stable_tls_copy_byte_equal"] = copy_ok
    _write_manifest(gen_dir, doc, kind="export")
    return doc


# ----------------------------------------------------------------------------------------------- generate
def replace_mesh_ca(bundle: bytes, old_ca_fp: str, new_ca_pem: bytes) -> bytes:
    out, replaced = [], 0
    for block in pem_blocks(bundle):
        if cert_facts(block)["sha256_fingerprint"] == old_ca_fp:
            out.append(new_ca_pem)
            replaced += 1
        else:
            out.append(block)
    if replaced != 1:
        raise RuntimeError(f"expected exactly one old mesh CA in bundle, found {replaced}")
    return b"".join(out)


def generate(gen_dir: Path, old_env: dict, days: int) -> dict:
    require_root()
    if gen_dir.exists():
        raise SystemExit(f"{gen_dir} exists; generations are immutable")
    gen_dir.mkdir(parents=True, mode=0o700)
    os.chown(gen_dir, 1001, 1001)
    kafka = gen_dir / "kafka"
    env_run = {**os.environ, "QDL_PHASE8_CERT_DAYS": str(days), "QDL_PHASE8_KAFKA_IMAGE": KAFKA_IMAGE,
               "QDL_PHASE8_CERT_UID": "1001", "QDL_PHASE8_CERT_GID": "1001"}
    # The store password must stay the deployed one: the brokers' static config and the dynamic truststore
    # override read the password files; it is passed through the environment, never printed.
    env_run["QDL_PHASE8_CERT_PASSWORD"] = (Path(old_env["QDL_TLS_KAFKA_DIR"]) / "store.password").read_text().strip()
    # The generator runs as the host user (1001), never as root: its own keytool container runs with the
    # caller's uid and must read the files it just wrote (QDL_PHASE8_CERT_UID/GID).
    kafka.mkdir(mode=0o755)
    os.chown(kafka, 1001, 1001)
    proc = subprocess.run(["bash", str(GENERATOR), str(kafka)], env=env_run, capture_output=True, timeout=600,
                          preexec_fn=_drop_to_host_user)
    if proc.returncode != 0:
        raise RuntimeError("phase80_generate_tls.sh failed: " + proc.stderr.decode(errors="replace")[-400:])
    if (kafka / "ca.key").exists():
        raise RuntimeError("ca.key retained; the generator must delete it")
    shutil.copy2(kafka / "truststore.jks", kafka / "truststore-kn-v220.jks")
    for f in kafka.iterdir():
        os.chown(f, 1001, 1001)
    ca_pem = (kafka / "ca.crt").read_bytes()
    old_ca_fp = cert_facts((Path(old_env["QDL_TLS_KAFKA_DIR"]) / "ca.crt").read_bytes())["sha256_fingerprint"]
    stable = gen_dir / "stable-certs"
    old_stable = Path(old_env["QDL_TLS_STABLE_CERTS_DIR"])
    for role, cn in STABLE_CLIENT_ROLES.items():
        (stable / role).mkdir(parents=True)
        shutil.copyfile(kafka / "ca.crt", stable / role / "ca.crt")
        shutil.copyfile(kafka / f"{cn}.crt", stable / role / "client.crt")
        shutil.copyfile(kafka / f"{cn}.key", stable / role / "client.key")
    for role, cn in STABLE_SERVER_ROLES.items():
        (stable / role).mkdir(parents=True)
        shutil.copyfile(kafka / "ca.crt", stable / role / "ca.crt")
        shutil.copyfile(kafka / f"{cn}.crt", stable / role / "server.crt")
        shutil.copyfile(kafka / f"{cn}.key", stable / role / "server.key")
        (stable / role / "client-ca-bundle.crt").write_bytes(
            replace_mesh_ca((old_stable / role / "client-ca-bundle.crt").read_bytes(), old_ca_fp, ca_pem))
    chown_tree(stable, QDL_UID, QDL_UID, 0o750, 0o440)
    kn = gen_dir / "kn-tls"
    kn.mkdir()
    shutil.copyfile(kafka / "ca.crt", kn / "broker-ca.crt")
    for cn in KN_PRINCIPALS:
        shutil.copyfile(kafka / f"{cn}.crt", kn / f"{cn}.crt")
        shutil.copyfile(kafka / f"{cn}.key", kn / f"{cn}.key")
    chown_tree(kn, QDL_UID, QDL_UID, 0o750, 0o400)
    consumer = gen_dir / "consumer-ca"
    consumer.mkdir()
    for key, name in (("QDL_TLS_QUERY_CLIENT_CA_FILE", "query-client-ca-bundle.crt"),
                      ("QDL_TLS_STREAM_CLIENT_CA_FILE", "stream-client-ca-bundle.crt")):
        (consumer / name).write_bytes(replace_mesh_ca(Path(old_env[key]).read_bytes(), old_ca_fp, ca_pem))
    chown_tree(consumer, 0, 0, 0o755, 0o444)
    ts = gen_dir / "identities/trading-system"
    ts.mkdir(parents=True)
    shutil.copyfile(kafka / "ca.crt", ts / "ca.crt")
    shutil.copyfile(kafka / "stable-trading-system.crt", ts / "client.crt")
    shutil.copyfile(kafka / "stable-trading-system.key", ts / "client.key")
    chown_tree(gen_dir / "identities", 1001, 1001, 0o775, 0o664)  # as the deployed identity dir (writer runs as root)
    env = {"QDL_TLS_GENERATION": gen_dir.name, "QDL_TLS_KAFKA_DIR": str(kafka),
           "QDL_TLS_STABLE_CERTS_DIR": str(stable), "QDL_TLS_KN_DIR": str(kn),
           "QDL_TLS_QUERY_CLIENT_CA_FILE": str(consumer / "query-client-ca-bundle.crt"),
           "QDL_TLS_STREAM_CLIENT_CA_FILE": str(consumer / "stream-client-ca-bundle.crt"),
           "QDL_TLS_TS_IDENTITY_DIR": str(ts)}
    write_env(gen_dir / "generation.env", env, f"NEW generation, {days}-day validity, created {now_utc()}")
    os.chown(gen_dir / "generation.env", 1001, 1001)
    doc = describe(env)
    doc["permissions"] = harden(gen_dir)
    doc["validity_days"] = days
    doc["generator_sha256"] = sha256_file(GENERATOR)
    _write_manifest(gen_dir, doc, kind="generate")
    return doc


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _git_rev() -> str:
    proc = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True)
    return proc.stdout.strip() or "unknown"


def _write_manifest(gen_dir: Path, doc: dict, *, kind: str) -> None:
    doc = {"schema": "qdl.mesh-tls-generation.v1", "kind": kind, "created_utc": now_utc(),
           "tool_rev": _git_rev(), **doc}
    path = gen_dir / "manifest.json"
    path.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    os.chown(path, 1001, 1001)
    os.chmod(path, 0o644)


# ----------------------------------------------------------------------------------------------- verify
def verify(old_env: dict, new_env: dict, *, min_days: int, at: dt.datetime | None = None) -> dict:
    at = at or dt.datetime.now(dt.timezone.utc)
    old, new = describe(old_env), describe(new_env)
    problems, checks = [], 0

    def check(ok, what):
        nonlocal checks
        checks += 1
        if not ok:
            problems.append(what)

    new_ca_pem = (Path(new_env["QDL_TLS_KAFKA_DIR"]) / "ca.crt").read_bytes()
    new_ca = cert_facts(new_ca_pem)
    old_ca = cert_facts((Path(old_env["QDL_TLS_KAFKA_DIR"]) / "ca.crt").read_bytes())
    check(new_ca["ca"] and new_ca["subject"] == new_ca["issuer"], "new CA is not a self-signed CA")
    check(new_ca["cn"] == MESH_CA_CN, "new CA CN changed")
    check(new_ca["sha256_fingerprint"] != old_ca["sha256_fingerprint"], "new CA equals old CA")
    check(not (Path(new_env["QDL_TLS_KAFKA_DIR"]) / "ca.key").exists(), "new CA key retained")
    expiry_floor = at + dt.timedelta(days=min_days)
    check(_parse(new_ca["not_after"]) >= expiry_floor, f"new CA expires before {min_days} days")
    for path, fp in new["ca_files"].items():
        check(fp == new_ca["sha256_fingerprint"], f"{path} is not the new CA")
    with tempfile.NamedTemporaryFile(suffix=".pem") as ca_file:
        ca_file.write(new_ca_pem)
        ca_file.flush()
        for s_new in slots(new_env):
            name = s_new["slot"]
            o, n = old["slots"][name], new["slots"][name]
            check(n["cn"] == o["cn"] == s_new["cn"], f"{name}: CN {o['cn']} -> {n['cn']} (expected {s_new['cn']})")
            san_ok = n["san"] == o["san"] or (not o["san"] and s_new["purpose"] == "sslclient"
                                              and n["san"] == sorted([f"DNS:{s_new['cn']}", "DNS:localhost"]))
            check(san_ok, f"{name}: SAN {o['san']} -> {n['san']}")
            check(set(o["eku"]) <= set(n["eku"]), f"{name}: EKU narrowed {o['eku']} -> {n['eku']}")
            # No keyUsage extension = no restriction (RFC 5280 4.2.1.3); the 2026-09-27 KN leaves carried one.
            check(not n["key_usage"] or set(o["key_usage"]) <= set(n["key_usage"]),
                  f"{name}: key usage narrowed {o['key_usage']} -> {n['key_usage']}")
            check(n["key_matches_cert"], f"{name}: private key does not match certificate")
            check(n["issuer"] == new_ca["subject"], f"{name}: issuer {n['issuer']}")
            check(_parse(n["not_after"]) >= expiry_floor, f"{name}: expires {n['not_after']}")
            # Not -x509_strict: the phase80 CA profile has no keyUsage extension (deployed 2026-08-22 CA alike,
            # accepted by rustls, Java and OpenSSL peers); the profile is kept unchanged on purpose.
            proc = run(["openssl", "verify", "-purpose", s_new["purpose"], "-CAfile", ca_file.name,
                        str(s_new["cert"])], check=False)
            check(proc.returncode == 0, f"{name}: openssl verify failed")
            proc = run(["openssl", "verify", "-CAfile", str(Path(old_env["QDL_TLS_KAFKA_DIR"]) / "ca.crt"),
                        str(s_new["cert"])], check=False)
            check(proc.returncode != 0, f"{name}: new leaf also verifies under the OLD CA")
    for name, chain in new["keystores"].items():
        leaf = chain[0] if chain else {}
        ochain = old["keystores"].get(name) or [{}]
        check(leaf.get("cn") == KEYSTORES[name] == ochain[0].get("cn"), f"keystore {name}: CN")
        check(leaf.get("san") == ochain[0].get("san"), f"keystore {name}: SAN {ochain[0].get('san')} -> {leaf.get('san')}")
        check(any(c["sha256_fingerprint"] == new_ca["sha256_fingerprint"] for c in chain[1:]),
              f"keystore {name}: chain lacks the new CA")
        check(_parse(leaf.get("not_after", "1970-01-01T00:00:00Z")) >= expiry_floor, f"keystore {name}: expiry")
    check(new["truststores"].get("truststore.jks") == [new_ca["sha256_fingerprint"]],
          "truststore.jks must hold exactly the new CA")
    check(new["truststores"].get("truststore-kn-v220.jks") == [new_ca["sha256_fingerprint"]],
          "truststore-kn-v220.jks must hold exactly the new CA")
    check(old["password_sha256"] == new["password_sha256"], "Kafka password files changed")
    for key, fps in old["client_ca_bundles"].items():
        expected = [new_ca["sha256_fingerprint"] if fp == old_ca["sha256_fingerprint"] else fp for fp in fps]
        check(new["client_ca_bundles"].get(key) == expected, f"{key}: client CA bundle is not old-with-CA-replaced")
    new_root = Path(new_env["QDL_TLS_KAFKA_DIR"]).parent
    for problem in perms_check(new_root):
        check(False, f"permissions: {problem}")
    result = {"schema": "qdl.mesh-tls-verify.v1", "verified_utc": now_utc(), "old": old_env["QDL_TLS_GENERATION"],
              "new": new_env["QDL_TLS_GENERATION"], "checks": checks, "problems": problems,
              "verdict": "PASS" if not problems else "FAIL",
              "old_ca": {k: old_ca[k] for k in ("sha256_fingerprint", "not_after")},
              "new_ca": {k: new_ca[k] for k in ("sha256_fingerprint", "not_before", "not_after")},
              "slots": {name: {"cn": new["slots"][name]["cn"], "san": new["slots"][name]["san"],
                               "eku": new["slots"][name]["eku"], "not_after": new["slots"][name]["not_after"],
                               "old_not_after": old["slots"][name]["not_after"]} for name in new["slots"]},
              "old_truststores": old["truststores"], "new_truststores": new["truststores"]}
    return result


def _parse(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


# ----------------------------------------------------------------------------------------------- main
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    e = sub.add_parser("export")
    e.add_argument("--gen-dir", type=Path, required=True)
    g = sub.add_parser("generate")
    g.add_argument("--gen-dir", type=Path, required=True)
    g.add_argument("--old-env", type=Path, required=True, help="generation.env of the deployed (exported) generation")
    g.add_argument("--days", type=int, default=DEFAULT_DAYS)
    h = sub.add_parser("harden", help="apply PERMS to an existing generation (bytes unchanged)")
    h.add_argument("--gen-dir", type=Path, required=True)
    c = sub.add_parser("perms", help="read-only permission check of a generation")
    c.add_argument("--gen-dir", type=Path, required=True)
    v = sub.add_parser("verify")
    v.add_argument("--old-env", type=Path, required=True)
    v.add_argument("--new-env", type=Path, required=True)
    v.add_argument("--min-days", type=int, default=300)
    v.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.mode == "export":
        doc = export(args.gen_dir)
        print(json.dumps({"exported": str(args.gen_dir), "stable_tls_copy_byte_equal": doc["stable_tls_copy_byte_equal"],
                          "slots": len(doc["slots"])}))
        return 0 if doc["stable_tls_copy_byte_equal"] else 2
    if args.mode == "generate":
        if not 30 <= args.days <= 825:
            raise SystemExit("--days must be within 30..825")
        doc = generate(args.gen_dir, read_env(args.old_env), args.days)
        print(json.dumps({"generated": str(args.gen_dir), "slots": len(doc["slots"]), "days": args.days}))
        return 0
    if args.mode == "harden":
        result = harden(args.gen_dir)
        print(json.dumps(result))
        return 0 if result["bytes_unchanged"] and not result["problems"] else 2
    if args.mode == "perms":
        require_root()
        problems = perms_check(args.gen_dir)
        print(json.dumps({"gen_dir": str(args.gen_dir), "problems": problems, "verdict": "PASS" if not problems else "FAIL"}))
        return 0 if not problems else 2
    require_root()
    result = verify(read_env(args.old_env), read_env(args.new_env), min_days=args.min_days)
    text = json.dumps(result, indent=1, sort_keys=True) + "\n"
    if args.out:
        args.out.write_text(text)
    print(json.dumps({"verdict": result["verdict"], "checks": result["checks"], "problems": result["problems"]}))
    return 0 if result["verdict"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
