#!/usr/bin/env python3
"""Report where the running stack disagrees with its own declared configuration.

Read-only. It changes nothing and takes no confirmation flag, so it is safe to
run at any time, in CI, and as the first step of any rollout.

Why it exists. On 2026-09-17 three separate configuration drifts were found by
walking into them one at a time, and every one of them had been sitting there
for weeks:

* `binance_bar_edge` held a checkpoint at `catalog_revision 7` while the image
  produced 8 and every packet on disk produced 9. It had been up 21 hours and
  had simply never re-read its configuration. The next restart - a recreate, or
  a host reboot under `unless-stopped` - was always going to crash-loop, and it
  did.
* The Compose override chain recorded in the container labels omitted two image
  overrides, so a plain `compose up -d` would have rolled the stream and query
  roles back to a release two versions old.
* Three projectors ran at a CPU ceiling their Compose file did not declare, so
  the next recreate would have silently halved it.

The common shape: a process holds a generation of configuration that no longer
exists anywhere, and nothing notices until it restarts. These checks turn that
from an accident into an observation.

Each check prints one line per subject and exits non-zero if any check fails, so
it can gate a rollout. A check that cannot run - a stopped container, an
unreadable file - is reported as SKIP and does not fail the run, because an
unreadable subject is not evidence of agreement.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

PROJECT = "qdl_v2_stable_candidate"
COMPOSE_FILE = "docker-compose.v2-stable.yml"

OK = "OK"
FAIL = "FAIL"
SKIP = "SKIP"


@dataclass
class Finding:
    check: str
    subject: str
    status: str
    detail: str


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)

    def add(self, check: str, subject: str, status: str, detail: str) -> None:
        self.findings.append(Finding(check, subject, status, detail))

    @property
    def failed(self) -> int:
        return sum(1 for f in self.findings if f.status == FAIL)

    @property
    def skipped(self) -> int:
        return sum(1 for f in self.findings if f.status == SKIP)


def _run(command: Sequence[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"command failed: {command[0]}")
    return result.stdout


def running_roles() -> list[str]:
    prefix = f"{PROJECT}-"
    names = _run(["docker", "ps", "--format", "{{.Names}}"]).split()
    return sorted(
        name[len(prefix):-2] for name in names if name.startswith(prefix)
    )


def _inspect(role: str, template: str) -> str:
    return _run(
        ["docker", "inspect", "-f", template, f"{PROJECT}-{role}-1"]
    ).strip()


def _exec(role: str, *command: str) -> str:
    return _run(["docker", "exec", f"{PROJECT}-{role}-1", *command])


def check_cpu_ceilings(report: Report, compose: dict[str, Any], roles: Iterable[str]) -> None:
    """Compose's declared `cpus` against the cgroup quota the role actually has.

    A live `docker update --cpus` is invisible to Compose, so a ceiling raised to
    survive an incident silently reverts at the next recreate.
    """

    for role in roles:
        declared = compose.get(role, {}).get("cpus")
        if declared is None:
            report.add("cpu", role, SKIP, "compose declares no cpus")
            continue
        try:
            raw = _exec(role, "cat", "/sys/fs/cgroup/cpu.max").split()
        except RuntimeError as error:
            report.add("cpu", role, SKIP, str(error))
            continue
        if len(raw) != 2 or raw[0] == "max":
            report.add("cpu", role, SKIP, f"no quota: {' '.join(raw)}")
            continue
        live = int(raw[0]) / int(raw[1])
        if abs(float(declared) - live) > 0.01:
            report.add(
                "cpu", role, FAIL,
                f"compose {float(declared):.2f}, running {live:.2f}",
            )
        else:
            report.add("cpu", role, OK, f"{live:.2f}")


def check_images(report: Report, roles: Iterable[str]) -> None:
    """The image digest each role runs, against the tag it was created from.

    This does not prove the tag still resolves to that digest; it records both so
    a rollout can be checked against it.
    """

    for role in roles:
        try:
            digest = _inspect(role, "{{.Image}}")
            tag = _inspect(role, "{{.Config.Image}}")
        except RuntimeError as error:
            report.add("image", role, SKIP, str(error))
            continue
        report.add("image", role, OK, f"{tag} -> {digest[:19]}")


def check_compose_chain(
    report: Report, roles: Sequence[str], env_file: str | None = None
) -> None:
    """Whether replaying a role's own Compose chain would change the image it runs.

    Each role records the chain it was created from. That chain is what a later
    `compose up -d <role>` replays, and if an image override has since been added
    in a different rollout the replay resolves an older image and moves the role
    backwards. Asking the question directly - resolve the chain, compare the
    image - is precise; comparing chains to each other is not, because roles
    recreated in different rollouts legitimately carry different chains.
    """

    import yaml

    for role in roles:
        try:
            label = _inspect(
                role, '{{index .Config.Labels "com.docker.compose.project.config_files"}}'
            )
            running = _inspect(role, "{{.Image}}")
        except RuntimeError as error:
            report.add("chain", role, SKIP, str(error))
            continue
        files = [part.strip() for part in label.split(",") if part.strip()]
        if not files:
            report.add("chain", role, SKIP, "no recorded chain")
            continue
        missing = [path for path in files if not Path(path).is_file()]
        if missing:
            report.add(
                "chain", role, FAIL,
                "chain names files that no longer exist, so this role cannot be "
                "recreated from its own chain: "
                + ", ".join(sorted(m.rsplit("/", 1)[-1] for m in missing)),
            )
            continue
        command = ["docker", "compose"]
        if env_file:
            command += ["--env-file", env_file]
        for path in files:
            command += ["-f", path]
        command += ["config"]
        try:
            resolved = yaml.safe_load(_run(command)).get("services", {})
        except Exception as error:  # compose needs env this context may lack
            report.add("chain", role, SKIP, f"cannot resolve: {str(error)[:60]}")
            continue
        declared = resolved.get(role, {}).get("image")
        if declared is None:
            report.add("chain", role, SKIP, "chain declares no image for this role")
            continue
        # The chain names a tag; the container records a digest. Resolve the tag
        # so a role is not reported as drifted against its own image.
        try:
            declared_digest = _run(
                ["docker", "image", "inspect", "-f", "{{.Id}}", str(declared)]
            ).strip()
        except RuntimeError:
            declared_digest = str(declared)
        if declared_digest != running:
            report.add(
                "chain", role, FAIL,
                f"replaying this role's own chain resolves {str(declared)} "
                f"({declared_digest[:19]}), running {running[:19]}",
            )
        else:
            report.add("chain", role, OK, f"{len(files)} files resolve to the running image")


def check_bar_edge_checkpoint(report: Report, role: str = "binance_bar_edge") -> None:
    """The bar edge's durable checkpoint against the catalog the role now loads.

    This is the exact failure that crash-looped the edge: a checkpoint written
    under a catalog generation that no configuration on the host still produces.
    The edge refuses to start rather than publish against a catalog it does not
    recognise, which is correct, so the drift only surfaces at restart.
    """

    try:
        env = _inspect(role, "{{range .Config.Env}}{{println .}}{{end}}").splitlines()
    except RuntimeError as error:
        report.add("bar-checkpoint", role, SKIP, str(error))
        return
    settings = dict(
        line.split("=", 1) for line in env if "=" in line
    )
    state_path = settings.get("QDL_STABLE_BAR_STATE_PATH")
    bindings = settings.get("QDL_STABLE_SOURCE_BINDINGS")
    if not state_path or not bindings:
        report.add("bar-checkpoint", role, SKIP, "role declares no bar state path")
        return
    try:
        payload = json.loads(_exec(role, "cat", state_path))
    except RuntimeError:
        report.add(
            "bar-checkpoint", role, OK,
            "no checkpoint yet; the next start bootstraps",
        )
        return
    except json.JSONDecodeError as error:
        report.add("bar-checkpoint", role, FAIL, f"checkpoint is not JSON: {error}")
        return
    script = (
        "from qdl.runtime.stable_catalog import StableSourceCatalog;"
        f"print(StableSourceCatalog.load({bindings!r}).catalog_revision)"
    )
    try:
        live = int(_exec(role, "python3", "-c", script).strip())
    except (RuntimeError, ValueError) as error:
        report.add("bar-checkpoint", role, SKIP, f"cannot read the live catalog: {error}")
        return
    stored = payload.get("catalog_revision")
    if stored != live:
        report.add(
            "bar-checkpoint", role, FAIL,
            f"checkpoint catalog_revision {stored}, runtime {live}; "
            "this role will crash-loop on its next restart",
        )
    else:
        report.add("bar-checkpoint", role, OK, f"catalog_revision {live}")


def check_bar_owners(report: Report, role: str = "query_v2_1", stale_seconds: int = 600) -> None:
    """Whether every BAR partition in the durable spool is still receiving records.

    Ownership cannot be inferred from the acquisition mode alone: the bar edge
    filters by mode for OKX but not for Binance, so a mode-based rule reports
    working bindings as broken. Observation is the honest test. A BAR partition
    whose newest record is older than the longest interval it carries has no
    component publishing it, and nothing else in the stack says so - the edge is
    healthy, the ingestor is healthy, and the data simply stops.
    """

    script = "\n".join((
        "import sqlite3, time, collections, json",
        "c = sqlite3.connect('file:/var/lib/qdl-stable/shared/canonical-cache.sqlite3?mode=ro', uri=True)",
        "c.execute('PRAGMA temp_store=MEMORY')",
        "now = time.time() * 1e9",
        "q = \"select partition_key, max(committed_at_ns) from events where partition_key like '%/bar/%' group by partition_key\"",
        "rows = [(k, (now - m) / 1e9) for k, m in c.execute(q) if m]",
        "g = collections.defaultdict(list)",
        "for k, a in rows: g[k.split('/')[-1].split('-')[0]].append(a)",
        "print(json.dumps({k: [len(v), min(v), max(v)] for k, v in g.items()}))",
    ))
    try:
        groups = json.loads(_exec(role, "python3", "-c", script).strip())
    except (RuntimeError, ValueError, json.JSONDecodeError) as error:
        report.add("bar-owner", "spool", SKIP, f"cannot read the spool: {str(error)[:70]}")
        return
    if not groups:
        report.add("bar-owner", "spool", SKIP, "no BAR partitions in the spool")
        return
    for venue, (count, newest, oldest) in sorted(groups.items()):
        if newest > stale_seconds:
            report.add(
                "bar-owner", venue, FAIL,
                f"{count} BAR partitions, newest record {newest:.0f}s old; "
                "no component is publishing these",
            )
        else:
            report.add(
                "bar-owner", venue, OK,
                f"{count} partitions, newest {newest:.0f}s, oldest {oldest:.0f}s",
            )


def load_compose(path: str) -> dict[str, Any]:
    import yaml

    with open(path) as handle:
        return yaml.safe_load(handle).get("services", {})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose", default=COMPOSE_FILE)
    parser.add_argument(
        "--env-file",
        help="env file Compose needs to resolve a chain; without it the chain "
             "check reports SKIP rather than guessing",
    )
    parser.add_argument("--json", action="store_true", help="emit findings as JSON")
    parser.add_argument(
        "--only", action="append",
        help="run only these checks (cpu, image, chain, bar-checkpoint, bar-owner)",
    )
    args = parser.parse_args(argv)

    report = Report()
    try:
        roles = running_roles()
    except RuntimeError as error:
        print(f"cannot list running roles: {error}", file=sys.stderr)
        return 2
    if not roles:
        print("no running roles for project " + PROJECT, file=sys.stderr)
        return 2

    compose = load_compose(args.compose)
    wanted = set(args.only or ["cpu", "image", "chain", "bar-checkpoint", "bar-owner"])
    if "cpu" in wanted:
        check_cpu_ceilings(report, compose, roles)
    if "image" in wanted:
        check_images(report, roles)
    if "chain" in wanted:
        check_compose_chain(report, roles, args.env_file)
    if "bar-checkpoint" in wanted:
        check_bar_edge_checkpoint(report)
    if "bar-owner" in wanted:
        check_bar_owners(report)

    if args.json:
        print(json.dumps([f.__dict__ for f in report.findings], indent=1))
    else:
        width = max(len(f.check) for f in report.findings) if report.findings else 4
        for finding in report.findings:
            if finding.status == OK and not args.only:
                continue
            print(f"{finding.status:<5}{finding.check:<{width + 2}}{finding.subject:<26}{finding.detail}")
        print(
            f"\n{len(report.findings)} checks, {report.failed} failed, "
            f"{report.skipped} skipped over {len(roles)} running roles"
        )
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
