#!/usr/bin/env python3
"""P18.4 stage-100 foreign-activity guard: an apparatus-only sidecar next to scripts/p184_gate_campaign.py.

Purpose (owner 2026-10-17, after the 105025Z run was tainted by local jobs a 30 s watch missed): watch the host at
1 s resolution for any process that belongs neither to the campaign nor to the production containers, record each one
(pid, user, cmdline, start, end, CPU), mark the campaign TAINTED when one overlaps the load (driver_started ..
driver_exit of the campaign timeline) and, with --abort, stop the campaign early instead of wasting the window.
It also runs two report-only samplers for correlation: rust_core progress lines (commit_ms / commit_wait_ms, at the
cadence the core prints them) and the Kafka log-cleaner phase per broker (log-cleaner.log).

Boundary: outside the scored metrics. It never changes a gate rule, threshold, budget, observer or addendum and does
not touch the campaign directory except reading timeline.jsonl / driver.pid / g6-ts/probe.jsonl; its output goes to
the sibling directory <campaign-dir>.guard/. Read-only on production: /proc reads, `docker events`, `docker logs -f`
of the three cores, `docker exec <kafka> tail -F` of the cleaner log, `docker inspect` of containers that appear.
The only actions are on the campaign's own processes, and only on a taint inside the load with --abort: SIGINT to the
driver PID (the same stop path guard_v2.sh uses), then SIGINT to the campaign process (its finally stops every child),
then SIGTERM of the campaign's own in-container G6 probe by the PID it printed and removal of the campaign's own G4
observer container by name. Nothing else is signalled, restarted or removed. Self-bounded by --deadline-seconds.

Classification (host processes = processes outside docker-*.scope cgroups; container processes are production or
campaign by container):
* exempt: descendants of --campaign-root (the run script that starts the campaign, this guard and the G11 watcher;
  labelled campaign / apparatus), docker/containerd plumbing (dockerd, containerd, shims, runc, docker-proxy/init),
  kernel threads;
* own_session: descendants of --own-session-pid (the coordinating agent's session); recorded, report-only;
* resident: host processes alive when the guard started; recorded only when one uses > --cpu-core core in a 1 s
  sample (report-only);
* foreign (TAINT when it overlaps the load): a new host process that is a docker CLI of any kind, or one of
  ssh/scp/sftp/rsync/tar/curl/wget/pip/uv/cargo/rustc/pytest/python, or another agent's command wrapper
  (codex, codex-linux-sandbox, bwrap), or any new host process using > --cpu-core core
  in a 1 s sample; a new container that is neither in the baseline nor the campaign's (name prefix or a mount of the
  campaign worktree / campaign directory; one that is gone before `docker inspect` is recorded as unknown);
* other_new: any other new host process (recorded, report-only).
usage: p184_foreign_activity_guard.py --campaign-dir DIR --campaign-root PID --run-root PATH [--own-session-pid PID]
       [--abort] [--deadline-seconds N]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
PLUMBING = {"dockerd", "containerd", "containerd-shim", "containerd-shim-runc-v2", "runc", "docker-proxy",
            "docker-init", "runc:[0:PARENT]", "runc:[1:CHILD]", "runc:[2:INIT]"}
DOCKER_CLI = {"docker", "docker-compose", "docker-buildx", "com.docker.cli"}
TOOLS = {"ssh", "scp", "sftp", "rsync", "tar", "curl", "wget", "pip", "pip3", "uv", "uvx", "cargo", "rustc",
         "pytest", "py.test"}
AGENT_EXEC = {"codex", "codex-linux-sandbox", "bwrap"}  # another agent's command execution wrappers
PYTHON = re.compile(r"^python(\d+(\.\d+)?)?$")
DOCKER_SCOPE = re.compile(r"docker-([0-9a-f]{64})\.scope")
CORES = ("qdl_v2_stable_candidate-rust_core-1", "qdl_v2_stable_candidate-rust_core_2-1",
         "qdl_v2_stable_candidate-rust_core_3-1")
BROKERS = ("qdl_v2_stable_candidate-kafka1-1", "qdl_v2_stable_candidate-kafka2-1", "qdl_v2_stable_candidate-kafka3-1")
CAMPAIGN_CONTAINER_PREFIXES = ("qdl-phase3-target-", "qdl-p184-g4obs-")
CLEANER_LINE = re.compile(r"^\[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})\] INFO (?P<msg>.*)$")
CLEANER_PHASES = (
    (re.compile(r"Beginning cleaning of log (?P<log>\S+?)\.? "), "begin"),
    (re.compile(r"Building offset map for (?:log )?(?P<log>[^\s.]+(?:\.[^\s.]+)*?)(?:\.\.\.| for )"), "offset_map"),
    (re.compile(r"Offset map for log (?P<log>\S+) complete"), "offset_map_done"),
    (re.compile(r"Cleaning log (?P<log>\S+) \(cleaning prior"), "cleaning"),
    (re.compile(r"Swapping in cleaned segment .* in log Log\(dir=\S+/(?P<log>[^/,]+),"), "swap"),
    (re.compile(r"Log cleaner thread \d+ cleaned log (?P<log>\S+)"), "done"),
)


def utc(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + f".{int((ts % 1) * 1000):03d}Z"


def boot_time() -> float:
    for line in Path("/proc/stat").read_text().splitlines():
        if line.startswith("btime "):
            return float(line.split()[1])
    raise RuntimeError("no btime in /proc/stat")


def parse_stat(text: str) -> dict:
    """/proc/<pid>/stat: comm may contain spaces/parens, so split at the last ')'."""
    head, _, rest = text.rpartition(")")
    fields = rest.split()
    return {"comm": head.split("(", 1)[1], "state": fields[0], "ppid": int(fields[1]),
            "cpu_ticks": int(fields[11]) + int(fields[12]), "start_ticks": int(fields[19])}


def read_proc(pid: int) -> dict | None:
    base = Path("/proc") / str(pid)
    try:
        stat = parse_stat((base / "stat").read_text())
        cmdline = (base / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
        cgroup = (base / "cgroup").read_text()
        uid = None
        for line in (base / "status").read_text().splitlines():
            if line.startswith("Uid:"):
                uid = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        return None
    match = DOCKER_SCOPE.search(cgroup)
    return {**stat, "pid": pid, "cmdline": cmdline[:600], "uid": uid,
            "container_id": match.group(1) if match else None, "kernel": stat["ppid"] == 2 or pid == 2}


def argv0_name(info: dict) -> str:
    first = (info.get("cmdline") or "").split(" ", 1)[0]
    return os.path.basename(first) if first else info.get("comm", "")


def classify_tool(info: dict) -> str | None:
    """Pattern class of a host process, or None. Docker CLI first: any docker invocation counts."""
    names = {info.get("comm", ""), argv0_name(info)}
    if names & DOCKER_CLI:
        return "docker_cli"
    if names & PLUMBING:
        return None
    agent = names & AGENT_EXEC
    if agent:
        return f"agent_exec:{sorted(agent)[0]}"
    for name in names:
        if name in TOOLS:
            return f"tool:{name}"
        if PYTHON.match(name):
            return "tool:python"
    return None


def cleaner_phase(line: str) -> dict | None:
    match = CLEANER_LINE.match(line.strip())
    if not match:
        return None
    for pattern, phase in CLEANER_PHASES:
        found = pattern.search(match.group("msg"))
        if found:
            return {"ts": match.group("ts"), "phase": phase, "log": found.group("log").rstrip(".")}
    return None


def core_progress(line: str) -> dict | None:
    try:
        item = json.loads(line)
    except ValueError:
        return None
    if not isinstance(item, dict) or item.get("event") != "qdl_realtime_core_progress":
        return None
    out = {}
    for key in ("commit_ms", "commit_wait_ms", "commit_hidden_ms", "collect_ms"):
        value = item.get(key) or {}
        out[key] = {k: value.get(k) for k in ("max", "mean", "n")}
    out["batches"] = item.get("batches")
    return out


class Guard:
    def __init__(self, args):
        self.args = args
        self.campaign_dir = Path(args.campaign_dir).resolve()
        self.out = Path(str(self.campaign_dir) + ".guard")
        self.out.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.btime = boot_time()
        self.me = os.getpid()
        self.known: dict[int, dict] = {}       # pid -> record (host processes and their classification)
        self.containers: dict[str, dict] = {}  # container id -> {name, class}
        self.foreign: list[dict] = []
        self.tainted = False
        self.aborted = False
        self.stop = threading.Event()
        self.followers: list[subprocess.Popen] = []
        self.phase = {"driver_started": None, "driver_exit": None, "g11_armed": None, "g11_exit": None}

    # ---- output -------------------------------------------------------------------------------------------------
    def write(self, name: str, item: dict) -> None:
        with self.lock, (self.out / name).open("a") as handle:
            handle.write(json.dumps(item, sort_keys=True, default=str) + "\n")

    # ---- process tree ---------------------------------------------------------------------------------------------
    def ancestry_label(self, pid: int, ppid: int) -> str | None:
        seen, current = 0, ppid
        chain = [pid]
        while current > 1 and seen < 64:
            if current == self.me:
                return "apparatus"
            if current == self.args.campaign_root:
                return "campaign"
            if self.args.own_session_pid and current == self.args.own_session_pid:
                return "own_session"
            chain.append(current)
            known = self.known.get(current)
            if known is not None:
                if known["label"] in ("campaign", "apparatus", "own_session"):
                    return known["label"]
                current = known["ppid"]
            else:
                info = read_proc(current)
                if info is None:
                    return None
                current = info["ppid"]
            seen += 1
        return None

    def start_ts(self, info: dict) -> float:
        return self.btime + info["start_ticks"] / CLK_TCK

    def discover(self, *, baseline: bool) -> None:
        now = time.time()
        try:
            pids = {int(p) for p in os.listdir("/proc") if p.isdigit()}
        except OSError:
            return
        for pid in pids - self.known.keys():
            info = read_proc(pid)
            if info is None:
                continue
            record = {"pid": pid, "ppid": info["ppid"], "comm": info["comm"], "cmdline": info["cmdline"],
                      "uid": info["uid"], "start": self.start_ts(info), "start_ticks": info["start_ticks"],
                      "cpu_ticks": info["cpu_ticks"], "container_id": info["container_id"], "seen": now,
                      "class": None, "label": None, "max_core": 0.0, "cpu_flagged": False}
            if info["kernel"]:
                record["label"] = "kernel"
            elif info["container_id"]:
                record["label"] = "container"
                self.note_container(info["container_id"], now, baseline=baseline)
            elif pid == self.me or pid == self.args.campaign_root:
                record["label"] = "apparatus" if pid == self.me else "campaign"
            elif {info["comm"], argv0_name(info)} & PLUMBING:
                record["label"] = "plumbing"
            else:
                label = self.ancestry_label(pid, info["ppid"])
                if label:
                    record["label"] = label
                elif baseline:
                    record["label"] = "resident"
                else:
                    record["label"] = "new"
                    record["class"] = classify_tool(info)
            self.known[pid] = record
            if record["label"] == "own_session" and not baseline:
                self.write("own_session.jsonl", {"pid": pid, "ppid": info["ppid"], "cmdline": info["cmdline"],
                                                 "start": utc(record["start"]), "seen": utc(now)})
            if record["label"] == "new":
                kind = "foreign" if record["class"] else "other_new"
                self.flag(record, kind, record["class"] or "other", now)
        for pid in list(self.known.keys() - pids):
            record = self.known.pop(pid)
            if record.get("flagged"):
                self.write("processes.jsonl", {"event": "end", "pid": pid, "end": utc(now),
                                               "max_core": round(record["max_core"], 3), "kind": record["flagged"],
                                               "cmdline": record["cmdline"]})
                for item in self.foreign:
                    if item["pid"] == pid and item.get("end") is None and item["start_ticks"] == record["start_ticks"]:
                        item["end"] = now
                        item["max_core"] = round(record["max_core"], 3)

    def flag(self, record: dict, kind: str, reason: str, now: float) -> None:
        record["flagged"] = kind
        entry = {"event": "start", "kind": kind, "reason": reason, "pid": record["pid"], "ppid": record["ppid"],
                 "uid": record["uid"], "comm": record["comm"], "cmdline": record["cmdline"],
                 "start": utc(record["start"]), "seen": utc(now), "phase": self.phase_name(now)}
        self.write("processes.jsonl", entry)
        if kind == "foreign":
            self.foreign.append({**entry, "start_ts": record["start"], "start_ticks": record["start_ticks"],
                                 "end": None, "max_core": 0.0})
            self.check_taint(now)

    # ---- containers -------------------------------------------------------------------------------------------------
    def note_container(self, cid: str, now: float, *, baseline: bool) -> None:
        if cid in self.containers:
            return
        name, mounts, inspected = None, [], False
        try:
            result = subprocess.run(["docker", "inspect", "--format", "{{.Name}}|{{range .Mounts}}{{.Source}};{{end}}",
                                     cid], capture_output=True, text=True, timeout=10)
            name, _, joined = result.stdout.strip().partition("|")
            name = name.lstrip("/")
            mounts = [m for m in joined.split(";") if m]
            inspected = result.returncode == 0 and bool(name)
        except (OSError, subprocess.TimeoutExpired):
            pass
        if baseline:
            cls = "baseline"
        elif name and name.startswith(CAMPAIGN_CONTAINER_PREFIXES):
            cls = "campaign"
        elif any(m.startswith(str(self.args.run_root)) or m.startswith(str(self.campaign_dir)) for m in mounts):
            cls = "campaign"
        elif not inspected:
            cls = "unknown"  # gone before inspect; a foreign `docker run` is still caught by its host docker CLI
        else:
            cls = "foreign"
        self.containers[cid] = {"name": name, "class": cls}
        if not baseline:
            entry = {"event": "container", "class": cls, "id": cid[:12], "name": name, "mounts": mounts[:8],
                     "seen": utc(now), "phase": self.phase_name(now)}
            self.write("processes.jsonl", entry)
            if cls == "foreign":
                self.foreign.append({**entry, "kind": "foreign", "reason": "container", "pid": None,
                                     "start_ts": now, "start_ticks": None, "end": None, "max_core": 0.0})
                self.check_taint(now)

    # ---- CPU ----------------------------------------------------------------------------------------------------------
    def sample_cpu(self, interval: float) -> None:
        now = time.time()
        for pid, record in list(self.known.items()):
            if record["label"] in ("container", "kernel", "plumbing", "campaign", "apparatus"):
                continue
            try:
                stat = parse_stat((Path("/proc") / str(pid) / "stat").read_text())
            except (OSError, ValueError, IndexError):
                continue
            if stat["start_ticks"] != record["start_ticks"]:
                continue
            delta = (stat["cpu_ticks"] - record["cpu_ticks"]) / CLK_TCK
            record["cpu_ticks"] = stat["cpu_ticks"]
            core = delta / interval if interval > 0 else 0.0
            record["max_core"] = max(record["max_core"], core)
            if core > self.args.cpu_core:
                self.write("cpu.jsonl", {"pid": pid, "label": record["label"], "core": round(core, 3),
                                         "at": utc(now), "cmdline": record["cmdline"][:200]})
                if record["label"] == "new" and record.get("flagged") != "foreign":
                    record["class"] = "cpu"
                    if record.get("flagged"):
                        self.write("processes.jsonl", {"event": "reclassified", "pid": pid, "kind": "foreign",
                                                       "reason": f"cpu>{self.args.cpu_core}", "at": utc(now)})
                    self.flag(record, "foreign", f"cpu>{self.args.cpu_core} core ({core:.2f})", now)

    # ---- campaign phase / taint / abort --------------------------------------------------------------------------------
    def read_timeline(self) -> None:
        path = self.campaign_dir / "timeline.jsonl"
        try:
            lines = path.read_text().splitlines()
        except OSError:
            return
        for line in lines:
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if item.get("kind") in self.phase and self.phase[item["kind"]] is None:
                self.phase[item["kind"]] = item["at_ns"] / 1e9

    def phase_name(self, now: float) -> str:
        p = self.phase
        if p["driver_started"] is None:
            return "pre_load"
        if p["driver_exit"] is None or now <= p["driver_exit"]:
            return "load"
        if p["g11_exit"] is None:
            return "post_load_g11"
        return "post_g11"

    def overlaps_load(self, item: dict, now: float) -> bool:
        start_load = self.phase["driver_started"]
        if start_load is None:
            return False
        end_load = self.phase["driver_exit"] or now
        end = item.get("end") or now
        return item["start_ts"] <= end_load and end >= start_load

    def check_taint(self, now: float) -> None:
        if self.tainted:
            return
        hits = [f for f in self.foreign if self.overlaps_load(f, now)]
        if not hits:
            return
        self.tainted = True
        marker = {"verdict": "TAINTED", "at": utc(now), "phase": self.phase_name(now),
                  "first": {k: hits[0].get(k) for k in ("kind", "reason", "pid", "cmdline", "name", "start", "seen")}}
        (self.out / "TAINTED").write_text(json.dumps(marker, sort_keys=True) + "\n")
        self.write("guard.jsonl", {"event": "TAINTED", **marker})
        if self.args.abort and self.phase["driver_exit"] is None and not self.aborted:
            threading.Thread(target=self.abort, daemon=True).start()

    def campaign_pid(self) -> int | None:
        for pid, record in self.known.items():
            if record["label"] == "campaign" and "p184_gate_campaign.py" in record["cmdline"] and \
                    " run " in f" {record['cmdline']} " and not record["cmdline"].startswith("timeout"):
                return pid
        return None

    def abort(self) -> None:
        """Stop the campaign early through its own stop paths; only campaign-owned processes/containers."""
        self.aborted = True
        log = []
        try:
            driver = int((self.campaign_dir / "driver.pid").read_text().strip())
        except (OSError, ValueError):
            driver = None
        if driver:
            try:
                os.kill(driver, signal.SIGINT)
                log.append(f"SIGINT driver {driver}")
            except OSError as exc:
                log.append(f"driver {driver}: {exc}")
            deadline = time.time() + 120
            while time.time() < deadline:
                try:
                    os.kill(driver, 0)
                except OSError:
                    break
                time.sleep(1)
        campaign = self.campaign_pid()
        if campaign:
            try:
                os.kill(campaign, signal.SIGINT)
                log.append(f"SIGINT campaign {campaign}")
            except OSError as exc:
                log.append(f"campaign {campaign}: {exc}")
            deadline = time.time() + 150
            while time.time() < deadline:
                try:
                    os.kill(campaign, 0)
                except OSError:
                    break
                time.sleep(1)
        probe = self.campaign_dir / "g6-ts" / "probe.jsonl"
        try:
            pid = json.loads(probe.read_text().splitlines()[0]).get("pid")
        except (OSError, ValueError, IndexError, AttributeError):
            pid = None
        if isinstance(pid, int) and pid > 1:
            result = subprocess.run(["docker", "exec", self.args.writer, "python", "-B", "-c",
                                     f"import os, signal; os.kill({pid}, signal.SIGTERM)"],
                                    capture_output=True, text=True, timeout=30)
            log.append(f"g6 probe in-container pid {pid} SIGTERM rc={result.returncode}")
        prefix = f"qdl-p184-g4obs-{self.campaign_dir.name}-"
        names = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True,
                               timeout=30).stdout.split()
        for name in names:
            if name.startswith(prefix):
                result = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=60)
                log.append(f"removed observer {name} rc={result.returncode}")
        (self.out / "ABORTED").write_text(json.dumps({"at": utc(time.time()), "actions": log}) + "\n")
        self.write("guard.jsonl", {"event": "ABORTED", "actions": log, "at": utc(time.time())})

    # ---- report-only followers ---------------------------------------------------------------------------------------
    def follow(self, name: str, command: list[str], parse, out_name: str) -> None:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                   stdin=subprocess.DEVNULL)
        self.followers.append(process)

        def reader():
            for line in process.stdout:
                if self.stop.is_set():
                    break
                parsed = parse(line)
                if parsed is not None:
                    self.write(out_name, {"src": name, "rx": utc(time.time()), **parsed})
        threading.Thread(target=reader, daemon=True).start()

    def start_followers(self) -> None:
        for core in CORES:
            self.follow(core.split("-")[1], ["docker", "logs", "-f", "--since", "1s", core], core_progress,
                        "cores.jsonl")
        for broker in BROKERS:
            self.follow(broker.split("-")[1], ["docker", "exec", broker, "tail", "-n", "0", "-F",
                                               "/opt/kafka/logs/log-cleaner.log"], cleaner_phase, "cleaner.jsonl")

        def events_parse(line):
            try:
                item = json.loads(line)
            except ValueError:
                return None
            actor = (item.get("Actor") or {}).get("Attributes") or {}
            return {"type": item.get("Type"), "action": item.get("Action"), "name": actor.get("name"),
                    "image": actor.get("image"), "id": (item.get("id") or "")[:12],
                    "time": utc((item.get("timeNano") or 0) / 1e9)}
        self.follow("docker_events", ["docker", "events", "--format", "{{json .}}"], events_parse, "docker_events.jsonl")

    # ---- host -----------------------------------------------------------------------------------------------------------
    def host_sample(self, previous: dict | None) -> dict:
        forks = None
        for line in Path("/proc/stat").read_text().splitlines():
            if line.startswith("processes "):
                forks = int(line.split()[1])
        plumbing = sum(r["cpu_ticks"] for r in self.known.values() if r["label"] == "plumbing")
        sample = {"t": time.time(), "forks": forks, "plumbing_ticks": plumbing,
                  "load1": float(Path("/proc/loadavg").read_text().split()[0])}
        if previous:
            sample["forks_per_s"] = forks - previous["forks"]
        return sample

    def refresh_plumbing_ticks(self) -> None:
        for pid, record in self.known.items():
            if record["label"] != "plumbing":
                continue
            try:
                record["cpu_ticks"] = parse_stat((Path("/proc") / str(pid) / "stat").read_text())["cpu_ticks"]
            except (OSError, ValueError, IndexError):
                pass

    # ---- main loop --------------------------------------------------------------------------------------------------
    def run(self) -> int:
        started = time.time()
        self.write("guard.jsonl", {"event": "start", "at": utc(started), "pid": self.me,
                                   "campaign_root": self.args.campaign_root, "own_session": self.args.own_session_pid,
                                   "abort": self.args.abort, "cpu_core": self.args.cpu_core,
                                   "scan_s": self.args.scan_seconds, "cpu_s": self.args.cpu_seconds})
        self.discover(baseline=True)
        resident = [{"pid": r["pid"], "uid": r["uid"], "cmdline": r["cmdline"][:200]}
                    for r in self.known.values() if r["label"] == "resident"]
        (self.out / "baseline.json").write_text(json.dumps(
            {"at": utc(started), "resident_host_processes": resident,
             "containers": sorted(c["name"] or "" for c in self.containers.values())}, indent=1) + "\n")
        self.start_followers()
        deadline = started + self.args.deadline_seconds
        last_cpu = time.monotonic()
        previous_host = self.host_sample(None)
        finished_at = None
        while time.time() < deadline:
            self.read_timeline()
            self.discover(baseline=False)
            mono = time.monotonic()
            if mono - last_cpu >= self.args.cpu_seconds:
                self.sample_cpu(mono - last_cpu)
                self.refresh_plumbing_ticks()
                host = self.host_sample(previous_host)
                host_out = {"at": utc(host["t"]), "load1": host["load1"], "forks_per_s": host.get("forks_per_s"),
                            "plumbing_core": round((host["plumbing_ticks"] - previous_host["plumbing_ticks"])
                                                   / CLK_TCK / (mono - last_cpu), 3), "phase": self.phase_name(host["t"])}
                self.write("host.jsonl", host_out)
                previous_host = host
                last_cpu = mono
                self.check_taint(time.time())
            if (self.campaign_dir / "scorecard.json").exists() or (self.out / "ABORTED").exists():
                finished_at = finished_at or time.time()
                if time.time() - finished_at > self.args.linger_seconds:
                    break
            elif not self.campaign_alive():
                finished_at = finished_at or time.time()
                if time.time() - finished_at > self.args.linger_seconds:
                    break
            time.sleep(self.args.scan_seconds)
        self.stop.set()
        for process in self.followers:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
        now = time.time()
        summary = {"schema": "qdl.p184.foreign-guard.v1", "verdict": "TAINTED" if self.tainted else "CLEAN",
                   "aborted": self.aborted, "started": utc(started), "ended": utc(now),
                   "phase_ns": {k: (int(v * 1e9) if v else None) for k, v in self.phase.items()},
                   "foreign": [{k: v for k, v in f.items() if k not in ("start_ticks",)} for f in self.foreign],
                   "foreign_overlapping_load": sum(1 for f in self.foreign if self.overlaps_load(f, now)),
                   "rule": "foreign = new host process (docker CLI any kind | ssh/scp/sftp/rsync/tar/curl/wget/pip/uv/"
                           "cargo/rustc/pytest/python | codex/codex-linux-sandbox/bwrap | "
                           f"> {self.args.cpu_core} core in a {self.args.cpu_seconds} s "
                           "sample) or new non-campaign container, outside the campaign tree and own session; TAINTED "
                           "when one overlaps driver_started..driver_exit"}
        (self.out / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True, default=str) + "\n")
        print(json.dumps({"verdict": summary["verdict"], "aborted": self.aborted, "out": str(self.out)}))
        return 0

    def campaign_alive(self) -> bool:
        try:
            os.kill(self.args.campaign_root, 0)
            return True
        except OSError:
            return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--campaign-dir", required=True)
    parser.add_argument("--campaign-root", type=int, required=True, help="PID of the script that runs the campaign")
    parser.add_argument("--run-root", required=True, help="DL run worktree (campaign containers mount it)")
    parser.add_argument("--own-session-pid", type=int, default=0)
    parser.add_argument("--writer", default="market_data_service_p184w")
    parser.add_argument("--abort", action="store_true")
    parser.add_argument("--cpu-core", type=float, default=0.2)
    parser.add_argument("--cpu-seconds", type=float, default=1.0)
    parser.add_argument("--scan-seconds", type=float, default=0.05,
                        help="new-PID scan period (short docker CLI calls live ~0.1 s)")
    parser.add_argument("--linger-seconds", type=float, default=20.0)
    parser.add_argument("--deadline-seconds", type=float, default=9000.0)
    args = parser.parse_args(argv)
    if not 0.5 <= args.cpu_seconds <= 2.0:
        raise SystemExit("--cpu-seconds must be within 1-2 s resolution (0.5..2.0)")
    return Guard(args).run()


if __name__ == "__main__":
    sys.exit(main())
