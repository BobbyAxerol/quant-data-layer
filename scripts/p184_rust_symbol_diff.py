#!/usr/bin/env python3
"""Symbol-level comparison of a rebuilt Rust release binary against the deployed one (dependency-bump equivalence).

Purpose
    A dependency bump (here rustls 0.23.45 -> 0.23.46) changes the crate metadata hash of every crate that depends
    on it, so whole-file and even loaded-section equality are lost although most code is unchanged. This tool says
    WHERE the binaries differ: per ELF section (``build_qdl_release_images.elf_sections``) and per function, from a
    normalized disassembly in which addresses, RIP displacements, Rust symbol hashes (``::h<16 hex>``), LTO suffixes
    (``.llvm.<n>``) and anonymous-constant hashes are masked, and every indirect GOT slot
    (``_GLOBAL_OFFSET_TABLE_+off``) is replaced by the symbol it is relocated to (``readelf -r`` GLOB_DAT/JUMP_SLOT
    name, or the RELATIVE addend when it is the start of a text symbol, else ``DATA``), so a function whose
    instructions and callees are the same compares equal wherever the linker placed it. A RIP-relative operand that
    addresses data (anything but the start of a text symbol) is reduced to ``<DATA>``; data bytes are compared at
    section level. Alignment padding after a function's last instruction is dropped, and a name is compared by its
    SET of distinct bodies (another identical copy of a monomorphization is an instance-count change, not a code
    change).

    Changed functions are classified by name: RUSTLS when the demangled name contains a rustls path (``rustls::``,
    ``tokio_rustls::``, ``hyper_rustls::``, also as a generic argument), OTHER otherwise. Every OTHER change is
    classified: CRATE_HASH_CONSTANTS when the bodies are equal once 64-bit ``movabs`` immediates are masked
    (``TypeId`` values derive from the crate metadata hash, which a dependency bump changes), CALLS_RUSTLS when the
    normalized diff references a rustls symbol, CODEGEN otherwise (same source, different machine code: inlining,
    register allocation, block layout). The bounded normalized diff of every OTHER change is kept. Verdict
    CONFINED_TO_RUSTLS needs no CODEGEN change and no added/removed OTHER name; otherwise NOT_CONFINED, which is a
    finding to report, not a tool failure (exit 0 either way).

    Limits (in every summary): see LIMITS.

Boundary
    Read-only on images: binaries are copied out with ``docker run --rm --network none --read-only`` and
    disassembled in the pinned builder image (binutils 2.40) with ``--network none`` and read-only mounts. It writes
    only below ``--out``. No runtime container, registry, Kafka/Redis/Postgres or git ref is touched.

usage: p184_rust_symbol_diff.py --release IMAGE --reference IMAGE --binary NAME [--binary NAME ...] --out DIR
"""
from __future__ import annotations

import argparse
import collections
import difflib
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import build_qdl_release_images as recipe  # noqa: E402

SCHEMA = "qdl.p184.rust-symbol-diff.v1"
BUILDER = recipe.BUILDER_IMAGE
HEADER_RE = re.compile(r"^([0-9a-f]+) <(.+)>:$")
INSN_RE = re.compile(r"^\s+[0-9a-f]+:\t(.*)$")
TARGET_RE = re.compile(r"\b[0-9a-f]+ <([^>]*)>")
RIP_RE = re.compile(r"-?0x[0-9a-f]+\(%rip\)")
COMMENT_ADDR_RE = re.compile(r"# [0-9a-f]+$")
HASH_RE = re.compile(r"::h[0-9a-f]{16}")
LLVM_RE = re.compile(r"\.llvm\.\d+")
ANON_RE = re.compile(r"anon\.[0-9a-f]{16,}\.")
GOT_RE = re.compile(r"<_GLOBAL_OFFSET_TABLE_\+0x([0-9a-f]+)>")
RELOC_RE = re.compile(r"^([0-9a-f]{16})\s+[0-9a-f]{16}\s+(R_X86_64_\w+)\s+(?:([0-9a-f]{16})\s+(.+?)\s+\+\s+([0-9a-f]+)|([0-9a-f]+))$")
NM_RE = re.compile(r"^([0-9a-f]{16}) (\w) (.+)$")
PADDING_RE = re.compile(r"^(?:(?:cs |data16 )*nop\w*\b|xchg\s+%ax,%ax|int3)")
MOVABS_RE = re.compile(r"movabs \$0x[0-9a-f]{9,16},")
RUSTLS_RE = re.compile(r"\b(?:tokio_|hyper_)?rustls::")
LIMITS = ("CODEGEN changes are same-source machine-code differences; the tool shows them, it does not prove "
          "them behaviour-equivalent",
          "RIP-relative data references (and GOT slots holding data addresses) are reduced to <DATA>: a change "
          "that only points an instruction at a different constant is not detected",
          "data sections are compared at section level only",
          "a function present under a new demangled name counts as removed + added")


def normalize_name(name: str) -> str:
    return ANON_RE.sub("anon.H.", LLVM_RE.sub("", HASH_RE.sub("", name)))


DATA_COMMENT_RE = re.compile(r"(\(%rip\).*# )<([^>]*)>")


def normalize_insn(text: str, got: dict[int, str] | None = None, text_names: set[str] | None = None) -> str:
    text = TARGET_RE.sub(lambda m: "<" + normalize_name(m.group(1)) + ">", text)
    if got is not None:
        text = GOT_RE.sub(lambda m: "<GOT:" + got.get(int(m.group(1), 16), "UNRESOLVED") + ">", text)
    if text_names is not None:
        text = DATA_COMMENT_RE.sub(lambda m: m.group(1) + ("<" + m.group(2) + ">" if m.group(2) in text_names
                                                            or m.group(2).startswith("GOT:") else "<DATA>"), text)
    text = RIP_RE.sub("X(%rip)", text)
    return COMMENT_ADDR_RE.sub("# X", text).rstrip()


def is_rustls(name: str) -> bool:
    return bool(RUSTLS_RE.search(name))


def text_symbol_names(nm_lines) -> set[str]:
    """Normalized names of text (code) symbols from ``nm -C`` (types t/T/W/w/i)."""
    out = set()
    for line in nm_lines:
        m = NM_RE.match(line.rstrip("\n"))
        if m and m.group(2) in "tTWwi":
            out.add(normalize_name(m.group(3)))
    return out


def got_map(readelf_lines, nm_lines) -> dict[int, str]:
    """GOT offset (from _GLOBAL_OFFSET_TABLE_) -> normalized target name (a text symbol, ``DATA`` for any other
    address, or the imported symbol name), from ``readelf -rW`` and ``nm -C``."""
    symbols, base, text = {}, None, set()
    for line in nm_lines:
        m = NM_RE.match(line.rstrip("\n"))
        if not m:
            continue
        addr, name = int(m.group(1), 16), m.group(3)
        if name == "_GLOBAL_OFFSET_TABLE_":
            base = addr
        if m.group(2) in "tTWwi":
            symbols.setdefault(addr, set()).add(normalize_name(name))
    if base is None:
        return {}

    def resolve(addr: int) -> str:
        return "|".join(sorted(symbols[addr])) if addr in symbols else "DATA"
    out = {}
    for line in readelf_lines:
        m = RELOC_RE.match(line.strip())
        if not m:
            continue
        offset = int(m.group(1), 16) - base
        if m.group(4):
            out[offset] = normalize_name(m.group(4).split("@")[0]) + (f"+{m.group(5)}" if m.group(5) != "0" else "")
        elif m.group(6):
            out[offset] = resolve(int(m.group(6), 16))
    return out


def functions(lines, got: dict[int, str] | None = None, text_names: set[str] | None = None):
    """(normalized name, normalized body lines) per function of an ``objdump -d -C --no-show-raw-insn`` stream."""
    name, body = None, []
    for raw in lines:
        line = raw.rstrip("\n")
        head = HEADER_RE.match(line)
        if head:
            if name is not None:
                yield name, strip_padding(body)
            name, body = normalize_name(head.group(2)), []
            continue
        insn = INSN_RE.match(line)
        if insn and name is not None:
            body.append(normalize_insn(insn.group(1), got, text_names))
    if name is not None:
        yield name, strip_padding(body)


def strip_padding(body: list[str]) -> list[str]:
    while body and PADDING_RE.match(body[-1]):
        body.pop()
    return body


def body_hash(body: list[str], mask_movabs: bool = False) -> str:
    text = "\n".join(body)
    if mask_movabs:
        text = MOVABS_RE.sub("movabs $IMM64,", text)
    return hashlib.sha256(text.encode()).hexdigest()


def digest_functions(lines, got: dict[int, str] | None = None, text_names: set[str] | None = None) -> dict:
    """name -> {"bodies": distinct body hashes, "masked": the same with 64-bit movabs immediates masked,
    "count": instances}."""
    bodies, masked, count = collections.defaultdict(set), collections.defaultdict(set), collections.Counter()
    for name, body in functions(lines, got, text_names):
        bodies[name].add(body_hash(body))
        masked[name].add(body_hash(body, mask_movabs=True))
        count[name] += 1
    return {name: {"bodies": sorted(bodies[name]), "masked": sorted(masked[name]), "count": count[name]}
            for name in bodies}


def compare_digests(release: dict, reference: dict) -> dict:
    common = set(release) & set(reference)
    changed = sorted(n for n in common if release[n]["bodies"] != reference[n]["bodies"])
    added = sorted(set(release) - set(reference))
    removed = sorted(set(reference) - set(release))

    def split(items):
        return {"rustls": [n for n in items if is_rustls(n)], "other": [n for n in items if not is_rustls(n)]}

    def total(side, rustls=None):
        return sum(v["count"] for n, v in side.items() if rustls is None or is_rustls(n) == rustls)
    return {"functions_release": total(release), "functions_reference": total(reference),
            "rustls_functions_release": total(release, True), "rustls_functions_reference": total(reference, True),
            "names_equal": len(common) - len(changed),
            "instance_count_changed": sorted(n for n in common if n not in changed
                                             and release[n]["count"] != reference[n]["count"]),
            "crate_hash_constants": sorted(n for n in changed if not is_rustls(n)
                                           and release[n]["masked"] == reference[n]["masked"]),
            "changed": split(changed), "added": split(added), "removed": split(removed)}


def other_diffs(release_lines, reference_lines, names: set[str], context: int = 2, limit: int = 60,
                release_sym=(None, None), reference_sym=(None, None)) -> dict:
    """Normalized unified diff (bounded) of every OTHER changed function; flags rustls references in the diff."""
    def bodies(lines, sym):
        out = collections.defaultdict(list)
        for name, body in functions(lines, *sym):
            if name in names:
                out[name].append(body)
        return out
    rel, ref = bodies(release_lines, release_sym), bodies(reference_lines, reference_sym)
    result = {}
    for name in sorted(names):
        a = ["\n---\n".join("\n".join(b) for b in sorted(ref.get(name, [])))]
        b = ["\n---\n".join("\n".join(x) for x in sorted(rel.get(name, [])))]
        diff = list(difflib.unified_diff(a[0].splitlines(), b[0].splitlines(), "reference", "release", n=context,
                                         lineterm=""))
        changed_lines = [d for d in diff[2:] if d[:1] in "+-"]
        result[name] = {"instances_reference": len(ref.get(name, [])), "instances_release": len(rel.get(name, [])),
                        "diff_lines": len(changed_lines),
                        "diff_mentions_rustls": any(is_rustls(d) for d in changed_lines),
                        "diff": diff[:limit]}
    return result


def run(argv: list[str], stdout=None) -> subprocess.CompletedProcess:
    return subprocess.run(argv, stdout=stdout or subprocess.PIPE, stderr=subprocess.PIPE, check=True)


def extract(image: str, binary: str, path: Path) -> None:
    with open(path, "wb") as out:
        run(recipe.extract_argv(image, binary), stdout=out)


def tool_lines(path: Path, *tool: str):
    """Stream a binutils tool's stdout over ``path`` run in the pinned builder (no network, read-only)."""
    argv = ["docker", "run", "--rm", "--pull=never", "--network", "none", "--read-only", "--cpus", "1",
            "-v", f"{path.parent}:/b:ro", "--entrypoint", tool[0], BUILDER, *tool[1:], f"/b/{path.name}"]
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1 << 20)
    try:
        yield from proc.stdout
    finally:
        proc.stdout.close()
        if proc.wait():
            raise RuntimeError(f"{tool[0]} failed for {path}")


def objdump_lines(path: Path):
    return tool_lines(path, "objdump", "-d", "-C", "--no-show-raw-insn")


def path_symbols(path: Path) -> tuple[dict[int, str], set[str]]:
    nm = list(tool_lines(path, "nm", "-C", "--defined-only"))
    return got_map(list(tool_lines(path, "readelf", "-rW")), nm), text_symbol_names(nm)


def section_report(release: bytes, reference: bytes) -> dict:
    rel, ref = recipe.elf_sections(release), recipe.elf_sections(reference)
    by_ref = {s["name"]: s for s in ref}
    rows = []
    for s in rel:
        other = by_ref.get(s["name"])
        rows.append({"name": s["name"], "loaded": s["loaded"], "size": s["size"],
                     "reference_size": other["size"] if other else None,
                     "equal": bool(other) and (s["size"], s["sha256"]) == (other["size"], other["sha256"])})
    return {"layout_equal": [(s["name"], s["type"], s["flags"]) for s in rel] ==
            [(s["name"], s["type"], s["flags"]) for s in ref],
            "only_reference": sorted(set(by_ref) - {s["name"] for s in rel}),
            "differing": [r["name"] for r in rows if not r["equal"]], "sections": rows}


def analyse(release_image: str, reference_image: str, binary: str, out: Path) -> dict:
    work = out / binary
    work.mkdir(parents=True, exist_ok=True)
    rel_path, ref_path = work / "release.bin", work / "reference.bin"
    extract(release_image, binary, rel_path)
    extract(reference_image, binary, ref_path)
    rel_bytes, ref_bytes = rel_path.read_bytes(), ref_path.read_bytes()
    report = {"binary": binary, "release_sha256": hashlib.sha256(rel_bytes).hexdigest(),
              "reference_sha256": hashlib.sha256(ref_bytes).hexdigest(),
              # rustls source paths compiled into panic locations / strings: present only with rustls code.
              "rustls_source_paths": {"release": len(re.findall(rb"rustls-0\.23\.\d+/src/", rel_bytes)),
                                      "reference": len(re.findall(rb"rustls-0\.23\.\d+/src/", ref_bytes))}}
    if report["release_sha256"] == report["reference_sha256"]:
        report.update(verdict="EQUAL")
    else:
        report["sections"] = section_report(rel_bytes, ref_bytes)
        rel_sym, ref_sym = path_symbols(rel_path), path_symbols(ref_path)
        report["got_slots"] = {"release": len(rel_sym[0]), "reference": len(ref_sym[0]),
                               "release_data": sum(v == "DATA" for v in rel_sym[0].values()),
                               "reference_data": sum(v == "DATA" for v in ref_sym[0].values())}
        cmp = compare_digests(digest_functions(objdump_lines(rel_path), *rel_sym),
                              digest_functions(objdump_lines(ref_path), *ref_sym))
        report["functions"] = cmp
        others = set(cmp["changed"]["other"])
        report["other_changed_detail"] = other_diffs(objdump_lines(rel_path), objdump_lines(ref_path), others,
                                                     release_sym=rel_sym, reference_sym=ref_sym)
        hashed = set(cmp["crate_hash_constants"])
        classes = {"CRATE_HASH_CONSTANTS": sorted(hashed), "CALLS_RUSTLS": [], "CODEGEN": []}
        for name, detail in report["other_changed_detail"].items():
            if name not in hashed:
                classes["CALLS_RUSTLS" if detail["diff_mentions_rustls"] else "CODEGEN"].append(name)
        report["other_changed_classes"] = classes
        clean = not (cmp["added"]["other"] or cmp["removed"]["other"] or classes["CODEGEN"])
        report["verdict"] = "CONFINED_TO_RUSTLS" if clean else "NOT_CONFINED"
    for path in (rel_path, ref_path):
        path.unlink()
    work.rmdir()
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--release", required=True, help="release image (id or tag)")
    parser.add_argument("--reference", required=True, help="deployed reference image (id or tag)")
    parser.add_argument("--binary", action="append", required=True, choices=recipe.RUST_BINARIES)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    summary = {"schema": SCHEMA, "release": args.release, "reference": args.reference, "builder": BUILDER,
               "limits": list(LIMITS), "binaries": {}}
    for binary in args.binary:
        report = analyse(args.release, args.reference, binary, args.out)
        (args.out / f"{binary}.json").write_text(json.dumps(report, indent=1, sort_keys=True))
        fn = report.get("functions", {})
        summary["binaries"][binary] = {
            "verdict": report["verdict"], "release_sha256": report["release_sha256"],
            "reference_sha256": report["reference_sha256"],
            "differing_sections": report.get("sections", {}).get("differing", []),
            "rustls_functions": [fn.get("rustls_functions_release"), fn.get("rustls_functions_reference")],
            "rustls_source_paths": report["rustls_source_paths"],
            "changed_rustls": len(fn.get("changed", {}).get("rustls", [])),
            "changed_other": len(fn.get("changed", {}).get("other", [])),
            "changed_other_by_class": {k: len(v) for k, v in report.get("other_changed_classes", {}).items()},
            "codegen_changed": report.get("other_changed_classes", {}).get("CODEGEN", []),
            "instance_count_changed": len(fn.get("instance_count_changed", [])),
            "added": {k: len(v) for k, v in fn.get("added", {}).items()},
            "removed": {k: len(v) for k, v in fn.get("removed", {}).items()},
            "added_other": fn.get("added", {}).get("other", []),
            "removed_other": fn.get("removed", {}).get("other", [])}
        print(json.dumps({binary: summary["binaries"][binary]}), flush=True)
    (args.out / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
