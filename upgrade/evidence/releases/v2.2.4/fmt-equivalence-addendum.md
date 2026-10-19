# v2.2.4 addendum: `cargo fmt` of qdl-stream-gateway does not change the deployed binaries

Date: 2026-10-18. Scope: PR #33 CI fix. No image rolled, no runtime action. Host lock `claude-dl-fmt-equiv` held for the
build and released.

## What changed

Commit `f6ce816` (`style(stream-gateway): apply cargo fmt`) reformats 6 files of `rust/qdl-stream-gateway`
(`src/hub.rs`, `src/replay.rs`, `src/service.rs`, `src/subscription.rs`, `examples/hot_path_profile.rs`,
`tests/native_stream.rs`) with `cargo fmt -p qdl-stream-gateway` in the pinned builder image
`sha256:551428d98db6b6523b3515fe040d7f9ed53abf625a78e0a1413f4e2056883817` (`--network none`). Whitespace and line
breaks only; `cargo fmt --check -p qdl-stream-gateway` passes afterwards.

## Method

`git archive f6ce816` mounted at `/src`, the same builder, flags and command as the fourth cargo invocation of
`scripts/build_qdl_release_images.py` (`CARGO_COMMANDS[3]`: `cargo build --offline --locked --release -p qdl-projector
--bin qdl-projector -p qdl-stream-gateway --bin qdl-stream-gateway`), `CARGO_HOME=/work/cargo`, `CARGO_BUILD_JOBS=2`,
a fresh `CARGO_TARGET_DIR` (full rebuild, 4m24s). Reference: the binaries inside
`qdl-v2-rust:2.2.4-c7ce7d3` = `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`
(the image the stream gateways and projectors run), extracted with `docker run --rm --network none --read-only`.
Rust sources between `c7ce7d3` and `f6ce816` differ in `qdl-kafka/src/bin/qdl-native-raw-ingestor.rs` (not part of
these binaries) and the fmt commit.

## Result

| Binary | Whole-file sha256 (rebuilt = deployed) | Verdict |
| --- | --- | --- |
| `qdl-projector` | `52ad5e33628e3ba4bf3d0c04d27147bff16b86afe576c4333c9a2845e00020d9` both | **EQUAL** |
| `qdl-stream-gateway` | rebuilt `539582ac...1ed4`, deployed `c07bfdef...a76f9` (13 953 192 vs 13 953 144 bytes) | differs, section analysis below |

Section analysis of `qdl-stream-gateway` (`loaded_section_verdict` helper plus the checks below; 34 sections, same names,
types and flags). Of the 29 loaded (SHF_ALLOC) sections 27 are byte-equal, including `.text` (8 202 831 B),
`.rodata` (1 134 020 B) and `.rela.dyn`. The differing sections are exactly:

1. `.note.gnu.build-id`: the linker hash over the output file (allowed, never executed).
2. `.symtab` / `.strtab` (not loaded): the only difference is the `.llvm.<n>` LTO-partition suffix of 780 symbol
   names; with that suffix stripped the multisets of 19 230 symbol names are identical.
3. `.data.rel.ro` (loaded, same address and size 407 776 B): 74 four-byte words differ. Each was resolved through
   `.rela.dyn` to its `core::panic::Location` (file pointer, file length, line, column) and the file string in
   `.rodata`: 69 `line` fields and 5 `column` fields, all in `rust/qdl-stream-gateway/src/{service,replay,hub,subscription}.rs`
   (service 54 line + 5 column, replay 11, hub 3, subscription 1); 0 differing words are unclassified. These are the
   expected panic-location line numbers moved by the reformatting; no pointer, length or other data word differs.

Verdict: **EQUIVALENT.** Executable code, read-only data and relocations are byte-identical; the only loaded bytes that
differ are panic-location line/column numbers (strings printed in a panic message), plus the build-id note. Nothing
else differs, so there is no behaviour change and no reason to re-certify or re-roll either binary.
