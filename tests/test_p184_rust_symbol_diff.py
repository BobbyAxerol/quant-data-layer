"""Unit tests for scripts/p184_rust_symbol_diff.py: name/instruction normalization, classification and comparison.
No docker call: the objdump stream is a literal."""
from __future__ import annotations

import unittest

from scripts import p184_rust_symbol_diff as d

REFERENCE = """
/b/x:     file format elf64-x86-64

Disassembly of section .text:

0000000000401000 <qdl_kafka::run::h0123456789abcdef>:
  401000:\tpush   %rbp
  401001:\tlea    0x1234(%rip),%rdi        # 402235 <anon.0123456789abcdef0123456789abcdef.7.llvm.42>
  401008:\tcall   405000 <rustls::conn::ConnectionCommon<T>::process_new_packets::hfedcba9876543210>
  40100d:\tjne    401000 <qdl_kafka::run::h0123456789abcdef>
  401010:\tret

0000000000401020 <qdl_core::parse::h1111111111111111>:
  401020:\tmov    $0x10,%eax
  401025:\tret

0000000000405000 <rustls::conn::ConnectionCommon<T>::process_new_packets::hfedcba9876543210>:
  405000:\tret
"""


def shifted(text: str) -> str:
    """Same code placed elsewhere with new crate hashes (what a dependency bump does to unchanged functions)."""
    return (text.replace("0123456789abcdef", "aaaaaaaaaaaaaaaa").replace("401", "501")
            .replace("0x1234(%rip)", "0x5678(%rip)").replace("h1111111111111111", "h2222222222222222"))


class NormalizationTests(unittest.TestCase):
    def test_names_lose_hash_lto_suffix_and_anon_hash(self):
        self.assertEqual(d.normalize_name("a::b::h0123456789abcdef.llvm.17"), "a::b")
        self.assertEqual(d.normalize_name("anon.0123456789abcdef0123456789abcdef.7"), "anon.H.7")

    def test_instruction_addresses_and_rip_displacements_are_masked_but_immediates_kept(self):
        self.assertEqual(d.normalize_insn("call   405000 <x::y::h0123456789abcdef>"), "call   <x::y>")
        self.assertEqual(d.normalize_insn("lea    -0x20(%rip),%rdi        # 4011e0"), "lea    X(%rip),%rdi        # X")
        self.assertEqual(d.normalize_insn("mov    $0x10,%eax"), "mov    $0x10,%eax")

    def test_rustls_classification_includes_generic_arguments(self):
        self.assertTrue(d.is_rustls("rustls::conn::f"))
        self.assertTrue(d.is_rustls("tokio_rustls::client::TlsStream<T>::poll"))
        self.assertTrue(d.is_rustls("core::ptr::drop_in_place<rustls::client::ClientConnection>"))
        self.assertFalse(d.is_rustls("rustls_pki_types::CertificateDer::from"))
        self.assertFalse(d.is_rustls("qdl_kafka::run"))


class CompareTests(unittest.TestCase):
    def test_relocated_unchanged_code_compares_equal(self):
        ref = d.digest_functions(REFERENCE.splitlines())
        rel = d.digest_functions(shifted(REFERENCE).splitlines())
        cmp = d.compare_digests(rel, ref)
        self.assertEqual(cmp["changed"], {"rustls": [], "other": []})
        self.assertEqual((cmp["added"], cmp["removed"]), ({"rustls": [], "other": []}, {"rustls": [], "other": []}))
        self.assertEqual(cmp["functions_release"], 3)

    def test_padding_and_a_duplicate_identical_instance_are_not_code_changes(self):
        text = REFERENCE.replace("  401025:\tret\n", "  401025:\tret\n  401026:\tcs nopw 0x0(%rax,%rax,1)\n")
        text += "\n0000000000406000 <qdl_core::parse::h3333333333333333>:\n  406000:\tmov    $0x10,%eax\n  406005:\tret\n"
        cmp = d.compare_digests(d.digest_functions(text.splitlines()), d.digest_functions(REFERENCE.splitlines()))
        self.assertEqual(cmp["changed"], {"rustls": [], "other": []})
        self.assertEqual(cmp["instance_count_changed"], ["qdl_core::parse"])

    def test_a_typeid_constant_change_is_classified_crate_hash(self):
        ref = REFERENCE.replace("  401020:\tmov    $0x10,%eax", "  401020:\tmovabs $0x765afbdd65119cb0,%rcx")
        rel = ref.replace("0x765afbdd65119cb0", "0xf40ed4b012558f36")
        cmp = d.compare_digests(d.digest_functions(rel.splitlines()), d.digest_functions(ref.splitlines()))
        self.assertEqual(cmp["changed"]["other"], ["qdl_core::parse"])
        self.assertEqual(cmp["crate_hash_constants"], ["qdl_core::parse"])

    def test_an_immediate_change_outside_rustls_is_other_and_has_no_rustls_reference(self):
        text = shifted(REFERENCE).replace("$0x10", "$0x11")
        cmp = d.compare_digests(d.digest_functions(text.splitlines()), d.digest_functions(REFERENCE.splitlines()))
        self.assertEqual(cmp["changed"]["other"], ["qdl_core::parse"])
        self.assertEqual(cmp["crate_hash_constants"], [])
        detail = d.other_diffs(text.splitlines(), REFERENCE.splitlines(), {"qdl_core::parse"})
        self.assertFalse(detail["qdl_core::parse"]["diff_mentions_rustls"])
        self.assertEqual(detail["qdl_core::parse"]["diff_lines"], 2)

    def test_a_rustls_change_is_classified_rustls(self):
        text = REFERENCE.replace("  405000:\tret", "  405000:\tnop\n  405001:\tret")
        cmp = d.compare_digests(d.digest_functions(text.splitlines()), d.digest_functions(REFERENCE.splitlines()))
        self.assertEqual(cmp["changed"]["rustls"], ["rustls::conn::ConnectionCommon<T>::process_new_packets"])
        self.assertEqual(cmp["changed"]["other"], [])

    def test_inlined_rustls_call_change_in_a_caller_is_flagged(self):
        text = REFERENCE.replace("process_new_packets::hfedcba9876543210>\n  40100d",
                                 "read_tls::hfedcba9876543210>\n  40100d")
        detail = d.other_diffs(text.splitlines(), REFERENCE.splitlines(), {"qdl_kafka::run"})
        self.assertTrue(detail["qdl_kafka::run"]["diff_mentions_rustls"])


if __name__ == "__main__":
    unittest.main()
