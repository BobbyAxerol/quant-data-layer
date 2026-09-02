from __future__ import annotations

import importlib
from enum import auto
import unittest

from qdl._compat import StrEnum


class Python310CompatTests(unittest.TestCase):
    def test_string_enum_keeps_wire_value_behavior(self) -> None:
        class Sample(StrEnum):
            ACTIVE = "ACTIVE"

        self.assertIsInstance(Sample.ACTIVE, str)
        self.assertEqual(Sample.ACTIVE, "ACTIVE")
        self.assertEqual(str(Sample.ACTIVE), "ACTIVE")
        self.assertEqual(Sample.ACTIVE.value, "ACTIVE")

    def test_auto_value_matches_stdlib_str_enum(self) -> None:
        class Sample(StrEnum):
            LIVE = auto()

        self.assertIsInstance(Sample.LIVE, str)
        self.assertEqual(Sample.LIVE.value, "live")
        self.assertEqual(str(Sample.LIVE), "live")

    def test_internal_string_enum_modules_import_through_shared_compat(self) -> None:
        modules = (
            "qdl.canary.phase9",
            "qdl.canary.phase92",
            "qdl.canary.phase93",
            "qdl.certification.phase103_consumer_acceptance",
            "qdl.certification.prerequisites",
            "qdl.consumer.manifest",
            "qdl.consumer.realtime_route",
            "qdl.consumer.universal_release",
            "qdl.data_quality.engine",
            "qdl.data_quality.source_authority",
            "qdl.domain.lifecycle",
            "qdl.domain.quantity",
            "qdl.query.contracts",
            "qdl.query.entitlement",
            "qdl.query.lifecycle",
            "qdl.security.data_plane",
            "qdl.warmup.contracts",
        )
        for name in modules:
            with self.subTest(module=name):
                importlib.import_module(name)
