from __future__ import annotations

import unittest

from qdl.runtime.stable import _env_flag


class PassThroughFlagTests(unittest.TestCase):
    """A flag that governs whether a data product is served must never guess."""

    NAME = "QDL_STABLE_PASS_THROUGH_ENABLED"

    def test_absent_or_blank_uses_the_declared_default(self):
        for env in ({}, {NAME_: ""}, {NAME_: "   "}):
            with self.subTest(env=env):
                self.assertFalse(_env_flag(env, NAME_, default=False))
                self.assertTrue(_env_flag(env, NAME_, default=True))

    def test_recognised_true_and_false_spellings(self):
        for raw in ("1", "true", "TRUE", "Yes", " on "):
            with self.subTest(raw=raw):
                self.assertTrue(_env_flag({NAME_: raw}, NAME_, default=False))
        for raw in ("0", "false", "FALSE", "No", " off "):
            with self.subTest(raw=raw):
                self.assertFalse(_env_flag({NAME_: raw}, NAME_, default=True))

    def test_an_unrecognised_value_fails_loudly(self):
        # A misspelling must not quietly disable the product it was meant to
        # enable, nor quietly enable one it was meant to disable.
        for raw in ("ture", "enabled", "2", "off!", "y"):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError) as caught:
                    _env_flag({NAME_: raw}, NAME_, default=False)
                self.assertIn(NAME_, str(caught.exception))


NAME_ = "QDL_STABLE_PASS_THROUGH_ENABLED"


if __name__ == "__main__":
    unittest.main()
