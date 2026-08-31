from __future__ import annotations

import unittest

from qdl.control.operator_env import require_control_admin_dsn


class OperatorEnvironmentTests(unittest.TestCase):
    def test_prefers_runtime_alias_when_only_runtime_is_declared(self) -> None:
        self.assertEqual(
            require_control_admin_dsn({"QDL_CONTROL_ADMIN_DSN": "postgresql://runtime"}),
            "postgresql://runtime",
        )

    def test_accepts_sealed_stable_bundle_alias(self) -> None:
        self.assertEqual(
            require_control_admin_dsn(
                {"QDL_STABLE_CONTROL_ADMIN_DSN": "postgresql://stable"}
            ),
            "postgresql://stable",
        )

    def test_rejects_conflicting_dual_aliases_without_echoing_values(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "aliases disagree") as caught:
            require_control_admin_dsn(
                {
                    "QDL_CONTROL_ADMIN_DSN": "postgresql://one",
                    "QDL_STABLE_CONTROL_ADMIN_DSN": "postgresql://two",
                }
            )
        self.assertNotIn("postgresql://one", str(caught.exception))
        self.assertNotIn("postgresql://two", str(caught.exception))

    def test_rejects_missing_alias(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "is required"):
            require_control_admin_dsn({})


if __name__ == "__main__":
    unittest.main()
