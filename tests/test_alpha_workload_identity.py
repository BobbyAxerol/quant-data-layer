from __future__ import annotations

import importlib.util
from pathlib import Path
import os
import stat
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/materialize_alpha_v2_identity.py"


def _module():
    spec = importlib.util.spec_from_file_location("alpha_workload_identity", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AlphaWorkloadIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.identity = _module()

    def _sources(self, root: Path) -> tuple[Path, Path]:
        tls = root / "tls-source"
        jwt = root / "jwt-source"
        tls.mkdir()
        jwt.mkdir()
        for name in ("ca.crt", "client.crt", "client.key"):
            (tls / name).write_text(f"{name}\n", encoding="utf-8")
        for name in ("private.key", "public.pem"):
            (jwt / name).write_text(f"{name}\n", encoding="utf-8")
        (tls / "client.key").chmod(0o440)
        (jwt / "private.key").chmod(0o440)
        return tls, jwt

    def test_dry_run_is_secret_free_and_does_not_write(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            tls, jwt = self._sources(root)
            state = root / "state"
            state.mkdir()
            result = self.identity.materialize_identity(
                tls_source=tls,
                jwt_source=jwt,
                state_root=state,
                workload_id="alpha.binance.paper",
                workload_uid=1000,
                workload_gid=1000,
                apply=False,
            )
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertFalse((state / "workload-identities").exists())
            self.assertFalse(result["secret_values_recorded"])
            self.assertNotIn("private.key", str(result))

    @unittest.skipUnless(os.geteuid() == 0, "apply ownership test requires root")
    def test_apply_is_atomic_and_private_for_declared_workload(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            tls, jwt = self._sources(root)
            state = root / "state"
            state.mkdir()
            result = self.identity.materialize_identity(
                tls_source=tls,
                jwt_source=jwt,
                state_root=state,
                workload_id="alpha.okx.paper",
                workload_uid=1000,
                workload_gid=1000,
                apply=True,
            )
            target = Path(result["target"])
            self.assertTrue(target.is_dir())
            self.assertFalse(any(path.name.startswith(".") for path in target.parent.iterdir()))
            self.assertEqual(stat.S_IMODE((target / "tls" / "client.key").stat().st_mode), 0o400)
            self.assertEqual(stat.S_IMODE((target / "jwt" / "private.key").stat().st_mode), 0o400)
            self.assertEqual(stat.S_IMODE((target / "tls" / "ca.crt").stat().st_mode), 0o444)
            self.assertEqual((target / "tls" / "client.key").stat().st_uid, 1000)
            self.assertEqual((target / "jwt" / "private.key").stat().st_gid, 1000)
            self.assertEqual(stat.S_IMODE((target / "tls").stat().st_mode), 0o500)
            self.assertEqual(stat.S_IMODE((target / "jwt").stat().st_mode), 0o500)
            self.assertEqual(stat.S_IMODE((tls / "client.key").stat().st_mode), 0o440)

    def test_rejects_symlink_or_unexpected_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            tls, jwt = self._sources(root)
            state = root / "state"
            state.mkdir()
            (tls / "extra.pem").write_text("extra\n", encoding="utf-8")
            with self.assertRaisesRegex(self.identity.IdentityMaterializationError, "files differ"):
                self.identity.materialize_identity(
                    tls_source=tls,
                    jwt_source=jwt,
                    state_root=state,
                    workload_id="alpha.binance.paper",
                    workload_uid=1000,
                    workload_gid=1000,
                    apply=False,
                )
            (tls / "extra.pem").unlink()
            (jwt / "link").symlink_to(jwt / "public.pem")
            with self.assertRaisesRegex(self.identity.IdentityMaterializationError, "non-regular"):
                self.identity.materialize_identity(
                    tls_source=tls,
                    jwt_source=jwt,
                    state_root=state,
                    workload_id="alpha.binance.paper",
                    workload_uid=1000,
                    workload_gid=1000,
                    apply=False,
                )


if __name__ == "__main__":
    unittest.main()
