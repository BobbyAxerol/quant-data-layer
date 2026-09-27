import base64
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from scripts.verify_runtime_dependencies import verify


class DependencyImageTests(unittest.TestCase):
    IMAGE = "qdl-v2-python@sha256:" + "a" * 64

    def check(self, *, version="1.0", content=b"original", marker=None, hashed=True):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "module.py"
            path.write_bytes(content)
            item = SimpleNamespace(hash=SimpleNamespace(mode="sha256", value=base64.urlsafe_b64encode(
                hashlib.sha256(b"original").digest()).rstrip(b"=").decode()) if hashed else None)
            dist = SimpleNamespace(version=version, files=[item], locate_file=lambda _: path)
            lock = {"package": [{"name": "example", "version": "1.0", "groups": ["main"]}]}
            if marker:
                lock["package"].append({"name": "platform-only", "version": "1.0", "groups": ["main"], "markers": marker})
            return verify(lock, self.IMAGE, lambda name: dist if name == "example" else self.fail("wrong platform"))

    def test_pinned_versions_and_record_hashes(self):
        result = self.check(marker='python_version < "2"')
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["verified_files"], 1)

    def test_mutated_files_are_refused(self):
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            self.check(content=b"modified")

    def test_wrong_version_is_refused(self):
        with self.assertRaisesRegex(ValueError, "version mismatch"):
            self.check(version="2.0")

    def test_missing_record_hashes_are_refused(self):
        with self.assertRaisesRegex(ValueError, "empty RECORD"):
            self.check(hashed=False)

    def test_unpinned_or_empty_input_is_refused(self):
        with self.assertRaisesRegex(ValueError, "digest-pinned"):
            verify({"package": []}, "qdl-v2-python:latest")
        with self.assertRaisesRegex(ValueError, "empty main"):
            verify({"package": []}, self.IMAGE)
