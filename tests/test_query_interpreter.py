"""The Query reader's interpreter settings (v2.1.1 Phase-3 stage-5 evidence)."""

from __future__ import annotations

import sys
import unittest

from qdl.runtime.stable import QUERY_GIL_SWITCH_INTERVAL_SECONDS, configure_query_interpreter


class QueryInterpreterTests(unittest.TestCase):
    def test_query_reader_shortens_the_gil_switch_interval(self):
        original = sys.getswitchinterval()
        try:
            configure_query_interpreter()
            self.assertAlmostEqual(sys.getswitchinterval(), QUERY_GIL_SWITCH_INTERVAL_SECONDS)
            self.assertLess(QUERY_GIL_SWITCH_INTERVAL_SECONDS, 0.005)
        finally:
            sys.setswitchinterval(original)


if __name__ == "__main__":
    unittest.main()
