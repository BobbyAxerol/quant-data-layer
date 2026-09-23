"""The Query reader's interpreter settings (v2.1.1 Phase-3 stage-5 evidence)."""

from __future__ import annotations

import gc
import sys
import unittest

from qdl.runtime.stable import (
    QUERY_GC_THRESHOLDS,
    QUERY_GIL_SWITCH_INTERVAL_SECONDS,
    configure_query_interpreter,
    freeze_query_startup_heap,
)


class QueryInterpreterTests(unittest.TestCase):
    def test_query_reader_shortens_the_gil_switch_interval_and_raises_gc_thresholds(self):
        original_interval, original_thresholds = sys.getswitchinterval(), gc.get_threshold()
        try:
            configure_query_interpreter()
            self.assertAlmostEqual(sys.getswitchinterval(), QUERY_GIL_SWITCH_INTERVAL_SECONDS)
            self.assertLess(QUERY_GIL_SWITCH_INTERVAL_SECONDS, 0.005)
            self.assertEqual(gc.get_threshold(), QUERY_GC_THRESHOLDS)
            self.assertGreater(QUERY_GC_THRESHOLDS[0], 700)
        finally:
            sys.setswitchinterval(original_interval)
            gc.set_threshold(*original_thresholds)

    def test_startup_heap_is_frozen_out_of_later_sweeps(self):
        try:
            freeze_query_startup_heap()
            self.assertGreater(gc.get_freeze_count(), 0)
        finally:
            gc.unfreeze()


if __name__ == "__main__":
    unittest.main()
