from __future__ import annotations

from enum import StrEnum


class BarLifecycle(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    IN_PROGRESS = "IN_PROGRESS"
    FINAL = "FINAL"
    REVISED = "REVISED"
    CANCELLED = "CANCELLED"
