"""Small Python-version compatibility primitives used by the internal core."""

from __future__ import annotations

from enum import Enum

try:  # Python 3.11+
    from enum import StrEnum as StrEnum
except ImportError:  # Python 3.10 remains a supported SDK/compiler runtime.
    class StrEnum(str, Enum):
        """Backport the observable behavior needed from :class:`enum.StrEnum`."""

        def __str__(self) -> str:
            return str(self.value)

        @staticmethod
        def _generate_next_value_(
            name: str,
            start: int,
            count: int,
            last_values: list[object],
        ) -> str:
            del start, count, last_values
            return name.lower()


__all__ = ["StrEnum"]
