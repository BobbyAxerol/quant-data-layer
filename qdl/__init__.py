"""Venue-neutral QDL V2 domain packages.

The existing ``app`` package remains the authoritative V1 compatibility
surface until an explicitly approved feed-by-feed cutover.
"""

from pathlib import Path
from pkgutil import extend_path


# Generated Protobuf packages live under ``generated/python/qdl``. Extending
# the namespace keeps generated files immutable while allowing handwritten
# domain modules to share the stable ``qdl`` package prefix.
__path__ = extend_path(__path__, __name__)
_generated_package = Path(__file__).resolve().parents[1] / "generated" / "python" / "qdl"
if _generated_package.is_dir():
    __path__.append(str(_generated_package))
