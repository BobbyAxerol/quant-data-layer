"""A liveness file a role rewrites while it is still doing its work.

R1.31. On 2026-09-19 two of three projectors stopped mid-recovery and stayed
`Up` for eleven minutes with nothing to notice it: Docker restarts a process
that exits and has no opinion about one that is alive and idle. The ingestors
already publish `session-liveness` files for their provider sessions, so they
could be checked; the bar edge writes its state only when a checkpoint moves,
which for the intervals it still owns can legitimately be hours apart, and the
projector wrote nothing at all.

This is the missing half: a file whose mtime means "the loop ran". A container
healthcheck reads its age and nothing else, so it needs no port, no client and
no parsing.

Boundary: it says the loop is turning. It does not say the data is correct, is
fresh at the venue, or is reaching a consumer - those are measured elsewhere and
must not be inferred from this.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path


def write_heartbeat(path: str | os.PathLike[str], *, role: str, detail: str = "") -> None:
    """Rewrite the heartbeat file atomically. Never raises into a serving loop.

    A heartbeat that crashes the role it watches is worse than no heartbeat, so
    every failure here is swallowed: the file simply stops advancing, which is
    exactly the signal the healthcheck is looking for anyway.
    """
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({
            "schema": "qdl.role-heartbeat.v1",
            "role": role,
            "detail": detail,
            "updated_at_ns": time.time_ns(),
            "pid": os.getpid(),
        })
        handle, temporary = tempfile.mkstemp(dir=str(target.parent), prefix=".hb-")
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(payload)
            os.replace(temporary, target)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise
    except Exception:  # noqa: BLE001 - a heartbeat must never break its own role
        return
