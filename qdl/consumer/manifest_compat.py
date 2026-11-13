"""Whether a consumer manifest revision is purely additive, and the token accept-set it may declare.

DL v2.3.0. Python (``qdl/security/data_plane.py``) and Rust
(``qdl-stream-gateway`` ``auth.rs``/``authority.rs``) refused every token whose
``consumer_manifest_revision`` was not exactly the active manifest revision
(401). That made each manifest bump a same-window coupling with the consumer's
JWT (CLAUDE.md section 4.1). Interfaces here are versioned and additive, and
readers accept several versions: so a manifest may declare
``metadata.min_token_revision`` and the readers accept any token revision in
``min_token_revision..revision``.

The declaration is only sound if every revision in that range grants a subset
of the current one. This module is the one place that decides it:

* ``additive_problems(old, new)``: empty when ``new`` keeps the identity
  (consumer, owner, subject, environment, sdk, execution dependency, rollback
  contract) and grants a superset (purposes, permissions, every requirement
  row, every quota at least as large);
* ``next_min_token_revision(previous, new)``: the renderer's rule. A purely
  additive successor keeps the previous floor; anything else resets the floor
  to the new revision, which is exactly the pre-v2.3.0 behaviour (same-window
  token bump, CLAUDE.md section 4.1 unchanged for non-additive changes).

Boundary: pure comparison of loaded manifests; no I/O.
"""

from __future__ import annotations

from qdl.consumer.manifest import ConsumerManifest

_IDENTITY_FIELDS = (
    "consumer_id", "owner", "subject", "environment", "sdk_major",
    "execution_dependency", "rollback_contract",
)
_QUOTA_FIELDS = (
    "requests_per_minute", "max_batch_items", "max_warmup_rows", "max_streams",
    "max_buffer_events",
)


def additive_problems(old: ConsumerManifest, new: ConsumerManifest) -> list[str]:
    """Reasons ``new`` is not a pure superset of ``old`` (empty = additive)."""

    problems: list[str] = []
    for name in _IDENTITY_FIELDS:
        if getattr(old, name) != getattr(new, name):
            problems.append(f"{name} changed")
    if new.manifest_revision <= old.manifest_revision:
        problems.append("revision did not increase")
    removed_purposes = old.allowed_purposes - new.allowed_purposes
    if removed_purposes:
        problems.append("purposes removed: " + ",".join(sorted(str(item) for item in removed_purposes)))
    removed_permissions = old.allowed_permissions - new.allowed_permissions
    if removed_permissions:
        problems.append("permissions removed: " + ",".join(sorted(removed_permissions)))
    for name in _QUOTA_FIELDS:
        if getattr(new.quotas, name) < getattr(old.quotas, name):
            problems.append(f"quota {name} lowered")
    removed_rows = set(old.requirements) - set(new.requirements)
    if removed_rows:
        problems.append(f"requirements removed or changed: {len(removed_rows)}")
    return problems


def is_additive(old: ConsumerManifest, new: ConsumerManifest) -> bool:
    return not additive_problems(old, new)


def next_min_token_revision(previous: ConsumerManifest | None, new: ConsumerManifest) -> int:
    """The accept-set floor a rendered successor may declare."""

    if previous is None or not is_additive(previous, new):
        return new.manifest_revision
    floor = previous.accepted_token_revisions.start
    return min(floor, new.manifest_revision)


def declared_floor_problems(previous: ConsumerManifest | None, new: ConsumerManifest) -> list[str]:
    """Gate check: the floor ``new`` declares is no lower than the rule allows."""

    allowed = next_min_token_revision(previous, new)
    declared = new.accepted_token_revisions.start
    if declared < allowed:
        reasons = [] if previous is None else additive_problems(previous, new)
        return [
            f"{new.consumer_id} r{new.manifest_revision} accepts tokens from r{declared} "
            f"but may accept only from r{allowed}"
            + (f" ({'; '.join(reasons)})" if reasons else "")
        ]
    return []
