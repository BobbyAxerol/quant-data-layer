"""Fail-closed observer checks; no release certificate or synthesized evidence."""


def evaluate(observation):
    from scripts.p184_dr05_dl_fault import schedule

    venue, fault = observation["venue"], observation["fault"]
    expected = [step["phase"] for step in schedule(venue, fault)]
    rows = observation.get("observations", [])
    checks = {"exact_phase_roster": [row.get("phase") for row in rows] == expected}
    if not checks["exact_phase_roster"]:
        return {"ok": False, "checks": checks}
    phases = {row["phase"]: row for row in rows}
    for phase in ("positive_control", "return_control", "post_fence_control"):
        row = phases[phase]
        snapshot = row["views"]["BOOK_SNAPSHOT"]["GetSnapshot"]
        checks[phase + ".reader_usable"] = snapshot.get("code") == "OK" and snapshot.get("cursor_valid") is True
        matches = row["canonical"].get("matched", [])
        checks[phase + ".canonical_match"] = any(
            value.get("kind") == "canonical" and value.get("venue") == venue
            and value.get("capture_id") == row["publication"]["capture_id"] for value in matches)
        live = row.get("ts_live", {})
        values = [value for value in live.get("results", []) if value.get("feed") == "BOOK_SNAPSHOT"]
        checks[phase + ".ts_live_usable"] = live.get("live") is True and len(values) == 1 and values[0].get("accepted") is True
    negative = "gap" if fault == "GAP_REFUSAL" else "new_generation_unanchored"
    row = phases[negative]
    checks[negative + ".canonical_refused"] = any(
        value.get("kind") == "quarantine" and value.get("reason") == "QUARANTINE_REASON_SEQUENCE_GAP"
        and value.get("capture_id") == row["publication"]["capture_id"]
        for value in row["canonical"].get("matched", []))
    checks[negative + ".feed_roster"] = set(row["views"]) == {"BOOK_SNAPSHOT", "BOOK_DELTA"}
    for feed, view in row["views"].items():
        status = view["GetFeedStatus"].get("status", {})
        snapshot = view["GetSnapshot"]
        checks[negative + "." + feed + ".not_eligible"] = status.get("execution_eligible") is not True
        checks[negative + "." + feed + ".snapshot_refused"] = snapshot.get("code") == "FAILED_PRECONDITION"
        # Expiring the old price by waiting is not proof of gap propagation.
        reason = snapshot.get("detail", "")
        checks[negative + "." + feed + ".fault_typed"] = (
            status.get("gap_open") is True or any(word in reason for word in ("GAP", "GENERATION", "DATA_NOT_READY")))
    live = row.get("ts_live", {})
    values = [value for value in live.get("results", []) if value.get("feed") == "BOOK_SNAPSHOT"]
    checks[negative + ".ts_live_refused"] = live.get("live") is True and len(values) == 1 and values[0].get("accepted") is False and values[0].get("outcome") in {"ENDPOINT_REFUSED", "APPLICATION_REFUSED"}
    old = phases["old_generation"]
    checks["old_generation.raw_committed"] = old["canonical"].get("raw_committed_next", -1) > old["publication"]["offset"]
    checks["old_generation.not_canonical"] = not any(
        value.get("kind") == "canonical" for value in old["canonical"].get("matched", []))
    checks["post_fence_generation"] = any(
        value.get("kind") == "canonical" and value.get("generation") == 8
        for value in phases["post_fence_control"]["canonical"].get("matched", []))
    return {"cell": "DR05." + fault + "@" + venue, "level": "ISOLATED_FAULT",
            "ok": all(checks.values()), "checks": checks,
            "outcome": "OBSERVED_CONTROLS_PASS" if all(checks.values()) else "OBSERVED_CONTROLS_FAIL"}
