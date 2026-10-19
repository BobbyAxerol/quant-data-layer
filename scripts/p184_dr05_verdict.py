"""Fail-closed observer checks; no release certificate or synthesized evidence."""


def evaluate(observation):
    from scripts.p184_dr05_dl_fault import schedule

    venue, fault = observation["venue"], observation["fault"]
    steps = schedule(venue, fault)
    expected = [step["phase"] for step in steps]
    # Expected generations come from the frames this run published (the schedule), never a constant (OF-G13-2):
    # the core's canonical connection_generation is the raw frame's (rust/qdl-realtime-core/src/lib.rs).
    generation = {step["phase"]: step["generation"] for step in steps}
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
            and value.get("capture_id") == row["publication"]["capture_id"]
            and value.get("generation") == generation[phase] for value in matches)
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
    # The post-fence control is canonical in the generation the run re-anchored (return_anchor), identity-matched,
    # and that generation is above the fenced old one.
    post = phases["post_fence_control"]
    checks["post_fence_generation"] = (
        generation["post_fence_control"] == generation["return_anchor"] > generation["old_generation"]
        and any(value.get("kind") == "canonical" and value.get("venue") == venue
                and value.get("capture_id") == post["publication"]["capture_id"]
                and value.get("generation") == generation["post_fence_control"]
                for value in post["canonical"].get("matched", [])))
    return {"cell": "DR05." + fault + "@" + venue, "level": "ISOLATED_FAULT",
            "ok": all(checks.values()), "checks": checks,
            "outcome": "OBSERVED_CONTROLS_PASS" if all(checks.values()) else "OBSERVED_CONTROLS_FAIL"}
