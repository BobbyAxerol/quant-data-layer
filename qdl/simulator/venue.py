from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class Scenario(str, Enum):
    NORMAL = "NORMAL"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    GAP = "GAP"
    CLOCK_SKEW = "CLOCK_SKEW"
    BURST = "BURST"
    CONNECTION_RESET = "CONNECTION_RESET"
    SYMBOL_DELIST = "SYMBOL_DELIST"
    SCHEMA_EXTENSION = "SCHEMA_EXTENSION"
    INVALID_DECIMAL = "INVALID_DECIMAL"
    MALFORMED = "MALFORMED"


@dataclass(frozen=True, slots=True)
class SimulatedFrame:
    scenario: Scenario
    sequence: int | None
    source_time_ns: int | None
    payload: Mapping[str, Any] | bytes


class DeterministicVenueSimulator:
    """Failure-fixture generator for tests; never accepted as provider evidence."""

    provenance = "TEST_SYNTHETIC_VENUE_SIMULATOR"

    def __init__(self, template: Mapping[str, Any], *, sequence_field: str = "sequence"):
        self._template = copy.deepcopy(dict(template))
        self._sequence_field = sequence_field

    def frames(
        self,
        scenario: Scenario,
        *,
        start_sequence: int = 1,
        count: int = 3,
        source_time_ns: int = 1_000_000_000,
    ) -> tuple[SimulatedFrame, ...]:
        if count <= 0 or start_sequence <= 0 or source_time_ns <= 0:
            raise ValueError("simulator bounds must be positive")
        if scenario is Scenario.CONNECTION_RESET:
            return (SimulatedFrame(scenario, None, None, {"event": "connection_reset"}),)
        if scenario is Scenario.SYMBOL_DELIST:
            return (SimulatedFrame(scenario, start_sequence, source_time_ns, {"event": "delist"}),)
        if scenario is Scenario.MALFORMED:
            return (SimulatedFrame(scenario, None, None, b"{malformed"),)

        sequences = list(range(start_sequence, start_sequence + count))
        if scenario is Scenario.DUPLICATE:
            sequences.insert(1, sequences[0])
        elif scenario is Scenario.OUT_OF_ORDER and len(sequences) >= 2:
            sequences[0], sequences[1] = sequences[1], sequences[0]
        elif scenario is Scenario.GAP:
            sequences = [start_sequence, start_sequence + 2]
        elif scenario is Scenario.BURST:
            sequences = list(range(start_sequence, start_sequence + max(count, 100)))

        output = []
        for index, sequence in enumerate(sequences):
            payload = copy.deepcopy(self._template)
            payload[self._sequence_field] = sequence
            if scenario is Scenario.SCHEMA_EXTENSION:
                payload["future_optional_field"] = {"version": 2}
            if scenario is Scenario.INVALID_DECIMAL:
                payload["price"] = "not-a-decimal"
            event_time = source_time_ns + index
            if scenario is Scenario.CLOCK_SKEW and index == len(sequences) - 1:
                event_time = source_time_ns - 1
            output.append(SimulatedFrame(scenario, sequence, event_time, payload))
        return tuple(output)
