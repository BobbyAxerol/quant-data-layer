"""Deterministic TEST_ONLY observations; no Docker, brokers, or provider claims."""

import copy
import unittest

from scripts.kn_recovery_state import RecoveryController, RecoveryPolicy, GATES, SCHEMA


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.roles = {name: {"id": digit * 64, "image": "sha256:" + digit * 64, "stage": stage}
                      for name, digit, stage in (("broker", "a", "infra"),
                                                 ("projector", "b", "projector"),
                                                 ("reader", "c", "reader"))}
        self.controller = self.make_controller()

    def make_controller(self, **kwargs):
        policy = dict(deadline_seconds=30, max_observation_age_seconds=5,
                      stable_samples=3, retry_seconds=2, max_start_attempts=2)
        policy.update(kwargs)
        return RecoveryController(RecoveryPolicy(**policy), self.roles, lambda: self.now)

    def observation(self, **updates):
        sample = {"schema": SCHEMA, "observed_monotonic": self.now,
                  "roles": {name: {"id": role["id"], "image": role["image"], "state": "running"}
                            for name, role in self.roles.items()}}
        sample.update(dict.fromkeys(GATES, True))
        sample.update(updates)
        return sample

    def step(self, **updates):
        return self.controller.step(self.observation(**updates))

    def serve(self):
        for _ in range(3):
            result = self.step()
            self.now += 1
        self.assertEqual(result["state"], "SERVING")

    def test_all_gates_need_consecutive_distinct_samples(self):
        for expected in (1, 2, 3):
            result = self.step()
            self.assertEqual(result["stable_samples"], expected)
            self.assertEqual(result["state"], "SERVING" if expected == 3 else "WAIT_CONSUMER")
            self.assertEqual(result["actions"], [])
            self.now += 1

    def test_false_gates_fail_closed_in_order(self):
        states = ("HOLD_PRESSURE", "WAIT_QUORUM", "WAIT_PROJECTOR", "WAIT_CONSUMER")
        for gate, state in zip(GATES, states):
            with self.subTest(gate=gate):
                self.controller = self.make_controller()
                result = self.step(**{gate: False})
                self.assertEqual(result["state"], state)
                self.assertEqual(result["actions"], [])
                self.assertEqual(result["stable_samples"], 0)

    def test_pressure_prevents_every_start(self):
        sample = self.observation(disk_safe=False)
        for role in sample["roles"].values():
            role["state"] = "exited"
        self.assertEqual(self.controller.step(sample)["actions"], [])

    def test_sequential_start_proposals_require_running_upstream_and_gate(self):
        sample = self.observation()
        for role in sample["roles"].values():
            role["state"] = "exited"
        for name, gate, state in (("broker", "quorum_isr_ready", "WAIT_QUORUM"),
                                  ("projector", "projector_progress_ready", "WAIT_PROJECTOR"),
                                  ("reader", "consumer_eligible", "WAIT_CONSUMER")):
            sample["observed_monotonic"] = self.now
            result = self.controller.step(sample)
            self.assertEqual(result["state"], state)
            self.assertEqual(result["actions"], [{"action": "start", "role": name,
                             "id": self.roles[name]["id"], "image": self.roles[name]["image"], "attempt": 1}])
            self.now += 1
            sample["roles"][name]["state"] = "running"
            sample[gate] = False
            sample["observed_monotonic"] = self.now
            held = self.controller.step(sample)
            self.assertEqual(held["state"], state)
            self.assertEqual(held["actions"], [])
            sample[gate] = True
            self.now += 1

    def test_never_start_running_restarting_or_other_non_exited_states(self):
        for state in ("running", "restarting", "paused", "created", "removing", "dead"):
            with self.subTest(state=state):
                sample = self.observation(quorum_isr_ready=False)
                sample["roles"]["broker"]["state"] = state
                self.assertEqual(self.make_controller().step(sample)["actions"], [])

    def test_malformed_observations_hold_unknown(self):
        variants = [None, [], {}, self.observation(schema="wrong")]
        for gate in GATES:
            for value in (None, "true", "false", 0, 1, [], {}):
                variants.append(self.observation(**{gate: value}))
            sample = self.observation()
            del sample[gate]
            variants.append(sample)
        for value in (None, True, "100", -1, float("nan"), float("inf"), 10 ** 1000):
            variants.append(self.observation(observed_monotonic=value))
        for field, value in (("id", "short"), ("image", "tag:latest"), ("state", "unknown"),
                             ("state", []), ("id", None)):
            sample = self.observation()
            sample["roles"]["broker"][field] = value
            variants.append(sample)
        for sample in variants:
            with self.subTest(sample=repr(sample)[:160]):
                result = self.make_controller().step(sample)
                self.assertEqual(result["state"], "HOLD_UNKNOWN")
                self.assertEqual(result["actions"], [])

    def test_exact_roster_required(self):
        for change in ("missing", "extra", "list", "missing_state"):
            sample = self.observation()
            if change == "missing":
                del sample["roles"]["reader"]
            elif change == "extra":
                sample["roles"]["other"] = dict(sample["roles"]["reader"])
            elif change == "list":
                sample["roles"] = list(sample["roles"].values())
            else:
                del sample["roles"]["broker"]["state"]
            self.assertEqual(self.make_controller().step(sample)["state"], "HOLD_UNKNOWN")

    def test_stale_future_and_boundary_age(self):
        for stamp in (94.999, 100.001):
            self.assertEqual(self.make_controller().step(self.observation(observed_monotonic=stamp))["state"],
                             "HOLD_UNKNOWN")
        self.assertEqual(self.step(observed_monotonic=95)["stable_samples"], 1)

    def test_identity_drift_blocks_sticky(self):
        for field, value in (("id", "d" * 64), ("image", "sha256:" + "d" * 64)):
            controller = self.make_controller()
            sample = self.observation()
            sample["roles"]["reader"][field] = value
            self.assertEqual(controller.step(sample)["state"], "BLOCKED")
            self.now += 1
            self.assertEqual(controller.step(self.observation())["state"], "BLOCKED")

    def test_duplicate_timestamp_has_no_credit_or_actions(self):
        sample = self.observation()
        self.controller.step(sample)
        self.now += 1
        self.assertEqual(self.controller.step(sample)["stable_samples"], 1)
        controller = self.make_controller()
        sample = self.observation()
        sample["roles"]["broker"]["state"] = "exited"
        self.assertEqual(len(controller.step(sample)["actions"]), 1)
        self.now += 3
        self.assertEqual(controller.step(sample)["actions"], [])

    def test_conflicting_and_regressing_timestamps_reset_stability(self):
        self.step()
        result = self.step(consumer_eligible=False)
        self.assertEqual(result["state"], "HOLD_UNKNOWN")
        self.assertEqual(result["stable_samples"], 0)
        self.assertEqual(self.step(observed_monotonic=99)["state"], "HOLD_UNKNOWN")

    def test_unknown_and_false_reset_stability(self):
        for unknown in (True, False):
            self.controller = self.make_controller()
            self.step()
            self.now += 1
            result = self.controller.step(None) if unknown else self.step(consumer_eligible=False)
            self.assertEqual(result["stable_samples"], 0)
            self.now += 1
            self.assertEqual(self.step()["stable_samples"], 1)

    def test_deadline_absolute_sticky_across_stage_changes_and_unknown(self):
        deadline = 130
        for offset, gate in ((0, "disk_safe"), (10, "quorum_isr_ready"),
                             (20, "projector_progress_ready"), (29, "consumer_eligible")):
            self.now = 100 + offset
            self.assertEqual(self.step(**{gate: False})["deadline_monotonic"], deadline)
        self.now = deadline
        self.assertEqual(self.controller.step(None)["state"], "BLOCKED")
        self.assertEqual(self.step()["state"], "BLOCKED")

    def test_slow_status_cannot_reset_or_beat_deadline(self):
        sample = self.observation()
        self.now = 130
        result = self.controller.step(sample)
        self.assertEqual(result["state"], "BLOCKED")
        self.assertEqual(result["actions"], [])

    def test_retry_backoff_and_ceiling(self):
        for offset, count, state in ((0, 1, "WAIT_QUORUM"), (1, 0, "WAIT_QUORUM"),
                                     (2, 1, "WAIT_QUORUM"), (5, 0, "WAIT_QUORUM"),
                                     (6, 0, "BLOCKED")):
            self.now = 100 + offset
            sample = self.observation()
            sample["roles"]["broker"]["state"] = "exited"
            result = self.controller.step(sample)
            self.assertEqual(result["state"], state)
            self.assertEqual(len(result["actions"]), count)

    def test_last_attempt_has_time_to_succeed(self):
        self.controller = self.make_controller(max_start_attempts=1)
        sample = self.observation()
        sample["roles"]["broker"]["state"] = "exited"
        self.controller.step(sample)
        self.now += 3
        self.assertEqual(self.step()["state"], "WAIT_CONSUMER")

    def test_multiple_exited_roles_only_one_action_each_step(self):
        self.roles["broker2"] = {"id": "d" * 64, "image": "sha256:" + "d" * 64, "stage": "infra"}
        self.controller = self.make_controller()
        for expected in ("broker", "broker2"):
            sample = self.observation()
            for name in ("broker", "broker2"):
                sample["roles"][name]["state"] = "exited"
            actions = self.controller.step(sample)["actions"]
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["role"], expected)
            self.now += 1

    def test_serving_continues_beyond_original_deadline(self):
        self.serve()
        self.now = 200
        self.assertEqual(self.step()["state"], "SERVING")
        sample = self.observation()
        self.now += 1
        self.assertEqual(self.controller.step(sample)["state"], "SERVING")

    def test_after_serving_failure_starts_one_new_bounded_episode(self):
        self.serve()
        self.now = 200
        result = self.step(disk_safe=False)
        self.assertEqual(result["state"], "HOLD_PRESSURE")
        self.assertEqual(result["deadline_monotonic"], 230)
        self.now = 210
        self.assertEqual(self.controller.step(None)["deadline_monotonic"], 230)
        self.now = 230
        self.assertEqual(self.step()["state"], "BLOCKED")

    def test_recovery_after_serving_resets_attempts_and_stability(self):
        sample = self.observation()
        sample["roles"]["reader"]["state"] = "exited"
        self.controller.step(sample)
        self.now += 1
        self.serve()
        sample = self.observation()
        sample["roles"]["reader"]["state"] = "exited"
        result = self.controller.step(sample)
        self.assertEqual(result["stable_samples"], 0)
        self.assertEqual(result["actions"][0]["attempt"], 1)
        self.now += 1
        self.serve()

    def test_stale_poll_after_serving_is_not_healthy(self):
        self.serve()
        sample = self.observation()
        self.now += 6
        result = self.controller.step(sample)
        self.assertEqual(result["state"], "HOLD_UNKNOWN")
        self.assertEqual(result["deadline_monotonic"], self.now + 30)

    def test_input_and_returned_proposal_cannot_mutate_pinned_roles(self):
        sample = self.observation()
        sample["roles"]["broker"]["state"] = "exited"
        original = copy.deepcopy(sample)
        result = self.controller.step(sample)
        self.assertEqual(sample, original)
        result["actions"][0]["id"] = "d" * 64
        self.roles["broker"]["id"] = "e" * 64
        self.now += 2
        sample["observed_monotonic"] = self.now
        self.assertEqual(self.controller.step(sample)["actions"][0]["id"], "a" * 64)

    def test_policy_and_roster_validation(self):
        for field in ("deadline_seconds", "max_observation_age_seconds", "retry_seconds",
                      "stable_samples", "max_start_attempts"):
            for value in (True, 0, -1, "2", None, float("inf"), float("nan")):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.make_controller(**{field: value})
        for field in ("stable_samples", "max_start_attempts"):
            with self.assertRaises(ValueError):
                self.make_controller(**{field: 1.5})
        for roles in ({}, {"x": {}}, {"x": self.roles["broker"], "y": self.roles["broker"]}):
            with self.assertRaises(ValueError):
                RecoveryController(self.controller.policy, roles)

    def test_controller_clock_regression_blocks(self):
        self.now = 99
        self.assertEqual(self.step()["state"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
