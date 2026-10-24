"""Synthetic event fixtures with a fake clock; no sleeps or runtime traffic."""

import unittest

from benchmarks.arrivals import AccountingError, ArrivalLedger


class FakeClock:
    def __init__(self):
        self.ms = 0

    def __call__(self):
        return self.ms


class ArrivalTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.ledger = ArrivalLedger([("a", 10), ("b", 20)], drain_deadline_ms=100, clock=self.clock)

    def assert_reconciled(self, ledger):
        result = ledger.snapshot()
        logical = result["logical"]
        attempts = result["attempts"]
        self.assertEqual(
            logical["scheduled"],
            sum(logical[k] for k in ledger.OUTCOMES) + logical["outstanding"],
        )
        self.assertEqual(
            attempts["started"],
            sum(attempts[k] for k in ledger.ATTEMPT_OUTCOMES) + attempts["outstanding"],
        )
        self.assertEqual(len(result["successful_latency_ms"]), logical["success"])
        return result

    def reject(self, message, operation):
        before = self.ledger.snapshot()
        with self.assertRaisesRegex(AccountingError, message):
            operation()
        self.assertEqual(before, self.ledger.snapshot())
        self.assert_reconciled(self.ledger)

    def test_normal_completion_includes_dispatch_delay_and_detaches_snapshot(self):
        self.clock.ms = 25
        self.ledger.dispatch("a", "1")
        self.clock.ms = 40
        self.ledger.finish_attempt("a", "1", "success")
        result = self.assert_reconciled(self.ledger)
        self.assertEqual(result["successful_latency_ms"], [30])
        self.assertEqual(result["arrivals"][0]["attempts"][0]["dispatch_delay_ms"], 15)
        self.assertEqual(result["logical"]["outstanding"], 1)
        result["arrivals"][0]["attempts"].clear()
        self.assertEqual(self.ledger.snapshot()["attempts"]["started"], 1)

    def test_retries_are_attempts_not_new_arrivals_or_successes(self):
        self.clock.ms = 20
        for attempt, outcome in [("1", "error"), ("2", "timeout"), ("3", "rejection")]:
            self.ledger.dispatch("a", attempt)
            self.clock.ms += 5
            self.ledger.finish_attempt("a", attempt, outcome, retry=True)
            result = self.assert_reconciled(self.ledger)
            self.assertEqual(result["logical"]["outstanding"], 2)
            self.assertEqual(result["logical"]["unsuccessful"], 0)
        self.ledger.dispatch("a", "4")
        self.clock.ms = 60
        self.ledger.finish_attempt("a", "4", "success")
        result = self.assert_reconciled(self.ledger)
        self.assertEqual(
            result["attempts"],
            dict(started=4, retries=3, success=1, rejection=1, timeout=1, error=1, outstanding=0),
        )
        self.assertEqual(result["logical"]["scheduled"], 2)
        self.assertEqual(result["logical"]["success"], 1)
        self.assertEqual(result["successful_latency_ms"], [50])

    def test_all_outcomes_and_saturation_reconcile_exactly(self):
        names = ["success", "rejection", "timeout", "error", "missed", "waiting", "active", "retry"]
        ledger = ArrivalLedger(
            [(name, 0) for name in names], drain_deadline_ms=100, clock=self.clock
        )
        for name in names[:4]:
            ledger.dispatch(name, "1")
            ledger.finish_attempt(name, "1", name)
            self.assert_reconciled(ledger)
        ledger.mark_saturated("missed")
        ledger.miss_dispatch("missed")
        ledger.mark_saturated("waiting")
        ledger.dispatch("active", "1")
        ledger.dispatch("retry", "1")
        ledger.finish_attempt("retry", "1", "error", retry=True)
        before = self.assert_reconciled(ledger)
        self.assertEqual(before["logical"]["outstanding"], 3)
        self.clock.ms = 120
        result = ledger.drain()
        self.assertEqual(
            result["logical"],
            dict(
                scheduled=8,
                success=1,
                rejection=1,
                timeout=4,
                error=1,
                missed_dispatch=1,
                outstanding=0,
                unsuccessful=7,
                drain_incomplete=3,
                saturated=2,
            ),
        )
        self.assertEqual(
            result["attempts"],
            dict(started=6, retries=0, success=1, rejection=1, timeout=2, error=2, outstanding=0),
        )
        self.assertEqual(result["successful_latency_ms"], [0])
        self.assertTrue(result["sealed"])
        for arrival in result["arrivals"][-3:]:
            self.assertEqual(arrival["completed_ms"], 100)
            self.assertEqual(arrival["reason"], "drain_incomplete")
        self.assert_reconciled(ledger)

    def test_explicit_timeout_with_and_without_attempt(self):
        self.clock.ms = 20
        self.ledger.dispatch("a", "1")
        self.clock.ms = 30
        self.ledger.timeout("a")
        self.ledger.timeout("b")
        result = self.assert_reconciled(self.ledger)
        self.assertEqual(result["logical"]["timeout"], 2)
        self.assertEqual(result["attempts"]["timeout"], 1)
        self.assertEqual(result["logical"]["drain_incomplete"], 0)
        self.assertEqual(result["successful_latency_ms"], [])
        self.reject("already terminal", lambda: self.ledger.finish_attempt("a", "1", "success"))

    def test_saturation_can_recover_without_losing_observation(self):
        self.clock.ms = 20
        self.ledger.mark_saturated("a")
        self.reject("saturation already", lambda: self.ledger.mark_saturated("a"))
        self.ledger.dispatch("a", "1")
        self.ledger.finish_attempt("a", "1", "success")
        result = self.assert_reconciled(self.ledger)
        self.assertEqual(result["logical"]["saturated"], 1)
        self.assertEqual(result["logical"]["success"], 1)
        self.assertEqual(result["arrivals"][0]["saturated_ms"], 20)

    def test_unknown_duplicate_and_inconsistent_transitions(self):
        self.clock.ms = 20
        self.reject("unknown arrival", lambda: self.ledger.dispatch("unknown", "1"))
        self.reject("unknown attempt", lambda: self.ledger.finish_attempt("a", "1", "success"))
        self.reject("nonempty string", lambda: self.ledger.dispatch("a", ""))
        self.ledger.dispatch("a", "1")
        self.reject("duplicate attempt", lambda: self.ledger.dispatch("a", "1"))
        self.reject("already outstanding", lambda: self.ledger.dispatch("a", "2"))
        self.reject("no attempts", lambda: self.ledger.miss_dispatch("a"))
        self.reject(
            "invalid attempt outcome", lambda: self.ledger.finish_attempt("a", "1", "other")
        )
        self.reject(
            "retry requires", lambda: self.ledger.finish_attempt("a", "1", "success", retry=True)
        )
        self.reject(
            "retry requires", lambda: self.ledger.finish_attempt("a", "1", "error", retry=1)
        )
        self.ledger.finish_attempt("a", "1", "error", retry=True)
        self.reject(
            "attempt already terminal", lambda: self.ledger.finish_attempt("a", "1", "success")
        )
        self.ledger.dispatch("a", "2")
        self.ledger.finish_attempt("a", "2", "success")
        for action in [
            lambda: self.ledger.dispatch("a", "3"),
            lambda: self.ledger.finish_attempt("a", "2", "success"),
            lambda: self.ledger.timeout("a"),
            lambda: self.ledger.miss_dispatch("a"),
            lambda: self.ledger.mark_saturated("a"),
        ]:
            self.reject("already terminal", action)

    def test_invalid_clock_and_event_order(self):
        self.reject("before scheduled", lambda: self.ledger.dispatch("a", "1"))
        for value in [-1, True, 1.5, float("nan"), float("inf"), "20", None]:
            self.clock.ms = value
            self.reject("integer milliseconds", lambda: self.ledger.dispatch("a", "1"))
        self.clock.ms = 30
        self.ledger.dispatch("a", "1")
        self.clock.ms = 29
        self.reject("backwards", lambda: self.ledger.finish_attempt("a", "1", "success"))
        self.reject("backwards", lambda: self.ledger.dispatch("b", "1"))
        self.clock.ms = 101
        self.reject("after drain deadline", lambda: self.ledger.finish_attempt("a", "1", "success"))

    def test_deadline_boundary_and_late_events_cannot_revise_totals(self):
        self.clock.ms = 99
        self.reject("not reached", self.ledger.drain)
        self.clock.ms = 100
        self.ledger.dispatch("a", "1")
        self.ledger.finish_attempt("a", "1", "success")
        self.ledger.drain()
        for now in [100, 101, 90]:
            self.clock.ms = now
            for action in [
                self.ledger.drain,
                lambda: self.ledger.dispatch("b", "1"),
                lambda: self.ledger.finish_attempt("a", "1", "success"),
                lambda: self.ledger.timeout("b"),
            ]:
                self.reject("ledger drained", action)

    def test_invalid_schedules_and_configuration(self):
        cases = [
            ([("a", 0), ("a", 1)], 100, "duplicate arrival"),
            ([("", 0)], 100, "nonempty string"),
            ([("a", 101)], 100, "after drain deadline"),
        ]
        for value in [-1, True, 1.5, float("inf"), None]:
            cases.append(([("a", value)], 100, "integer milliseconds"))
            cases.append(([], value, "integer milliseconds"))
        for schedule, deadline, error in cases:
            with self.subTest(schedule=schedule, deadline=deadline):
                with self.assertRaisesRegex(AccountingError, error):
                    ArrivalLedger(schedule, drain_deadline_ms=deadline, clock=self.clock)
        for schedule in [None, 1, ["ab"], [("a",)], [("a", 0, 1)]]:
            with self.subTest(schedule=schedule):
                with self.assertRaisesRegex(AccountingError, "schedule"):
                    ArrivalLedger(schedule, drain_deadline_ms=100, clock=self.clock)
        with self.assertRaisesRegex(AccountingError, "callable"):
            ArrivalLedger([], drain_deadline_ms=0, clock=None)

    def test_empty_schedule_and_replay_are_deterministic(self):
        empty = ArrivalLedger([], drain_deadline_ms=0, clock=self.clock)
        self.assertEqual(empty.drain()["logical"]["scheduled"], 0)

        def replay():
            clock = FakeClock()
            ledger = ArrivalLedger([("a", 0)], drain_deadline_ms=10, clock=clock)
            ledger.dispatch("a", "1")
            clock.ms = 5
            ledger.finish_attempt("a", "1", "success")
            clock.ms = 10
            return ledger.drain()

        self.assertEqual(replay(), replay())


if __name__ == "__main__":
    unittest.main()
