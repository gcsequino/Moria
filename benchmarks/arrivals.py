"""Deterministic offline arrival accounting; no scheduler, I/O, or default clock."""

from collections import Counter
from copy import deepcopy


class AccountingError(ValueError):
    """An invalid schedule or event; rejected operations leave the ledger unchanged."""


def _timestamp(value, label):
    if type(value) is not int or value < 0:
        raise AccountingError(f"{label}: expected nonnegative integer milliseconds")
    return value


def _identity(value, label):
    if not isinstance(value, str) or not value.strip():
        raise AccountingError(f"{label}: expected nonempty string")
    return value


class ArrivalLedger:
    """Account for a fixed schedule using an injected monotonic millisecond clock.

    Attempts are sequential per arrival. An unsuccessful attempt may permit a
    retry; only a terminal logical outcome removes an arrival from outstanding.
    All events at the deadline are allowed until drain() seals the ledger.
    """

    OUTCOMES = ("success", "rejection", "timeout", "error", "missed_dispatch")
    ATTEMPT_OUTCOMES = ("success", "rejection", "timeout", "error")

    def __init__(self, schedule, *, drain_deadline_ms, clock):
        self._deadline = _timestamp(drain_deadline_ms, "drain_deadline_ms")
        if not callable(clock):
            raise AccountingError("clock: expected a callable returning milliseconds")
        self._clock = clock
        self._last_event = 0
        self._sealed = False
        self._arrivals = {}
        try:
            entries = iter(schedule)
        except TypeError as error:
            raise AccountingError(
                "schedule: expected an iterable of (ID, milliseconds) pairs"
            ) from error
        for index, entry in enumerate(entries):
            if not isinstance(entry, (tuple, list)) or len(entry) != 2:
                raise AccountingError(f"schedule[{index}]: expected an (ID, milliseconds) pair")
            arrival_id, scheduled_ms = entry
            _identity(arrival_id, "arrival_id")
            _timestamp(scheduled_ms, f"{arrival_id}: scheduled_ms")
            if arrival_id in self._arrivals:
                raise AccountingError(f"{arrival_id}: duplicate arrival ID")
            if scheduled_ms > self._deadline:
                raise AccountingError(f"{arrival_id}: scheduled after drain deadline")
            self._arrivals[arrival_id] = {
                "arrival_id": arrival_id,
                "scheduled_ms": scheduled_ms,
                "saturated_ms": None,
                "attempts": [],
                "outcome": None,
                "completed_ms": None,
                "reason": None,
            }

    def _now(self):
        if self._sealed:
            raise AccountingError("ledger drained: late events cannot change totals")
        now = _timestamp(self._clock(), "clock")
        if now < self._last_event:
            raise AccountingError("clock: event time moved backwards")
        return now

    def _event(self, arrival_id):
        _identity(arrival_id, "arrival_id")
        if arrival_id not in self._arrivals:
            raise AccountingError(f"{arrival_id}: unknown arrival")
        now = self._now()
        arrival = self._arrivals[arrival_id]
        if now > self._deadline:
            raise AccountingError("event after drain deadline: call drain()")
        if now < arrival["scheduled_ms"]:
            raise AccountingError(f"{arrival_id}: event before scheduled arrival")
        if arrival["outcome"] is not None:
            raise AccountingError(f"{arrival_id}: logical outcome already terminal")
        return arrival, now

    @staticmethod
    def _active(arrival):
        attempts = arrival["attempts"]
        if attempts and attempts[-1]["outcome"] is None:
            return attempts[-1]
        return None

    @staticmethod
    def _complete(arrival, outcome, now, reason):
        arrival.update(outcome=outcome, completed_ms=now, reason=reason)

    def mark_saturated(self, arrival_id):
        """Retain one generator-saturation observation, without dropping an arrival."""
        arrival, now = self._event(arrival_id)
        if arrival["saturated_ms"] is not None:
            raise AccountingError(f"{arrival_id}: saturation already recorded")
        arrival["saturated_ms"] = now
        self._last_event = now

    def dispatch(self, arrival_id, attempt_id):
        arrival, now = self._event(arrival_id)
        _identity(attempt_id, "attempt_id")
        if any(a["attempt_id"] == attempt_id for a in arrival["attempts"]):
            raise AccountingError(f"{arrival_id}/{attempt_id}: duplicate attempt ID")
        if self._active(arrival):
            raise AccountingError(f"{arrival_id}: an attempt is already outstanding")
        arrival["attempts"].append(
            {
                "attempt_id": attempt_id,
                "dispatched_ms": now,
                "dispatch_delay_ms": now - arrival["scheduled_ms"],
                "outcome": None,
                "completed_ms": None,
                "reason": None,
            }
        )
        self._last_event = now

    def finish_attempt(self, arrival_id, attempt_id, outcome, *, retry=False):
        """Finish an attempt; retry=True keeps an unsuccessful arrival outstanding."""
        arrival, now = self._event(arrival_id)
        _identity(attempt_id, "attempt_id")
        if outcome not in self.ATTEMPT_OUTCOMES:
            raise AccountingError(f"{arrival_id}: invalid attempt outcome {outcome!r}")
        if type(retry) is not bool or (retry and outcome == "success"):
            raise AccountingError(f"{arrival_id}: retry requires an unsuccessful attempt")
        attempt = next((a for a in arrival["attempts"] if a["attempt_id"] == attempt_id), None)
        if attempt is None:
            raise AccountingError(f"{arrival_id}/{attempt_id}: unknown attempt")
        if attempt["outcome"] is not None:
            raise AccountingError(f"{arrival_id}/{attempt_id}: attempt already terminal")
        attempt.update(outcome=outcome, completed_ms=now, reason="response")
        if not retry:
            self._complete(arrival, outcome, now, "attempt_completed")
        self._last_event = now

    def miss_dispatch(self, arrival_id):
        arrival, now = self._event(arrival_id)
        if arrival["attempts"]:
            raise AccountingError(f"{arrival_id}: missed dispatch requires no attempts")
        self._complete(arrival, "missed_dispatch", now, "missed_dispatch")
        self._last_event = now

    def timeout(self, arrival_id):
        """Expire a logical arrival, including any active attempt, at clock time."""
        arrival, now = self._event(arrival_id)
        self._expire(arrival, now, "explicit_timeout")
        self._last_event = now

    def _expire(self, arrival, now, reason):
        attempt = self._active(arrival)
        if attempt:
            attempt.update(outcome="timeout", completed_ms=now, reason=reason)
        self._complete(arrival, "timeout", now, reason)

    def drain(self):
        """Seal at the declared deadline, even when invoked later; never fabricate attempts."""
        now = self._now()
        if now < self._deadline:
            raise AccountingError("drain: clock has not reached drain deadline")
        for arrival in self._arrivals.values():
            if arrival["outcome"] is None:
                self._expire(arrival, self._deadline, "drain_incomplete")
        self._last_event = now
        self._sealed = True
        return self.snapshot()

    def snapshot(self):
        """Return detached records and exact counters; does not read or advance the clock."""
        arrivals = list(self._arrivals.values())
        attempts = [a for arrival in arrivals for a in arrival["attempts"]]
        logical = Counter(a["outcome"] for a in arrivals)
        attempted = Counter(a["outcome"] for a in attempts)
        return {
            "drain_deadline_ms": self._deadline,
            "sealed": self._sealed,
            "logical": {
                "scheduled": len(arrivals),
                **{key: logical[key] for key in self.OUTCOMES},
                "outstanding": logical[None],
                "unsuccessful": sum(logical[key] for key in self.OUTCOMES[1:]),
                "drain_incomplete": sum(a["reason"] == "drain_incomplete" for a in arrivals),
                "saturated": sum(a["saturated_ms"] is not None for a in arrivals),
            },
            "attempts": {
                "started": len(attempts),
                "retries": sum(max(0, len(a["attempts"]) - 1) for a in arrivals),
                **{key: attempted[key] for key in self.ATTEMPT_OUTCOMES},
                "outstanding": attempted[None],
            },
            "successful_latency_ms": [
                a["completed_ms"] - a["scheduled_ms"] for a in arrivals if a["outcome"] == "success"
            ],
            "arrivals": deepcopy(arrivals),
        }
