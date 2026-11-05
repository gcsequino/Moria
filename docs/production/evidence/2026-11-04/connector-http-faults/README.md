# Connector HTTP and capability-set qualification

The complete focused custody fixture passed 13/13 tests, zero failures, skips or
cancellations, in 228.41 seconds. Its twelve subcases execute the actual registered
native Sync worker, isolated Python/Arrow guest, eight operator-approved host-only
credentials, deterministic local HTTP peer and versioned-S3 Dataset provider.

Alongside the earlier custody/typed-readback/cursor/revocation checks, five actual
provider failures now cover redirects, truncated Content-Length, HTTP503, a body
larger than the 768-KiB response bound and the original 30-second HTTP timeout.
Each failure makes exactly one upstream request and one sandbox attempt, records
no publication and preserves the complete prior Dataset head and checkpoint.
The worker remains alive for the subsequent cases. No per-operation deadline,
response limit, root watchdog or assertion was relaxed.

The implementation advertises all four Source capability names in a different
order, while the requested operation remains batchRead. Its successful execution
proves membership-based capability admission; it does not dispatch or qualify the
other advertised capabilities. The host still rejects unsupported operations and
write modes before code.

The immutable base combines the current child-binding SQL with Connector custody
and capability membership. All nine native artifacts and 81 unit cases match its
source digest; complete static checks passed on both base and fixture overlay.
The later multi-operation ACL optimization is independently qualified and is not
included in this runtime trial. All owned controllers, scratch pools and temporary
directories were absent after completion.

Remaining N09 work includes acknowledgement loss, started-worker death, checkpoint
conflict, other write modes/coverage and discovery/continuous/writeback handlers.
Full composed qualification, current independent CI and production target-load
certification remain required.
