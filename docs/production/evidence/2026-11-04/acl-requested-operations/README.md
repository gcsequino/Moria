# Evaluate only valid requested filesystem operations

The statement-local data access batch previously computed read, write and publish outcomes for every origin even when the batch only requested reads. It now derives a bounded invocation-wide set of valid requested operations and always includes read for dependency origins. The original per-request validation and denial order remain unchanged. This prepass reads only operation strings; it does not resolve future resources, return labels early, cache authority across calls, or change any guard, lock, durability setting or deadline.

The actual internal helper's operation-array regression is red on the former implementation: a read-only batch received publish/read/write instead of only read. The complete changed filesystem Source file passed **20/20**, zero skipped/cancelled, in **29.742 seconds**. Actual operation arrays now contain one operation for read-only, two for write-only or read/write, and three when publish is requested. Existing current/pinned Source proofs, marking/deny, independent-operation, cycle, restricted-role and concurrent-update tests remained enabled.

Additional mixed-entry regressions preserve SOURCE_FILE_PROOF_REQUIRED, NOT_FOUND and FORBIDDEN precedence with an unsupported future operation, a future null entry, and an object with extra fields. Invalid future entries are still rejected only when their original iteration reaches them.

The exact combined source built **all nine native artifacts** in **18.390 seconds**. Complete static checks, all-nine provenance verification and **99 native unit tests** passed in **34.307 seconds**, zero ignored. Executable hashes and the native source digest are retained. The SQL front-gate overlays did not invoke their unchanged native binaries and are identified accordingly.

Owned controllers, labeled children, scratch pools and temporary paths were verified absent after both SQL gates. The change establishes reduced requested policy work and security equivalence; the original full-width 16-head/67-input producer profile and full Pipeline acceptance are still required. It does not establish throughput or capacity certification.
