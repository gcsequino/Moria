# Native catalog and permanent indexing trials

Every retained directory contains actual output from its stated immutable source;
failed trials remain failures. A green prerequisite or subtest does not make its
combined run green. The source manifest, generated schema, Cargo artifact paths,
copied executable hashes and mandatory preflight output identify later trials.

| Trial | Actual result and finding |
| --- | --- |
| version-pins-failed001/002 | The original 15-second fixture boundary was exceeded; one adversarial POSIX mutation initially could not overwrite correctly immutable bytes. |
| version-pins-diagnostic003 | Diagnostic 60-second wait passed 54 tests. It did not establish compliance with the original 15-second boundary. |
| artifact-failed001/002 | A stale filename-selected harness caused artifact/schema mismatch. These are artifact faults, not writer behavior evidence. |
| writer-exploratory003 | Correct Cargo-resolved artifacts passed 35 tests, including scalar writes and the original 15-second boundary after within-call SQL guard factoring. Preflight was not executed before this exploratory run. |
| writer-fixture-failed004 | 87 of 89 tests passed; a legacy read-event exact-JSON assertion rejected an added zero historical-row field. Both providers' two-generation writer/history cases passed. The fixture now omits the field unless history was requested. |
| writer-prerequisite-failed005 | Mandatory preflight passed; fixture databases could not initialize because a pipeline view override dropped existing evaluation columns. No component success is claimed. |
| writer-build-failed006 | Snapshot captured a pipeline call to a private filesystem module; the build failed before execution. |
| writer-indexing-failed007 | All native catalog cases passed, including real UUID and scalar-array writes on POSIX and MinIO, two generations and exact old/current readback. The combined run failed on a legacy-null managed-binding decode in indexing and a new fixture filename error. |
| service-mapped-failed008 | N25's ten cases, the nine V2 indexing cases and provenance cases passed. New managed source indexing exposed raw-JSON schema comparison; the invalid 256-character published Source field was separately rejected by the existing 100-character compiler bound. |
| service-mapped-failed009 | Typed schema equality passed. Actual mapped indexing exposed a planner selecting target aliases from physical Dataset columns. The planner now shares full source-layout validation with the scanner. |
| service-mapped-failed010 | The small mapped pipeline and protected HTTP queries worked; actual epoch refresh and captured-generation policy tests passed. A negative fixture passed a PostgreSQL array where JSON was required. The maximum-width profile still failed during planning and remains under investigation. |

The SQL optimization retained the same 4,366 Dataset guard invocations in the
comparable exploratory fixture while reducing ordinary session resolution calls
from 25,236 to 4,413. It factors repeated current-state derivation only within one
guard invocation. It does not cache authority between calls or remove provider
before/after checks, lease checks, security epochs or fences. Actual function
counts and timings are retained in the raw output.

UUID and scalar-array evidence covers constructed bounded native outputs. It does
not certify allocation bounds for arbitrary externally produced Parquet, capacity
targets, retention expiration or physical garbage collection.
