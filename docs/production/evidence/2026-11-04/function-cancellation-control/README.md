# Function cancellation control receipt

The CLI integration fixture expected invocation state from cancellation, but the
accepted public protocol returns `{id,cancelRequested:true}` uniformly. The fixture
now checks that exact receipt, then uses authenticated CLI wait to verify the
terminal cancelled state. It still waits for worker cleanup and verifies the real
Docker container is absent. Production API and cancellation behavior are unchanged.

Frozen base `adb825f` uses 112 identical native inputs from the qualified runtime
cohort. All nine artifact checks and 79 native units passed. Full static checks
passed before building the fixture overlay. The original full integration file
failed only the receipt assertion; its transcript is retained under `red`.
The corrected full file passed all 17 tests with no skips or cancellations in
94.17 seconds. Controller, scratch pools and temporary directory were removed.
The integrated revision still requires its complete independent quality gate.
