# Bounded Agent integration fixtures

One shared setup/body now registers three independent roots, each retaining the
original 300-second timeout. The 21 original subcases are registered exactly once
and keep their original assertions and waits. Full file result: 24 cases, 22 pass,
2 fail, zero cancellations or skips, 363.665 seconds. Admission/model and
isolation/lifecycle roots pass in 84.89 and 141.36 seconds. The service root takes
136.80 seconds; its genuine Automation-to-Function child admission barrier fails
at line1798, plus its enclosing root. The other five service subcases pass.

This exposes an existing service composition failure previously hidden by the
single fixture's aggregate timeout. It is not a full Agent qualification pass.

The run also encounters the old host cleanup's case-sensitive Docker absence
message check. Only its exact owned pool/temporary paths were subsequently
removed after verifying no container references and loop/mount/backing identity;
cleanup records are retained. The fixed cleanup path's independent success and
failure lifecycle smokes are under development-gates/harness-lifecycle-final.
The ordinary final gate must still run with the corrected harness.
