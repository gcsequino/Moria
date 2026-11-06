# Existing private-session reuse regression, pre-fix

The exact older workflow image is recorded in manifest.json. Only an archived
TypeScript test overlay changes: required admission and descendant cases run;
eight unrelated cases explicitly skip for this diagnostic. Real PostgreSQL
connections hold SHARE locks on the accepted parent and child executions.

The first credential creation correctly cannot obtain its required exclusive
fence while these readers are held. After creation with readers released, exact
credential reuse incorrectly requests the same exclusive execution lock and
fails with 55P03 under a test-local 500ms lock timeout. This fails the new
regression at the second issue call. Production lock timeouts and native source
are unchanged. This is red diagnostic evidence, not suite qualification.

Result: one passing subcase, one failing subcase and its failing parent, eight
explicit diagnostic skips, no cancellation. Owned controller and scratch are
removed by the recorded quality runner.
