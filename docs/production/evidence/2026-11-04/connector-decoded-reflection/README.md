# Decoded credential reflection and fixture isolation

The host previously searched only raw HTTP body bytes. JSON Unicode escapes in
values or object keys bypassed that check, so the callback completed instead of
withholding the response. The retained red gate passes12/15 tests and fails both
escaped cases plus their parent. No real credentials were used.

The host now checks bounded decoded JSON strings, keys and scalar values before
releasing a response. The raw-body check and static public error remain. One native
unit covers escaped values/keys, nested values, quoted credentials, normalized
numeric values and a benign response. The stronger actual guest test attempts to
stage a reflected body as its opaque checkpoint; no material may reach that row
or a guest log. The complete final focused gate passes15/15, zero skips/cancels,
in225.82s Node time (227.07s controller wall time), with all original custody,
typed readback, HTTP failure and ten-second revocation checks retained.

Full static quality, all nine native artifact fingerprints and99 actual native
units pass. A Python-format-only failed static attempt is preserved separately;
the corrected driver overlay leaves native inputs unchanged. source.json records
the exact final three source files. Pending Evaluation changes in the mutable
checkout were excluded from this frozen proof.

The complete Pipeline+Connector file also exposed scratch-instance reuse: each
root creates a distinct database incarnation. The quality driver now supplies a
third fresh two-slot ext4 pool dedicated to the Connector fixture. It never resets
or adopts another instance's owner marker. A real Node self-check verifies three
independent roots/six filesystem UUIDs, fresh owner state, exact exit7 propagation
and cleanup. The final Connector gate uses that third pool. All owned controllers,
three pools and temporary directories were absent afterward.

The separate Pipeline producer still expires before its first16-head seed group
completes; full Pipeline qualification, final integrated quality/CI, remaining
Connector capabilities and production capacity are not established here.
