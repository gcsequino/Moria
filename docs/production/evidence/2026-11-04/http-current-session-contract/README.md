# Current-session HTTP fixture contracts

The HTTP integration fixture expected a disabled principal's cached credential to
return forbidden. The accepted [serving contract](../../../contracts/serving.md)
permanently revokes issued credentials on disable, returning unauthenticated.
The failed assertion prevented the fixture from reenabling its shared principal,
which then poisoned its browser, managed-cell and admission checks. Reenabling a
principal also does not revive its previously issued bearer or CLI credential.

The principal-disable case now has its own workload actor. It verifies 401 and
`UNAUTHENTICATED`, unchanged credential counts, continued rejection after reenable,
and an unaffected independent ordinary bearer. Cleanup reenables the test actor
even when its assertion fails. The separate CLI approval case first issues its
own profile credential, then verifies the existing 403 generation fence and no
silent replacement after a disable/enable pulse. Local workload recovery remains
unchanged. No production authentication, revocation, approval, limit or deadline
changed; the complete original file remains selected.

## Validation

Both trials used identical native runtime artifacts and fresh independent scratch
pools and controllers. Only the HTTP test fixture changed:

- Original fixture: 0/10 passed, 10 failed, no skips; first failure was 401 versus
  the stale 403 expectation and the remaining shared-principal failures followed.
- Corrected fixture: **10/10 passed, no skips**, 12.03 seconds including controller
  setup/cleanup. Coverage includes both permanent credential/approval fences,
  exact-origin browser sessions, real managed-cell DB partition, compiler4,
  model32 and shared64 admission budgets, metrics permissions, approval denial
  across restart, and bounded SIGTERM drain.
- Matching provenance for all nine native executables/harnesses and all 79 native
  unit tests passed. TypeScript type checking and fixture formatting passed.
- Both controllers, scratch pools and temporary directories were verified removed.

Frozen runtime base: `949f9478903fc26e1478ec675bbd59121810bd62`.
Native source digest: `ffce48f7b0a1e702cec2a6ce819eed5d304bb6b3917945f8ef9d21e4f69334fb`.
Corrected image: `sha256:c76cf717f32e1f7d92dd1f64c31bb3218e14111e791c1b4f4843726451a798c8`.
The fixture branch was subsequently advanced to `6cfd8c5909f6f2227d50ca220482d073edb66cac`;
its public authentication implementations are unchanged. The newer private job
origin change has separate evidence; these runs do not qualify the complete
combined platform and are not a capacity certification.

Stored transcripts normalize trailing whitespace only; test outcomes are unchanged.
