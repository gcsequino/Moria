# Actual Source Search credential integration, v24

The exact v24 executable and schema are selected by hash from the immutable snapshot in `manifest.json`. Two external TypeScript inputs are copied and hashed alongside it; the original failed combined v24 run remains in `native-integration/20261031T142542Z`.

All seven tests pass with zero skips. The real Source Model/Provider/Policy/Secret fixture verifies operator approval, independent service authority, accepted approval generation, disable/reenable fencing before provider work, complete projection, atomic Action replay, embedding reuse, replacement claims and changed Model Source acceptance.

The exact-purpose revocation assertion accepts only `FULL_SOURCE_PURPOSE_REVOKED` or `AUTHORIZATION_CHANGED`: the independent full-source and current Model guards can detect the same revocation first. The actual upstream socket must close, the execution must fail, and both covered revision and visible document heads must remain unchanged. Neither guard nor the deadline changed.

The later worker candidate fairness change is absent from this executable and remains unqualified by this run.
