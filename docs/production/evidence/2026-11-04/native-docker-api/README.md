# Native Docker API negotiation

The independent Linux CI runner uses Docker 28.0.4, whose maximum API is 1.48.
The native sandbox previously sent every REST and attach request to API 1.51.
An actual isolated Docker 28.0.4 daemon rejected that path with HTTP 400:
`client version 1.51 is too new. Maximum supported API version is 1.48`.
The same nonexistent-container inspection at API 1.48 returned the expected 404.
The recorded responses (with normalized text line endings) and daemon metadata are retained here.

The client now reads the bounded unversioned `/version` response once per shared
client, selects the highest overlapping API up to 1.51, and requires at least
1.41. That minimum includes the existing one-shot CPU accounting request and all
current container and scratch mount fields. REST, raw attach, scratch creation
and cleanup use the selected version. Unsupported or malformed metadata fails
closed. Isolation settings, resource limits and deadlines are unchanged.

References: [Docker API versions](https://docs.docker.com/reference/api/engine/)
and [API 1.41 specification](https://docs.docker.com/reference/api/engine/version/v1.41.yaml).

## Validation

- Base revision: `4e2b6617c4158ed8a3de6df2d84e1ca281917184`.
- Frozen native source digest: `6b73a0bf1abb9ce97ca1b1c94e9873e1ddc25086196522508308301e8a292a8d`.
- Rust formatting and all nine debug executables/harnesses compiled successfully
  with two compiler jobs, two CPUs and 4 GiB memory; compilation took 37.27 seconds.
- `pnpm check:indexing` verified all nine matching artifacts and all 79 native unit
  tests passed, with no skips or ignored tests, in 2.52 seconds total.
- New tests exercise numeric overlap and minimum versions, malformed metadata,
  real Unix HTTP requests through the native client, retry after failed
  negotiation, escaped path segments and sharing the selected version across clones.
- Subsequent actual Docker 28.0.4 execution passed the complete native sandbox
  file (9/9, no skips) and existing TypeScript Docker compatibility file (1/1,
  no skips), using the separately committed completion challenge change. See
  `actual-docker28/`. The frozen combined native digest is
  `7988218af28168cca79ecfb6254047beafc034f04368543caf50f30b76515dc6`.
- These results do not claim the complete platform gate, independent CI success
  or production capacity.

The real daemon proof uses a private network namespace and separate containerd,
data and execution roots. No AWS instance was started for this check.
