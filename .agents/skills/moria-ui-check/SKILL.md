---
name: moria-ui-check
description: Verify changed Moria browser flows against a disposable local instance, including permissions, persistence and visible error states.
---

# Moria browser verification

Use for changed browser behavior in apps/management, apps/snotel or hosted example
apps. Read the affected app guide and tests first; existing documentation may
describe older routes, so confirm current behavior in the code.

Use the Linux environment from `moria-verify`. Start a disposable instance with a
new `MORIA_LOCAL_DIR` and free unprivileged ports. Run `pnpm build` then `pnpm start`.
Read apps/local/README.md for supported port/environment options. Never point this
check at a production instance or reuse its credentials. Open the printed loopback
URL using the available browser tool; keep access-key fragments out of screenshots
and saved reports. Use synthetic input and no external data sync unless needed.

Exercise the affected flow through real controls. For asset changes, cover import,
selection, edit/save, refresh and persistence as applicable. For permissions,
verify an authorized path and a denied path; disabled controls alone do not prove
server authorization. Inspect narrow and wide layouts, keyboard focus, loading,
empty and error states relevant to the change. Inspect console/network failures
when the browser tool exposes them.

Record the exact build revision, scenario, expected/observed outcome and sanitized
screenshots. Restart the same disposable instance when persistence is affected.
Stop only the process started for this check. Preserve evidence outside tracked
files (for example `artifacts/ui/`, already ignored). If no browser or runtime is
available, report the unverified flow explicitly. Never claim a build or HTTP-only
test as a completed visual check. Add a regression test for meaningful behavior
using the repository's existing test infrastructure when suitable.
