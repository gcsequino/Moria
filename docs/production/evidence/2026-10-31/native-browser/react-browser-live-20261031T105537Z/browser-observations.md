# Interactive browser observations

Chrome exercised the shipped React management application and a separately published private frontend. The browser created an Object with a real Action, retried the same idempotency key, observed a native Function returning 42 with its succeeded terminal event, and read back a separately created private Object. Cross-application browser access was blocked. The backend independently checked the effective Object names and actual created UIDs.

The outer trial failed because its final Function assertion compared JSON numeric text `42` with the stored `42.0`. That assertion has been corrected to a typed numeric comparison for the subsequent composed-instance proof. These artifacts record successful interactions within a failed outer trial; they do not qualify the complete instance.

`metadata.json`, `source-manifest.json` and the archived external driver identify the exact immutable native executable and commands. `browser-artifacts.json` hashes the token-free screenshots and accessibility snapshots. Private recovery tickets, fixture credentials and private state files are excluded.
