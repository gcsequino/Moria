# Moria administration console

The bundled React console uses the native API and an authenticated management
session. Sign in with an explicit administrator invitation or obtain a local
operator session with `pnpm moria auth recover --app management`.
[Authentication guide](AUTH.md)

- **Files and sources** browses authorized entries and publishes Source releases. [Filesystem and Source](FILESYSTEM.md)
- **Published resources** shows current resource definitions and opens their operation forms.
- **Runtime status** shows the native instance status. Worker and resource
  availability depend on trusted instance configuration.
- Resource forms use the native object, action, link, Function, Agent, Evaluation,
  Pipeline, Model and Search routes. Current principal and application grants
  determine which requests can succeed.

Use the CLI to inspect or recover promoted console assets.

```sh
pnpm moria apps list
pnpm moria apps recover management
```

Keep local recovery credentials private. [Application hosting](APP-HOST.md) ·
[Local instance](../apps/local/README.md) · [Browser acceptance](https://github.com/gcsequino/Moria/issues/84)
