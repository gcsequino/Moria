# API, scheduler and worker processes

The Linux combined launcher uses the native serving process and scheduler. Its optional `MORIA_NATIVE_WORKERS` list starts configured native worker domains. Operational listeners and workers also have independent entry points.

| Process                  | Command                                                                    |
| ------------------------ | -------------------------------------------------------------------------- |
| Combined local instance  | `pnpm start`                                                               |
| Native API               | `pnpm exec tsx apps/local/src/api.ts`                                      |
| Native scheduler         | `pnpm exec tsx apps/local/src/scheduler.ts`                                |
| Native worker            | `pnpm exec tsx apps/local/src/worker.ts` with `MORIA_NATIVE_WORKER_DOMAIN` |
| Static artifact delivery | [Independent static host](STATIC-DELIVERY.md)                              |

Independent runtime processes require external PostgreSQL with matching schema and scoped credentials. Select the native binary through `MORIA_RUNTIME_BINARY` if necessary. A worker requires the registration and environment settings for its domain. The combined launcher owns its child processes, drains them on shutdown and treats unexpected required-child exit as a local instance failure.

Use the [database guide](../DATABASE.md) for schema and role ownership, the [native composition guide](NATIVE-INSTANCE-COMPOSITION.md) for concrete registrations, and the [execution contract](contracts/execution.md) for fencing, attempt and receipt guarantees.
