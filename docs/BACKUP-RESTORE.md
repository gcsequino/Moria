# Instance backup and restore verification

An instance includes PostgreSQL, both Iceberg warehouses, application releases and active pointers, local CLI credentials, and the auth configuration/session encryption key. Keep backups private: they contain credentials and session records. Copying only PostgreSQL is not an instance backup.

Create a consistent offline backup after stopping the runtime and confirming its PostgreSQL process has exited. Copy the entire instance directory to a new backup location. Do not copy a running PostgreSQL directory as a substitute for a supported online backup. Restore to the original instance path while services are stopped; retain the previous directory until verification succeeds. Existing Iceberg metadata stores absolute warehouse paths, so arbitrary relocation is not supported yet.

## Isolated drill

From the repository root:

```sh
pnpm exec tsx scripts/verify-backup.ts <offline-backup-instance-directory>
```

The script creates a unique retained clone under `.local/restore-drill-*`, verifies every copied file with SHA-256, starts only the cloned PostgreSQL database on an available loopback port, and stops it before exiting. It reads restored serving objects and overlays, verifies all app releases, and exercises the same local recovery ticket/session service used by the CLI for management and Snow. It never starts HTTP, scheduled jobs, ingestion, or Google authentication. Tokens and passwords are not printed. Retained clones contain private data and should receive the same protection as backups.

A clean backup has no `postgres/postmaster.pid`. For a crash-consistent backup independently confirmed to have been copied only after the source database exited, the explicit `--offline-crash-backup` option allows removal of the stale PID file **in the new clone only** and PostgreSQL WAL recovery. This is not permission to copy or operate on a live database.

## October 21 drill

The pre-durable offline backup was restored in isolation. All 1,187 files matched byte-for-byte. Restored serving data contained four assets, five stations and 1,925 observations, with two asset overlay versions. The four effective assets had the exact pre-deployment SHA-256 digest. All twelve saved app releases verified, and both management and Snow local recovery sessions authenticated and logged out successfully.

This backup predates durable job tables and weather publications. The drill does not establish recovery of those newer live records, production Google login, or full HTTP service startup. Iceberg files were checked as bytes; this drill does not open their tables at a relocated warehouse path. Real PostgreSQL/Iceberg execution tests separately exercise publication and worker recovery. Automated backup rotation, off-device backup, relocation, and unattended operating-system restart remain future work.
