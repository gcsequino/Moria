# Instance backup and restore

A native recovery set binds a consistent PostgreSQL archive to the exact retained Dataset and filesystem objects, required keys, and a sealed manifest. Keep the archive, manifest, recovery credentials and copied bytes private. Preserve the manifest receipt separately from the backup provider. [Recovery contract and operator commands](production/contracts/recovery-content.md)

Rehearse restore on an isolated target with fresh database role credentials and independently configured storage. Start with external dispatch disabled. Verify the captured bytes and manifest, restore the archived role ownership and grants, rotate the incarnation, rebind verified content, and reconcile jobs and uncertain deliveries before approving dispatch. Application frontends, images and other derived artifacts have separate requalification requirements. [Lifecycle and recovery](production/contracts/lifecycle.md)

The native checkpoint has scoped exact-version v2 and v3 restore proofs. Composed clean-host acceptance remains open. [Recovery issue](https://github.com/gcsequino/Moria/issues/84) · [Verification evidence](IMPLEMENTATION.md#native-integration-and-production-goals--november-5-2026)
