# Recovery content custody

The PostgreSQL archive alone is insufficient. A recovery set contains an exported database snapshot, the exact retained Dataset and filesystem objects reachable from that snapshot, encryption keys, and a sealed external manifest. Native recovery is an operator maintenance capability, not an ordinary Dataset or filesystem permission.

`RecoveryAuthority.create` commits a preparing set before capture. Capture acquires the shared session retention lock before opening its repeatable-read transaction, exports that snapshot, and enumerates retained `snapshot_history`, prepared native snapshot candidates, and filesystem content versions. Separate current transactions persist fenced membership and retention pins. Those later writes are represented by the external inventory; they are not assumed visible in the exported dump. The shared retention lock remains held through sealing. Physical collection must acquire the exclusive lock before its final reference check and deletion.

The live backup read guard requires the exact set owner, fence, incarnation, unexpired lease, immutable member identity, and captured namespace state. It deliberately permits custody of an already disabled retained namespace. A later namespace, classification, managed binding, or provider change invalidates capture. It cannot grant ordinary Dataset reads or source writes. Backup writes use a dedicated configured namespace and durable, exact issued-object membership before provider effects.

Source objects stream through the existing bounded POSIX or S3 provider and retain expected size, SHA-256, and provider version. Duplicate logical identities must have identical sealed descriptors. Every native snapshot closure is verified against its original ordered digest. Neither an object prefix nor an arbitrary physical URL is a grant.

The inventory is a backward chain of immutable pages containing at most 128 members and 256 KiB per page. Each member records its original logical object, namespace revision, source kind, physical binding fingerprint, source provider version, and sealed backup copy/version. The root binds the recovery set, member count, referenced bytes, ordered inventory digest, last page, and rebuild dispositions. Counters are exact integers bounded by 2^53−1. Operator limits separately bound roots, members, bytes, durable membership state, dump size, and elapsed time. Private SQLite scratch has a fixed page quota and bounded cache; it is never a catalog reference or an authority source.

The PostgreSQL custom archive uses 8 MiB parts and backward pages of at most 128 descriptors. Owners, grants, and revocations are preserved. The clean host must provision the required database roles with fresh credentials before restore; omitted ACLs or substituted security-definer owners are unacceptable. Keys are separate sealed objects, and their host paths and bytes never enter the inventory.

`OfflineRecovery::open` accepts only roots obtained from the externally verified manifest. It needs no old database lease. It verifies the inventory and archive descriptor chains into a private bounded membership ledger, rejects cycles/conflicting identities, and admits only exact child objects. Its reader pins provider versions and is usable by the key and archive verifiers. `copy_inventory` restores original logical object IDs to an explicitly configured target namespace map, verifies complete bytes and hashes, supports exact immutable recovery, and rechecks target versions before returning a privately constructed `RecoveredContentProof`. The proof contains target physical fingerprints for guarded metadata rebind; it cannot be deserialized from a declared success flag.

Restoration starts with dispatch disabled outside the restored schema. Verification, incarnation rotation, ownership fencing, content rebind, and key installation precede explicit dispatch approval. Old worker, service, execution, indexing, model, destination, automation, and projector capabilities remain disabled. External index artifacts, execution images, model engines, promoted AppHost frontend bundles, and unpublished private partsets have explicit rebuild or requalification dispositions. AppHost bundles live outside SourceFS; application readiness requires the exact declared frontend digest to be installed. They do not become valid merely because the global restore gate is approved.

Legacy Dataset snapshot references are currently rejected by content capture; they are never silently omitted. The combined POSIX/MinIO multipart metadata, filesystem-content, key, fencing and exact-retry recovery fixture passes 20 cases with zero skips (`native-recovery/20261030T235343Z`). Managed typed Dataset and retained Iceberg readback, physical collection, complete retention policy and external-reader allocation bounds remain separate completion requirements. The focused offline POSIX tests prove exact membership, idempotent copy, key verification, and corruption/version denial; they do not certify disaster recovery or capacity.

## Operator commands and approval boundary

The native executable accepts these bounded JSON commands on stdin. These are
operator maintenance commands with a directly configured database credential;
they are not public HTTP routes or ordinary user capabilities. The recovery
connection holds a real schema admission lease and may use the explicit operator
role needed to preserve owners and ACLs. Ordinary runtime schema admission still
rejects administrative or schema-owning roles. Key destinations must resolve to
the restored host's actual configured `MORIA_LOCAL_DIR/auth/session-key` for the
session key; an unrelated verified copy is insufficient.

- `moria-runtime recovery capture`: `{requestKey, owner, retentionSeconds}`.
  Required configuration names the dedicated backup namespace, bounded recovery
  limits, PostgreSQL 18 `pg_dump`, private key files, and `MORIA_LOCAL_DIR`.
  The declared session key must equal the actual `auth/session-key` bytes and
  both physical file versions must stay unchanged through capture. The result
  is a sealed manifest descriptor, its SHA-256 and a durable retention receipt.
  Preserve that receipt independently of the backup provider.
- `moria-runtime restore-from`: `{restore, manifest, targetNamespaces}`.
  The manifest descriptor comes from that independent receipt. The target is an
  empty PostgreSQL 18 database with the captured role attributes and memberships,
  fresh credentials, explicit target storage bindings, private archive scratch
  and exact key destination paths. `MORIA_ISOLATED_RESTORE=1` is required.
  Physical target storage must differ from both the original source and backup;
  logical object identities and namespace revisions stay unchanged.
- `moria-runtime recovery inspect-restore`: `{restore}` returns the durable
  receipt and dispatch state. Exact retries reconcile a completed restore;
  changed identifiers or hashes cannot reuse the result.
- `moria-runtime recovery approve-cli`: `{}` approves the actual current
  operator-installed CLI key/profile configuration only after the restored
  principals and application are explicitly reapproved. Keys used by an earlier
  incarnation are rejected. Disabling an approval permanently fences credentials
  issued at its old generation, including a missed disable/re-enable pulse.
- `moria-runtime recovery assign-cell`: `{restore, environment, owner, cell}`
  moves a restored paused placement to a newly registered current-incarnation
  cell using the exact expected owner tuple. It stays paused; ordinary directory
  activation is a separate explicit operation. Identity-domain transfer similarly
  preserves reservations and uses the current expected owner.

A protected bootstrap record exists before importing the database archive and
is deliberately excluded from it. Ordinary native connections check its dispatch
bit before schema admission. A failed or interrupted restore therefore cannot
briefly dispatch from restored old credentials. The single final transaction
records privately constructed metadata/content/key proofs, performs guarded
physical rebinds, checks that old consumers remain disabled, and approves both
bootstrap and runtime gates. Provider IO and hashing happen before that short
transaction. Approval does not reenable old principals, workers, service peers,
models, indexing/projector bindings, automations or delivery destinations.

All pending external deliveries restored from a snapshot are uncertain: absence
of a send at the snapshot boundary does not prove that no later send happened.
Operators must reconcile them against the actual destination before deciding
whether a new attempt is safe. No automatic retry claims external exactly-once
behavior.
