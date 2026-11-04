# Native data-engine I/O and spill contract

Trusted Transform/Sync engines run as isolated OCI guests supervised by the
native runtime. A guest receives declared logical input/output references and
broker handles. It receives no database, provider or host filesystem credentials.
The native broker owns dataset pins, current authority, physical namespace
selection, output roots and atomic grouped catalog publication.

Input and output use continuous Arrow IPC streams. Transport chunks are at most
1 MiB; logical IPC messages are at most 16 MiB, metadata at most 1 MiB and record
batches at most 1,000 rows. Schema, field nodes, buffers, offsets, null counts,
UTF-8 and per-cell complexity are checked before Arrow decoding. Compression,
dictionaries and unsupported physical types are rejected. A legitimate accepted
wide row is not rejected by the transport chunk limit. Empty typed streams retain
schema and Iceberg field IDs. Output completion requires EOS and no incomplete
frame; roots remain private until the domain publication transaction commits.

Input bytes stream under current lease/authority checks. Output writers preserve
C03 types and Iceberg UID/scalar-array annotations. The broker enforces declared
aggregate byte ceilings and records only structural byte/row counters. Guest
references never become host paths. Functions keep their own protocol limits.

Each engine attempt reserves one operator-provisioned dedicated ext4 filesystem
in its claim transaction, before guest creation. Registration verifies the mount,
filesystem UUID, exact root, finite capacity, mount options, root ownership and
sticky guest-writable directory. A persistent root-owned owner marker binds slot
and incarnation. A free-space poll or ordinary shared directory is insufficient.

A slot remains occupied after lease expiry or uncertain creation. Docker creation
uses a deterministic name and private reservation label, with creation intent
committed first. Cleanup verifies the exact container identity, terminal state
and non-forced removal before deleting spill files. A durable cleanup nonce is
committed before blocking deletion. Cancellation cannot release a slot while its
old cleanup thread might continue; an interrupted cleanup remains quarantined.
The guest runs as UID 65532 without capabilities; the trusted host supervisor must
have the actual privileges needed to remove hostile guest-owned files.

`deploy/test/provision-engine-scratch.py` provisions fresh bounded loopback ext4
filesystems for isolated Linux tests. Every new database/trial uses a fresh pool;
a forgotten reservation cannot be bypassed by registering the same filesystem in
a new database. Actual OCI/spill and whole-platform qualification are recorded
separately from parser/unit checks.
