# Permanent native indexing registration

`indexing_service::admit_in` and `install_in` receive trusted compiled metadata
inside the caller's environment/filesystem admission transaction. Admission for
every target runs before any final binding captures its security epoch. The
accepted input includes canonical Dataset UUID, target UUID and schema, the full
Dataset `sourceColumns`, target-to-source `sourceMapping`, explicit output
markings, partitions, deadline and optional provisioned artifact namespace.
Neither interface accepts a physical path, provider endpoint or credential.

The immutable definition also captures the current serving definition/revision,
projection revision, environment UUID and instance incarnation. Unmapped Dataset
columns remain available only as canonical policy input. Object values contain
mapped properties; action overlays apply to those mapped values. Identity mapping
may omit a duplicate raw row, in which case canonical source equals object base.

Registration requires an actual current `RegisteredService` target for domain
`indexing`, source read and target index operations, current filesystem read
access and complete inherited markings within the explicit output floor. It
creates an immutable metadata execution binding and a hash-bound indexing
definition association. This metadata binding cannot request a generic execution
or mint a generic job credential. Bytes and publication use actual indexing
build/task claims, owners, fences, attempts, incarnation and bounded deadlines.

Before accepting a new source head, the permanent registrar revalidates the
current service purpose, exact captured code/schema/layout, source binding,
namespace, filesystem access, retained labels and previously admitted provenance.
An unrelated epoch change can create fresh metadata and a fresh indexing binding
at the resulting current epoch. It cannot rewrite an accepted build or proof,
broaden the explicit marking floor, replace an original user session hash, or
borrow the Dataset publisher's credential. A changed definition or revoked
purpose produces durable blocked indexing lag while the valid Dataset save
still commits.

The service binding revision is C03 SHA-256 of an object containing four strings:
`definitionHash`, `instanceEpoch`, `metadataBinding`, `securityEpoch`. Epochs use
canonical decimal strings, including values beyond JavaScript's exact-number
range. The SQL and Rust implementations share this exact wire format.

The trusted operator CLI `indexing-service register-existing` accepts only
environment, target UUID, expected current binding revision and registered
service target revision. It reconstructs current accepted schemas and policies;
provider configuration remains operator-owned. Public workload/browser requests
cannot submit metadata authority or private indexing credentials.


Candidate admission failures remain local to their accepted build. A known
immutable authorization or build rejection discovered before a claim marks that
selected build failed under its existing row lock, without inventing a worker
owner/fence or overwriting a committed success. The drain continues with other
work after that failure commits. Unknown SQL, connection and schema failures
still stop the process. Publication retains its full current-authority checks.
