# SNOTEL milestone

Implemented: official AWDB connector, raw and normalized Iceberg datasets, station/observation object types, a separate read-only API binding, and the hosted React Snow Observatory. See [SNOTEL.md](SNOTEL.md) for source semantics, configuration, CLI recovery, access scope and limitations; see [IMPLEMENTATION.md](IMPLEMENTATION.md) for verification evidence.

Future work includes operator-configurable recurring refreshes, durable connector jobs, station discovery beyond explicit triplets, additional measurements, and per-user identity. No scheduled refresh is enabled by this milestone.
