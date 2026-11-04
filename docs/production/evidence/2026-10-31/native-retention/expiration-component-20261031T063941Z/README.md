# Dataset expiration component checkpoint

Frozen source 20261031T063554295001Z-c04-expiration-private-v1, manifest SHA-256 316f8a6abdf112e25c8de70468c6236ebcfd2e5e950321260db86566c6f60c84. Schema generation, TypeScript, metadata-pruning unit (1/1), and actual native Source/writer/two-round managed recovery regression (15/15, zero skips, 290.36s) passed. Exact compiler paths and binary digests are in manifest.json.

This validates the controller compilation and compatibility with existing real publication, typed reader and recovery paths. Dedicated finite-policy expiration, qualified S3 reclamation, pins/admission/CAS, backup custody, crash/takeover and provider fault tests remain separate gates. No physical reclamation completion or capacity pass is claimed.
