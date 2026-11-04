# Resource data provenance

`resource_data_edges(environment, derived, source)` is an indexed, append-only resource dependency graph. Resource UUIDs identify filesystem resources. Legacy Dataset sources require an explicitly registered Dataset head. Current published resource bindings resolve aliases without guessing from folders or names. Ambiguous references fail. There are no per-object edges.

`resource_data_labels` retains the union of labels from every Dataset namespace that has ever been enabled. A disabled namespace that was never enabled contributes nothing. Disabling, replacing or deleting a previously enabled namespace cannot declassify history. An explicit audited declassification protocol remains separate.

Node and native resource reads traverse the same graph using distinct recursive membership. They enforce retained labels and the current filesystem read ACL of each source origin, including deleted/inactive source rejection. The target retains its requested operation. A derived ObjectType read does not confer raw Dataset domain permission. Graph traversal stops at 4,096 resources and fails with `RESOURCE_LIMIT` beyond that boundary. This is a bounded control-plane graph; whole-profile capacity is measured separately.

New edges and retained labels increment the environment security epoch. Running work, pins and cursors must recheck their captured epoch. Publication cannot acknowledge a new policy epoch on behalf of existing work.

Admission occurs before immutable work acceptance. `Catalog.admitDataProvenance(client, sessionHash, targets, sources, operation)` requires a fresh verified ordinary origin, current target write/index permission, source read permission, filesystem ACLs and complete label coverage. Index registration checks its declared output marking floor first, admits the source edge, then captures current authority and its immutable fingerprint. Dataset bindings conservatively admit every `expectedHeads × pinnedInputs` edge; their INSERT returns the resulting security epoch. Snapshot and index publication assert these declared edges and reject undeclared lineage.

Native Dataset writes additionally require every inherited input marking in the selected physical namespace's floor. Retained whole-Dataset classification does not permit dropping that physical destination floor. The reserved `filesystem/content` namespace cannot be registered as a Dataset source or indexing artifact; file content uses its own per-object filesystem authority.

`Catalog.datasetClassification` and native `runtime_dataset_kind` require a current published resource of kind `Dataset`, with matching active release digest. RestrictedView/ObjectType resources cannot become whole Datasets by inserting a head or storage namespace.

Functional evidence includes real published native indexing followed by namespace tightening/disable and Node/native denial parity, and real SourceService-published Dataset/ObjectType resources followed by inherited source ACL denial. These cases do not certify a large provenance graph or full mixed-load capacity.
