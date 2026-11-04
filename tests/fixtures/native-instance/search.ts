import { publishNativeSource } from '../native-source/setup.js';

export async function prepareInstanceSearch(input: {
  baseUrl: string;
  token: string;
  environment: string;
  dependencies: { rootId: string; digest: string }[];
  marking: string;
}) {
  const reference = 'instance_search/RetrievalCollection/Records';
  const published = await publishNativeSource(
    input.baseUrl,
    input.token,
    [
      {
        path: '.ontology.json',
        text: JSON.stringify({
          version: 1,
          name: 'Whole instance retrieval',
          namespace: 'instance_search',
          dependencies: input.dependencies,
        }),
      },
      {
        path: 'records.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'RetrievalCollection',
          metadata: { namespace: 'instance_search', name: 'Records' },
          spec: {
            source: 'source_runtime/ObjectType/Record',
            textProperty: 'name',
            embeddingModel: 'instance_models/Model/Embedding',
            chunkSize: 1000,
            chunkOverlap: 100,
            metric: 'cosine',
            searchMode: 'exact',
          },
        }),
      },
    ],
    'ontology',
  );
  return {
    published,
    collection: published.resource(reference),
    profiles: [
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference,
        configuration: {
          serviceBindingRevision: 'whole-instance-search-v1',
          resourceClass: 'search',
          partitions: 2,
          timeoutSeconds: 1800,
          markings: [input.marking],
        },
      },
    ],
  };
}
