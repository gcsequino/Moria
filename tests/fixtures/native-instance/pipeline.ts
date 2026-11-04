import type { Resource } from '../../../packages/contracts/types.js';
import { publishNativeSource } from '../native-source/setup.js';

export const instancePipelineProgram = `def transform(ctx):
    connection = ctx.connection()
    connection.register('actual_cases', ctx.read('instance_logic/Dataset/Cases'))
    ctx.write('instance_pipeline/Dataset/ReviewedCases', connection.sql('SELECT * FROM actual_cases ORDER BY "caseId"'))
    print('whole-instance cases copied through DuckDB')
`;

export async function prepareInstancePipeline(input: {
  baseUrl: string;
  token: string;
  environment: string;
  dependency: { rootId: string; digest: string };
  artifact: { uri: string; digest: string };
  adapter: { uri: string; digest: string };
  serviceAuthority: string;
  namespace: { warehouseId: string; tableId: string; revision: string; icebergUuid: string };
  marking: string;
}) {
  const text = { type: 'string' as const, nullable: false };
  const columns = { caseId: text, expectedJson: text, inputJson: text };
  const transform: Resource = {
    apiVersion: 'moria/v1alpha2',
    kind: 'Transform',
    metadata: { namespace: 'instance_pipeline', name: 'ReviewCases' },
    spec: {
      runtime: 'python-duckdb-v1',
      artifact: input.artifact,
      inputs: ['instance_logic/Dataset/Cases'],
      outputs: ['instance_pipeline/Dataset/ReviewedCases'],
      limits: { timeoutSeconds: 120, memoryMiB: 512, cpuMillis: 60000 },
    },
  };
  const published = await publishNativeSource(
    input.baseUrl,
    input.token,
    [
      {
        path: '.repository.json',
        text: JSON.stringify({
          version: 1,
          name: 'Whole instance pipeline',
          namespace: 'instance_pipeline',
          runtime: 'python-duckdb-v1',
          dependencies: [input.dependency],
        }),
      },
      { path: 'main.py', text: instancePipelineProgram },
      {
        path: 'reviewed.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Dataset',
          metadata: { namespace: 'instance_pipeline', name: 'ReviewedCases' },
          spec: { columns, primaryKey: ['caseId'], writeModes: ['replace'] },
        }),
      },
    ],
    'repository',
    [transform],
  );
  const { icebergUuid, ...namespace } = input.namespace;
  const schema = {
    type: 'struct',
    'schema-id': 0,
    'identifier-field-ids': [],
    fields: Object.keys(columns)
      .sort()
      .map((name, index) => ({
        id: index + 1,
        name,
        required: true,
        type: 'string',
      })),
  };
  return {
    published,
    transform: published.resource('instance_pipeline/Transform/ReviewCases'),
    output: published.resource('instance_pipeline/Dataset/ReviewedCases'),
    profiles: [
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: 'instance_pipeline/Dataset/ReviewedCases',
        configuration: { namespace, icebergUuid, markings: [input.marking] },
      },
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: 'instance_pipeline/Transform/ReviewCases',
        configuration: {
          serviceAuthority: input.serviceAuthority,
          serviceBindingRevision: 'whole-instance-pipeline-v1',
          resourceClass: 'engine-spill',
          hardSpill: true,
          adapter: input.adapter,
          outputs: { 'instance_pipeline/Dataset/ReviewedCases': { namespace, schema } },
          limits: {
            timeoutSeconds: 120,
            memoryMiB: 512,
            cpuMillis: 60000,
            inputBytes: 134217728,
            outputBytes: 134217728,
          },
          connector: null,
        },
      },
    ],
  };
}
