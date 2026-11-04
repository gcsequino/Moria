import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { publishNativeSource } from '../native-source/setup.js';

type Model = { name: string; revision: string };
export async function prepareInstanceModels(input: {
  baseUrl: string;
  token: string;
  environment: string;
  foundation: { rootId: string; digest: string };
  marking: string;
  endpoint: string;
  chat: Model;
  embedding: Model;
}) {
  const endpoint = new URL(input.endpoint);
  assert(
    ['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname),
    'Whole-instance inference must use the explicitly selected local provider',
  );
  const namespace = 'instance_models';
  const reference = (kind: string, name: string) => `${namespace}/${kind}/${name}`;
  const policy = {
    allowedDestinations: ['local-instance-inference'],
    allowedMarkings: [input.marking],
  };
  const declarations = [
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'Policy',
      metadata: { namespace, name: 'Local' },
      spec: { type: 'modelDestination', ...policy },
    },
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'SecretReference',
      metadata: { namespace, name: 'Credential' },
      spec: {
        binding: 'local-instance-provider',
        consumers: [reference('ModelProvider', 'Local')],
      },
    },
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'ModelProvider',
      metadata: { namespace, name: 'Local' },
      spec: {
        adapter: 'openai-compatible-v1',
        endpoint: endpoint.href,
        destination: policy.allowedDestinations[0],
        destinationPolicy: reference('Policy', 'Local'),
        secret: reference('SecretReference', 'Credential'),
      },
    },
    ...[
      { name: 'Chat', model: input.chat, capabilities: ['chat', 'stream', 'tools', 'structured'] },
      { name: 'Embedding', model: input.embedding, capabilities: ['embed'] },
    ].map(({ name, model, capabilities }) => ({
      apiVersion: 'moria/v1alpha2',
      kind: 'Model',
      metadata: { namespace, name },
      spec: {
        provider: reference('ModelProvider', 'Local'),
        upstreamName: model.name,
        versionLabel: model.revision,
        mutable: false,
        capabilities,
      },
    })),
  ];
  const published = await publishNativeSource(input.baseUrl, input.token, [
    {
      path: '.ontology.json',
      text: JSON.stringify({
        version: 1,
        name: 'Local instance inference',
        namespace,
        dependencies: [input.foundation],
      }),
    },
    ...declarations.map((resource) => ({
      path: `${resource.metadata.name}.${resource.kind}.resource.json`,
      text: JSON.stringify(resource),
    })),
  ]);
  const modelBinding = (model: Model, embedding = false) => ({
    endpointRef: 'INSTANCE_MODEL_ENDPOINT',
    secretRef: 'INSTANCE_MODEL_MATERIAL',
    model: model.name,
    modelRevision: model.revision,
    resourceClass: 'models',
    destination: policy.allowedDestinations[0],
    markings: [input.marking],
    destinationPolicy: policy,
    capabilities: embedding ? ['embed'] : ['chat', 'stream', 'tools', 'structured'],
    ...(embedding ? { dimensions: 768, embeddingProtocol: 'ollama-v1' } : {}),
  });
  return {
    published,
    chat: published.resource(reference('Model', 'Chat')),
    embedding: published.resource(reference('Model', 'Embedding')),
    secret: published.resource(reference('SecretReference', 'Credential')),
    profiles: [
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: reference('Model', 'Chat'),
        configuration: { binding: modelBinding(input.chat) },
      },
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: reference('Model', 'Embedding'),
        configuration: { binding: modelBinding(input.embedding, true) },
      },
    ],
    // Kept in the fixture process only; never included in public evidence or UI state.
    environment: {
      INSTANCE_MODEL_ENDPOINT: endpoint.href,
      INSTANCE_MODEL_MATERIAL: randomUUID(),
    },
  };
}
