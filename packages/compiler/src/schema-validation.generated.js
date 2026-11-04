import * as runtime0Module from 'ajv/dist/runtime/ucs2length.js';
const runtime0 =
  runtime0Module.default && typeof runtime0Module.default === 'object'
    ? runtime0Module.default
    : runtime0Module;
import * as runtime1Module from 'ajv/dist/runtime/equal.js';
const runtime1 =
  runtime1Module.default && typeof runtime1Module.default === 'object'
    ? runtime1Module.default
    : runtime1Module;
import * as runtime2Module from 'ajv-formats/dist/formats.js';
const runtime2 =
  runtime2Module.default && typeof runtime2Module.default === 'object'
    ? runtime2Module.default
    : runtime2Module;
('use strict');
export const validateShape = validate52;
const schema19 = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  $id: 'https://spec.example.invalid/moria/v0.2/manifest.schema.json',
  title: 'Moria deployment unit manifest v0.2',
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    projectId: { type: 'string', format: 'uuid' },
    deploymentUnitId: { type: 'string', format: 'uuid' },
    resources: { type: 'array', items: { $ref: 'resources.schema.json' }, minItems: 1 },
    dependencies: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          package: { type: 'string', minLength: 1 },
          digest: { type: 'string', pattern: '^sha256:[0-9a-f]{64}$' },
        },
        required: ['package', 'digest'],
        additionalProperties: false,
      },
      minItems: 0,
    },
  },
  required: ['apiVersion', 'projectId', 'deploymentUnitId', 'resources'],
  additionalProperties: false,
};
const formats0 = /^(?:urn:uuid:)?[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
const schema20 = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  $id: 'https://spec.example.invalid/moria/v0.2/resources.schema.json',
  title: 'Moria resource declaration v0.2',
  $ref: '#/$defs/Resource',
  $defs: {
    ScalarType: { $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/Scalar' },
    ValueType: { $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/Column' },
    Fields: { $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/Fields' },
    Expr: {
      oneOf: [
        {
          type: 'object',
          properties: {
            literal: {
              oneOf: [
                { type: ['string', 'number', 'boolean', 'null'] },
                {
                  type: 'array',
                  items: { type: ['string', 'number', 'boolean', 'null'] },
                  minItems: 0,
                },
              ],
            },
          },
          required: ['literal'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: { field: { type: 'string', minLength: 1 } },
          required: ['field'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: { principalAttribute: { type: 'string', minLength: 1 } },
          required: ['principalAttribute'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: { parameter: { type: 'string', minLength: 1 } },
          required: ['parameter'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { enum: ['eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in', 'contains'] },
            args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 2, maxItems: 2 },
          },
          required: ['op', 'args'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { enum: ['and', 'or'] },
            args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 2 },
          },
          required: ['op', 'args'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { enum: ['not', 'isNull'] },
            args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 1, maxItems: 1 },
          },
          required: ['op', 'args'],
          additionalProperties: false,
        },
      ],
    },
    Scope: {
      type: 'object',
      properties: {
        target: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        operations: {
          type: 'array',
          items: { type: 'string', minLength: 1 },
          minItems: 1,
          uniqueItems: true,
        },
      },
      required: ['target', 'operations'],
      additionalProperties: false,
    },
    Limits: {
      type: 'object',
      properties: {
        timeoutSeconds: { type: 'integer', minimum: 1 },
        memoryMiB: { type: 'integer', minimum: 1 },
        cpuMillis: { type: 'integer', minimum: 1 },
      },
      required: ['timeoutSeconds'],
      additionalProperties: false,
    },
    Artifact: {
      type: 'object',
      properties: {
        uri: { type: 'string', minLength: 1 },
        digest: { type: 'string', pattern: '^sha256:[0-9a-f]{64}$' },
      },
      required: ['uri', 'digest'],
      additionalProperties: false,
    },
    Metadata: {
      type: 'object',
      properties: {
        namespace: { type: 'string', pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
        name: { type: 'string', pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
        markings: {
          type: 'array',
          items: {
            type: 'string',
            pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
          },
          minItems: 0,
          uniqueItems: true,
        },
      },
      required: ['namespace', 'name'],
      additionalProperties: false,
    },
    Edit: {
      oneOf: [
        {
          type: 'object',
          properties: {
            op: { const: 'set' },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            uid: { $ref: '#/$defs/Expr' },
            property: { type: 'string', minLength: 1 },
            value: { $ref: '#/$defs/Expr' },
          },
          required: ['op', 'targetType', 'uid', 'property', 'value'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { const: 'clearOverride' },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            uid: { $ref: '#/$defs/Expr' },
            property: { type: 'string', minLength: 1 },
          },
          required: ['op', 'targetType', 'uid', 'property'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { enum: ['delete', 'restore'] },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            uid: { $ref: '#/$defs/Expr' },
          },
          required: ['op', 'targetType', 'uid'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { const: 'create' },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            identityKey: { $ref: '#/$defs/Expr' },
            values: {
              type: 'object',
              propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
              additionalProperties: { $ref: '#/$defs/Expr' },
              minProperties: 1,
            },
          },
          required: ['op', 'targetType', 'identityKey', 'values'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            op: { enum: ['link', 'unlink'] },
            linkType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            sourceUid: { $ref: '#/$defs/Expr' },
            targetUid: { $ref: '#/$defs/Expr' },
          },
          required: ['op', 'linkType', 'sourceUid', 'targetUid'],
          additionalProperties: false,
        },
      ],
    },
    Namespace: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Namespace' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: { description: { type: 'string', minLength: 1 } },
          required: [],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Dataset: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Dataset' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            columns: { $ref: '#/$defs/Fields' },
            primaryKey: {
              type: 'array',
              items: { type: 'string', minLength: 1 },
              minItems: 1,
              uniqueItems: true,
            },
            writeModes: {
              type: 'array',
              items: { enum: ['replace', 'append', 'upsert', 'delete'] },
              minItems: 1,
              uniqueItems: true,
            },
            identityDomain: { type: 'string', minLength: 1 },
          },
          required: ['columns', 'writeModes'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Transform: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Transform' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            inputs: {
              type: 'array',
              items: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              minItems: 0,
              uniqueItems: true,
            },
            outputs: {
              type: 'array',
              items: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              minItems: 1,
              uniqueItems: true,
            },
            artifact: { $ref: '#/$defs/Artifact' },
            runtime: { const: 'python-duckdb-v1' },
            limits: { $ref: '#/$defs/Limits' },
          },
          required: ['inputs', 'outputs', 'artifact', 'runtime', 'limits'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    ConnectorImplementation: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'ConnectorImplementation' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            artifact: { $ref: '#/$defs/Artifact' },
            abiVersion: { const: 'connector/v1' },
            capabilities: {
              type: 'array',
              items: { enum: ['batchRead', 'schemaDiscovery', 'continuousRead', 'writeback'] },
              minItems: 1,
              uniqueItems: true,
            },
            configurationSchema: { type: 'object' },
          },
          required: ['artifact', 'abiVersion', 'capabilities', 'configurationSchema'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Connection: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Connection' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            implementation: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            configuration: { type: 'object' },
            secrets: {
              type: 'array',
              items: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              minItems: 0,
              uniqueItems: true,
            },
            output: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            mode: { enum: ['replace', 'append', 'upsert', 'delete'] },
            coverage: { enum: ['full', 'incremental', 'appendOnly'] },
            stream: { type: 'string', minLength: 1 },
          },
          required: [
            'implementation',
            'configuration',
            'secrets',
            'output',
            'mode',
            'coverage',
            'stream',
          ],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    ObjectType: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'ObjectType' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            primaryKey: { type: 'string', minLength: 1 },
            properties: { $ref: '#/$defs/Fields' },
            source: {
              type: 'object',
              properties: {
                resource: {
                  type: 'string',
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                mapping: {
                  type: 'object',
                  propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
                  additionalProperties: { type: 'string', minLength: 1 },
                  minProperties: 1,
                },
              },
              required: ['resource', 'mapping'],
              additionalProperties: false,
            },
            readPolicy: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            writePolicy: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            propertyPolicies: {
              type: 'object',
              propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
              additionalProperties: {
                type: 'object',
                properties: {
                  read: {
                    type: 'string',
                    pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                  },
                  write: {
                    type: 'string',
                    pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                  },
                },
                required: [],
                additionalProperties: false,
              },
              minProperties: 0,
            },
          },
          required: ['primaryKey', 'properties', 'source'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    LinkType: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'LinkType' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            sourceType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            cardinality: { enum: ['manyToOne', 'oneToOne', 'manyToMany'] },
            mode: { enum: ['mapped', 'actionManaged'] },
            sourceProperty: { type: 'string', minLength: 1 },
            targetProperty: { type: 'string', minLength: 1 },
            readPolicy: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
          },
          required: ['sourceType', 'targetType', 'cardinality', 'mode'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Action: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Action' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            inputs: { $ref: '#/$defs/Fields' },
            targetType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            targetParameter: { type: 'string', minLength: 1 },
            condition: { $ref: '#/$defs/Expr' },
            edits: { type: 'array', items: { $ref: '#/$defs/Edit' }, minItems: 1 },
            function: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
          },
          required: ['inputs', 'targetType', 'condition'],
          additionalProperties: false,
          oneOf: [
            { required: ['edits'], not: { required: ['function'] } },
            { required: ['function'], not: { required: ['edits'] } },
          ],
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Function: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Function' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            artifact: { $ref: '#/$defs/Artifact' },
            runtime: { const: 'typescript-v1' },
            inputs: {
              $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/FunctionFields',
            },
            outputs: {
              $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/FunctionFields',
            },
            effect: { enum: ['readOnly', 'editProposal', 'readWrite'] },
            limits: { $ref: '#/$defs/Limits' },
          },
          required: ['artifact', 'runtime', 'inputs', 'outputs', 'effect', 'limits'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Application: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Application' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            mode: { enum: ['browser', 'service', 'delegated'] },
            scopes: { type: 'array', items: { $ref: '#/$defs/Scope' }, minItems: 0 },
            frontend: { $ref: '#/$defs/Artifact' },
          },
          required: ['mode', 'scopes'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    AgentAdapter: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'AgentAdapter' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            artifact: { $ref: '#/$defs/Artifact' },
            abiVersion: { const: 'agent/v1' },
            capabilities: {
              type: 'array',
              items: { enum: ['events', 'cancel', 'checkpoint', 'resume'] },
              minItems: 1,
              uniqueItems: true,
            },
          },
          required: ['artifact', 'abiVersion', 'capabilities'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Agent: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Agent' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            artifact: { $ref: '#/$defs/Artifact' },
            adapter: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            application: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            model: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            inputs: { $ref: '#/$defs/Fields' },
            outputs: { $ref: '#/$defs/Fields' },
            limits: { $ref: '#/$defs/Limits' },
          },
          required: ['artifact', 'adapter', 'application', 'model', 'inputs', 'outputs', 'limits'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    ModelProvider: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'ModelProvider' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            adapter: { const: 'openai-compatible-v1' },
            endpoint: { type: 'string', format: 'uri', pattern: '^https?://[^\\s/]+' },
            destination: { type: 'string', minLength: 1 },
            secret: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            destinationPolicy: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
          },
          required: ['adapter', 'endpoint', 'destination', 'destinationPolicy'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Model: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Model' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            provider: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            upstreamName: { type: 'string', minLength: 1 },
            versionLabel: { type: 'string', minLength: 1 },
            mutable: { type: 'boolean' },
            capabilities: {
              type: 'array',
              items: { enum: ['chat', 'tools', 'structuredOutput', 'streaming', 'embeddings'] },
              minItems: 1,
              uniqueItems: true,
            },
            embeddingDimensions: { type: 'integer', minimum: 1 },
          },
          required: ['provider', 'upstreamName', 'mutable', 'capabilities'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    RetrievalCollection: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'RetrievalCollection' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            source: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            textProperty: { type: 'string', minLength: 1 },
            embeddingModel: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            chunkSize: { type: 'integer', minimum: 1 },
            chunkOverlap: { type: 'integer', minimum: 0 },
            metric: { const: 'cosine' },
            searchMode: { const: 'exact' },
          },
          required: [
            'source',
            'textProperty',
            'embeddingModel',
            'chunkSize',
            'chunkOverlap',
            'metric',
            'searchMode',
          ],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    EvaluationSuite: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'EvaluationSuite' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            agent: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            fixtures: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            evaluator: { $ref: '#/$defs/Artifact' },
            repeats: { type: 'integer', minimum: 1 },
          },
          required: ['agent', 'fixtures', 'evaluator', 'repeats'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Automation: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Automation' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            operation: { enum: ['build', 'sync', 'invokeAgent', 'evaluate'] },
            target: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            application: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            intervalSeconds: { type: 'integer', minimum: 1 },
            input: { type: 'object' },
            overlap: { const: 'skip' },
            missed: { const: 'coalesce' },
          },
          required: [
            'operation',
            'target',
            'application',
            'intervalSeconds',
            'input',
            'overlap',
            'missed',
          ],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Policy: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Policy' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          oneOf: [
            {
              type: 'object',
              properties: {
                type: {
                  enum: ['row', 'objectRead', 'objectWrite', 'propertyRead', 'propertyWrite'],
                },
                expression: { $ref: '#/$defs/Expr' },
              },
              required: ['type', 'expression'],
              additionalProperties: false,
            },
            {
              type: 'object',
              properties: {
                type: { const: 'modelDestination' },
                allowedDestinations: {
                  type: 'array',
                  items: { type: 'string', minLength: 1 },
                  minItems: 1,
                  uniqueItems: true,
                },
                allowedMarkings: {
                  type: 'array',
                  items: {
                    type: 'string',
                    pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                  },
                  minItems: 0,
                  uniqueItems: true,
                },
              },
              required: ['type', 'allowedDestinations', 'allowedMarkings'],
              additionalProperties: false,
            },
          ],
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    SecretReference: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'SecretReference' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            binding: { type: 'string', minLength: 1 },
            consumers: {
              type: 'array',
              items: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              minItems: 1,
              uniqueItems: true,
            },
          },
          required: ['binding', 'consumers'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Marking: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Marking' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            eligibilityGroup: { type: 'string', minLength: 1 },
            declassifierGroup: { type: 'string', minLength: 1 },
          },
          required: ['eligibilityGroup', 'declassifierGroup'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    RestrictedView: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'RestrictedView' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            dataset: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            policy: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
          },
          required: ['dataset', 'policy'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Grant: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'Grant' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          type: 'object',
          properties: {
            subjects: {
              type: 'array',
              items: { type: 'string', minLength: 1 },
              minItems: 1,
              uniqueItems: true,
            },
            application: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            target: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            operations: {
              type: 'array',
              items: { type: 'string', minLength: 1 },
              minItems: 1,
              uniqueItems: true,
            },
            environment: { type: 'string', minLength: 1 },
          },
          required: ['subjects', 'application', 'target', 'operations', 'environment'],
          additionalProperties: false,
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    ObjectSet: {
      type: 'object',
      properties: {
        apiVersion: { const: 'moria/v1alpha2' },
        kind: { const: 'ObjectSet' },
        metadata: { $ref: '#/$defs/Metadata' },
        spec: {
          oneOf: [
            {
              type: 'object',
              properties: {
                mode: { const: 'static' },
                objectType: {
                  type: 'string',
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                uids: {
                  type: 'array',
                  items: { type: 'string', format: 'uuid' },
                  minItems: 0,
                  uniqueItems: true,
                },
              },
              required: ['mode', 'objectType', 'uids'],
              additionalProperties: false,
            },
            {
              type: 'object',
              properties: {
                mode: { const: 'dynamic' },
                objectType: {
                  type: 'string',
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                where: { $ref: '#/$defs/Expr' },
              },
              required: ['mode', 'objectType', 'where'],
              additionalProperties: false,
            },
          ],
        },
      },
      required: ['apiVersion', 'kind', 'metadata', 'spec'],
      additionalProperties: false,
    },
    Resource: {
      oneOf: [
        { $ref: '#/$defs/Namespace' },
        { $ref: '#/$defs/Dataset' },
        { $ref: '#/$defs/Transform' },
        { $ref: '#/$defs/ConnectorImplementation' },
        { $ref: '#/$defs/Connection' },
        { $ref: '#/$defs/ObjectType' },
        { $ref: '#/$defs/LinkType' },
        { $ref: '#/$defs/Action' },
        { $ref: '#/$defs/Function' },
        { $ref: '#/$defs/Application' },
        { $ref: '#/$defs/AgentAdapter' },
        { $ref: '#/$defs/Agent' },
        { $ref: '#/$defs/ModelProvider' },
        { $ref: '#/$defs/Model' },
        { $ref: '#/$defs/RetrievalCollection' },
        { $ref: '#/$defs/EvaluationSuite' },
        { $ref: '#/$defs/Automation' },
        { $ref: '#/$defs/Policy' },
        { $ref: '#/$defs/SecretReference' },
        { $ref: '#/$defs/Marking' },
        { $ref: '#/$defs/RestrictedView' },
        { $ref: '#/$defs/Grant' },
        { $ref: '#/$defs/ObjectSet' },
      ],
    },
  },
};
const schema21 = {
  oneOf: [
    { $ref: '#/$defs/Namespace' },
    { $ref: '#/$defs/Dataset' },
    { $ref: '#/$defs/Transform' },
    { $ref: '#/$defs/ConnectorImplementation' },
    { $ref: '#/$defs/Connection' },
    { $ref: '#/$defs/ObjectType' },
    { $ref: '#/$defs/LinkType' },
    { $ref: '#/$defs/Action' },
    { $ref: '#/$defs/Function' },
    { $ref: '#/$defs/Application' },
    { $ref: '#/$defs/AgentAdapter' },
    { $ref: '#/$defs/Agent' },
    { $ref: '#/$defs/ModelProvider' },
    { $ref: '#/$defs/Model' },
    { $ref: '#/$defs/RetrievalCollection' },
    { $ref: '#/$defs/EvaluationSuite' },
    { $ref: '#/$defs/Automation' },
    { $ref: '#/$defs/Policy' },
    { $ref: '#/$defs/SecretReference' },
    { $ref: '#/$defs/Marking' },
    { $ref: '#/$defs/RestrictedView' },
    { $ref: '#/$defs/Grant' },
    { $ref: '#/$defs/ObjectSet' },
  ],
};
const schema22 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Namespace' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: { description: { type: 'string', minLength: 1 } },
      required: [],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema23 = {
  type: 'object',
  properties: {
    namespace: { type: 'string', pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
    name: { type: 'string', pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
    markings: {
      type: 'array',
      items: {
        type: 'string',
        pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
      },
      minItems: 0,
      uniqueItems: true,
    },
  },
  required: ['namespace', 'name'],
  additionalProperties: false,
};
const pattern3 = new RegExp('^[A-Za-z_][A-Za-z0-9_-]*$', 'u');
const pattern5 = new RegExp(
  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
  'u',
);
function validate56(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate56.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.namespace === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'namespace' },
        message: "must have required property '" + 'namespace' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.name === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'name' },
        message: "must have required property '" + 'name' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'namespace' || key0 === 'name' || key0 === 'markings')) {
        const err2 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
    }
    if (data.namespace !== undefined) {
      let data0 = data.namespace;
      if (typeof data0 === 'string') {
        if (!pattern3.test(data0)) {
          const err3 = {
            instancePath: instancePath + '/namespace',
            schemaPath: '#/properties/namespace/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
            message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_-]*$' + '"',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
      } else {
        const err4 = {
          instancePath: instancePath + '/namespace',
          schemaPath: '#/properties/namespace/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.name !== undefined) {
      let data1 = data.name;
      if (typeof data1 === 'string') {
        if (!pattern3.test(data1)) {
          const err5 = {
            instancePath: instancePath + '/name',
            schemaPath: '#/properties/name/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]*$' },
            message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_-]*$' + '"',
          };
          if (vErrors === null) {
            vErrors = [err5];
          } else {
            vErrors.push(err5);
          }
          errors++;
        }
      } else {
        const err6 = {
          instancePath: instancePath + '/name',
          schemaPath: '#/properties/name/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.markings !== undefined) {
      let data2 = data.markings;
      if (Array.isArray(data2)) {
        if (data2.length < 0) {
          const err7 = {
            instancePath: instancePath + '/markings',
            schemaPath: '#/properties/markings/minItems',
            keyword: 'minItems',
            params: { limit: 0 },
            message: 'must NOT have fewer than 0 items',
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        const len0 = data2.length;
        for (let i0 = 0; i0 < len0; i0++) {
          let data3 = data2[i0];
          if (typeof data3 === 'string') {
            if (!pattern5.test(data3)) {
              const err8 = {
                instancePath: instancePath + '/markings/' + i0,
                schemaPath: '#/properties/markings/items/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err8];
              } else {
                vErrors.push(err8);
              }
              errors++;
            }
          } else {
            const err9 = {
              instancePath: instancePath + '/markings/' + i0,
              schemaPath: '#/properties/markings/items/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        let i1 = data2.length;
        let j0;
        if (i1 > 1) {
          const indices0 = {};
          for (; i1--;) {
            let item0 = data2[i1];
            if (typeof item0 !== 'string') {
              continue;
            }
            if (typeof indices0[item0] == 'number') {
              j0 = indices0[item0];
              const err10 = {
                instancePath: instancePath + '/markings',
                schemaPath: '#/properties/markings/uniqueItems',
                keyword: 'uniqueItems',
                params: { i: i1, j: j0 },
                message:
                  'must NOT have duplicate items (items ## ' +
                  j0 +
                  ' and ' +
                  i1 +
                  ' are identical)',
              };
              if (vErrors === null) {
                vErrors = [err10];
              } else {
                vErrors.push(err10);
              }
              errors++;
              break;
            }
            indices0[item0] = i1;
          }
        }
      } else {
        const err11 = {
          instancePath: instancePath + '/markings',
          schemaPath: '#/properties/markings/type',
          keyword: 'type',
          params: { type: 'array' },
          message: 'must be array',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      }
    }
  } else {
    const err12 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err12];
    } else {
      vErrors.push(err12);
    }
    errors++;
  }
  validate56.errors = vErrors;
  return errors === 0;
}
validate56.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const func1 = runtime0.default;
function validate55(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate55.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Namespace' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Namespace' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        for (const key1 in data3) {
          if (!(key1 === 'description')) {
            const err7 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err7];
            } else {
              vErrors.push(err7);
            }
            errors++;
          }
        }
        if (data3.description !== undefined) {
          let data4 = data3.description;
          if (typeof data4 === 'string') {
            if (func1(data4) < 1) {
              const err8 = {
                instancePath: instancePath + '/spec/description',
                schemaPath: '#/properties/spec/properties/description/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err8];
              } else {
                vErrors.push(err8);
              }
              errors++;
            }
          } else {
            const err9 = {
              instancePath: instancePath + '/spec/description',
              schemaPath: '#/properties/spec/properties/description/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
      } else {
        const err10 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err10];
        } else {
          vErrors.push(err10);
        }
        errors++;
      }
    }
  } else {
    const err11 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err11];
    } else {
      vErrors.push(err11);
    }
    errors++;
  }
  validate55.errors = vErrors;
  return errors === 0;
}
validate55.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema24 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Dataset' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        columns: { $ref: '#/$defs/Fields' },
        primaryKey: {
          type: 'array',
          items: { type: 'string', minLength: 1 },
          minItems: 1,
          uniqueItems: true,
        },
        writeModes: {
          type: 'array',
          items: { enum: ['replace', 'append', 'upsert', 'delete'] },
          minItems: 1,
          uniqueItems: true,
        },
        identityDomain: { type: 'string', minLength: 1 },
      },
      required: ['columns', 'writeModes'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema29 = {
  type: 'object',
  propertyNames: {
    pattern: '^[A-Za-z_][A-Za-z0-9_]*$',
    maxLength: 100,
    not: { enum: ['constructor', 'prototype', '__proto__'] },
  },
  additionalProperties: { $ref: '#/$defs/Column' },
  minProperties: 1,
};
const pattern8 = new RegExp('^[A-Za-z_][A-Za-z0-9_]*$', 'u');
const schema30 = {
  oneOf: [
    { $ref: '#/$defs/Scalar' },
    {
      type: 'object',
      properties: {
        type: { const: 'array' },
        nullable: { type: 'boolean' },
        items: {
          allOf: [{ $ref: '#/$defs/Scalar' }, { properties: { nullable: { const: false } } }],
        },
      },
      required: ['type', 'nullable', 'items'],
      additionalProperties: false,
    },
  ],
};
const schema28 = {
  oneOf: [
    {
      type: 'object',
      properties: {
        type: { enum: ['string', 'boolean', 'int64', 'float64', 'date', 'timestamp', 'uid'] },
        nullable: { type: 'boolean' },
      },
      required: ['type', 'nullable'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        type: { const: 'decimal' },
        nullable: { type: 'boolean' },
        precision: { type: 'integer', minimum: 1, maximum: 38 },
        scale: { type: 'integer', minimum: 0, maximum: 38 },
      },
      required: ['type', 'nullable', 'precision', 'scale'],
      additionalProperties: false,
    },
  ],
};
function validate65(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate65.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.type === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'type' },
        message: "must have required property '" + 'type' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.nullable === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'nullable' },
        message: "must have required property '" + 'nullable' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'type' || key0 === 'nullable')) {
        const err2 = {
          instancePath,
          schemaPath: '#/oneOf/0/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
    }
    if (data.type !== undefined) {
      let data0 = data.type;
      if (!(
        data0 === 'string' ||
        data0 === 'boolean' ||
        data0 === 'int64' ||
        data0 === 'float64' ||
        data0 === 'date' ||
        data0 === 'timestamp' ||
        data0 === 'uid'
      )) {
        const err3 = {
          instancePath: instancePath + '/type',
          schemaPath: '#/oneOf/0/properties/type/enum',
          keyword: 'enum',
          params: { allowedValues: schema28.oneOf[0].properties.type.enum },
          message: 'must be equal to one of the allowed values',
        };
        if (vErrors === null) {
          vErrors = [err3];
        } else {
          vErrors.push(err3);
        }
        errors++;
      }
    }
    if (data.nullable !== undefined) {
      if (typeof data.nullable !== 'boolean') {
        const err4 = {
          instancePath: instancePath + '/nullable',
          schemaPath: '#/oneOf/0/properties/nullable/type',
          keyword: 'type',
          params: { type: 'boolean' },
          message: 'must be boolean',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
  } else {
    const err5 = {
      instancePath,
      schemaPath: '#/oneOf/0/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err5];
    } else {
      vErrors.push(err5);
    }
    errors++;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
    var props0 = true;
  }
  const _errs7 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.type === undefined) {
      const err6 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'type' },
        message: "must have required property '" + 'type' + "'",
      };
      if (vErrors === null) {
        vErrors = [err6];
      } else {
        vErrors.push(err6);
      }
      errors++;
    }
    if (data.nullable === undefined) {
      const err7 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'nullable' },
        message: "must have required property '" + 'nullable' + "'",
      };
      if (vErrors === null) {
        vErrors = [err7];
      } else {
        vErrors.push(err7);
      }
      errors++;
    }
    if (data.precision === undefined) {
      const err8 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'precision' },
        message: "must have required property '" + 'precision' + "'",
      };
      if (vErrors === null) {
        vErrors = [err8];
      } else {
        vErrors.push(err8);
      }
      errors++;
    }
    if (data.scale === undefined) {
      const err9 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'scale' },
        message: "must have required property '" + 'scale' + "'",
      };
      if (vErrors === null) {
        vErrors = [err9];
      } else {
        vErrors.push(err9);
      }
      errors++;
    }
    for (const key1 in data) {
      if (!(key1 === 'type' || key1 === 'nullable' || key1 === 'precision' || key1 === 'scale')) {
        const err10 = {
          instancePath,
          schemaPath: '#/oneOf/1/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key1 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err10];
        } else {
          vErrors.push(err10);
        }
        errors++;
      }
    }
    if (data.type !== undefined) {
      if ('decimal' !== data.type) {
        const err11 = {
          instancePath: instancePath + '/type',
          schemaPath: '#/oneOf/1/properties/type/const',
          keyword: 'const',
          params: { allowedValue: 'decimal' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      }
    }
    if (data.nullable !== undefined) {
      if (typeof data.nullable !== 'boolean') {
        const err12 = {
          instancePath: instancePath + '/nullable',
          schemaPath: '#/oneOf/1/properties/nullable/type',
          keyword: 'type',
          params: { type: 'boolean' },
          message: 'must be boolean',
        };
        if (vErrors === null) {
          vErrors = [err12];
        } else {
          vErrors.push(err12);
        }
        errors++;
      }
    }
    if (data.precision !== undefined) {
      let data4 = data.precision;
      if (!(typeof data4 == 'number' && !(data4 % 1) && !isNaN(data4))) {
        const err13 = {
          instancePath: instancePath + '/precision',
          schemaPath: '#/oneOf/1/properties/precision/type',
          keyword: 'type',
          params: { type: 'integer' },
          message: 'must be integer',
        };
        if (vErrors === null) {
          vErrors = [err13];
        } else {
          vErrors.push(err13);
        }
        errors++;
      }
      if (typeof data4 == 'number') {
        if (data4 > 38 || isNaN(data4)) {
          const err14 = {
            instancePath: instancePath + '/precision',
            schemaPath: '#/oneOf/1/properties/precision/maximum',
            keyword: 'maximum',
            params: { comparison: '<=', limit: 38 },
            message: 'must be <= 38',
          };
          if (vErrors === null) {
            vErrors = [err14];
          } else {
            vErrors.push(err14);
          }
          errors++;
        }
        if (data4 < 1 || isNaN(data4)) {
          const err15 = {
            instancePath: instancePath + '/precision',
            schemaPath: '#/oneOf/1/properties/precision/minimum',
            keyword: 'minimum',
            params: { comparison: '>=', limit: 1 },
            message: 'must be >= 1',
          };
          if (vErrors === null) {
            vErrors = [err15];
          } else {
            vErrors.push(err15);
          }
          errors++;
        }
      }
    }
    if (data.scale !== undefined) {
      let data5 = data.scale;
      if (!(typeof data5 == 'number' && !(data5 % 1) && !isNaN(data5))) {
        const err16 = {
          instancePath: instancePath + '/scale',
          schemaPath: '#/oneOf/1/properties/scale/type',
          keyword: 'type',
          params: { type: 'integer' },
          message: 'must be integer',
        };
        if (vErrors === null) {
          vErrors = [err16];
        } else {
          vErrors.push(err16);
        }
        errors++;
      }
      if (typeof data5 == 'number') {
        if (data5 > 38 || isNaN(data5)) {
          const err17 = {
            instancePath: instancePath + '/scale',
            schemaPath: '#/oneOf/1/properties/scale/maximum',
            keyword: 'maximum',
            params: { comparison: '<=', limit: 38 },
            message: 'must be <= 38',
          };
          if (vErrors === null) {
            vErrors = [err17];
          } else {
            vErrors.push(err17);
          }
          errors++;
        }
        if (data5 < 0 || isNaN(data5)) {
          const err18 = {
            instancePath: instancePath + '/scale',
            schemaPath: '#/oneOf/1/properties/scale/minimum',
            keyword: 'minimum',
            params: { comparison: '>=', limit: 0 },
            message: 'must be >= 0',
          };
          if (vErrors === null) {
            vErrors = [err18];
          } else {
            vErrors.push(err18);
          }
          errors++;
        }
      }
    }
  } else {
    const err19 = {
      instancePath,
      schemaPath: '#/oneOf/1/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err19];
    } else {
      vErrors.push(err19);
    }
    errors++;
  }
  var _valid0 = _errs7 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true) {
        props0 = true;
      }
    }
  }
  if (!valid0) {
    const err20 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err20];
    } else {
      vErrors.push(err20);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate65.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate65.evaluated = { dynamicProps: true, dynamicItems: false };
function validate69(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate69.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (
    !validate65(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate65.errors : vErrors.concat(validate65.errors);
    errors = vErrors.length;
  } else {
    var props0 = validate65.evaluated.props;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
  }
  const _errs2 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.type === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'type' },
        message: "must have required property '" + 'type' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.nullable === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'nullable' },
        message: "must have required property '" + 'nullable' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.items === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'items' },
        message: "must have required property '" + 'items' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'type' || key0 === 'nullable' || key0 === 'items')) {
        const err3 = {
          instancePath,
          schemaPath: '#/oneOf/1/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err3];
        } else {
          vErrors.push(err3);
        }
        errors++;
      }
    }
    if (data.type !== undefined) {
      if ('array' !== data.type) {
        const err4 = {
          instancePath: instancePath + '/type',
          schemaPath: '#/oneOf/1/properties/type/const',
          keyword: 'const',
          params: { allowedValue: 'array' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.nullable !== undefined) {
      if (typeof data.nullable !== 'boolean') {
        const err5 = {
          instancePath: instancePath + '/nullable',
          schemaPath: '#/oneOf/1/properties/nullable/type',
          keyword: 'type',
          params: { type: 'boolean' },
          message: 'must be boolean',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.items !== undefined) {
      let data2 = data.items;
      if (
        !validate65(data2, {
          instancePath: instancePath + '/items',
          parentData: data,
          parentDataProperty: 'items',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate65.errors : vErrors.concat(validate65.errors);
        errors = vErrors.length;
      } else {
        var props1 = validate65.evaluated.props;
      }
      if (data2 && typeof data2 == 'object' && !Array.isArray(data2)) {
        if (data2.nullable !== undefined) {
          if (false !== data2.nullable) {
            const err6 = {
              instancePath: instancePath + '/items/nullable',
              schemaPath: '#/oneOf/1/properties/items/allOf/1/properties/nullable/const',
              keyword: 'const',
              params: { allowedValue: false },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err6];
            } else {
              vErrors.push(err6);
            }
            errors++;
          }
        }
      }
      if (props1 !== true) {
        props1 = props1 || {};
        props1.nullable = true;
      }
    }
  } else {
    const err7 = {
      instancePath,
      schemaPath: '#/oneOf/1/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err7];
    } else {
      vErrors.push(err7);
    }
    errors++;
  }
  var _valid0 = _errs2 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true) {
        props0 = true;
      }
    }
  }
  if (!valid0) {
    const err8 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err8];
    } else {
      vErrors.push(err8);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate69.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate69.evaluated = { dynamicProps: true, dynamicItems: false };
function validate68(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate68.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (Object.keys(data).length < 1) {
      const err0 = {
        instancePath,
        schemaPath: '#/minProperties',
        keyword: 'minProperties',
        params: { limit: 1 },
        message: 'must NOT have fewer than 1 properties',
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    for (const key0 in data) {
      const _errs1 = errors;
      const _errs2 = errors;
      const _errs3 = errors;
      if (!(key0 === 'constructor' || key0 === 'prototype' || key0 === '__proto__')) {
        const err1 = {};
        if (vErrors === null) {
          vErrors = [err1];
        } else {
          vErrors.push(err1);
        }
        errors++;
      }
      var valid1 = _errs3 === errors;
      if (valid1) {
        const err2 = {
          instancePath,
          schemaPath: '#/propertyNames/not',
          keyword: 'not',
          params: {},
          message: 'must NOT be valid',
          propertyName: key0,
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      } else {
        errors = _errs2;
        if (vErrors !== null) {
          if (_errs2) {
            vErrors.length = _errs2;
          } else {
            vErrors = null;
          }
        }
      }
      if (typeof key0 === 'string') {
        if (func1(key0) > 100) {
          const err3 = {
            instancePath,
            schemaPath: '#/propertyNames/maxLength',
            keyword: 'maxLength',
            params: { limit: 100 },
            message: 'must NOT have more than 100 characters',
            propertyName: key0,
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
        if (!pattern8.test(key0)) {
          const err4 = {
            instancePath,
            schemaPath: '#/propertyNames/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
            message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_]*$' + '"',
            propertyName: key0,
          };
          if (vErrors === null) {
            vErrors = [err4];
          } else {
            vErrors.push(err4);
          }
          errors++;
        }
      }
      var valid0 = _errs1 === errors;
      if (!valid0) {
        const err5 = {
          instancePath,
          schemaPath: '#/propertyNames',
          keyword: 'propertyNames',
          params: { propertyName: key0 },
          message: 'property name must be valid',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    for (const key1 in data) {
      if (
        !validate69(data[key1], {
          instancePath: instancePath + '/' + key1.replace(/~/g, '~0').replace(/\//g, '~1'),
          parentData: data,
          parentDataProperty: key1,
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate69.errors : vErrors.concat(validate69.errors);
        errors = vErrors.length;
      }
    }
  } else {
    const err6 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err6];
    } else {
      vErrors.push(err6);
    }
    errors++;
  }
  validate68.errors = vErrors;
  return errors === 0;
}
validate68.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const func0 = runtime1.default;
function validate59(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate59.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Dataset' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Dataset' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.columns === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'columns' },
            message: "must have required property '" + 'columns' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.writeModes === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'writeModes' },
            message: "must have required property '" + 'writeModes' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'columns' ||
            key1 === 'primaryKey' ||
            key1 === 'writeModes' ||
            key1 === 'identityDomain'
          )) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.columns !== undefined) {
          if (
            !validate68(data3.columns, {
              instancePath: instancePath + '/spec/columns',
              parentData: data3,
              parentDataProperty: 'columns',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate68.errors : vErrors.concat(validate68.errors);
            errors = vErrors.length;
          }
        }
        if (data3.primaryKey !== undefined) {
          let data5 = data3.primaryKey;
          if (Array.isArray(data5)) {
            if (data5.length < 1) {
              const err10 = {
                instancePath: instancePath + '/spec/primaryKey',
                schemaPath: '#/properties/spec/properties/primaryKey/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err10];
              } else {
                vErrors.push(err10);
              }
              errors++;
            }
            const len0 = data5.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data6 = data5[i0];
              if (typeof data6 === 'string') {
                if (func1(data6) < 1) {
                  const err11 = {
                    instancePath: instancePath + '/spec/primaryKey/' + i0,
                    schemaPath: '#/properties/spec/properties/primaryKey/items/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err11];
                  } else {
                    vErrors.push(err11);
                  }
                  errors++;
                }
              } else {
                const err12 = {
                  instancePath: instancePath + '/spec/primaryKey/' + i0,
                  schemaPath: '#/properties/spec/properties/primaryKey/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err12];
                } else {
                  vErrors.push(err12);
                }
                errors++;
              }
            }
            let i1 = data5.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data5[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err13 = {
                    instancePath: instancePath + '/spec/primaryKey',
                    schemaPath: '#/properties/spec/properties/primaryKey/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err13];
                  } else {
                    vErrors.push(err13);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err14 = {
              instancePath: instancePath + '/spec/primaryKey',
              schemaPath: '#/properties/spec/properties/primaryKey/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.writeModes !== undefined) {
          let data7 = data3.writeModes;
          if (Array.isArray(data7)) {
            if (data7.length < 1) {
              const err15 = {
                instancePath: instancePath + '/spec/writeModes',
                schemaPath: '#/properties/spec/properties/writeModes/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err15];
              } else {
                vErrors.push(err15);
              }
              errors++;
            }
            const len1 = data7.length;
            for (let i2 = 0; i2 < len1; i2++) {
              let data8 = data7[i2];
              if (!(
                data8 === 'replace' ||
                data8 === 'append' ||
                data8 === 'upsert' ||
                data8 === 'delete'
              )) {
                const err16 = {
                  instancePath: instancePath + '/spec/writeModes/' + i2,
                  schemaPath: '#/properties/spec/properties/writeModes/items/enum',
                  keyword: 'enum',
                  params: {
                    allowedValues: schema24.properties.spec.properties.writeModes.items.enum,
                  },
                  message: 'must be equal to one of the allowed values',
                };
                if (vErrors === null) {
                  vErrors = [err16];
                } else {
                  vErrors.push(err16);
                }
                errors++;
              }
            }
            let i3 = data7.length;
            let j1;
            if (i3 > 1) {
              outer0: for (; i3--;) {
                for (j1 = i3; j1--;) {
                  if (func0(data7[i3], data7[j1])) {
                    const err17 = {
                      instancePath: instancePath + '/spec/writeModes',
                      schemaPath: '#/properties/spec/properties/writeModes/uniqueItems',
                      keyword: 'uniqueItems',
                      params: { i: i3, j: j1 },
                      message:
                        'must NOT have duplicate items (items ## ' +
                        j1 +
                        ' and ' +
                        i3 +
                        ' are identical)',
                    };
                    if (vErrors === null) {
                      vErrors = [err17];
                    } else {
                      vErrors.push(err17);
                    }
                    errors++;
                    break outer0;
                  }
                }
              }
            }
          } else {
            const err18 = {
              instancePath: instancePath + '/spec/writeModes',
              schemaPath: '#/properties/spec/properties/writeModes/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err18];
            } else {
              vErrors.push(err18);
            }
            errors++;
          }
        }
        if (data3.identityDomain !== undefined) {
          let data9 = data3.identityDomain;
          if (typeof data9 === 'string') {
            if (func1(data9) < 1) {
              const err19 = {
                instancePath: instancePath + '/spec/identityDomain',
                schemaPath: '#/properties/spec/properties/identityDomain/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err19];
              } else {
                vErrors.push(err19);
              }
              errors++;
            }
          } else {
            const err20 = {
              instancePath: instancePath + '/spec/identityDomain',
              schemaPath: '#/properties/spec/properties/identityDomain/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err20];
            } else {
              vErrors.push(err20);
            }
            errors++;
          }
        }
      } else {
        const err21 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err21];
        } else {
          vErrors.push(err21);
        }
        errors++;
      }
    }
  } else {
    const err22 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err22];
    } else {
      vErrors.push(err22);
    }
    errors++;
  }
  validate59.errors = vErrors;
  return errors === 0;
}
validate59.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema31 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Transform' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        inputs: {
          type: 'array',
          items: {
            type: 'string',
            pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
          },
          minItems: 0,
          uniqueItems: true,
        },
        outputs: {
          type: 'array',
          items: {
            type: 'string',
            pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
          },
          minItems: 1,
          uniqueItems: true,
        },
        artifact: { $ref: '#/$defs/Artifact' },
        runtime: { const: 'python-duckdb-v1' },
        limits: { $ref: '#/$defs/Limits' },
      },
      required: ['inputs', 'outputs', 'artifact', 'runtime', 'limits'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema32 = {
  type: 'object',
  properties: {
    uri: { type: 'string', minLength: 1 },
    digest: { type: 'string', pattern: '^sha256:[0-9a-f]{64}$' },
  },
  required: ['uri', 'digest'],
  additionalProperties: false,
};
const pattern11 = new RegExp('^sha256:[0-9a-f]{64}$', 'u');
function validate77(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate77.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.uri === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'uri' },
        message: "must have required property '" + 'uri' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.digest === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'digest' },
        message: "must have required property '" + 'digest' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'uri' || key0 === 'digest')) {
        const err2 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
    }
    if (data.uri !== undefined) {
      let data0 = data.uri;
      if (typeof data0 === 'string') {
        if (func1(data0) < 1) {
          const err3 = {
            instancePath: instancePath + '/uri',
            schemaPath: '#/properties/uri/minLength',
            keyword: 'minLength',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 characters',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
      } else {
        const err4 = {
          instancePath: instancePath + '/uri',
          schemaPath: '#/properties/uri/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.digest !== undefined) {
      let data1 = data.digest;
      if (typeof data1 === 'string') {
        if (!pattern11.test(data1)) {
          const err5 = {
            instancePath: instancePath + '/digest',
            schemaPath: '#/properties/digest/pattern',
            keyword: 'pattern',
            params: { pattern: '^sha256:[0-9a-f]{64}$' },
            message: 'must match pattern "' + '^sha256:[0-9a-f]{64}$' + '"',
          };
          if (vErrors === null) {
            vErrors = [err5];
          } else {
            vErrors.push(err5);
          }
          errors++;
        }
      } else {
        const err6 = {
          instancePath: instancePath + '/digest',
          schemaPath: '#/properties/digest/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
  } else {
    const err7 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err7];
    } else {
      vErrors.push(err7);
    }
    errors++;
  }
  validate77.errors = vErrors;
  return errors === 0;
}
validate77.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema33 = {
  type: 'object',
  properties: {
    timeoutSeconds: { type: 'integer', minimum: 1 },
    memoryMiB: { type: 'integer', minimum: 1 },
    cpuMillis: { type: 'integer', minimum: 1 },
  },
  required: ['timeoutSeconds'],
  additionalProperties: false,
};
function validate79(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate79.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.timeoutSeconds === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'timeoutSeconds' },
        message: "must have required property '" + 'timeoutSeconds' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'timeoutSeconds' || key0 === 'memoryMiB' || key0 === 'cpuMillis')) {
        const err1 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err1];
        } else {
          vErrors.push(err1);
        }
        errors++;
      }
    }
    if (data.timeoutSeconds !== undefined) {
      let data0 = data.timeoutSeconds;
      if (!(typeof data0 == 'number' && !(data0 % 1) && !isNaN(data0))) {
        const err2 = {
          instancePath: instancePath + '/timeoutSeconds',
          schemaPath: '#/properties/timeoutSeconds/type',
          keyword: 'type',
          params: { type: 'integer' },
          message: 'must be integer',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
      if (typeof data0 == 'number') {
        if (data0 < 1 || isNaN(data0)) {
          const err3 = {
            instancePath: instancePath + '/timeoutSeconds',
            schemaPath: '#/properties/timeoutSeconds/minimum',
            keyword: 'minimum',
            params: { comparison: '>=', limit: 1 },
            message: 'must be >= 1',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
      }
    }
    if (data.memoryMiB !== undefined) {
      let data1 = data.memoryMiB;
      if (!(typeof data1 == 'number' && !(data1 % 1) && !isNaN(data1))) {
        const err4 = {
          instancePath: instancePath + '/memoryMiB',
          schemaPath: '#/properties/memoryMiB/type',
          keyword: 'type',
          params: { type: 'integer' },
          message: 'must be integer',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
      if (typeof data1 == 'number') {
        if (data1 < 1 || isNaN(data1)) {
          const err5 = {
            instancePath: instancePath + '/memoryMiB',
            schemaPath: '#/properties/memoryMiB/minimum',
            keyword: 'minimum',
            params: { comparison: '>=', limit: 1 },
            message: 'must be >= 1',
          };
          if (vErrors === null) {
            vErrors = [err5];
          } else {
            vErrors.push(err5);
          }
          errors++;
        }
      }
    }
    if (data.cpuMillis !== undefined) {
      let data2 = data.cpuMillis;
      if (!(typeof data2 == 'number' && !(data2 % 1) && !isNaN(data2))) {
        const err6 = {
          instancePath: instancePath + '/cpuMillis',
          schemaPath: '#/properties/cpuMillis/type',
          keyword: 'type',
          params: { type: 'integer' },
          message: 'must be integer',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
      if (typeof data2 == 'number') {
        if (data2 < 1 || isNaN(data2)) {
          const err7 = {
            instancePath: instancePath + '/cpuMillis',
            schemaPath: '#/properties/cpuMillis/minimum',
            keyword: 'minimum',
            params: { comparison: '>=', limit: 1 },
            message: 'must be >= 1',
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
      }
    }
  } else {
    const err8 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err8];
    } else {
      vErrors.push(err8);
    }
    errors++;
  }
  validate79.errors = vErrors;
  return errors === 0;
}
validate79.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
function validate75(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate75.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Transform' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Transform' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.inputs === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'inputs' },
            message: "must have required property '" + 'inputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.outputs === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'outputs' },
            message: "must have required property '" + 'outputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.artifact === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'artifact' },
            message: "must have required property '" + 'artifact' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.runtime === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'runtime' },
            message: "must have required property '" + 'runtime' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.limits === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'limits' },
            message: "must have required property '" + 'limits' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'inputs' ||
            key1 === 'outputs' ||
            key1 === 'artifact' ||
            key1 === 'runtime' ||
            key1 === 'limits'
          )) {
            const err12 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.inputs !== undefined) {
          let data4 = data3.inputs;
          if (Array.isArray(data4)) {
            if (data4.length < 0) {
              const err13 = {
                instancePath: instancePath + '/spec/inputs',
                schemaPath: '#/properties/spec/properties/inputs/minItems',
                keyword: 'minItems',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 items',
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            const len0 = data4.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data5 = data4[i0];
              if (typeof data5 === 'string') {
                if (!pattern5.test(data5)) {
                  const err14 = {
                    instancePath: instancePath + '/spec/inputs/' + i0,
                    schemaPath: '#/properties/spec/properties/inputs/items/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err14];
                  } else {
                    vErrors.push(err14);
                  }
                  errors++;
                }
              } else {
                const err15 = {
                  instancePath: instancePath + '/spec/inputs/' + i0,
                  schemaPath: '#/properties/spec/properties/inputs/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err15];
                } else {
                  vErrors.push(err15);
                }
                errors++;
              }
            }
            let i1 = data4.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data4[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err16 = {
                    instancePath: instancePath + '/spec/inputs',
                    schemaPath: '#/properties/spec/properties/inputs/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err16];
                  } else {
                    vErrors.push(err16);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err17 = {
              instancePath: instancePath + '/spec/inputs',
              schemaPath: '#/properties/spec/properties/inputs/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.outputs !== undefined) {
          let data6 = data3.outputs;
          if (Array.isArray(data6)) {
            if (data6.length < 1) {
              const err18 = {
                instancePath: instancePath + '/spec/outputs',
                schemaPath: '#/properties/spec/properties/outputs/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
            const len1 = data6.length;
            for (let i2 = 0; i2 < len1; i2++) {
              let data7 = data6[i2];
              if (typeof data7 === 'string') {
                if (!pattern5.test(data7)) {
                  const err19 = {
                    instancePath: instancePath + '/spec/outputs/' + i2,
                    schemaPath: '#/properties/spec/properties/outputs/items/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err19];
                  } else {
                    vErrors.push(err19);
                  }
                  errors++;
                }
              } else {
                const err20 = {
                  instancePath: instancePath + '/spec/outputs/' + i2,
                  schemaPath: '#/properties/spec/properties/outputs/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err20];
                } else {
                  vErrors.push(err20);
                }
                errors++;
              }
            }
            let i3 = data6.length;
            let j1;
            if (i3 > 1) {
              const indices1 = {};
              for (; i3--;) {
                let item1 = data6[i3];
                if (typeof item1 !== 'string') {
                  continue;
                }
                if (typeof indices1[item1] == 'number') {
                  j1 = indices1[item1];
                  const err21 = {
                    instancePath: instancePath + '/spec/outputs',
                    schemaPath: '#/properties/spec/properties/outputs/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i3, j: j1 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j1 +
                      ' and ' +
                      i3 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err21];
                  } else {
                    vErrors.push(err21);
                  }
                  errors++;
                  break;
                }
                indices1[item1] = i3;
              }
            }
          } else {
            const err22 = {
              instancePath: instancePath + '/spec/outputs',
              schemaPath: '#/properties/spec/properties/outputs/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err22];
            } else {
              vErrors.push(err22);
            }
            errors++;
          }
        }
        if (data3.artifact !== undefined) {
          if (
            !validate77(data3.artifact, {
              instancePath: instancePath + '/spec/artifact',
              parentData: data3,
              parentDataProperty: 'artifact',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.runtime !== undefined) {
          if ('python-duckdb-v1' !== data3.runtime) {
            const err23 = {
              instancePath: instancePath + '/spec/runtime',
              schemaPath: '#/properties/spec/properties/runtime/const',
              keyword: 'const',
              params: { allowedValue: 'python-duckdb-v1' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
        }
        if (data3.limits !== undefined) {
          if (
            !validate79(data3.limits, {
              instancePath: instancePath + '/spec/limits',
              parentData: data3,
              parentDataProperty: 'limits',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate79.errors : vErrors.concat(validate79.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err24 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err24];
        } else {
          vErrors.push(err24);
        }
        errors++;
      }
    }
  } else {
    const err25 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err25];
    } else {
      vErrors.push(err25);
    }
    errors++;
  }
  validate75.errors = vErrors;
  return errors === 0;
}
validate75.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema34 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'ConnectorImplementation' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        artifact: { $ref: '#/$defs/Artifact' },
        abiVersion: { const: 'connector/v1' },
        capabilities: {
          type: 'array',
          items: { enum: ['batchRead', 'schemaDiscovery', 'continuousRead', 'writeback'] },
          minItems: 1,
          uniqueItems: true,
        },
        configurationSchema: { type: 'object' },
      },
      required: ['artifact', 'abiVersion', 'capabilities', 'configurationSchema'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate82(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate82.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('ConnectorImplementation' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'ConnectorImplementation' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.artifact === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'artifact' },
            message: "must have required property '" + 'artifact' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.abiVersion === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'abiVersion' },
            message: "must have required property '" + 'abiVersion' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.capabilities === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'capabilities' },
            message: "must have required property '" + 'capabilities' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.configurationSchema === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'configurationSchema' },
            message: "must have required property '" + 'configurationSchema' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'artifact' ||
            key1 === 'abiVersion' ||
            key1 === 'capabilities' ||
            key1 === 'configurationSchema'
          )) {
            const err11 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.artifact !== undefined) {
          if (
            !validate77(data3.artifact, {
              instancePath: instancePath + '/spec/artifact',
              parentData: data3,
              parentDataProperty: 'artifact',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.abiVersion !== undefined) {
          if ('connector/v1' !== data3.abiVersion) {
            const err12 = {
              instancePath: instancePath + '/spec/abiVersion',
              schemaPath: '#/properties/spec/properties/abiVersion/const',
              keyword: 'const',
              params: { allowedValue: 'connector/v1' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.capabilities !== undefined) {
          let data6 = data3.capabilities;
          if (Array.isArray(data6)) {
            if (data6.length < 1) {
              const err13 = {
                instancePath: instancePath + '/spec/capabilities',
                schemaPath: '#/properties/spec/properties/capabilities/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            const len0 = data6.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data7 = data6[i0];
              if (!(
                data7 === 'batchRead' ||
                data7 === 'schemaDiscovery' ||
                data7 === 'continuousRead' ||
                data7 === 'writeback'
              )) {
                const err14 = {
                  instancePath: instancePath + '/spec/capabilities/' + i0,
                  schemaPath: '#/properties/spec/properties/capabilities/items/enum',
                  keyword: 'enum',
                  params: {
                    allowedValues: schema34.properties.spec.properties.capabilities.items.enum,
                  },
                  message: 'must be equal to one of the allowed values',
                };
                if (vErrors === null) {
                  vErrors = [err14];
                } else {
                  vErrors.push(err14);
                }
                errors++;
              }
            }
            let i1 = data6.length;
            let j0;
            if (i1 > 1) {
              outer0: for (; i1--;) {
                for (j0 = i1; j0--;) {
                  if (func0(data6[i1], data6[j0])) {
                    const err15 = {
                      instancePath: instancePath + '/spec/capabilities',
                      schemaPath: '#/properties/spec/properties/capabilities/uniqueItems',
                      keyword: 'uniqueItems',
                      params: { i: i1, j: j0 },
                      message:
                        'must NOT have duplicate items (items ## ' +
                        j0 +
                        ' and ' +
                        i1 +
                        ' are identical)',
                    };
                    if (vErrors === null) {
                      vErrors = [err15];
                    } else {
                      vErrors.push(err15);
                    }
                    errors++;
                    break outer0;
                  }
                }
              }
            }
          } else {
            const err16 = {
              instancePath: instancePath + '/spec/capabilities',
              schemaPath: '#/properties/spec/properties/capabilities/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.configurationSchema !== undefined) {
          let data8 = data3.configurationSchema;
          if (!(data8 && typeof data8 == 'object' && !Array.isArray(data8))) {
            const err17 = {
              instancePath: instancePath + '/spec/configurationSchema',
              schemaPath: '#/properties/spec/properties/configurationSchema/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
      } else {
        const err18 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err18];
        } else {
          vErrors.push(err18);
        }
        errors++;
      }
    }
  } else {
    const err19 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err19];
    } else {
      vErrors.push(err19);
    }
    errors++;
  }
  validate82.errors = vErrors;
  return errors === 0;
}
validate82.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema35 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Connection' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        implementation: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        configuration: { type: 'object' },
        secrets: {
          type: 'array',
          items: {
            type: 'string',
            pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
          },
          minItems: 0,
          uniqueItems: true,
        },
        output: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        mode: { enum: ['replace', 'append', 'upsert', 'delete'] },
        coverage: { enum: ['full', 'incremental', 'appendOnly'] },
        stream: { type: 'string', minLength: 1 },
      },
      required: [
        'implementation',
        'configuration',
        'secrets',
        'output',
        'mode',
        'coverage',
        'stream',
      ],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate86(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate86.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Connection' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Connection' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.implementation === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'implementation' },
            message: "must have required property '" + 'implementation' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.configuration === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'configuration' },
            message: "must have required property '" + 'configuration' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.secrets === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'secrets' },
            message: "must have required property '" + 'secrets' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.output === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'output' },
            message: "must have required property '" + 'output' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.mode === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'mode' },
            message: "must have required property '" + 'mode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        if (data3.coverage === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'coverage' },
            message: "must have required property '" + 'coverage' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        if (data3.stream === undefined) {
          const err13 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'stream' },
            message: "must have required property '" + 'stream' + "'",
          };
          if (vErrors === null) {
            vErrors = [err13];
          } else {
            vErrors.push(err13);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'implementation' ||
            key1 === 'configuration' ||
            key1 === 'secrets' ||
            key1 === 'output' ||
            key1 === 'mode' ||
            key1 === 'coverage' ||
            key1 === 'stream'
          )) {
            const err14 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.implementation !== undefined) {
          let data4 = data3.implementation;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err15 = {
                instancePath: instancePath + '/spec/implementation',
                schemaPath: '#/properties/spec/properties/implementation/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err15];
              } else {
                vErrors.push(err15);
              }
              errors++;
            }
          } else {
            const err16 = {
              instancePath: instancePath + '/spec/implementation',
              schemaPath: '#/properties/spec/properties/implementation/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.configuration !== undefined) {
          let data5 = data3.configuration;
          if (!(data5 && typeof data5 == 'object' && !Array.isArray(data5))) {
            const err17 = {
              instancePath: instancePath + '/spec/configuration',
              schemaPath: '#/properties/spec/properties/configuration/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.secrets !== undefined) {
          let data6 = data3.secrets;
          if (Array.isArray(data6)) {
            if (data6.length < 0) {
              const err18 = {
                instancePath: instancePath + '/spec/secrets',
                schemaPath: '#/properties/spec/properties/secrets/minItems',
                keyword: 'minItems',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 items',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
            const len0 = data6.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data7 = data6[i0];
              if (typeof data7 === 'string') {
                if (!pattern5.test(data7)) {
                  const err19 = {
                    instancePath: instancePath + '/spec/secrets/' + i0,
                    schemaPath: '#/properties/spec/properties/secrets/items/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err19];
                  } else {
                    vErrors.push(err19);
                  }
                  errors++;
                }
              } else {
                const err20 = {
                  instancePath: instancePath + '/spec/secrets/' + i0,
                  schemaPath: '#/properties/spec/properties/secrets/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err20];
                } else {
                  vErrors.push(err20);
                }
                errors++;
              }
            }
            let i1 = data6.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data6[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err21 = {
                    instancePath: instancePath + '/spec/secrets',
                    schemaPath: '#/properties/spec/properties/secrets/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err21];
                  } else {
                    vErrors.push(err21);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err22 = {
              instancePath: instancePath + '/spec/secrets',
              schemaPath: '#/properties/spec/properties/secrets/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err22];
            } else {
              vErrors.push(err22);
            }
            errors++;
          }
        }
        if (data3.output !== undefined) {
          let data8 = data3.output;
          if (typeof data8 === 'string') {
            if (!pattern5.test(data8)) {
              const err23 = {
                instancePath: instancePath + '/spec/output',
                schemaPath: '#/properties/spec/properties/output/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err23];
              } else {
                vErrors.push(err23);
              }
              errors++;
            }
          } else {
            const err24 = {
              instancePath: instancePath + '/spec/output',
              schemaPath: '#/properties/spec/properties/output/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err24];
            } else {
              vErrors.push(err24);
            }
            errors++;
          }
        }
        if (data3.mode !== undefined) {
          let data9 = data3.mode;
          if (!(
            data9 === 'replace' ||
            data9 === 'append' ||
            data9 === 'upsert' ||
            data9 === 'delete'
          )) {
            const err25 = {
              instancePath: instancePath + '/spec/mode',
              schemaPath: '#/properties/spec/properties/mode/enum',
              keyword: 'enum',
              params: { allowedValues: schema35.properties.spec.properties.mode.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err25];
            } else {
              vErrors.push(err25);
            }
            errors++;
          }
        }
        if (data3.coverage !== undefined) {
          let data10 = data3.coverage;
          if (!(data10 === 'full' || data10 === 'incremental' || data10 === 'appendOnly')) {
            const err26 = {
              instancePath: instancePath + '/spec/coverage',
              schemaPath: '#/properties/spec/properties/coverage/enum',
              keyword: 'enum',
              params: { allowedValues: schema35.properties.spec.properties.coverage.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
        if (data3.stream !== undefined) {
          let data11 = data3.stream;
          if (typeof data11 === 'string') {
            if (func1(data11) < 1) {
              const err27 = {
                instancePath: instancePath + '/spec/stream',
                schemaPath: '#/properties/spec/properties/stream/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err27];
              } else {
                vErrors.push(err27);
              }
              errors++;
            }
          } else {
            const err28 = {
              instancePath: instancePath + '/spec/stream',
              schemaPath: '#/properties/spec/properties/stream/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err28];
            } else {
              vErrors.push(err28);
            }
            errors++;
          }
        }
      } else {
        const err29 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err29];
        } else {
          vErrors.push(err29);
        }
        errors++;
      }
    }
  } else {
    const err30 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err30];
    } else {
      vErrors.push(err30);
    }
    errors++;
  }
  validate86.errors = vErrors;
  return errors === 0;
}
validate86.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema36 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'ObjectType' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        primaryKey: { type: 'string', minLength: 1 },
        properties: { $ref: '#/$defs/Fields' },
        source: {
          type: 'object',
          properties: {
            resource: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            mapping: {
              type: 'object',
              propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
              additionalProperties: { type: 'string', minLength: 1 },
              minProperties: 1,
            },
          },
          required: ['resource', 'mapping'],
          additionalProperties: false,
        },
        readPolicy: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        writePolicy: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        propertyPolicies: {
          type: 'object',
          propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
          additionalProperties: {
            type: 'object',
            properties: {
              read: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              write: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
            },
            required: [],
            additionalProperties: false,
          },
          minProperties: 0,
        },
      },
      required: ['primaryKey', 'properties', 'source'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate89(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate89.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('ObjectType' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'ObjectType' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.primaryKey === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'primaryKey' },
            message: "must have required property '" + 'primaryKey' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.properties === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'properties' },
            message: "must have required property '" + 'properties' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.source === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'source' },
            message: "must have required property '" + 'source' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'primaryKey' ||
            key1 === 'properties' ||
            key1 === 'source' ||
            key1 === 'readPolicy' ||
            key1 === 'writePolicy' ||
            key1 === 'propertyPolicies'
          )) {
            const err10 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data3.primaryKey !== undefined) {
          let data4 = data3.primaryKey;
          if (typeof data4 === 'string') {
            if (func1(data4) < 1) {
              const err11 = {
                instancePath: instancePath + '/spec/primaryKey',
                schemaPath: '#/properties/spec/properties/primaryKey/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err11];
              } else {
                vErrors.push(err11);
              }
              errors++;
            }
          } else {
            const err12 = {
              instancePath: instancePath + '/spec/primaryKey',
              schemaPath: '#/properties/spec/properties/primaryKey/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.properties !== undefined) {
          if (
            !validate68(data3.properties, {
              instancePath: instancePath + '/spec/properties',
              parentData: data3,
              parentDataProperty: 'properties',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate68.errors : vErrors.concat(validate68.errors);
            errors = vErrors.length;
          }
        }
        if (data3.source !== undefined) {
          let data6 = data3.source;
          if (data6 && typeof data6 == 'object' && !Array.isArray(data6)) {
            if (data6.resource === undefined) {
              const err13 = {
                instancePath: instancePath + '/spec/source',
                schemaPath: '#/properties/spec/properties/source/required',
                keyword: 'required',
                params: { missingProperty: 'resource' },
                message: "must have required property '" + 'resource' + "'",
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            if (data6.mapping === undefined) {
              const err14 = {
                instancePath: instancePath + '/spec/source',
                schemaPath: '#/properties/spec/properties/source/required',
                keyword: 'required',
                params: { missingProperty: 'mapping' },
                message: "must have required property '" + 'mapping' + "'",
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
            for (const key2 in data6) {
              if (!(key2 === 'resource' || key2 === 'mapping')) {
                const err15 = {
                  instancePath: instancePath + '/spec/source',
                  schemaPath: '#/properties/spec/properties/source/additionalProperties',
                  keyword: 'additionalProperties',
                  params: { additionalProperty: key2 },
                  message: 'must NOT have additional properties',
                };
                if (vErrors === null) {
                  vErrors = [err15];
                } else {
                  vErrors.push(err15);
                }
                errors++;
              }
            }
            if (data6.resource !== undefined) {
              let data7 = data6.resource;
              if (typeof data7 === 'string') {
                if (!pattern5.test(data7)) {
                  const err16 = {
                    instancePath: instancePath + '/spec/source/resource',
                    schemaPath: '#/properties/spec/properties/source/properties/resource/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err16];
                  } else {
                    vErrors.push(err16);
                  }
                  errors++;
                }
              } else {
                const err17 = {
                  instancePath: instancePath + '/spec/source/resource',
                  schemaPath: '#/properties/spec/properties/source/properties/resource/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err17];
                } else {
                  vErrors.push(err17);
                }
                errors++;
              }
            }
            if (data6.mapping !== undefined) {
              let data8 = data6.mapping;
              if (data8 && typeof data8 == 'object' && !Array.isArray(data8)) {
                if (Object.keys(data8).length < 1) {
                  const err18 = {
                    instancePath: instancePath + '/spec/source/mapping',
                    schemaPath:
                      '#/properties/spec/properties/source/properties/mapping/minProperties',
                    keyword: 'minProperties',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 properties',
                  };
                  if (vErrors === null) {
                    vErrors = [err18];
                  } else {
                    vErrors.push(err18);
                  }
                  errors++;
                }
                for (const key3 in data8) {
                  const _errs18 = errors;
                  if (typeof key3 === 'string') {
                    if (!pattern8.test(key3)) {
                      const err19 = {
                        instancePath: instancePath + '/spec/source/mapping',
                        schemaPath:
                          '#/properties/spec/properties/source/properties/mapping/propertyNames/pattern',
                        keyword: 'pattern',
                        params: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
                        message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_]*$' + '"',
                        propertyName: key3,
                      };
                      if (vErrors === null) {
                        vErrors = [err19];
                      } else {
                        vErrors.push(err19);
                      }
                      errors++;
                    }
                  }
                  var valid3 = _errs18 === errors;
                  if (!valid3) {
                    const err20 = {
                      instancePath: instancePath + '/spec/source/mapping',
                      schemaPath:
                        '#/properties/spec/properties/source/properties/mapping/propertyNames',
                      keyword: 'propertyNames',
                      params: { propertyName: key3 },
                      message: 'property name must be valid',
                    };
                    if (vErrors === null) {
                      vErrors = [err20];
                    } else {
                      vErrors.push(err20);
                    }
                    errors++;
                  }
                }
                for (const key4 in data8) {
                  let data9 = data8[key4];
                  if (typeof data9 === 'string') {
                    if (func1(data9) < 1) {
                      const err21 = {
                        instancePath:
                          instancePath +
                          '/spec/source/mapping/' +
                          key4.replace(/~/g, '~0').replace(/\//g, '~1'),
                        schemaPath:
                          '#/properties/spec/properties/source/properties/mapping/additionalProperties/minLength',
                        keyword: 'minLength',
                        params: { limit: 1 },
                        message: 'must NOT have fewer than 1 characters',
                      };
                      if (vErrors === null) {
                        vErrors = [err21];
                      } else {
                        vErrors.push(err21);
                      }
                      errors++;
                    }
                  } else {
                    const err22 = {
                      instancePath:
                        instancePath +
                        '/spec/source/mapping/' +
                        key4.replace(/~/g, '~0').replace(/\//g, '~1'),
                      schemaPath:
                        '#/properties/spec/properties/source/properties/mapping/additionalProperties/type',
                      keyword: 'type',
                      params: { type: 'string' },
                      message: 'must be string',
                    };
                    if (vErrors === null) {
                      vErrors = [err22];
                    } else {
                      vErrors.push(err22);
                    }
                    errors++;
                  }
                }
              } else {
                const err23 = {
                  instancePath: instancePath + '/spec/source/mapping',
                  schemaPath: '#/properties/spec/properties/source/properties/mapping/type',
                  keyword: 'type',
                  params: { type: 'object' },
                  message: 'must be object',
                };
                if (vErrors === null) {
                  vErrors = [err23];
                } else {
                  vErrors.push(err23);
                }
                errors++;
              }
            }
          } else {
            const err24 = {
              instancePath: instancePath + '/spec/source',
              schemaPath: '#/properties/spec/properties/source/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err24];
            } else {
              vErrors.push(err24);
            }
            errors++;
          }
        }
        if (data3.readPolicy !== undefined) {
          let data10 = data3.readPolicy;
          if (typeof data10 === 'string') {
            if (!pattern5.test(data10)) {
              const err25 = {
                instancePath: instancePath + '/spec/readPolicy',
                schemaPath: '#/properties/spec/properties/readPolicy/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err25];
              } else {
                vErrors.push(err25);
              }
              errors++;
            }
          } else {
            const err26 = {
              instancePath: instancePath + '/spec/readPolicy',
              schemaPath: '#/properties/spec/properties/readPolicy/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
        if (data3.writePolicy !== undefined) {
          let data11 = data3.writePolicy;
          if (typeof data11 === 'string') {
            if (!pattern5.test(data11)) {
              const err27 = {
                instancePath: instancePath + '/spec/writePolicy',
                schemaPath: '#/properties/spec/properties/writePolicy/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err27];
              } else {
                vErrors.push(err27);
              }
              errors++;
            }
          } else {
            const err28 = {
              instancePath: instancePath + '/spec/writePolicy',
              schemaPath: '#/properties/spec/properties/writePolicy/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err28];
            } else {
              vErrors.push(err28);
            }
            errors++;
          }
        }
        if (data3.propertyPolicies !== undefined) {
          let data12 = data3.propertyPolicies;
          if (data12 && typeof data12 == 'object' && !Array.isArray(data12)) {
            if (Object.keys(data12).length < 0) {
              const err29 = {
                instancePath: instancePath + '/spec/propertyPolicies',
                schemaPath: '#/properties/spec/properties/propertyPolicies/minProperties',
                keyword: 'minProperties',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 properties',
              };
              if (vErrors === null) {
                vErrors = [err29];
              } else {
                vErrors.push(err29);
              }
              errors++;
            }
            for (const key5 in data12) {
              const _errs28 = errors;
              if (typeof key5 === 'string') {
                if (!pattern8.test(key5)) {
                  const err30 = {
                    instancePath: instancePath + '/spec/propertyPolicies',
                    schemaPath:
                      '#/properties/spec/properties/propertyPolicies/propertyNames/pattern',
                    keyword: 'pattern',
                    params: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
                    message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_]*$' + '"',
                    propertyName: key5,
                  };
                  if (vErrors === null) {
                    vErrors = [err30];
                  } else {
                    vErrors.push(err30);
                  }
                  errors++;
                }
              }
              var valid5 = _errs28 === errors;
              if (!valid5) {
                const err31 = {
                  instancePath: instancePath + '/spec/propertyPolicies',
                  schemaPath: '#/properties/spec/properties/propertyPolicies/propertyNames',
                  keyword: 'propertyNames',
                  params: { propertyName: key5 },
                  message: 'property name must be valid',
                };
                if (vErrors === null) {
                  vErrors = [err31];
                } else {
                  vErrors.push(err31);
                }
                errors++;
              }
            }
            for (const key6 in data12) {
              let data13 = data12[key6];
              if (data13 && typeof data13 == 'object' && !Array.isArray(data13)) {
                for (const key7 in data13) {
                  if (!(key7 === 'read' || key7 === 'write')) {
                    const err32 = {
                      instancePath:
                        instancePath +
                        '/spec/propertyPolicies/' +
                        key6.replace(/~/g, '~0').replace(/\//g, '~1'),
                      schemaPath:
                        '#/properties/spec/properties/propertyPolicies/additionalProperties/additionalProperties',
                      keyword: 'additionalProperties',
                      params: { additionalProperty: key7 },
                      message: 'must NOT have additional properties',
                    };
                    if (vErrors === null) {
                      vErrors = [err32];
                    } else {
                      vErrors.push(err32);
                    }
                    errors++;
                  }
                }
                if (data13.read !== undefined) {
                  let data14 = data13.read;
                  if (typeof data14 === 'string') {
                    if (!pattern5.test(data14)) {
                      const err33 = {
                        instancePath:
                          instancePath +
                          '/spec/propertyPolicies/' +
                          key6.replace(/~/g, '~0').replace(/\//g, '~1') +
                          '/read',
                        schemaPath:
                          '#/properties/spec/properties/propertyPolicies/additionalProperties/properties/read/pattern',
                        keyword: 'pattern',
                        params: {
                          pattern:
                            '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                        },
                        message:
                          'must match pattern "' +
                          '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                          '"',
                      };
                      if (vErrors === null) {
                        vErrors = [err33];
                      } else {
                        vErrors.push(err33);
                      }
                      errors++;
                    }
                  } else {
                    const err34 = {
                      instancePath:
                        instancePath +
                        '/spec/propertyPolicies/' +
                        key6.replace(/~/g, '~0').replace(/\//g, '~1') +
                        '/read',
                      schemaPath:
                        '#/properties/spec/properties/propertyPolicies/additionalProperties/properties/read/type',
                      keyword: 'type',
                      params: { type: 'string' },
                      message: 'must be string',
                    };
                    if (vErrors === null) {
                      vErrors = [err34];
                    } else {
                      vErrors.push(err34);
                    }
                    errors++;
                  }
                }
                if (data13.write !== undefined) {
                  let data15 = data13.write;
                  if (typeof data15 === 'string') {
                    if (!pattern5.test(data15)) {
                      const err35 = {
                        instancePath:
                          instancePath +
                          '/spec/propertyPolicies/' +
                          key6.replace(/~/g, '~0').replace(/\//g, '~1') +
                          '/write',
                        schemaPath:
                          '#/properties/spec/properties/propertyPolicies/additionalProperties/properties/write/pattern',
                        keyword: 'pattern',
                        params: {
                          pattern:
                            '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                        },
                        message:
                          'must match pattern "' +
                          '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                          '"',
                      };
                      if (vErrors === null) {
                        vErrors = [err35];
                      } else {
                        vErrors.push(err35);
                      }
                      errors++;
                    }
                  } else {
                    const err36 = {
                      instancePath:
                        instancePath +
                        '/spec/propertyPolicies/' +
                        key6.replace(/~/g, '~0').replace(/\//g, '~1') +
                        '/write',
                      schemaPath:
                        '#/properties/spec/properties/propertyPolicies/additionalProperties/properties/write/type',
                      keyword: 'type',
                      params: { type: 'string' },
                      message: 'must be string',
                    };
                    if (vErrors === null) {
                      vErrors = [err36];
                    } else {
                      vErrors.push(err36);
                    }
                    errors++;
                  }
                }
              } else {
                const err37 = {
                  instancePath:
                    instancePath +
                    '/spec/propertyPolicies/' +
                    key6.replace(/~/g, '~0').replace(/\//g, '~1'),
                  schemaPath:
                    '#/properties/spec/properties/propertyPolicies/additionalProperties/type',
                  keyword: 'type',
                  params: { type: 'object' },
                  message: 'must be object',
                };
                if (vErrors === null) {
                  vErrors = [err37];
                } else {
                  vErrors.push(err37);
                }
                errors++;
              }
            }
          } else {
            const err38 = {
              instancePath: instancePath + '/spec/propertyPolicies',
              schemaPath: '#/properties/spec/properties/propertyPolicies/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err38];
            } else {
              vErrors.push(err38);
            }
            errors++;
          }
        }
      } else {
        const err39 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err39];
        } else {
          vErrors.push(err39);
        }
        errors++;
      }
    }
  } else {
    const err40 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err40];
    } else {
      vErrors.push(err40);
    }
    errors++;
  }
  validate89.errors = vErrors;
  return errors === 0;
}
validate89.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema37 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'LinkType' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        sourceType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        cardinality: { enum: ['manyToOne', 'oneToOne', 'manyToMany'] },
        mode: { enum: ['mapped', 'actionManaged'] },
        sourceProperty: { type: 'string', minLength: 1 },
        targetProperty: { type: 'string', minLength: 1 },
        readPolicy: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
      },
      required: ['sourceType', 'targetType', 'cardinality', 'mode'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate93(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate93.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('LinkType' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'LinkType' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.sourceType === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'sourceType' },
            message: "must have required property '" + 'sourceType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.targetType === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'targetType' },
            message: "must have required property '" + 'targetType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.cardinality === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'cardinality' },
            message: "must have required property '" + 'cardinality' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.mode === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'mode' },
            message: "must have required property '" + 'mode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'sourceType' ||
            key1 === 'targetType' ||
            key1 === 'cardinality' ||
            key1 === 'mode' ||
            key1 === 'sourceProperty' ||
            key1 === 'targetProperty' ||
            key1 === 'readPolicy'
          )) {
            const err11 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.sourceType !== undefined) {
          let data4 = data3.sourceType;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err12 = {
                instancePath: instancePath + '/spec/sourceType',
                schemaPath: '#/properties/spec/properties/sourceType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/sourceType',
              schemaPath: '#/properties/spec/properties/sourceType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
        if (data3.targetType !== undefined) {
          let data5 = data3.targetType;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err14 = {
                instancePath: instancePath + '/spec/targetType',
                schemaPath: '#/properties/spec/properties/targetType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
          } else {
            const err15 = {
              instancePath: instancePath + '/spec/targetType',
              schemaPath: '#/properties/spec/properties/targetType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.cardinality !== undefined) {
          let data6 = data3.cardinality;
          if (!(data6 === 'manyToOne' || data6 === 'oneToOne' || data6 === 'manyToMany')) {
            const err16 = {
              instancePath: instancePath + '/spec/cardinality',
              schemaPath: '#/properties/spec/properties/cardinality/enum',
              keyword: 'enum',
              params: { allowedValues: schema37.properties.spec.properties.cardinality.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.mode !== undefined) {
          let data7 = data3.mode;
          if (!(data7 === 'mapped' || data7 === 'actionManaged')) {
            const err17 = {
              instancePath: instancePath + '/spec/mode',
              schemaPath: '#/properties/spec/properties/mode/enum',
              keyword: 'enum',
              params: { allowedValues: schema37.properties.spec.properties.mode.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.sourceProperty !== undefined) {
          let data8 = data3.sourceProperty;
          if (typeof data8 === 'string') {
            if (func1(data8) < 1) {
              const err18 = {
                instancePath: instancePath + '/spec/sourceProperty',
                schemaPath: '#/properties/spec/properties/sourceProperty/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
          } else {
            const err19 = {
              instancePath: instancePath + '/spec/sourceProperty',
              schemaPath: '#/properties/spec/properties/sourceProperty/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data3.targetProperty !== undefined) {
          let data9 = data3.targetProperty;
          if (typeof data9 === 'string') {
            if (func1(data9) < 1) {
              const err20 = {
                instancePath: instancePath + '/spec/targetProperty',
                schemaPath: '#/properties/spec/properties/targetProperty/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err20];
              } else {
                vErrors.push(err20);
              }
              errors++;
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/spec/targetProperty',
              schemaPath: '#/properties/spec/properties/targetProperty/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
        if (data3.readPolicy !== undefined) {
          let data10 = data3.readPolicy;
          if (typeof data10 === 'string') {
            if (!pattern5.test(data10)) {
              const err22 = {
                instancePath: instancePath + '/spec/readPolicy',
                schemaPath: '#/properties/spec/properties/readPolicy/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
          } else {
            const err23 = {
              instancePath: instancePath + '/spec/readPolicy',
              schemaPath: '#/properties/spec/properties/readPolicy/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
        }
      } else {
        const err24 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err24];
        } else {
          vErrors.push(err24);
        }
        errors++;
      }
    }
  } else {
    const err25 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err25];
    } else {
      vErrors.push(err25);
    }
    errors++;
  }
  validate93.errors = vErrors;
  return errors === 0;
}
validate93.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema38 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Action' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        inputs: { $ref: '#/$defs/Fields' },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        targetParameter: { type: 'string', minLength: 1 },
        condition: { $ref: '#/$defs/Expr' },
        edits: { type: 'array', items: { $ref: '#/$defs/Edit' }, minItems: 1 },
        function: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
      },
      required: ['inputs', 'targetType', 'condition'],
      additionalProperties: false,
      oneOf: [
        { required: ['edits'], not: { required: ['function'] } },
        { required: ['function'], not: { required: ['edits'] } },
      ],
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema39 = {
  oneOf: [
    {
      type: 'object',
      properties: {
        literal: {
          oneOf: [
            { type: ['string', 'number', 'boolean', 'null'] },
            {
              type: 'array',
              items: { type: ['string', 'number', 'boolean', 'null'] },
              minItems: 0,
            },
          ],
        },
      },
      required: ['literal'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: { field: { type: 'string', minLength: 1 } },
      required: ['field'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: { principalAttribute: { type: 'string', minLength: 1 } },
      required: ['principalAttribute'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: { parameter: { type: 'string', minLength: 1 } },
      required: ['parameter'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { enum: ['eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in', 'contains'] },
        args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 2, maxItems: 2 },
      },
      required: ['op', 'args'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { enum: ['and', 'or'] },
        args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 2 },
      },
      required: ['op', 'args'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { enum: ['not', 'isNull'] },
        args: { type: 'array', items: { $ref: '#/$defs/Expr' }, minItems: 1, maxItems: 1 },
      },
      required: ['op', 'args'],
      additionalProperties: false,
    },
  ],
};
const wrapper3 = { validate: validate99 };
function validate99(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate99.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.literal === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'literal' },
        message: "must have required property '" + 'literal' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'literal')) {
        const err1 = {
          instancePath,
          schemaPath: '#/oneOf/0/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err1];
        } else {
          vErrors.push(err1);
        }
        errors++;
      }
    }
    if (data.literal !== undefined) {
      let data0 = data.literal;
      const _errs5 = errors;
      let valid2 = false;
      let passing1 = null;
      const _errs6 = errors;
      if (
        typeof data0 !== 'string' &&
        !(typeof data0 == 'number') &&
        typeof data0 !== 'boolean' &&
        data0 !== null
      ) {
        const err2 = {
          instancePath: instancePath + '/literal',
          schemaPath: '#/oneOf/0/properties/literal/oneOf/0/type',
          keyword: 'type',
          params: { type: schema39.oneOf[0].properties.literal.oneOf[0].type },
          message: 'must be string,number,boolean,null',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
      var _valid1 = _errs6 === errors;
      if (_valid1) {
        valid2 = true;
        passing1 = 0;
      }
      const _errs8 = errors;
      if (Array.isArray(data0)) {
        if (data0.length < 0) {
          const err3 = {
            instancePath: instancePath + '/literal',
            schemaPath: '#/oneOf/0/properties/literal/oneOf/1/minItems',
            keyword: 'minItems',
            params: { limit: 0 },
            message: 'must NOT have fewer than 0 items',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
        const len0 = data0.length;
        for (let i0 = 0; i0 < len0; i0++) {
          let data1 = data0[i0];
          if (
            typeof data1 !== 'string' &&
            !(typeof data1 == 'number') &&
            typeof data1 !== 'boolean' &&
            data1 !== null
          ) {
            const err4 = {
              instancePath: instancePath + '/literal/' + i0,
              schemaPath: '#/oneOf/0/properties/literal/oneOf/1/items/type',
              keyword: 'type',
              params: { type: schema39.oneOf[0].properties.literal.oneOf[1].items.type },
              message: 'must be string,number,boolean,null',
            };
            if (vErrors === null) {
              vErrors = [err4];
            } else {
              vErrors.push(err4);
            }
            errors++;
          }
        }
      } else {
        const err5 = {
          instancePath: instancePath + '/literal',
          schemaPath: '#/oneOf/0/properties/literal/oneOf/1/type',
          keyword: 'type',
          params: { type: 'array' },
          message: 'must be array',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
      var _valid1 = _errs8 === errors;
      if (_valid1 && valid2) {
        valid2 = false;
        passing1 = [passing1, 1];
      } else {
        if (_valid1) {
          valid2 = true;
          passing1 = 1;
        }
      }
      if (!valid2) {
        const err6 = {
          instancePath: instancePath + '/literal',
          schemaPath: '#/oneOf/0/properties/literal/oneOf',
          keyword: 'oneOf',
          params: { passingSchemas: passing1 },
          message: 'must match exactly one schema in oneOf',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      } else {
        errors = _errs5;
        if (vErrors !== null) {
          if (_errs5) {
            vErrors.length = _errs5;
          } else {
            vErrors = null;
          }
        }
      }
    }
  } else {
    const err7 = {
      instancePath,
      schemaPath: '#/oneOf/0/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err7];
    } else {
      vErrors.push(err7);
    }
    errors++;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
    var props0 = true;
  }
  const _errs12 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.field === undefined) {
      const err8 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'field' },
        message: "must have required property '" + 'field' + "'",
      };
      if (vErrors === null) {
        vErrors = [err8];
      } else {
        vErrors.push(err8);
      }
      errors++;
    }
    for (const key1 in data) {
      if (!(key1 === 'field')) {
        const err9 = {
          instancePath,
          schemaPath: '#/oneOf/1/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key1 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err9];
        } else {
          vErrors.push(err9);
        }
        errors++;
      }
    }
    if (data.field !== undefined) {
      let data2 = data.field;
      if (typeof data2 === 'string') {
        if (func1(data2) < 1) {
          const err10 = {
            instancePath: instancePath + '/field',
            schemaPath: '#/oneOf/1/properties/field/minLength',
            keyword: 'minLength',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 characters',
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
      } else {
        const err11 = {
          instancePath: instancePath + '/field',
          schemaPath: '#/oneOf/1/properties/field/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      }
    }
  } else {
    const err12 = {
      instancePath,
      schemaPath: '#/oneOf/1/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err12];
    } else {
      vErrors.push(err12);
    }
    errors++;
  }
  var _valid0 = _errs12 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true) {
        props0 = true;
      }
    }
    const _errs17 = errors;
    if (data && typeof data == 'object' && !Array.isArray(data)) {
      if (data.principalAttribute === undefined) {
        const err13 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'principalAttribute' },
          message: "must have required property '" + 'principalAttribute' + "'",
        };
        if (vErrors === null) {
          vErrors = [err13];
        } else {
          vErrors.push(err13);
        }
        errors++;
      }
      for (const key2 in data) {
        if (!(key2 === 'principalAttribute')) {
          const err14 = {
            instancePath,
            schemaPath: '#/oneOf/2/additionalProperties',
            keyword: 'additionalProperties',
            params: { additionalProperty: key2 },
            message: 'must NOT have additional properties',
          };
          if (vErrors === null) {
            vErrors = [err14];
          } else {
            vErrors.push(err14);
          }
          errors++;
        }
      }
      if (data.principalAttribute !== undefined) {
        let data3 = data.principalAttribute;
        if (typeof data3 === 'string') {
          if (func1(data3) < 1) {
            const err15 = {
              instancePath: instancePath + '/principalAttribute',
              schemaPath: '#/oneOf/2/properties/principalAttribute/minLength',
              keyword: 'minLength',
              params: { limit: 1 },
              message: 'must NOT have fewer than 1 characters',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        } else {
          const err16 = {
            instancePath: instancePath + '/principalAttribute',
            schemaPath: '#/oneOf/2/properties/principalAttribute/type',
            keyword: 'type',
            params: { type: 'string' },
            message: 'must be string',
          };
          if (vErrors === null) {
            vErrors = [err16];
          } else {
            vErrors.push(err16);
          }
          errors++;
        }
      }
    } else {
      const err17 = {
        instancePath,
        schemaPath: '#/oneOf/2/type',
        keyword: 'type',
        params: { type: 'object' },
        message: 'must be object',
      };
      if (vErrors === null) {
        vErrors = [err17];
      } else {
        vErrors.push(err17);
      }
      errors++;
    }
    var _valid0 = _errs17 === errors;
    if (_valid0 && valid0) {
      valid0 = false;
      passing0 = [passing0, 2];
    } else {
      if (_valid0) {
        valid0 = true;
        passing0 = 2;
        if (props0 !== true) {
          props0 = true;
        }
      }
      const _errs22 = errors;
      if (data && typeof data == 'object' && !Array.isArray(data)) {
        if (data.parameter === undefined) {
          const err18 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'parameter' },
            message: "must have required property '" + 'parameter' + "'",
          };
          if (vErrors === null) {
            vErrors = [err18];
          } else {
            vErrors.push(err18);
          }
          errors++;
        }
        for (const key3 in data) {
          if (!(key3 === 'parameter')) {
            const err19 = {
              instancePath,
              schemaPath: '#/oneOf/3/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key3 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data.parameter !== undefined) {
          let data4 = data.parameter;
          if (typeof data4 === 'string') {
            if (func1(data4) < 1) {
              const err20 = {
                instancePath: instancePath + '/parameter',
                schemaPath: '#/oneOf/3/properties/parameter/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err20];
              } else {
                vErrors.push(err20);
              }
              errors++;
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/parameter',
              schemaPath: '#/oneOf/3/properties/parameter/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
      } else {
        const err22 = {
          instancePath,
          schemaPath: '#/oneOf/3/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err22];
        } else {
          vErrors.push(err22);
        }
        errors++;
      }
      var _valid0 = _errs22 === errors;
      if (_valid0 && valid0) {
        valid0 = false;
        passing0 = [passing0, 3];
      } else {
        if (_valid0) {
          valid0 = true;
          passing0 = 3;
          if (props0 !== true) {
            props0 = true;
          }
        }
        const _errs27 = errors;
        if (data && typeof data == 'object' && !Array.isArray(data)) {
          if (data.op === undefined) {
            const err23 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'op' },
              message: "must have required property '" + 'op' + "'",
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
          if (data.args === undefined) {
            const err24 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'args' },
              message: "must have required property '" + 'args' + "'",
            };
            if (vErrors === null) {
              vErrors = [err24];
            } else {
              vErrors.push(err24);
            }
            errors++;
          }
          for (const key4 in data) {
            if (!(key4 === 'op' || key4 === 'args')) {
              const err25 = {
                instancePath,
                schemaPath: '#/oneOf/4/additionalProperties',
                keyword: 'additionalProperties',
                params: { additionalProperty: key4 },
                message: 'must NOT have additional properties',
              };
              if (vErrors === null) {
                vErrors = [err25];
              } else {
                vErrors.push(err25);
              }
              errors++;
            }
          }
          if (data.op !== undefined) {
            let data5 = data.op;
            if (!(
              data5 === 'eq' ||
              data5 === 'ne' ||
              data5 === 'lt' ||
              data5 === 'lte' ||
              data5 === 'gt' ||
              data5 === 'gte' ||
              data5 === 'in' ||
              data5 === 'contains'
            )) {
              const err26 = {
                instancePath: instancePath + '/op',
                schemaPath: '#/oneOf/4/properties/op/enum',
                keyword: 'enum',
                params: { allowedValues: schema39.oneOf[4].properties.op.enum },
                message: 'must be equal to one of the allowed values',
              };
              if (vErrors === null) {
                vErrors = [err26];
              } else {
                vErrors.push(err26);
              }
              errors++;
            }
          }
          if (data.args !== undefined) {
            let data6 = data.args;
            if (Array.isArray(data6)) {
              if (data6.length > 2) {
                const err27 = {
                  instancePath: instancePath + '/args',
                  schemaPath: '#/oneOf/4/properties/args/maxItems',
                  keyword: 'maxItems',
                  params: { limit: 2 },
                  message: 'must NOT have more than 2 items',
                };
                if (vErrors === null) {
                  vErrors = [err27];
                } else {
                  vErrors.push(err27);
                }
                errors++;
              }
              if (data6.length < 2) {
                const err28 = {
                  instancePath: instancePath + '/args',
                  schemaPath: '#/oneOf/4/properties/args/minItems',
                  keyword: 'minItems',
                  params: { limit: 2 },
                  message: 'must NOT have fewer than 2 items',
                };
                if (vErrors === null) {
                  vErrors = [err28];
                } else {
                  vErrors.push(err28);
                }
                errors++;
              }
              const len1 = data6.length;
              for (let i1 = 0; i1 < len1; i1++) {
                if (
                  !wrapper3.validate(data6[i1], {
                    instancePath: instancePath + '/args/' + i1,
                    parentData: data6,
                    parentDataProperty: i1,
                    rootData,
                    dynamicAnchors,
                  })
                ) {
                  vErrors =
                    vErrors === null
                      ? wrapper3.validate.errors
                      : vErrors.concat(wrapper3.validate.errors);
                  errors = vErrors.length;
                }
              }
            } else {
              const err29 = {
                instancePath: instancePath + '/args',
                schemaPath: '#/oneOf/4/properties/args/type',
                keyword: 'type',
                params: { type: 'array' },
                message: 'must be array',
              };
              if (vErrors === null) {
                vErrors = [err29];
              } else {
                vErrors.push(err29);
              }
              errors++;
            }
          }
        } else {
          const err30 = {
            instancePath,
            schemaPath: '#/oneOf/4/type',
            keyword: 'type',
            params: { type: 'object' },
            message: 'must be object',
          };
          if (vErrors === null) {
            vErrors = [err30];
          } else {
            vErrors.push(err30);
          }
          errors++;
        }
        var _valid0 = _errs27 === errors;
        if (_valid0 && valid0) {
          valid0 = false;
          passing0 = [passing0, 4];
        } else {
          if (_valid0) {
            valid0 = true;
            passing0 = 4;
            if (props0 !== true) {
              props0 = true;
            }
          }
          const _errs34 = errors;
          if (data && typeof data == 'object' && !Array.isArray(data)) {
            if (data.op === undefined) {
              const err31 = {
                instancePath,
                schemaPath: '#/oneOf/5/required',
                keyword: 'required',
                params: { missingProperty: 'op' },
                message: "must have required property '" + 'op' + "'",
              };
              if (vErrors === null) {
                vErrors = [err31];
              } else {
                vErrors.push(err31);
              }
              errors++;
            }
            if (data.args === undefined) {
              const err32 = {
                instancePath,
                schemaPath: '#/oneOf/5/required',
                keyword: 'required',
                params: { missingProperty: 'args' },
                message: "must have required property '" + 'args' + "'",
              };
              if (vErrors === null) {
                vErrors = [err32];
              } else {
                vErrors.push(err32);
              }
              errors++;
            }
            for (const key5 in data) {
              if (!(key5 === 'op' || key5 === 'args')) {
                const err33 = {
                  instancePath,
                  schemaPath: '#/oneOf/5/additionalProperties',
                  keyword: 'additionalProperties',
                  params: { additionalProperty: key5 },
                  message: 'must NOT have additional properties',
                };
                if (vErrors === null) {
                  vErrors = [err33];
                } else {
                  vErrors.push(err33);
                }
                errors++;
              }
            }
            if (data.op !== undefined) {
              let data8 = data.op;
              if (!(data8 === 'and' || data8 === 'or')) {
                const err34 = {
                  instancePath: instancePath + '/op',
                  schemaPath: '#/oneOf/5/properties/op/enum',
                  keyword: 'enum',
                  params: { allowedValues: schema39.oneOf[5].properties.op.enum },
                  message: 'must be equal to one of the allowed values',
                };
                if (vErrors === null) {
                  vErrors = [err34];
                } else {
                  vErrors.push(err34);
                }
                errors++;
              }
            }
            if (data.args !== undefined) {
              let data9 = data.args;
              if (Array.isArray(data9)) {
                if (data9.length < 2) {
                  const err35 = {
                    instancePath: instancePath + '/args',
                    schemaPath: '#/oneOf/5/properties/args/minItems',
                    keyword: 'minItems',
                    params: { limit: 2 },
                    message: 'must NOT have fewer than 2 items',
                  };
                  if (vErrors === null) {
                    vErrors = [err35];
                  } else {
                    vErrors.push(err35);
                  }
                  errors++;
                }
                const len2 = data9.length;
                for (let i2 = 0; i2 < len2; i2++) {
                  if (
                    !wrapper3.validate(data9[i2], {
                      instancePath: instancePath + '/args/' + i2,
                      parentData: data9,
                      parentDataProperty: i2,
                      rootData,
                      dynamicAnchors,
                    })
                  ) {
                    vErrors =
                      vErrors === null
                        ? wrapper3.validate.errors
                        : vErrors.concat(wrapper3.validate.errors);
                    errors = vErrors.length;
                  }
                }
              } else {
                const err36 = {
                  instancePath: instancePath + '/args',
                  schemaPath: '#/oneOf/5/properties/args/type',
                  keyword: 'type',
                  params: { type: 'array' },
                  message: 'must be array',
                };
                if (vErrors === null) {
                  vErrors = [err36];
                } else {
                  vErrors.push(err36);
                }
                errors++;
              }
            }
          } else {
            const err37 = {
              instancePath,
              schemaPath: '#/oneOf/5/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err37];
            } else {
              vErrors.push(err37);
            }
            errors++;
          }
          var _valid0 = _errs34 === errors;
          if (_valid0 && valid0) {
            valid0 = false;
            passing0 = [passing0, 5];
          } else {
            if (_valid0) {
              valid0 = true;
              passing0 = 5;
              if (props0 !== true) {
                props0 = true;
              }
            }
            const _errs41 = errors;
            if (data && typeof data == 'object' && !Array.isArray(data)) {
              if (data.op === undefined) {
                const err38 = {
                  instancePath,
                  schemaPath: '#/oneOf/6/required',
                  keyword: 'required',
                  params: { missingProperty: 'op' },
                  message: "must have required property '" + 'op' + "'",
                };
                if (vErrors === null) {
                  vErrors = [err38];
                } else {
                  vErrors.push(err38);
                }
                errors++;
              }
              if (data.args === undefined) {
                const err39 = {
                  instancePath,
                  schemaPath: '#/oneOf/6/required',
                  keyword: 'required',
                  params: { missingProperty: 'args' },
                  message: "must have required property '" + 'args' + "'",
                };
                if (vErrors === null) {
                  vErrors = [err39];
                } else {
                  vErrors.push(err39);
                }
                errors++;
              }
              for (const key6 in data) {
                if (!(key6 === 'op' || key6 === 'args')) {
                  const err40 = {
                    instancePath,
                    schemaPath: '#/oneOf/6/additionalProperties',
                    keyword: 'additionalProperties',
                    params: { additionalProperty: key6 },
                    message: 'must NOT have additional properties',
                  };
                  if (vErrors === null) {
                    vErrors = [err40];
                  } else {
                    vErrors.push(err40);
                  }
                  errors++;
                }
              }
              if (data.op !== undefined) {
                let data11 = data.op;
                if (!(data11 === 'not' || data11 === 'isNull')) {
                  const err41 = {
                    instancePath: instancePath + '/op',
                    schemaPath: '#/oneOf/6/properties/op/enum',
                    keyword: 'enum',
                    params: { allowedValues: schema39.oneOf[6].properties.op.enum },
                    message: 'must be equal to one of the allowed values',
                  };
                  if (vErrors === null) {
                    vErrors = [err41];
                  } else {
                    vErrors.push(err41);
                  }
                  errors++;
                }
              }
              if (data.args !== undefined) {
                let data12 = data.args;
                if (Array.isArray(data12)) {
                  if (data12.length > 1) {
                    const err42 = {
                      instancePath: instancePath + '/args',
                      schemaPath: '#/oneOf/6/properties/args/maxItems',
                      keyword: 'maxItems',
                      params: { limit: 1 },
                      message: 'must NOT have more than 1 items',
                    };
                    if (vErrors === null) {
                      vErrors = [err42];
                    } else {
                      vErrors.push(err42);
                    }
                    errors++;
                  }
                  if (data12.length < 1) {
                    const err43 = {
                      instancePath: instancePath + '/args',
                      schemaPath: '#/oneOf/6/properties/args/minItems',
                      keyword: 'minItems',
                      params: { limit: 1 },
                      message: 'must NOT have fewer than 1 items',
                    };
                    if (vErrors === null) {
                      vErrors = [err43];
                    } else {
                      vErrors.push(err43);
                    }
                    errors++;
                  }
                  const len3 = data12.length;
                  for (let i3 = 0; i3 < len3; i3++) {
                    if (
                      !wrapper3.validate(data12[i3], {
                        instancePath: instancePath + '/args/' + i3,
                        parentData: data12,
                        parentDataProperty: i3,
                        rootData,
                        dynamicAnchors,
                      })
                    ) {
                      vErrors =
                        vErrors === null
                          ? wrapper3.validate.errors
                          : vErrors.concat(wrapper3.validate.errors);
                      errors = vErrors.length;
                    }
                  }
                } else {
                  const err44 = {
                    instancePath: instancePath + '/args',
                    schemaPath: '#/oneOf/6/properties/args/type',
                    keyword: 'type',
                    params: { type: 'array' },
                    message: 'must be array',
                  };
                  if (vErrors === null) {
                    vErrors = [err44];
                  } else {
                    vErrors.push(err44);
                  }
                  errors++;
                }
              }
            } else {
              const err45 = {
                instancePath,
                schemaPath: '#/oneOf/6/type',
                keyword: 'type',
                params: { type: 'object' },
                message: 'must be object',
              };
              if (vErrors === null) {
                vErrors = [err45];
              } else {
                vErrors.push(err45);
              }
              errors++;
            }
            var _valid0 = _errs41 === errors;
            if (_valid0 && valid0) {
              valid0 = false;
              passing0 = [passing0, 6];
            } else {
              if (_valid0) {
                valid0 = true;
                passing0 = 6;
                if (props0 !== true) {
                  props0 = true;
                }
              }
            }
          }
        }
      }
    }
  }
  if (!valid0) {
    const err46 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err46];
    } else {
      vErrors.push(err46);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate99.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate99.evaluated = { dynamicProps: true, dynamicItems: false };
const schema40 = {
  oneOf: [
    {
      type: 'object',
      properties: {
        op: { const: 'set' },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        uid: { $ref: '#/$defs/Expr' },
        property: { type: 'string', minLength: 1 },
        value: { $ref: '#/$defs/Expr' },
      },
      required: ['op', 'targetType', 'uid', 'property', 'value'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { const: 'clearOverride' },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        uid: { $ref: '#/$defs/Expr' },
        property: { type: 'string', minLength: 1 },
      },
      required: ['op', 'targetType', 'uid', 'property'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { enum: ['delete', 'restore'] },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        uid: { $ref: '#/$defs/Expr' },
      },
      required: ['op', 'targetType', 'uid'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { const: 'create' },
        targetType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        identityKey: { $ref: '#/$defs/Expr' },
        values: {
          type: 'object',
          propertyNames: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
          additionalProperties: { $ref: '#/$defs/Expr' },
          minProperties: 1,
        },
      },
      required: ['op', 'targetType', 'identityKey', 'values'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        op: { enum: ['link', 'unlink'] },
        linkType: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        sourceUid: { $ref: '#/$defs/Expr' },
        targetUid: { $ref: '#/$defs/Expr' },
      },
      required: ['op', 'linkType', 'sourceUid', 'targetUid'],
      additionalProperties: false,
    },
  ],
};
function validate101(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate101.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.op === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'op' },
        message: "must have required property '" + 'op' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.targetType === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'targetType' },
        message: "must have required property '" + 'targetType' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.uid === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'uid' },
        message: "must have required property '" + 'uid' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.property === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'property' },
        message: "must have required property '" + 'property' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    if (data.value === undefined) {
      const err4 = {
        instancePath,
        schemaPath: '#/oneOf/0/required',
        keyword: 'required',
        params: { missingProperty: 'value' },
        message: "must have required property '" + 'value' + "'",
      };
      if (vErrors === null) {
        vErrors = [err4];
      } else {
        vErrors.push(err4);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(
        key0 === 'op' ||
        key0 === 'targetType' ||
        key0 === 'uid' ||
        key0 === 'property' ||
        key0 === 'value'
      )) {
        const err5 = {
          instancePath,
          schemaPath: '#/oneOf/0/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.op !== undefined) {
      if ('set' !== data.op) {
        const err6 = {
          instancePath: instancePath + '/op',
          schemaPath: '#/oneOf/0/properties/op/const',
          keyword: 'const',
          params: { allowedValue: 'set' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.targetType !== undefined) {
      let data1 = data.targetType;
      if (typeof data1 === 'string') {
        if (!pattern5.test(data1)) {
          const err7 = {
            instancePath: instancePath + '/targetType',
            schemaPath: '#/oneOf/0/properties/targetType/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' },
            message:
              'must match pattern "' +
              '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
              '"',
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
      } else {
        const err8 = {
          instancePath: instancePath + '/targetType',
          schemaPath: '#/oneOf/0/properties/targetType/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err8];
        } else {
          vErrors.push(err8);
        }
        errors++;
      }
    }
    if (data.uid !== undefined) {
      if (
        !validate99(data.uid, {
          instancePath: instancePath + '/uid',
          parentData: data,
          parentDataProperty: 'uid',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
        errors = vErrors.length;
      }
    }
    if (data.property !== undefined) {
      let data3 = data.property;
      if (typeof data3 === 'string') {
        if (func1(data3) < 1) {
          const err9 = {
            instancePath: instancePath + '/property',
            schemaPath: '#/oneOf/0/properties/property/minLength',
            keyword: 'minLength',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 characters',
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
      } else {
        const err10 = {
          instancePath: instancePath + '/property',
          schemaPath: '#/oneOf/0/properties/property/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err10];
        } else {
          vErrors.push(err10);
        }
        errors++;
      }
    }
    if (data.value !== undefined) {
      if (
        !validate99(data.value, {
          instancePath: instancePath + '/value',
          parentData: data,
          parentDataProperty: 'value',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
        errors = vErrors.length;
      }
    }
  } else {
    const err11 = {
      instancePath,
      schemaPath: '#/oneOf/0/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err11];
    } else {
      vErrors.push(err11);
    }
    errors++;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
    var props2 = true;
  }
  const _errs11 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.op === undefined) {
      const err12 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'op' },
        message: "must have required property '" + 'op' + "'",
      };
      if (vErrors === null) {
        vErrors = [err12];
      } else {
        vErrors.push(err12);
      }
      errors++;
    }
    if (data.targetType === undefined) {
      const err13 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'targetType' },
        message: "must have required property '" + 'targetType' + "'",
      };
      if (vErrors === null) {
        vErrors = [err13];
      } else {
        vErrors.push(err13);
      }
      errors++;
    }
    if (data.uid === undefined) {
      const err14 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'uid' },
        message: "must have required property '" + 'uid' + "'",
      };
      if (vErrors === null) {
        vErrors = [err14];
      } else {
        vErrors.push(err14);
      }
      errors++;
    }
    if (data.property === undefined) {
      const err15 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'property' },
        message: "must have required property '" + 'property' + "'",
      };
      if (vErrors === null) {
        vErrors = [err15];
      } else {
        vErrors.push(err15);
      }
      errors++;
    }
    for (const key1 in data) {
      if (!(key1 === 'op' || key1 === 'targetType' || key1 === 'uid' || key1 === 'property')) {
        const err16 = {
          instancePath,
          schemaPath: '#/oneOf/1/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key1 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err16];
        } else {
          vErrors.push(err16);
        }
        errors++;
      }
    }
    if (data.op !== undefined) {
      if ('clearOverride' !== data.op) {
        const err17 = {
          instancePath: instancePath + '/op',
          schemaPath: '#/oneOf/1/properties/op/const',
          keyword: 'const',
          params: { allowedValue: 'clearOverride' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err17];
        } else {
          vErrors.push(err17);
        }
        errors++;
      }
    }
    if (data.targetType !== undefined) {
      let data6 = data.targetType;
      if (typeof data6 === 'string') {
        if (!pattern5.test(data6)) {
          const err18 = {
            instancePath: instancePath + '/targetType',
            schemaPath: '#/oneOf/1/properties/targetType/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' },
            message:
              'must match pattern "' +
              '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
              '"',
          };
          if (vErrors === null) {
            vErrors = [err18];
          } else {
            vErrors.push(err18);
          }
          errors++;
        }
      } else {
        const err19 = {
          instancePath: instancePath + '/targetType',
          schemaPath: '#/oneOf/1/properties/targetType/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err19];
        } else {
          vErrors.push(err19);
        }
        errors++;
      }
    }
    if (data.uid !== undefined) {
      if (
        !validate99(data.uid, {
          instancePath: instancePath + '/uid',
          parentData: data,
          parentDataProperty: 'uid',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
        errors = vErrors.length;
      }
    }
    if (data.property !== undefined) {
      let data8 = data.property;
      if (typeof data8 === 'string') {
        if (func1(data8) < 1) {
          const err20 = {
            instancePath: instancePath + '/property',
            schemaPath: '#/oneOf/1/properties/property/minLength',
            keyword: 'minLength',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 characters',
          };
          if (vErrors === null) {
            vErrors = [err20];
          } else {
            vErrors.push(err20);
          }
          errors++;
        }
      } else {
        const err21 = {
          instancePath: instancePath + '/property',
          schemaPath: '#/oneOf/1/properties/property/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err21];
        } else {
          vErrors.push(err21);
        }
        errors++;
      }
    }
  } else {
    const err22 = {
      instancePath,
      schemaPath: '#/oneOf/1/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err22];
    } else {
      vErrors.push(err22);
    }
    errors++;
  }
  var _valid0 = _errs11 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props2 !== true) {
        props2 = true;
      }
    }
    const _errs20 = errors;
    if (data && typeof data == 'object' && !Array.isArray(data)) {
      if (data.op === undefined) {
        const err23 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'op' },
          message: "must have required property '" + 'op' + "'",
        };
        if (vErrors === null) {
          vErrors = [err23];
        } else {
          vErrors.push(err23);
        }
        errors++;
      }
      if (data.targetType === undefined) {
        const err24 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'targetType' },
          message: "must have required property '" + 'targetType' + "'",
        };
        if (vErrors === null) {
          vErrors = [err24];
        } else {
          vErrors.push(err24);
        }
        errors++;
      }
      if (data.uid === undefined) {
        const err25 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'uid' },
          message: "must have required property '" + 'uid' + "'",
        };
        if (vErrors === null) {
          vErrors = [err25];
        } else {
          vErrors.push(err25);
        }
        errors++;
      }
      for (const key2 in data) {
        if (!(key2 === 'op' || key2 === 'targetType' || key2 === 'uid')) {
          const err26 = {
            instancePath,
            schemaPath: '#/oneOf/2/additionalProperties',
            keyword: 'additionalProperties',
            params: { additionalProperty: key2 },
            message: 'must NOT have additional properties',
          };
          if (vErrors === null) {
            vErrors = [err26];
          } else {
            vErrors.push(err26);
          }
          errors++;
        }
      }
      if (data.op !== undefined) {
        let data9 = data.op;
        if (!(data9 === 'delete' || data9 === 'restore')) {
          const err27 = {
            instancePath: instancePath + '/op',
            schemaPath: '#/oneOf/2/properties/op/enum',
            keyword: 'enum',
            params: { allowedValues: schema40.oneOf[2].properties.op.enum },
            message: 'must be equal to one of the allowed values',
          };
          if (vErrors === null) {
            vErrors = [err27];
          } else {
            vErrors.push(err27);
          }
          errors++;
        }
      }
      if (data.targetType !== undefined) {
        let data10 = data.targetType;
        if (typeof data10 === 'string') {
          if (!pattern5.test(data10)) {
            const err28 = {
              instancePath: instancePath + '/targetType',
              schemaPath: '#/oneOf/2/properties/targetType/pattern',
              keyword: 'pattern',
              params: {
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              message:
                'must match pattern "' +
                '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                '"',
            };
            if (vErrors === null) {
              vErrors = [err28];
            } else {
              vErrors.push(err28);
            }
            errors++;
          }
        } else {
          const err29 = {
            instancePath: instancePath + '/targetType',
            schemaPath: '#/oneOf/2/properties/targetType/type',
            keyword: 'type',
            params: { type: 'string' },
            message: 'must be string',
          };
          if (vErrors === null) {
            vErrors = [err29];
          } else {
            vErrors.push(err29);
          }
          errors++;
        }
      }
      if (data.uid !== undefined) {
        if (
          !validate99(data.uid, {
            instancePath: instancePath + '/uid',
            parentData: data,
            parentDataProperty: 'uid',
            rootData,
            dynamicAnchors,
          })
        ) {
          vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
          errors = vErrors.length;
        }
      }
    } else {
      const err30 = {
        instancePath,
        schemaPath: '#/oneOf/2/type',
        keyword: 'type',
        params: { type: 'object' },
        message: 'must be object',
      };
      if (vErrors === null) {
        vErrors = [err30];
      } else {
        vErrors.push(err30);
      }
      errors++;
    }
    var _valid0 = _errs20 === errors;
    if (_valid0 && valid0) {
      valid0 = false;
      passing0 = [passing0, 2];
    } else {
      if (_valid0) {
        valid0 = true;
        passing0 = 2;
        if (props2 !== true) {
          props2 = true;
        }
      }
      const _errs27 = errors;
      if (data && typeof data == 'object' && !Array.isArray(data)) {
        if (data.op === undefined) {
          const err31 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'op' },
            message: "must have required property '" + 'op' + "'",
          };
          if (vErrors === null) {
            vErrors = [err31];
          } else {
            vErrors.push(err31);
          }
          errors++;
        }
        if (data.targetType === undefined) {
          const err32 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'targetType' },
            message: "must have required property '" + 'targetType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err32];
          } else {
            vErrors.push(err32);
          }
          errors++;
        }
        if (data.identityKey === undefined) {
          const err33 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'identityKey' },
            message: "must have required property '" + 'identityKey' + "'",
          };
          if (vErrors === null) {
            vErrors = [err33];
          } else {
            vErrors.push(err33);
          }
          errors++;
        }
        if (data.values === undefined) {
          const err34 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'values' },
            message: "must have required property '" + 'values' + "'",
          };
          if (vErrors === null) {
            vErrors = [err34];
          } else {
            vErrors.push(err34);
          }
          errors++;
        }
        for (const key3 in data) {
          if (!(
            key3 === 'op' ||
            key3 === 'targetType' ||
            key3 === 'identityKey' ||
            key3 === 'values'
          )) {
            const err35 = {
              instancePath,
              schemaPath: '#/oneOf/3/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key3 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err35];
            } else {
              vErrors.push(err35);
            }
            errors++;
          }
        }
        if (data.op !== undefined) {
          if ('create' !== data.op) {
            const err36 = {
              instancePath: instancePath + '/op',
              schemaPath: '#/oneOf/3/properties/op/const',
              keyword: 'const',
              params: { allowedValue: 'create' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err36];
            } else {
              vErrors.push(err36);
            }
            errors++;
          }
        }
        if (data.targetType !== undefined) {
          let data13 = data.targetType;
          if (typeof data13 === 'string') {
            if (!pattern5.test(data13)) {
              const err37 = {
                instancePath: instancePath + '/targetType',
                schemaPath: '#/oneOf/3/properties/targetType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err37];
              } else {
                vErrors.push(err37);
              }
              errors++;
            }
          } else {
            const err38 = {
              instancePath: instancePath + '/targetType',
              schemaPath: '#/oneOf/3/properties/targetType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err38];
            } else {
              vErrors.push(err38);
            }
            errors++;
          }
        }
        if (data.identityKey !== undefined) {
          if (
            !validate99(data.identityKey, {
              instancePath: instancePath + '/identityKey',
              parentData: data,
              parentDataProperty: 'identityKey',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
            errors = vErrors.length;
          }
        }
        if (data.values !== undefined) {
          let data15 = data.values;
          if (data15 && typeof data15 == 'object' && !Array.isArray(data15)) {
            if (Object.keys(data15).length < 1) {
              const err39 = {
                instancePath: instancePath + '/values',
                schemaPath: '#/oneOf/3/properties/values/minProperties',
                keyword: 'minProperties',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 properties',
              };
              if (vErrors === null) {
                vErrors = [err39];
              } else {
                vErrors.push(err39);
              }
              errors++;
            }
            for (const key4 in data15) {
              const _errs36 = errors;
              if (typeof key4 === 'string') {
                if (!pattern8.test(key4)) {
                  const err40 = {
                    instancePath: instancePath + '/values',
                    schemaPath: '#/oneOf/3/properties/values/propertyNames/pattern',
                    keyword: 'pattern',
                    params: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
                    message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_]*$' + '"',
                    propertyName: key4,
                  };
                  if (vErrors === null) {
                    vErrors = [err40];
                  } else {
                    vErrors.push(err40);
                  }
                  errors++;
                }
              }
              var valid5 = _errs36 === errors;
              if (!valid5) {
                const err41 = {
                  instancePath: instancePath + '/values',
                  schemaPath: '#/oneOf/3/properties/values/propertyNames',
                  keyword: 'propertyNames',
                  params: { propertyName: key4 },
                  message: 'property name must be valid',
                };
                if (vErrors === null) {
                  vErrors = [err41];
                } else {
                  vErrors.push(err41);
                }
                errors++;
              }
            }
            for (const key5 in data15) {
              if (
                !validate99(data15[key5], {
                  instancePath:
                    instancePath + '/values/' + key5.replace(/~/g, '~0').replace(/\//g, '~1'),
                  parentData: data15,
                  parentDataProperty: key5,
                  rootData,
                  dynamicAnchors,
                })
              ) {
                vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
                errors = vErrors.length;
              }
            }
          } else {
            const err42 = {
              instancePath: instancePath + '/values',
              schemaPath: '#/oneOf/3/properties/values/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err42];
            } else {
              vErrors.push(err42);
            }
            errors++;
          }
        }
      } else {
        const err43 = {
          instancePath,
          schemaPath: '#/oneOf/3/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err43];
        } else {
          vErrors.push(err43);
        }
        errors++;
      }
      var _valid0 = _errs27 === errors;
      if (_valid0 && valid0) {
        valid0 = false;
        passing0 = [passing0, 3];
      } else {
        if (_valid0) {
          valid0 = true;
          passing0 = 3;
          if (props2 !== true) {
            props2 = true;
          }
        }
        const _errs39 = errors;
        if (data && typeof data == 'object' && !Array.isArray(data)) {
          if (data.op === undefined) {
            const err44 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'op' },
              message: "must have required property '" + 'op' + "'",
            };
            if (vErrors === null) {
              vErrors = [err44];
            } else {
              vErrors.push(err44);
            }
            errors++;
          }
          if (data.linkType === undefined) {
            const err45 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'linkType' },
              message: "must have required property '" + 'linkType' + "'",
            };
            if (vErrors === null) {
              vErrors = [err45];
            } else {
              vErrors.push(err45);
            }
            errors++;
          }
          if (data.sourceUid === undefined) {
            const err46 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'sourceUid' },
              message: "must have required property '" + 'sourceUid' + "'",
            };
            if (vErrors === null) {
              vErrors = [err46];
            } else {
              vErrors.push(err46);
            }
            errors++;
          }
          if (data.targetUid === undefined) {
            const err47 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'targetUid' },
              message: "must have required property '" + 'targetUid' + "'",
            };
            if (vErrors === null) {
              vErrors = [err47];
            } else {
              vErrors.push(err47);
            }
            errors++;
          }
          for (const key6 in data) {
            if (!(
              key6 === 'op' ||
              key6 === 'linkType' ||
              key6 === 'sourceUid' ||
              key6 === 'targetUid'
            )) {
              const err48 = {
                instancePath,
                schemaPath: '#/oneOf/4/additionalProperties',
                keyword: 'additionalProperties',
                params: { additionalProperty: key6 },
                message: 'must NOT have additional properties',
              };
              if (vErrors === null) {
                vErrors = [err48];
              } else {
                vErrors.push(err48);
              }
              errors++;
            }
          }
          if (data.op !== undefined) {
            let data17 = data.op;
            if (!(data17 === 'link' || data17 === 'unlink')) {
              const err49 = {
                instancePath: instancePath + '/op',
                schemaPath: '#/oneOf/4/properties/op/enum',
                keyword: 'enum',
                params: { allowedValues: schema40.oneOf[4].properties.op.enum },
                message: 'must be equal to one of the allowed values',
              };
              if (vErrors === null) {
                vErrors = [err49];
              } else {
                vErrors.push(err49);
              }
              errors++;
            }
          }
          if (data.linkType !== undefined) {
            let data18 = data.linkType;
            if (typeof data18 === 'string') {
              if (!pattern5.test(data18)) {
                const err50 = {
                  instancePath: instancePath + '/linkType',
                  schemaPath: '#/oneOf/4/properties/linkType/pattern',
                  keyword: 'pattern',
                  params: {
                    pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                  },
                  message:
                    'must match pattern "' +
                    '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                    '"',
                };
                if (vErrors === null) {
                  vErrors = [err50];
                } else {
                  vErrors.push(err50);
                }
                errors++;
              }
            } else {
              const err51 = {
                instancePath: instancePath + '/linkType',
                schemaPath: '#/oneOf/4/properties/linkType/type',
                keyword: 'type',
                params: { type: 'string' },
                message: 'must be string',
              };
              if (vErrors === null) {
                vErrors = [err51];
              } else {
                vErrors.push(err51);
              }
              errors++;
            }
          }
          if (data.sourceUid !== undefined) {
            if (
              !validate99(data.sourceUid, {
                instancePath: instancePath + '/sourceUid',
                parentData: data,
                parentDataProperty: 'sourceUid',
                rootData,
                dynamicAnchors,
              })
            ) {
              vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
              errors = vErrors.length;
            }
          }
          if (data.targetUid !== undefined) {
            if (
              !validate99(data.targetUid, {
                instancePath: instancePath + '/targetUid',
                parentData: data,
                parentDataProperty: 'targetUid',
                rootData,
                dynamicAnchors,
              })
            ) {
              vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
              errors = vErrors.length;
            }
          }
        } else {
          const err52 = {
            instancePath,
            schemaPath: '#/oneOf/4/type',
            keyword: 'type',
            params: { type: 'object' },
            message: 'must be object',
          };
          if (vErrors === null) {
            vErrors = [err52];
          } else {
            vErrors.push(err52);
          }
          errors++;
        }
        var _valid0 = _errs39 === errors;
        if (_valid0 && valid0) {
          valid0 = false;
          passing0 = [passing0, 4];
        } else {
          if (_valid0) {
            valid0 = true;
            passing0 = 4;
            if (props2 !== true) {
              props2 = true;
            }
          }
        }
      }
    }
  }
  if (!valid0) {
    const err53 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err53];
    } else {
      vErrors.push(err53);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate101.errors = vErrors;
  evaluated0.props = props2;
  return errors === 0;
}
validate101.evaluated = { dynamicProps: true, dynamicItems: false };
function validate96(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate96.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Action' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Action' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      const _errs7 = errors;
      let valid1 = false;
      let passing0 = null;
      const _errs8 = errors;
      const _errs9 = errors;
      const _errs10 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        let missing0;
        if (data3.function === undefined && (missing0 = 'function')) {
          const err7 = {};
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
      }
      var valid2 = _errs10 === errors;
      if (valid2) {
        const err8 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/0/not',
          keyword: 'not',
          params: {},
          message: 'must NOT be valid',
        };
        if (vErrors === null) {
          vErrors = [err8];
        } else {
          vErrors.push(err8);
        }
        errors++;
      } else {
        errors = _errs9;
        if (vErrors !== null) {
          if (_errs9) {
            vErrors.length = _errs9;
          } else {
            vErrors = null;
          }
        }
      }
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.edits === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'edits' },
            message: "must have required property '" + 'edits' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
      }
      var _valid0 = _errs8 === errors;
      if (_valid0) {
        valid1 = true;
        passing0 = 0;
      }
      const _errs11 = errors;
      const _errs12 = errors;
      const _errs13 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        let missing1;
        if (data3.edits === undefined && (missing1 = 'edits')) {
          const err10 = {};
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
      }
      var valid3 = _errs13 === errors;
      if (valid3) {
        const err11 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/1/not',
          keyword: 'not',
          params: {},
          message: 'must NOT be valid',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      } else {
        errors = _errs12;
        if (vErrors !== null) {
          if (_errs12) {
            vErrors.length = _errs12;
          } else {
            vErrors = null;
          }
        }
      }
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.function === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'function' },
            message: "must have required property '" + 'function' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
      }
      var _valid0 = _errs11 === errors;
      if (_valid0 && valid1) {
        valid1 = false;
        passing0 = [passing0, 1];
      } else {
        if (_valid0) {
          valid1 = true;
          passing0 = 1;
        }
      }
      if (!valid1) {
        const err13 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf',
          keyword: 'oneOf',
          params: { passingSchemas: passing0 },
          message: 'must match exactly one schema in oneOf',
        };
        if (vErrors === null) {
          vErrors = [err13];
        } else {
          vErrors.push(err13);
        }
        errors++;
      } else {
        errors = _errs7;
        if (vErrors !== null) {
          if (_errs7) {
            vErrors.length = _errs7;
          } else {
            vErrors = null;
          }
        }
      }
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.inputs === undefined) {
          const err14 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'inputs' },
            message: "must have required property '" + 'inputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err14];
          } else {
            vErrors.push(err14);
          }
          errors++;
        }
        if (data3.targetType === undefined) {
          const err15 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'targetType' },
            message: "must have required property '" + 'targetType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err15];
          } else {
            vErrors.push(err15);
          }
          errors++;
        }
        if (data3.condition === undefined) {
          const err16 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'condition' },
            message: "must have required property '" + 'condition' + "'",
          };
          if (vErrors === null) {
            vErrors = [err16];
          } else {
            vErrors.push(err16);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'inputs' ||
            key1 === 'targetType' ||
            key1 === 'targetParameter' ||
            key1 === 'condition' ||
            key1 === 'edits' ||
            key1 === 'function'
          )) {
            const err17 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.inputs !== undefined) {
          if (
            !validate68(data3.inputs, {
              instancePath: instancePath + '/spec/inputs',
              parentData: data3,
              parentDataProperty: 'inputs',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate68.errors : vErrors.concat(validate68.errors);
            errors = vErrors.length;
          }
        }
        if (data3.targetType !== undefined) {
          let data5 = data3.targetType;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err18 = {
                instancePath: instancePath + '/spec/targetType',
                schemaPath: '#/properties/spec/properties/targetType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
          } else {
            const err19 = {
              instancePath: instancePath + '/spec/targetType',
              schemaPath: '#/properties/spec/properties/targetType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data3.targetParameter !== undefined) {
          let data6 = data3.targetParameter;
          if (typeof data6 === 'string') {
            if (func1(data6) < 1) {
              const err20 = {
                instancePath: instancePath + '/spec/targetParameter',
                schemaPath: '#/properties/spec/properties/targetParameter/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err20];
              } else {
                vErrors.push(err20);
              }
              errors++;
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/spec/targetParameter',
              schemaPath: '#/properties/spec/properties/targetParameter/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
        if (data3.condition !== undefined) {
          if (
            !validate99(data3.condition, {
              instancePath: instancePath + '/spec/condition',
              parentData: data3,
              parentDataProperty: 'condition',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
            errors = vErrors.length;
          }
        }
        if (data3.edits !== undefined) {
          let data8 = data3.edits;
          if (Array.isArray(data8)) {
            if (data8.length < 1) {
              const err22 = {
                instancePath: instancePath + '/spec/edits',
                schemaPath: '#/properties/spec/properties/edits/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
            const len0 = data8.length;
            for (let i0 = 0; i0 < len0; i0++) {
              if (
                !validate101(data8[i0], {
                  instancePath: instancePath + '/spec/edits/' + i0,
                  parentData: data8,
                  parentDataProperty: i0,
                  rootData,
                  dynamicAnchors,
                })
              ) {
                vErrors =
                  vErrors === null ? validate101.errors : vErrors.concat(validate101.errors);
                errors = vErrors.length;
              }
            }
          } else {
            const err23 = {
              instancePath: instancePath + '/spec/edits',
              schemaPath: '#/properties/spec/properties/edits/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
        }
        if (data3.function !== undefined) {
          let data10 = data3.function;
          if (typeof data10 === 'string') {
            if (!pattern5.test(data10)) {
              const err24 = {
                instancePath: instancePath + '/spec/function',
                schemaPath: '#/properties/spec/properties/function/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err24];
              } else {
                vErrors.push(err24);
              }
              errors++;
            }
          } else {
            const err25 = {
              instancePath: instancePath + '/spec/function',
              schemaPath: '#/properties/spec/properties/function/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err25];
            } else {
              vErrors.push(err25);
            }
            errors++;
          }
        }
      } else {
        const err26 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err26];
        } else {
          vErrors.push(err26);
        }
        errors++;
      }
    }
  } else {
    const err27 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err27];
    } else {
      vErrors.push(err27);
    }
    errors++;
  }
  validate96.errors = vErrors;
  return errors === 0;
}
validate96.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema41 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Function' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        artifact: { $ref: '#/$defs/Artifact' },
        runtime: { const: 'typescript-v1' },
        inputs: {
          $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/FunctionFields',
        },
        outputs: {
          $ref: 'https://spec.example.invalid/moria/types/v1/schema.json#/$defs/FunctionFields',
        },
        effect: { enum: ['readOnly', 'editProposal', 'readWrite'] },
        limits: { $ref: '#/$defs/Limits' },
      },
      required: ['artifact', 'runtime', 'inputs', 'outputs', 'effect', 'limits'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema42 = {
  type: 'object',
  propertyNames: {
    pattern: '^[A-Za-z_][A-Za-z0-9_]*$',
    maxLength: 100,
    not: { enum: ['constructor', 'prototype', '__proto__'] },
  },
  additionalProperties: { $ref: '#/$defs/Type' },
  minProperties: 0,
};
const schema26 = {
  oneOf: [
    { $ref: '#/$defs/OntologyObject' },
    { $ref: '#/$defs/Scalar' },
    {
      type: 'object',
      properties: {
        type: { const: 'array' },
        nullable: { type: 'boolean' },
        items: { $ref: '#/$defs/Type' },
      },
      required: ['type', 'nullable', 'items'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        type: { const: 'list' },
        nullable: { type: 'boolean' },
        items: { $ref: '#/$defs/Type' },
      },
      required: ['type', 'items'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: {
        type: { const: 'object' },
        nullable: { type: 'boolean' },
        properties: {
          type: 'object',
          propertyNames: {
            pattern: '^[A-Za-z_][A-Za-z0-9_-]{0,99}$',
            not: { enum: ['constructor', 'prototype', '__proto__'] },
          },
          additionalProperties: { $ref: '#/$defs/Type' },
        },
        optional: { type: 'array', items: { type: 'string' }, uniqueItems: true },
      },
      required: ['type', 'properties'],
      additionalProperties: false,
    },
    {
      type: 'object',
      properties: { ref: { type: 'string', minLength: 1 } },
      required: ['ref'],
      additionalProperties: false,
    },
  ],
};
const schema27 = {
  type: 'object',
  properties: {
    type: { const: 'ontologyObject' },
    nullable: { type: 'boolean' },
    objectType: { type: 'string', pattern: '^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$' },
  },
  required: ['type', 'nullable', 'objectType'],
  additionalProperties: false,
};
const pattern6 = new RegExp('^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$', 'u');
function validate63(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate63.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.type === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'type' },
        message: "must have required property '" + 'type' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.nullable === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'nullable' },
        message: "must have required property '" + 'nullable' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.objectType === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'objectType' },
        message: "must have required property '" + 'objectType' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'type' || key0 === 'nullable' || key0 === 'objectType')) {
        const err3 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err3];
        } else {
          vErrors.push(err3);
        }
        errors++;
      }
    }
    if (data.type !== undefined) {
      if ('ontologyObject' !== data.type) {
        const err4 = {
          instancePath: instancePath + '/type',
          schemaPath: '#/properties/type/const',
          keyword: 'const',
          params: { allowedValue: 'ontologyObject' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.nullable !== undefined) {
      if (typeof data.nullable !== 'boolean') {
        const err5 = {
          instancePath: instancePath + '/nullable',
          schemaPath: '#/properties/nullable/type',
          keyword: 'type',
          params: { type: 'boolean' },
          message: 'must be boolean',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.objectType !== undefined) {
      let data2 = data.objectType;
      if (typeof data2 === 'string') {
        if (!pattern6.test(data2)) {
          const err6 = {
            instancePath: instancePath + '/objectType',
            schemaPath: '#/properties/objectType/pattern',
            keyword: 'pattern',
            params: { pattern: '^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$' },
            message: 'must match pattern "' + '^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$' + '"',
          };
          if (vErrors === null) {
            vErrors = [err6];
          } else {
            vErrors.push(err6);
          }
          errors++;
        }
      } else {
        const err7 = {
          instancePath: instancePath + '/objectType',
          schemaPath: '#/properties/objectType/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err7];
        } else {
          vErrors.push(err7);
        }
        errors++;
      }
    }
  } else {
    const err8 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err8];
    } else {
      vErrors.push(err8);
    }
    errors++;
  }
  validate63.errors = vErrors;
  return errors === 0;
}
validate63.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const wrapper0 = { validate: validate62 };
const pattern7 = new RegExp('^[A-Za-z_][A-Za-z0-9_-]{0,99}$', 'u');
function validate62(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate62.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (
    !validate63(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate63.errors : vErrors.concat(validate63.errors);
    errors = vErrors.length;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
    var props0 = true;
  }
  const _errs2 = errors;
  if (
    !validate65(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate65.errors : vErrors.concat(validate65.errors);
    errors = vErrors.length;
  } else {
    var props1 = validate65.evaluated.props;
  }
  var _valid0 = _errs2 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true && props1 !== undefined) {
        if (props1 === true) {
          props0 = true;
        } else {
          props0 = props0 || {};
          Object.assign(props0, props1);
        }
      }
    }
    const _errs3 = errors;
    if (data && typeof data == 'object' && !Array.isArray(data)) {
      if (data.type === undefined) {
        const err0 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'type' },
          message: "must have required property '" + 'type' + "'",
        };
        if (vErrors === null) {
          vErrors = [err0];
        } else {
          vErrors.push(err0);
        }
        errors++;
      }
      if (data.nullable === undefined) {
        const err1 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'nullable' },
          message: "must have required property '" + 'nullable' + "'",
        };
        if (vErrors === null) {
          vErrors = [err1];
        } else {
          vErrors.push(err1);
        }
        errors++;
      }
      if (data.items === undefined) {
        const err2 = {
          instancePath,
          schemaPath: '#/oneOf/2/required',
          keyword: 'required',
          params: { missingProperty: 'items' },
          message: "must have required property '" + 'items' + "'",
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
      for (const key0 in data) {
        if (!(key0 === 'type' || key0 === 'nullable' || key0 === 'items')) {
          const err3 = {
            instancePath,
            schemaPath: '#/oneOf/2/additionalProperties',
            keyword: 'additionalProperties',
            params: { additionalProperty: key0 },
            message: 'must NOT have additional properties',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
      }
      if (data.type !== undefined) {
        if ('array' !== data.type) {
          const err4 = {
            instancePath: instancePath + '/type',
            schemaPath: '#/oneOf/2/properties/type/const',
            keyword: 'const',
            params: { allowedValue: 'array' },
            message: 'must be equal to constant',
          };
          if (vErrors === null) {
            vErrors = [err4];
          } else {
            vErrors.push(err4);
          }
          errors++;
        }
      }
      if (data.nullable !== undefined) {
        if (typeof data.nullable !== 'boolean') {
          const err5 = {
            instancePath: instancePath + '/nullable',
            schemaPath: '#/oneOf/2/properties/nullable/type',
            keyword: 'type',
            params: { type: 'boolean' },
            message: 'must be boolean',
          };
          if (vErrors === null) {
            vErrors = [err5];
          } else {
            vErrors.push(err5);
          }
          errors++;
        }
      }
      if (data.items !== undefined) {
        if (
          !wrapper0.validate(data.items, {
            instancePath: instancePath + '/items',
            parentData: data,
            parentDataProperty: 'items',
            rootData,
            dynamicAnchors,
          })
        ) {
          vErrors =
            vErrors === null ? wrapper0.validate.errors : vErrors.concat(wrapper0.validate.errors);
          errors = vErrors.length;
        }
      }
    } else {
      const err6 = {
        instancePath,
        schemaPath: '#/oneOf/2/type',
        keyword: 'type',
        params: { type: 'object' },
        message: 'must be object',
      };
      if (vErrors === null) {
        vErrors = [err6];
      } else {
        vErrors.push(err6);
      }
      errors++;
    }
    var _valid0 = _errs3 === errors;
    if (_valid0 && valid0) {
      valid0 = false;
      passing0 = [passing0, 2];
    } else {
      if (_valid0) {
        valid0 = true;
        passing0 = 2;
        if (props0 !== true) {
          props0 = true;
        }
      }
      const _errs10 = errors;
      if (data && typeof data == 'object' && !Array.isArray(data)) {
        if (data.type === undefined) {
          const err7 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'type' },
            message: "must have required property '" + 'type' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data.items === undefined) {
          const err8 = {
            instancePath,
            schemaPath: '#/oneOf/3/required',
            keyword: 'required',
            params: { missingProperty: 'items' },
            message: "must have required property '" + 'items' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data) {
          if (!(key1 === 'type' || key1 === 'nullable' || key1 === 'items')) {
            const err9 = {
              instancePath,
              schemaPath: '#/oneOf/3/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data.type !== undefined) {
          if ('list' !== data.type) {
            const err10 = {
              instancePath: instancePath + '/type',
              schemaPath: '#/oneOf/3/properties/type/const',
              keyword: 'const',
              params: { allowedValue: 'list' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data.nullable !== undefined) {
          if (typeof data.nullable !== 'boolean') {
            const err11 = {
              instancePath: instancePath + '/nullable',
              schemaPath: '#/oneOf/3/properties/nullable/type',
              keyword: 'type',
              params: { type: 'boolean' },
              message: 'must be boolean',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data.items !== undefined) {
          if (
            !wrapper0.validate(data.items, {
              instancePath: instancePath + '/items',
              parentData: data,
              parentDataProperty: 'items',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors =
              vErrors === null
                ? wrapper0.validate.errors
                : vErrors.concat(wrapper0.validate.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err12 = {
          instancePath,
          schemaPath: '#/oneOf/3/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err12];
        } else {
          vErrors.push(err12);
        }
        errors++;
      }
      var _valid0 = _errs10 === errors;
      if (_valid0 && valid0) {
        valid0 = false;
        passing0 = [passing0, 3];
      } else {
        if (_valid0) {
          valid0 = true;
          passing0 = 3;
          if (props0 !== true) {
            props0 = true;
          }
        }
        const _errs17 = errors;
        if (data && typeof data == 'object' && !Array.isArray(data)) {
          if (data.type === undefined) {
            const err13 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'type' },
              message: "must have required property '" + 'type' + "'",
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
          if (data.properties === undefined) {
            const err14 = {
              instancePath,
              schemaPath: '#/oneOf/4/required',
              keyword: 'required',
              params: { missingProperty: 'properties' },
              message: "must have required property '" + 'properties' + "'",
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
          for (const key2 in data) {
            if (!(
              key2 === 'type' ||
              key2 === 'nullable' ||
              key2 === 'properties' ||
              key2 === 'optional'
            )) {
              const err15 = {
                instancePath,
                schemaPath: '#/oneOf/4/additionalProperties',
                keyword: 'additionalProperties',
                params: { additionalProperty: key2 },
                message: 'must NOT have additional properties',
              };
              if (vErrors === null) {
                vErrors = [err15];
              } else {
                vErrors.push(err15);
              }
              errors++;
            }
          }
          if (data.type !== undefined) {
            if ('object' !== data.type) {
              const err16 = {
                instancePath: instancePath + '/type',
                schemaPath: '#/oneOf/4/properties/type/const',
                keyword: 'const',
                params: { allowedValue: 'object' },
                message: 'must be equal to constant',
              };
              if (vErrors === null) {
                vErrors = [err16];
              } else {
                vErrors.push(err16);
              }
              errors++;
            }
          }
          if (data.nullable !== undefined) {
            if (typeof data.nullable !== 'boolean') {
              const err17 = {
                instancePath: instancePath + '/nullable',
                schemaPath: '#/oneOf/4/properties/nullable/type',
                keyword: 'type',
                params: { type: 'boolean' },
                message: 'must be boolean',
              };
              if (vErrors === null) {
                vErrors = [err17];
              } else {
                vErrors.push(err17);
              }
              errors++;
            }
          }
          if (data.properties !== undefined) {
            let data8 = data.properties;
            if (data8 && typeof data8 == 'object' && !Array.isArray(data8)) {
              for (const key3 in data8) {
                const _errs25 = errors;
                const _errs26 = errors;
                const _errs27 = errors;
                if (!(key3 === 'constructor' || key3 === 'prototype' || key3 === '__proto__')) {
                  const err18 = {};
                  if (vErrors === null) {
                    vErrors = [err18];
                  } else {
                    vErrors.push(err18);
                  }
                  errors++;
                }
                var valid5 = _errs27 === errors;
                if (valid5) {
                  const err19 = {
                    instancePath: instancePath + '/properties',
                    schemaPath: '#/oneOf/4/properties/properties/propertyNames/not',
                    keyword: 'not',
                    params: {},
                    message: 'must NOT be valid',
                    propertyName: key3,
                  };
                  if (vErrors === null) {
                    vErrors = [err19];
                  } else {
                    vErrors.push(err19);
                  }
                  errors++;
                } else {
                  errors = _errs26;
                  if (vErrors !== null) {
                    if (_errs26) {
                      vErrors.length = _errs26;
                    } else {
                      vErrors = null;
                    }
                  }
                }
                if (typeof key3 === 'string') {
                  if (!pattern7.test(key3)) {
                    const err20 = {
                      instancePath: instancePath + '/properties',
                      schemaPath: '#/oneOf/4/properties/properties/propertyNames/pattern',
                      keyword: 'pattern',
                      params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]{0,99}$' },
                      message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_-]{0,99}$' + '"',
                      propertyName: key3,
                    };
                    if (vErrors === null) {
                      vErrors = [err20];
                    } else {
                      vErrors.push(err20);
                    }
                    errors++;
                  }
                }
                var valid4 = _errs25 === errors;
                if (!valid4) {
                  const err21 = {
                    instancePath: instancePath + '/properties',
                    schemaPath: '#/oneOf/4/properties/properties/propertyNames',
                    keyword: 'propertyNames',
                    params: { propertyName: key3 },
                    message: 'property name must be valid',
                  };
                  if (vErrors === null) {
                    vErrors = [err21];
                  } else {
                    vErrors.push(err21);
                  }
                  errors++;
                }
              }
              for (const key4 in data8) {
                if (
                  !wrapper0.validate(data8[key4], {
                    instancePath:
                      instancePath + '/properties/' + key4.replace(/~/g, '~0').replace(/\//g, '~1'),
                    parentData: data8,
                    parentDataProperty: key4,
                    rootData,
                    dynamicAnchors,
                  })
                ) {
                  vErrors =
                    vErrors === null
                      ? wrapper0.validate.errors
                      : vErrors.concat(wrapper0.validate.errors);
                  errors = vErrors.length;
                }
              }
            } else {
              const err22 = {
                instancePath: instancePath + '/properties',
                schemaPath: '#/oneOf/4/properties/properties/type',
                keyword: 'type',
                params: { type: 'object' },
                message: 'must be object',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
          }
          if (data.optional !== undefined) {
            let data10 = data.optional;
            if (Array.isArray(data10)) {
              const len0 = data10.length;
              for (let i0 = 0; i0 < len0; i0++) {
                if (typeof data10[i0] !== 'string') {
                  const err23 = {
                    instancePath: instancePath + '/optional/' + i0,
                    schemaPath: '#/oneOf/4/properties/optional/items/type',
                    keyword: 'type',
                    params: { type: 'string' },
                    message: 'must be string',
                  };
                  if (vErrors === null) {
                    vErrors = [err23];
                  } else {
                    vErrors.push(err23);
                  }
                  errors++;
                }
              }
              let i1 = data10.length;
              let j0;
              if (i1 > 1) {
                const indices0 = {};
                for (; i1--;) {
                  let item0 = data10[i1];
                  if (typeof item0 !== 'string') {
                    continue;
                  }
                  if (typeof indices0[item0] == 'number') {
                    j0 = indices0[item0];
                    const err24 = {
                      instancePath: instancePath + '/optional',
                      schemaPath: '#/oneOf/4/properties/optional/uniqueItems',
                      keyword: 'uniqueItems',
                      params: { i: i1, j: j0 },
                      message:
                        'must NOT have duplicate items (items ## ' +
                        j0 +
                        ' and ' +
                        i1 +
                        ' are identical)',
                    };
                    if (vErrors === null) {
                      vErrors = [err24];
                    } else {
                      vErrors.push(err24);
                    }
                    errors++;
                    break;
                  }
                  indices0[item0] = i1;
                }
              }
            } else {
              const err25 = {
                instancePath: instancePath + '/optional',
                schemaPath: '#/oneOf/4/properties/optional/type',
                keyword: 'type',
                params: { type: 'array' },
                message: 'must be array',
              };
              if (vErrors === null) {
                vErrors = [err25];
              } else {
                vErrors.push(err25);
              }
              errors++;
            }
          }
        } else {
          const err26 = {
            instancePath,
            schemaPath: '#/oneOf/4/type',
            keyword: 'type',
            params: { type: 'object' },
            message: 'must be object',
          };
          if (vErrors === null) {
            vErrors = [err26];
          } else {
            vErrors.push(err26);
          }
          errors++;
        }
        var _valid0 = _errs17 === errors;
        if (_valid0 && valid0) {
          valid0 = false;
          passing0 = [passing0, 4];
        } else {
          if (_valid0) {
            valid0 = true;
            passing0 = 4;
            if (props0 !== true) {
              props0 = true;
            }
          }
          const _errs34 = errors;
          if (data && typeof data == 'object' && !Array.isArray(data)) {
            if (data.ref === undefined) {
              const err27 = {
                instancePath,
                schemaPath: '#/oneOf/5/required',
                keyword: 'required',
                params: { missingProperty: 'ref' },
                message: "must have required property '" + 'ref' + "'",
              };
              if (vErrors === null) {
                vErrors = [err27];
              } else {
                vErrors.push(err27);
              }
              errors++;
            }
            for (const key5 in data) {
              if (!(key5 === 'ref')) {
                const err28 = {
                  instancePath,
                  schemaPath: '#/oneOf/5/additionalProperties',
                  keyword: 'additionalProperties',
                  params: { additionalProperty: key5 },
                  message: 'must NOT have additional properties',
                };
                if (vErrors === null) {
                  vErrors = [err28];
                } else {
                  vErrors.push(err28);
                }
                errors++;
              }
            }
            if (data.ref !== undefined) {
              let data12 = data.ref;
              if (typeof data12 === 'string') {
                if (func1(data12) < 1) {
                  const err29 = {
                    instancePath: instancePath + '/ref',
                    schemaPath: '#/oneOf/5/properties/ref/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err29];
                  } else {
                    vErrors.push(err29);
                  }
                  errors++;
                }
              } else {
                const err30 = {
                  instancePath: instancePath + '/ref',
                  schemaPath: '#/oneOf/5/properties/ref/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err30];
                } else {
                  vErrors.push(err30);
                }
                errors++;
              }
            }
          } else {
            const err31 = {
              instancePath,
              schemaPath: '#/oneOf/5/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err31];
            } else {
              vErrors.push(err31);
            }
            errors++;
          }
          var _valid0 = _errs34 === errors;
          if (_valid0 && valid0) {
            valid0 = false;
            passing0 = [passing0, 5];
          } else {
            if (_valid0) {
              valid0 = true;
              passing0 = 5;
              if (props0 !== true) {
                props0 = true;
              }
            }
          }
        }
      }
    }
  }
  if (!valid0) {
    const err32 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err32];
    } else {
      vErrors.push(err32);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate62.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate62.evaluated = { dynamicProps: true, dynamicItems: false };
function validate115(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate115.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (Object.keys(data).length < 0) {
      const err0 = {
        instancePath,
        schemaPath: '#/minProperties',
        keyword: 'minProperties',
        params: { limit: 0 },
        message: 'must NOT have fewer than 0 properties',
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    for (const key0 in data) {
      const _errs1 = errors;
      const _errs2 = errors;
      const _errs3 = errors;
      if (!(key0 === 'constructor' || key0 === 'prototype' || key0 === '__proto__')) {
        const err1 = {};
        if (vErrors === null) {
          vErrors = [err1];
        } else {
          vErrors.push(err1);
        }
        errors++;
      }
      var valid1 = _errs3 === errors;
      if (valid1) {
        const err2 = {
          instancePath,
          schemaPath: '#/propertyNames/not',
          keyword: 'not',
          params: {},
          message: 'must NOT be valid',
          propertyName: key0,
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      } else {
        errors = _errs2;
        if (vErrors !== null) {
          if (_errs2) {
            vErrors.length = _errs2;
          } else {
            vErrors = null;
          }
        }
      }
      if (typeof key0 === 'string') {
        if (func1(key0) > 100) {
          const err3 = {
            instancePath,
            schemaPath: '#/propertyNames/maxLength',
            keyword: 'maxLength',
            params: { limit: 100 },
            message: 'must NOT have more than 100 characters',
            propertyName: key0,
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
        if (!pattern8.test(key0)) {
          const err4 = {
            instancePath,
            schemaPath: '#/propertyNames/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_]*$' },
            message: 'must match pattern "' + '^[A-Za-z_][A-Za-z0-9_]*$' + '"',
            propertyName: key0,
          };
          if (vErrors === null) {
            vErrors = [err4];
          } else {
            vErrors.push(err4);
          }
          errors++;
        }
      }
      var valid0 = _errs1 === errors;
      if (!valid0) {
        const err5 = {
          instancePath,
          schemaPath: '#/propertyNames',
          keyword: 'propertyNames',
          params: { propertyName: key0 },
          message: 'property name must be valid',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    for (const key1 in data) {
      if (
        !validate62(data[key1], {
          instancePath: instancePath + '/' + key1.replace(/~/g, '~0').replace(/\//g, '~1'),
          parentData: data,
          parentDataProperty: key1,
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate62.errors : vErrors.concat(validate62.errors);
        errors = vErrors.length;
      }
    }
  } else {
    const err6 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err6];
    } else {
      vErrors.push(err6);
    }
    errors++;
  }
  validate115.errors = vErrors;
  return errors === 0;
}
validate115.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
function validate112(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate112.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Function' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Function' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.artifact === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'artifact' },
            message: "must have required property '" + 'artifact' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.runtime === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'runtime' },
            message: "must have required property '" + 'runtime' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.inputs === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'inputs' },
            message: "must have required property '" + 'inputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.outputs === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'outputs' },
            message: "must have required property '" + 'outputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.effect === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'effect' },
            message: "must have required property '" + 'effect' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        if (data3.limits === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'limits' },
            message: "must have required property '" + 'limits' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'artifact' ||
            key1 === 'runtime' ||
            key1 === 'inputs' ||
            key1 === 'outputs' ||
            key1 === 'effect' ||
            key1 === 'limits'
          )) {
            const err13 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
        if (data3.artifact !== undefined) {
          if (
            !validate77(data3.artifact, {
              instancePath: instancePath + '/spec/artifact',
              parentData: data3,
              parentDataProperty: 'artifact',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.runtime !== undefined) {
          if ('typescript-v1' !== data3.runtime) {
            const err14 = {
              instancePath: instancePath + '/spec/runtime',
              schemaPath: '#/properties/spec/properties/runtime/const',
              keyword: 'const',
              params: { allowedValue: 'typescript-v1' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.inputs !== undefined) {
          if (
            !validate115(data3.inputs, {
              instancePath: instancePath + '/spec/inputs',
              parentData: data3,
              parentDataProperty: 'inputs',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate115.errors : vErrors.concat(validate115.errors);
            errors = vErrors.length;
          }
        }
        if (data3.outputs !== undefined) {
          if (
            !validate115(data3.outputs, {
              instancePath: instancePath + '/spec/outputs',
              parentData: data3,
              parentDataProperty: 'outputs',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate115.errors : vErrors.concat(validate115.errors);
            errors = vErrors.length;
          }
        }
        if (data3.effect !== undefined) {
          let data8 = data3.effect;
          if (!(data8 === 'readOnly' || data8 === 'editProposal' || data8 === 'readWrite')) {
            const err15 = {
              instancePath: instancePath + '/spec/effect',
              schemaPath: '#/properties/spec/properties/effect/enum',
              keyword: 'enum',
              params: { allowedValues: schema41.properties.spec.properties.effect.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.limits !== undefined) {
          if (
            !validate79(data3.limits, {
              instancePath: instancePath + '/spec/limits',
              parentData: data3,
              parentDataProperty: 'limits',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate79.errors : vErrors.concat(validate79.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err16 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err16];
        } else {
          vErrors.push(err16);
        }
        errors++;
      }
    }
  } else {
    const err17 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err17];
    } else {
      vErrors.push(err17);
    }
    errors++;
  }
  validate112.errors = vErrors;
  return errors === 0;
}
validate112.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema43 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Application' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        mode: { enum: ['browser', 'service', 'delegated'] },
        scopes: { type: 'array', items: { $ref: '#/$defs/Scope' }, minItems: 0 },
        frontend: { $ref: '#/$defs/Artifact' },
      },
      required: ['mode', 'scopes'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const schema44 = {
  type: 'object',
  properties: {
    target: {
      type: 'string',
      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
    },
    operations: {
      type: 'array',
      items: { type: 'string', minLength: 1 },
      minItems: 1,
      uniqueItems: true,
    },
  },
  required: ['target', 'operations'],
  additionalProperties: false,
};
function validate123(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate123.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.target === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'target' },
        message: "must have required property '" + 'target' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.operations === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'operations' },
        message: "must have required property '" + 'operations' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'target' || key0 === 'operations')) {
        const err2 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err2];
        } else {
          vErrors.push(err2);
        }
        errors++;
      }
    }
    if (data.target !== undefined) {
      let data0 = data.target;
      if (typeof data0 === 'string') {
        if (!pattern5.test(data0)) {
          const err3 = {
            instancePath: instancePath + '/target',
            schemaPath: '#/properties/target/pattern',
            keyword: 'pattern',
            params: { pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' },
            message:
              'must match pattern "' +
              '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
              '"',
          };
          if (vErrors === null) {
            vErrors = [err3];
          } else {
            vErrors.push(err3);
          }
          errors++;
        }
      } else {
        const err4 = {
          instancePath: instancePath + '/target',
          schemaPath: '#/properties/target/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.operations !== undefined) {
      let data1 = data.operations;
      if (Array.isArray(data1)) {
        if (data1.length < 1) {
          const err5 = {
            instancePath: instancePath + '/operations',
            schemaPath: '#/properties/operations/minItems',
            keyword: 'minItems',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 items',
          };
          if (vErrors === null) {
            vErrors = [err5];
          } else {
            vErrors.push(err5);
          }
          errors++;
        }
        const len0 = data1.length;
        for (let i0 = 0; i0 < len0; i0++) {
          let data2 = data1[i0];
          if (typeof data2 === 'string') {
            if (func1(data2) < 1) {
              const err6 = {
                instancePath: instancePath + '/operations/' + i0,
                schemaPath: '#/properties/operations/items/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err6];
              } else {
                vErrors.push(err6);
              }
              errors++;
            }
          } else {
            const err7 = {
              instancePath: instancePath + '/operations/' + i0,
              schemaPath: '#/properties/operations/items/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err7];
            } else {
              vErrors.push(err7);
            }
            errors++;
          }
        }
        let i1 = data1.length;
        let j0;
        if (i1 > 1) {
          const indices0 = {};
          for (; i1--;) {
            let item0 = data1[i1];
            if (typeof item0 !== 'string') {
              continue;
            }
            if (typeof indices0[item0] == 'number') {
              j0 = indices0[item0];
              const err8 = {
                instancePath: instancePath + '/operations',
                schemaPath: '#/properties/operations/uniqueItems',
                keyword: 'uniqueItems',
                params: { i: i1, j: j0 },
                message:
                  'must NOT have duplicate items (items ## ' +
                  j0 +
                  ' and ' +
                  i1 +
                  ' are identical)',
              };
              if (vErrors === null) {
                vErrors = [err8];
              } else {
                vErrors.push(err8);
              }
              errors++;
              break;
            }
            indices0[item0] = i1;
          }
        }
      } else {
        const err9 = {
          instancePath: instancePath + '/operations',
          schemaPath: '#/properties/operations/type',
          keyword: 'type',
          params: { type: 'array' },
          message: 'must be array',
        };
        if (vErrors === null) {
          vErrors = [err9];
        } else {
          vErrors.push(err9);
        }
        errors++;
      }
    }
  } else {
    const err10 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err10];
    } else {
      vErrors.push(err10);
    }
    errors++;
  }
  validate123.errors = vErrors;
  return errors === 0;
}
validate123.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
function validate121(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate121.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Application' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Application' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.mode === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'mode' },
            message: "must have required property '" + 'mode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.scopes === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'scopes' },
            message: "must have required property '" + 'scopes' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'mode' || key1 === 'scopes' || key1 === 'frontend')) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.mode !== undefined) {
          let data4 = data3.mode;
          if (!(data4 === 'browser' || data4 === 'service' || data4 === 'delegated')) {
            const err10 = {
              instancePath: instancePath + '/spec/mode',
              schemaPath: '#/properties/spec/properties/mode/enum',
              keyword: 'enum',
              params: { allowedValues: schema43.properties.spec.properties.mode.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data3.scopes !== undefined) {
          let data5 = data3.scopes;
          if (Array.isArray(data5)) {
            if (data5.length < 0) {
              const err11 = {
                instancePath: instancePath + '/spec/scopes',
                schemaPath: '#/properties/spec/properties/scopes/minItems',
                keyword: 'minItems',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 items',
              };
              if (vErrors === null) {
                vErrors = [err11];
              } else {
                vErrors.push(err11);
              }
              errors++;
            }
            const len0 = data5.length;
            for (let i0 = 0; i0 < len0; i0++) {
              if (
                !validate123(data5[i0], {
                  instancePath: instancePath + '/spec/scopes/' + i0,
                  parentData: data5,
                  parentDataProperty: i0,
                  rootData,
                  dynamicAnchors,
                })
              ) {
                vErrors =
                  vErrors === null ? validate123.errors : vErrors.concat(validate123.errors);
                errors = vErrors.length;
              }
            }
          } else {
            const err12 = {
              instancePath: instancePath + '/spec/scopes',
              schemaPath: '#/properties/spec/properties/scopes/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.frontend !== undefined) {
          if (
            !validate77(data3.frontend, {
              instancePath: instancePath + '/spec/frontend',
              parentData: data3,
              parentDataProperty: 'frontend',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err13 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err13];
        } else {
          vErrors.push(err13);
        }
        errors++;
      }
    }
  } else {
    const err14 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err14];
    } else {
      vErrors.push(err14);
    }
    errors++;
  }
  validate121.errors = vErrors;
  return errors === 0;
}
validate121.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema45 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'AgentAdapter' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        artifact: { $ref: '#/$defs/Artifact' },
        abiVersion: { const: 'agent/v1' },
        capabilities: {
          type: 'array',
          items: { enum: ['events', 'cancel', 'checkpoint', 'resume'] },
          minItems: 1,
          uniqueItems: true,
        },
      },
      required: ['artifact', 'abiVersion', 'capabilities'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate127(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate127.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('AgentAdapter' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'AgentAdapter' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.artifact === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'artifact' },
            message: "must have required property '" + 'artifact' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.abiVersion === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'abiVersion' },
            message: "must have required property '" + 'abiVersion' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.capabilities === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'capabilities' },
            message: "must have required property '" + 'capabilities' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'artifact' || key1 === 'abiVersion' || key1 === 'capabilities')) {
            const err10 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data3.artifact !== undefined) {
          if (
            !validate77(data3.artifact, {
              instancePath: instancePath + '/spec/artifact',
              parentData: data3,
              parentDataProperty: 'artifact',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.abiVersion !== undefined) {
          if ('agent/v1' !== data3.abiVersion) {
            const err11 = {
              instancePath: instancePath + '/spec/abiVersion',
              schemaPath: '#/properties/spec/properties/abiVersion/const',
              keyword: 'const',
              params: { allowedValue: 'agent/v1' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.capabilities !== undefined) {
          let data6 = data3.capabilities;
          if (Array.isArray(data6)) {
            if (data6.length < 1) {
              const err12 = {
                instancePath: instancePath + '/spec/capabilities',
                schemaPath: '#/properties/spec/properties/capabilities/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
            const len0 = data6.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data7 = data6[i0];
              if (!(
                data7 === 'events' ||
                data7 === 'cancel' ||
                data7 === 'checkpoint' ||
                data7 === 'resume'
              )) {
                const err13 = {
                  instancePath: instancePath + '/spec/capabilities/' + i0,
                  schemaPath: '#/properties/spec/properties/capabilities/items/enum',
                  keyword: 'enum',
                  params: {
                    allowedValues: schema45.properties.spec.properties.capabilities.items.enum,
                  },
                  message: 'must be equal to one of the allowed values',
                };
                if (vErrors === null) {
                  vErrors = [err13];
                } else {
                  vErrors.push(err13);
                }
                errors++;
              }
            }
            let i1 = data6.length;
            let j0;
            if (i1 > 1) {
              outer0: for (; i1--;) {
                for (j0 = i1; j0--;) {
                  if (func0(data6[i1], data6[j0])) {
                    const err14 = {
                      instancePath: instancePath + '/spec/capabilities',
                      schemaPath: '#/properties/spec/properties/capabilities/uniqueItems',
                      keyword: 'uniqueItems',
                      params: { i: i1, j: j0 },
                      message:
                        'must NOT have duplicate items (items ## ' +
                        j0 +
                        ' and ' +
                        i1 +
                        ' are identical)',
                    };
                    if (vErrors === null) {
                      vErrors = [err14];
                    } else {
                      vErrors.push(err14);
                    }
                    errors++;
                    break outer0;
                  }
                }
              }
            }
          } else {
            const err15 = {
              instancePath: instancePath + '/spec/capabilities',
              schemaPath: '#/properties/spec/properties/capabilities/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
      } else {
        const err16 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err16];
        } else {
          vErrors.push(err16);
        }
        errors++;
      }
    }
  } else {
    const err17 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err17];
    } else {
      vErrors.push(err17);
    }
    errors++;
  }
  validate127.errors = vErrors;
  return errors === 0;
}
validate127.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema46 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Agent' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        artifact: { $ref: '#/$defs/Artifact' },
        adapter: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        application: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        model: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        inputs: { $ref: '#/$defs/Fields' },
        outputs: { $ref: '#/$defs/Fields' },
        limits: { $ref: '#/$defs/Limits' },
      },
      required: ['artifact', 'adapter', 'application', 'model', 'inputs', 'outputs', 'limits'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate131(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate131.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Agent' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Agent' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.artifact === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'artifact' },
            message: "must have required property '" + 'artifact' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.adapter === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'adapter' },
            message: "must have required property '" + 'adapter' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.application === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'application' },
            message: "must have required property '" + 'application' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.model === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'model' },
            message: "must have required property '" + 'model' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.inputs === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'inputs' },
            message: "must have required property '" + 'inputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        if (data3.outputs === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'outputs' },
            message: "must have required property '" + 'outputs' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        if (data3.limits === undefined) {
          const err13 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'limits' },
            message: "must have required property '" + 'limits' + "'",
          };
          if (vErrors === null) {
            vErrors = [err13];
          } else {
            vErrors.push(err13);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'artifact' ||
            key1 === 'adapter' ||
            key1 === 'application' ||
            key1 === 'model' ||
            key1 === 'inputs' ||
            key1 === 'outputs' ||
            key1 === 'limits'
          )) {
            const err14 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.artifact !== undefined) {
          if (
            !validate77(data3.artifact, {
              instancePath: instancePath + '/spec/artifact',
              parentData: data3,
              parentDataProperty: 'artifact',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.adapter !== undefined) {
          let data5 = data3.adapter;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err15 = {
                instancePath: instancePath + '/spec/adapter',
                schemaPath: '#/properties/spec/properties/adapter/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err15];
              } else {
                vErrors.push(err15);
              }
              errors++;
            }
          } else {
            const err16 = {
              instancePath: instancePath + '/spec/adapter',
              schemaPath: '#/properties/spec/properties/adapter/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.application !== undefined) {
          let data6 = data3.application;
          if (typeof data6 === 'string') {
            if (!pattern5.test(data6)) {
              const err17 = {
                instancePath: instancePath + '/spec/application',
                schemaPath: '#/properties/spec/properties/application/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err17];
              } else {
                vErrors.push(err17);
              }
              errors++;
            }
          } else {
            const err18 = {
              instancePath: instancePath + '/spec/application',
              schemaPath: '#/properties/spec/properties/application/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err18];
            } else {
              vErrors.push(err18);
            }
            errors++;
          }
        }
        if (data3.model !== undefined) {
          let data7 = data3.model;
          if (typeof data7 === 'string') {
            if (!pattern5.test(data7)) {
              const err19 = {
                instancePath: instancePath + '/spec/model',
                schemaPath: '#/properties/spec/properties/model/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err19];
              } else {
                vErrors.push(err19);
              }
              errors++;
            }
          } else {
            const err20 = {
              instancePath: instancePath + '/spec/model',
              schemaPath: '#/properties/spec/properties/model/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err20];
            } else {
              vErrors.push(err20);
            }
            errors++;
          }
        }
        if (data3.inputs !== undefined) {
          if (
            !validate68(data3.inputs, {
              instancePath: instancePath + '/spec/inputs',
              parentData: data3,
              parentDataProperty: 'inputs',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate68.errors : vErrors.concat(validate68.errors);
            errors = vErrors.length;
          }
        }
        if (data3.outputs !== undefined) {
          if (
            !validate68(data3.outputs, {
              instancePath: instancePath + '/spec/outputs',
              parentData: data3,
              parentDataProperty: 'outputs',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate68.errors : vErrors.concat(validate68.errors);
            errors = vErrors.length;
          }
        }
        if (data3.limits !== undefined) {
          if (
            !validate79(data3.limits, {
              instancePath: instancePath + '/spec/limits',
              parentData: data3,
              parentDataProperty: 'limits',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate79.errors : vErrors.concat(validate79.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err21 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err21];
        } else {
          vErrors.push(err21);
        }
        errors++;
      }
    }
  } else {
    const err22 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err22];
    } else {
      vErrors.push(err22);
    }
    errors++;
  }
  validate131.errors = vErrors;
  return errors === 0;
}
validate131.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema47 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'ModelProvider' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        adapter: { const: 'openai-compatible-v1' },
        endpoint: { type: 'string', format: 'uri', pattern: '^https?://[^\\s/]+' },
        destination: { type: 'string', minLength: 1 },
        secret: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        destinationPolicy: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
      },
      required: ['adapter', 'endpoint', 'destination', 'destinationPolicy'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
const formats4 = runtime2.fullFormats.uri;
const pattern38 = new RegExp('^https?://[^\\s/]+', 'u');
function validate138(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate138.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('ModelProvider' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'ModelProvider' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.adapter === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'adapter' },
            message: "must have required property '" + 'adapter' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.endpoint === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'endpoint' },
            message: "must have required property '" + 'endpoint' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.destination === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'destination' },
            message: "must have required property '" + 'destination' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.destinationPolicy === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'destinationPolicy' },
            message: "must have required property '" + 'destinationPolicy' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'adapter' ||
            key1 === 'endpoint' ||
            key1 === 'destination' ||
            key1 === 'secret' ||
            key1 === 'destinationPolicy'
          )) {
            const err11 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.adapter !== undefined) {
          if ('openai-compatible-v1' !== data3.adapter) {
            const err12 = {
              instancePath: instancePath + '/spec/adapter',
              schemaPath: '#/properties/spec/properties/adapter/const',
              keyword: 'const',
              params: { allowedValue: 'openai-compatible-v1' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.endpoint !== undefined) {
          let data5 = data3.endpoint;
          if (typeof data5 === 'string') {
            if (!pattern38.test(data5)) {
              const err13 = {
                instancePath: instancePath + '/spec/endpoint',
                schemaPath: '#/properties/spec/properties/endpoint/pattern',
                keyword: 'pattern',
                params: { pattern: '^https?://[^\\s/]+' },
                message: 'must match pattern "' + '^https?://[^\\s/]+' + '"',
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            if (!formats4(data5)) {
              const err14 = {
                instancePath: instancePath + '/spec/endpoint',
                schemaPath: '#/properties/spec/properties/endpoint/format',
                keyword: 'format',
                params: { format: 'uri' },
                message: 'must match format "' + 'uri' + '"',
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
          } else {
            const err15 = {
              instancePath: instancePath + '/spec/endpoint',
              schemaPath: '#/properties/spec/properties/endpoint/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.destination !== undefined) {
          let data6 = data3.destination;
          if (typeof data6 === 'string') {
            if (func1(data6) < 1) {
              const err16 = {
                instancePath: instancePath + '/spec/destination',
                schemaPath: '#/properties/spec/properties/destination/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err16];
              } else {
                vErrors.push(err16);
              }
              errors++;
            }
          } else {
            const err17 = {
              instancePath: instancePath + '/spec/destination',
              schemaPath: '#/properties/spec/properties/destination/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.secret !== undefined) {
          let data7 = data3.secret;
          if (typeof data7 === 'string') {
            if (!pattern5.test(data7)) {
              const err18 = {
                instancePath: instancePath + '/spec/secret',
                schemaPath: '#/properties/spec/properties/secret/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
          } else {
            const err19 = {
              instancePath: instancePath + '/spec/secret',
              schemaPath: '#/properties/spec/properties/secret/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data3.destinationPolicy !== undefined) {
          let data8 = data3.destinationPolicy;
          if (typeof data8 === 'string') {
            if (!pattern5.test(data8)) {
              const err20 = {
                instancePath: instancePath + '/spec/destinationPolicy',
                schemaPath: '#/properties/spec/properties/destinationPolicy/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err20];
              } else {
                vErrors.push(err20);
              }
              errors++;
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/spec/destinationPolicy',
              schemaPath: '#/properties/spec/properties/destinationPolicy/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
      } else {
        const err22 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err22];
        } else {
          vErrors.push(err22);
        }
        errors++;
      }
    }
  } else {
    const err23 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err23];
    } else {
      vErrors.push(err23);
    }
    errors++;
  }
  validate138.errors = vErrors;
  return errors === 0;
}
validate138.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema48 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Model' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        provider: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        upstreamName: { type: 'string', minLength: 1 },
        versionLabel: { type: 'string', minLength: 1 },
        mutable: { type: 'boolean' },
        capabilities: {
          type: 'array',
          items: { enum: ['chat', 'tools', 'structuredOutput', 'streaming', 'embeddings'] },
          minItems: 1,
          uniqueItems: true,
        },
        embeddingDimensions: { type: 'integer', minimum: 1 },
      },
      required: ['provider', 'upstreamName', 'mutable', 'capabilities'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate141(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate141.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Model' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Model' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.provider === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'provider' },
            message: "must have required property '" + 'provider' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.upstreamName === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'upstreamName' },
            message: "must have required property '" + 'upstreamName' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.mutable === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'mutable' },
            message: "must have required property '" + 'mutable' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.capabilities === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'capabilities' },
            message: "must have required property '" + 'capabilities' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'provider' ||
            key1 === 'upstreamName' ||
            key1 === 'versionLabel' ||
            key1 === 'mutable' ||
            key1 === 'capabilities' ||
            key1 === 'embeddingDimensions'
          )) {
            const err11 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.provider !== undefined) {
          let data4 = data3.provider;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err12 = {
                instancePath: instancePath + '/spec/provider',
                schemaPath: '#/properties/spec/properties/provider/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/provider',
              schemaPath: '#/properties/spec/properties/provider/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
        if (data3.upstreamName !== undefined) {
          let data5 = data3.upstreamName;
          if (typeof data5 === 'string') {
            if (func1(data5) < 1) {
              const err14 = {
                instancePath: instancePath + '/spec/upstreamName',
                schemaPath: '#/properties/spec/properties/upstreamName/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
          } else {
            const err15 = {
              instancePath: instancePath + '/spec/upstreamName',
              schemaPath: '#/properties/spec/properties/upstreamName/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.versionLabel !== undefined) {
          let data6 = data3.versionLabel;
          if (typeof data6 === 'string') {
            if (func1(data6) < 1) {
              const err16 = {
                instancePath: instancePath + '/spec/versionLabel',
                schemaPath: '#/properties/spec/properties/versionLabel/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err16];
              } else {
                vErrors.push(err16);
              }
              errors++;
            }
          } else {
            const err17 = {
              instancePath: instancePath + '/spec/versionLabel',
              schemaPath: '#/properties/spec/properties/versionLabel/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.mutable !== undefined) {
          if (typeof data3.mutable !== 'boolean') {
            const err18 = {
              instancePath: instancePath + '/spec/mutable',
              schemaPath: '#/properties/spec/properties/mutable/type',
              keyword: 'type',
              params: { type: 'boolean' },
              message: 'must be boolean',
            };
            if (vErrors === null) {
              vErrors = [err18];
            } else {
              vErrors.push(err18);
            }
            errors++;
          }
        }
        if (data3.capabilities !== undefined) {
          let data8 = data3.capabilities;
          if (Array.isArray(data8)) {
            if (data8.length < 1) {
              const err19 = {
                instancePath: instancePath + '/spec/capabilities',
                schemaPath: '#/properties/spec/properties/capabilities/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err19];
              } else {
                vErrors.push(err19);
              }
              errors++;
            }
            const len0 = data8.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data9 = data8[i0];
              if (!(
                data9 === 'chat' ||
                data9 === 'tools' ||
                data9 === 'structuredOutput' ||
                data9 === 'streaming' ||
                data9 === 'embeddings'
              )) {
                const err20 = {
                  instancePath: instancePath + '/spec/capabilities/' + i0,
                  schemaPath: '#/properties/spec/properties/capabilities/items/enum',
                  keyword: 'enum',
                  params: {
                    allowedValues: schema48.properties.spec.properties.capabilities.items.enum,
                  },
                  message: 'must be equal to one of the allowed values',
                };
                if (vErrors === null) {
                  vErrors = [err20];
                } else {
                  vErrors.push(err20);
                }
                errors++;
              }
            }
            let i1 = data8.length;
            let j0;
            if (i1 > 1) {
              outer0: for (; i1--;) {
                for (j0 = i1; j0--;) {
                  if (func0(data8[i1], data8[j0])) {
                    const err21 = {
                      instancePath: instancePath + '/spec/capabilities',
                      schemaPath: '#/properties/spec/properties/capabilities/uniqueItems',
                      keyword: 'uniqueItems',
                      params: { i: i1, j: j0 },
                      message:
                        'must NOT have duplicate items (items ## ' +
                        j0 +
                        ' and ' +
                        i1 +
                        ' are identical)',
                    };
                    if (vErrors === null) {
                      vErrors = [err21];
                    } else {
                      vErrors.push(err21);
                    }
                    errors++;
                    break outer0;
                  }
                }
              }
            }
          } else {
            const err22 = {
              instancePath: instancePath + '/spec/capabilities',
              schemaPath: '#/properties/spec/properties/capabilities/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err22];
            } else {
              vErrors.push(err22);
            }
            errors++;
          }
        }
        if (data3.embeddingDimensions !== undefined) {
          let data10 = data3.embeddingDimensions;
          if (!(typeof data10 == 'number' && !(data10 % 1) && !isNaN(data10))) {
            const err23 = {
              instancePath: instancePath + '/spec/embeddingDimensions',
              schemaPath: '#/properties/spec/properties/embeddingDimensions/type',
              keyword: 'type',
              params: { type: 'integer' },
              message: 'must be integer',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
          if (typeof data10 == 'number') {
            if (data10 < 1 || isNaN(data10)) {
              const err24 = {
                instancePath: instancePath + '/spec/embeddingDimensions',
                schemaPath: '#/properties/spec/properties/embeddingDimensions/minimum',
                keyword: 'minimum',
                params: { comparison: '>=', limit: 1 },
                message: 'must be >= 1',
              };
              if (vErrors === null) {
                vErrors = [err24];
              } else {
                vErrors.push(err24);
              }
              errors++;
            }
          }
        }
      } else {
        const err25 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err25];
        } else {
          vErrors.push(err25);
        }
        errors++;
      }
    }
  } else {
    const err26 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err26];
    } else {
      vErrors.push(err26);
    }
    errors++;
  }
  validate141.errors = vErrors;
  return errors === 0;
}
validate141.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema49 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'RetrievalCollection' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        source: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        textProperty: { type: 'string', minLength: 1 },
        embeddingModel: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        chunkSize: { type: 'integer', minimum: 1 },
        chunkOverlap: { type: 'integer', minimum: 0 },
        metric: { const: 'cosine' },
        searchMode: { const: 'exact' },
      },
      required: [
        'source',
        'textProperty',
        'embeddingModel',
        'chunkSize',
        'chunkOverlap',
        'metric',
        'searchMode',
      ],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate144(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate144.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('RetrievalCollection' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'RetrievalCollection' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.source === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'source' },
            message: "must have required property '" + 'source' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.textProperty === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'textProperty' },
            message: "must have required property '" + 'textProperty' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.embeddingModel === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'embeddingModel' },
            message: "must have required property '" + 'embeddingModel' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.chunkSize === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'chunkSize' },
            message: "must have required property '" + 'chunkSize' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.chunkOverlap === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'chunkOverlap' },
            message: "must have required property '" + 'chunkOverlap' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        if (data3.metric === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'metric' },
            message: "must have required property '" + 'metric' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        if (data3.searchMode === undefined) {
          const err13 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'searchMode' },
            message: "must have required property '" + 'searchMode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err13];
          } else {
            vErrors.push(err13);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'source' ||
            key1 === 'textProperty' ||
            key1 === 'embeddingModel' ||
            key1 === 'chunkSize' ||
            key1 === 'chunkOverlap' ||
            key1 === 'metric' ||
            key1 === 'searchMode'
          )) {
            const err14 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.source !== undefined) {
          let data4 = data3.source;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err15 = {
                instancePath: instancePath + '/spec/source',
                schemaPath: '#/properties/spec/properties/source/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err15];
              } else {
                vErrors.push(err15);
              }
              errors++;
            }
          } else {
            const err16 = {
              instancePath: instancePath + '/spec/source',
              schemaPath: '#/properties/spec/properties/source/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.textProperty !== undefined) {
          let data5 = data3.textProperty;
          if (typeof data5 === 'string') {
            if (func1(data5) < 1) {
              const err17 = {
                instancePath: instancePath + '/spec/textProperty',
                schemaPath: '#/properties/spec/properties/textProperty/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err17];
              } else {
                vErrors.push(err17);
              }
              errors++;
            }
          } else {
            const err18 = {
              instancePath: instancePath + '/spec/textProperty',
              schemaPath: '#/properties/spec/properties/textProperty/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err18];
            } else {
              vErrors.push(err18);
            }
            errors++;
          }
        }
        if (data3.embeddingModel !== undefined) {
          let data6 = data3.embeddingModel;
          if (typeof data6 === 'string') {
            if (!pattern5.test(data6)) {
              const err19 = {
                instancePath: instancePath + '/spec/embeddingModel',
                schemaPath: '#/properties/spec/properties/embeddingModel/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err19];
              } else {
                vErrors.push(err19);
              }
              errors++;
            }
          } else {
            const err20 = {
              instancePath: instancePath + '/spec/embeddingModel',
              schemaPath: '#/properties/spec/properties/embeddingModel/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err20];
            } else {
              vErrors.push(err20);
            }
            errors++;
          }
        }
        if (data3.chunkSize !== undefined) {
          let data7 = data3.chunkSize;
          if (!(typeof data7 == 'number' && !(data7 % 1) && !isNaN(data7))) {
            const err21 = {
              instancePath: instancePath + '/spec/chunkSize',
              schemaPath: '#/properties/spec/properties/chunkSize/type',
              keyword: 'type',
              params: { type: 'integer' },
              message: 'must be integer',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
          if (typeof data7 == 'number') {
            if (data7 < 1 || isNaN(data7)) {
              const err22 = {
                instancePath: instancePath + '/spec/chunkSize',
                schemaPath: '#/properties/spec/properties/chunkSize/minimum',
                keyword: 'minimum',
                params: { comparison: '>=', limit: 1 },
                message: 'must be >= 1',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
          }
        }
        if (data3.chunkOverlap !== undefined) {
          let data8 = data3.chunkOverlap;
          if (!(typeof data8 == 'number' && !(data8 % 1) && !isNaN(data8))) {
            const err23 = {
              instancePath: instancePath + '/spec/chunkOverlap',
              schemaPath: '#/properties/spec/properties/chunkOverlap/type',
              keyword: 'type',
              params: { type: 'integer' },
              message: 'must be integer',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
          if (typeof data8 == 'number') {
            if (data8 < 0 || isNaN(data8)) {
              const err24 = {
                instancePath: instancePath + '/spec/chunkOverlap',
                schemaPath: '#/properties/spec/properties/chunkOverlap/minimum',
                keyword: 'minimum',
                params: { comparison: '>=', limit: 0 },
                message: 'must be >= 0',
              };
              if (vErrors === null) {
                vErrors = [err24];
              } else {
                vErrors.push(err24);
              }
              errors++;
            }
          }
        }
        if (data3.metric !== undefined) {
          if ('cosine' !== data3.metric) {
            const err25 = {
              instancePath: instancePath + '/spec/metric',
              schemaPath: '#/properties/spec/properties/metric/const',
              keyword: 'const',
              params: { allowedValue: 'cosine' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err25];
            } else {
              vErrors.push(err25);
            }
            errors++;
          }
        }
        if (data3.searchMode !== undefined) {
          if ('exact' !== data3.searchMode) {
            const err26 = {
              instancePath: instancePath + '/spec/searchMode',
              schemaPath: '#/properties/spec/properties/searchMode/const',
              keyword: 'const',
              params: { allowedValue: 'exact' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
      } else {
        const err27 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err27];
        } else {
          vErrors.push(err27);
        }
        errors++;
      }
    }
  } else {
    const err28 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err28];
    } else {
      vErrors.push(err28);
    }
    errors++;
  }
  validate144.errors = vErrors;
  return errors === 0;
}
validate144.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema50 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'EvaluationSuite' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        agent: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        fixtures: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        evaluator: { $ref: '#/$defs/Artifact' },
        repeats: { type: 'integer', minimum: 1 },
      },
      required: ['agent', 'fixtures', 'evaluator', 'repeats'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate147(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate147.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('EvaluationSuite' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'EvaluationSuite' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.agent === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'agent' },
            message: "must have required property '" + 'agent' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.fixtures === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'fixtures' },
            message: "must have required property '" + 'fixtures' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.evaluator === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'evaluator' },
            message: "must have required property '" + 'evaluator' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.repeats === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'repeats' },
            message: "must have required property '" + 'repeats' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'agent' ||
            key1 === 'fixtures' ||
            key1 === 'evaluator' ||
            key1 === 'repeats'
          )) {
            const err11 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.agent !== undefined) {
          let data4 = data3.agent;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err12 = {
                instancePath: instancePath + '/spec/agent',
                schemaPath: '#/properties/spec/properties/agent/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/agent',
              schemaPath: '#/properties/spec/properties/agent/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
        if (data3.fixtures !== undefined) {
          let data5 = data3.fixtures;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err14 = {
                instancePath: instancePath + '/spec/fixtures',
                schemaPath: '#/properties/spec/properties/fixtures/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
          } else {
            const err15 = {
              instancePath: instancePath + '/spec/fixtures',
              schemaPath: '#/properties/spec/properties/fixtures/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.evaluator !== undefined) {
          if (
            !validate77(data3.evaluator, {
              instancePath: instancePath + '/spec/evaluator',
              parentData: data3,
              parentDataProperty: 'evaluator',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate77.errors : vErrors.concat(validate77.errors);
            errors = vErrors.length;
          }
        }
        if (data3.repeats !== undefined) {
          let data7 = data3.repeats;
          if (!(typeof data7 == 'number' && !(data7 % 1) && !isNaN(data7))) {
            const err16 = {
              instancePath: instancePath + '/spec/repeats',
              schemaPath: '#/properties/spec/properties/repeats/type',
              keyword: 'type',
              params: { type: 'integer' },
              message: 'must be integer',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
          if (typeof data7 == 'number') {
            if (data7 < 1 || isNaN(data7)) {
              const err17 = {
                instancePath: instancePath + '/spec/repeats',
                schemaPath: '#/properties/spec/properties/repeats/minimum',
                keyword: 'minimum',
                params: { comparison: '>=', limit: 1 },
                message: 'must be >= 1',
              };
              if (vErrors === null) {
                vErrors = [err17];
              } else {
                vErrors.push(err17);
              }
              errors++;
            }
          }
        }
      } else {
        const err18 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err18];
        } else {
          vErrors.push(err18);
        }
        errors++;
      }
    }
  } else {
    const err19 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err19];
    } else {
      vErrors.push(err19);
    }
    errors++;
  }
  validate147.errors = vErrors;
  return errors === 0;
}
validate147.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema51 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Automation' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        operation: { enum: ['build', 'sync', 'invokeAgent', 'evaluate'] },
        target: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        application: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        intervalSeconds: { type: 'integer', minimum: 1 },
        input: { type: 'object' },
        overlap: { const: 'skip' },
        missed: { const: 'coalesce' },
      },
      required: [
        'operation',
        'target',
        'application',
        'intervalSeconds',
        'input',
        'overlap',
        'missed',
      ],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate151(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate151.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Automation' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Automation' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.operation === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'operation' },
            message: "must have required property '" + 'operation' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.target === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'target' },
            message: "must have required property '" + 'target' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.application === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'application' },
            message: "must have required property '" + 'application' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.intervalSeconds === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'intervalSeconds' },
            message: "must have required property '" + 'intervalSeconds' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.input === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'input' },
            message: "must have required property '" + 'input' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        if (data3.overlap === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'overlap' },
            message: "must have required property '" + 'overlap' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        if (data3.missed === undefined) {
          const err13 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'missed' },
            message: "must have required property '" + 'missed' + "'",
          };
          if (vErrors === null) {
            vErrors = [err13];
          } else {
            vErrors.push(err13);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'operation' ||
            key1 === 'target' ||
            key1 === 'application' ||
            key1 === 'intervalSeconds' ||
            key1 === 'input' ||
            key1 === 'overlap' ||
            key1 === 'missed'
          )) {
            const err14 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err14];
            } else {
              vErrors.push(err14);
            }
            errors++;
          }
        }
        if (data3.operation !== undefined) {
          let data4 = data3.operation;
          if (!(
            data4 === 'build' ||
            data4 === 'sync' ||
            data4 === 'invokeAgent' ||
            data4 === 'evaluate'
          )) {
            const err15 = {
              instancePath: instancePath + '/spec/operation',
              schemaPath: '#/properties/spec/properties/operation/enum',
              keyword: 'enum',
              params: { allowedValues: schema51.properties.spec.properties.operation.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.target !== undefined) {
          let data5 = data3.target;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err16 = {
                instancePath: instancePath + '/spec/target',
                schemaPath: '#/properties/spec/properties/target/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err16];
              } else {
                vErrors.push(err16);
              }
              errors++;
            }
          } else {
            const err17 = {
              instancePath: instancePath + '/spec/target',
              schemaPath: '#/properties/spec/properties/target/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.application !== undefined) {
          let data6 = data3.application;
          if (typeof data6 === 'string') {
            if (!pattern5.test(data6)) {
              const err18 = {
                instancePath: instancePath + '/spec/application',
                schemaPath: '#/properties/spec/properties/application/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
          } else {
            const err19 = {
              instancePath: instancePath + '/spec/application',
              schemaPath: '#/properties/spec/properties/application/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data3.intervalSeconds !== undefined) {
          let data7 = data3.intervalSeconds;
          if (!(typeof data7 == 'number' && !(data7 % 1) && !isNaN(data7))) {
            const err20 = {
              instancePath: instancePath + '/spec/intervalSeconds',
              schemaPath: '#/properties/spec/properties/intervalSeconds/type',
              keyword: 'type',
              params: { type: 'integer' },
              message: 'must be integer',
            };
            if (vErrors === null) {
              vErrors = [err20];
            } else {
              vErrors.push(err20);
            }
            errors++;
          }
          if (typeof data7 == 'number') {
            if (data7 < 1 || isNaN(data7)) {
              const err21 = {
                instancePath: instancePath + '/spec/intervalSeconds',
                schemaPath: '#/properties/spec/properties/intervalSeconds/minimum',
                keyword: 'minimum',
                params: { comparison: '>=', limit: 1 },
                message: 'must be >= 1',
              };
              if (vErrors === null) {
                vErrors = [err21];
              } else {
                vErrors.push(err21);
              }
              errors++;
            }
          }
        }
        if (data3.input !== undefined) {
          let data8 = data3.input;
          if (!(data8 && typeof data8 == 'object' && !Array.isArray(data8))) {
            const err22 = {
              instancePath: instancePath + '/spec/input',
              schemaPath: '#/properties/spec/properties/input/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err22];
            } else {
              vErrors.push(err22);
            }
            errors++;
          }
        }
        if (data3.overlap !== undefined) {
          if ('skip' !== data3.overlap) {
            const err23 = {
              instancePath: instancePath + '/spec/overlap',
              schemaPath: '#/properties/spec/properties/overlap/const',
              keyword: 'const',
              params: { allowedValue: 'skip' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
        }
        if (data3.missed !== undefined) {
          if ('coalesce' !== data3.missed) {
            const err24 = {
              instancePath: instancePath + '/spec/missed',
              schemaPath: '#/properties/spec/properties/missed/const',
              keyword: 'const',
              params: { allowedValue: 'coalesce' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err24];
            } else {
              vErrors.push(err24);
            }
            errors++;
          }
        }
      } else {
        const err25 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err25];
        } else {
          vErrors.push(err25);
        }
        errors++;
      }
    }
  } else {
    const err26 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err26];
    } else {
      vErrors.push(err26);
    }
    errors++;
  }
  validate151.errors = vErrors;
  return errors === 0;
}
validate151.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema52 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Policy' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      oneOf: [
        {
          type: 'object',
          properties: {
            type: { enum: ['row', 'objectRead', 'objectWrite', 'propertyRead', 'propertyWrite'] },
            expression: { $ref: '#/$defs/Expr' },
          },
          required: ['type', 'expression'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            type: { const: 'modelDestination' },
            allowedDestinations: {
              type: 'array',
              items: { type: 'string', minLength: 1 },
              minItems: 1,
              uniqueItems: true,
            },
            allowedMarkings: {
              type: 'array',
              items: {
                type: 'string',
                pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
              },
              minItems: 0,
              uniqueItems: true,
            },
          },
          required: ['type', 'allowedDestinations', 'allowedMarkings'],
          additionalProperties: false,
        },
      ],
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate154(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate154.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Policy' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Policy' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      const _errs6 = errors;
      let valid1 = false;
      let passing0 = null;
      const _errs7 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.type === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'type' },
            message: "must have required property '" + 'type' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.expression === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'expression' },
            message: "must have required property '" + 'expression' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'type' || key1 === 'expression')) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/oneOf/0/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.type !== undefined) {
          let data4 = data3.type;
          if (!(
            data4 === 'row' ||
            data4 === 'objectRead' ||
            data4 === 'objectWrite' ||
            data4 === 'propertyRead' ||
            data4 === 'propertyWrite'
          )) {
            const err10 = {
              instancePath: instancePath + '/spec/type',
              schemaPath: '#/properties/spec/oneOf/0/properties/type/enum',
              keyword: 'enum',
              params: { allowedValues: schema52.properties.spec.oneOf[0].properties.type.enum },
              message: 'must be equal to one of the allowed values',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data3.expression !== undefined) {
          if (
            !validate99(data3.expression, {
              instancePath: instancePath + '/spec/expression',
              parentData: data3,
              parentDataProperty: 'expression',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err11 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/0/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      }
      var _valid0 = _errs7 === errors;
      if (_valid0) {
        valid1 = true;
        passing0 = 0;
        var props1 = true;
      }
      const _errs12 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.type === undefined) {
          const err12 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'type' },
            message: "must have required property '" + 'type' + "'",
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        if (data3.allowedDestinations === undefined) {
          const err13 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'allowedDestinations' },
            message: "must have required property '" + 'allowedDestinations' + "'",
          };
          if (vErrors === null) {
            vErrors = [err13];
          } else {
            vErrors.push(err13);
          }
          errors++;
        }
        if (data3.allowedMarkings === undefined) {
          const err14 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'allowedMarkings' },
            message: "must have required property '" + 'allowedMarkings' + "'",
          };
          if (vErrors === null) {
            vErrors = [err14];
          } else {
            vErrors.push(err14);
          }
          errors++;
        }
        for (const key2 in data3) {
          if (!(key2 === 'type' || key2 === 'allowedDestinations' || key2 === 'allowedMarkings')) {
            const err15 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/oneOf/1/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key2 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err15];
            } else {
              vErrors.push(err15);
            }
            errors++;
          }
        }
        if (data3.type !== undefined) {
          if ('modelDestination' !== data3.type) {
            const err16 = {
              instancePath: instancePath + '/spec/type',
              schemaPath: '#/properties/spec/oneOf/1/properties/type/const',
              keyword: 'const',
              params: { allowedValue: 'modelDestination' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
        if (data3.allowedDestinations !== undefined) {
          let data7 = data3.allowedDestinations;
          if (Array.isArray(data7)) {
            if (data7.length < 1) {
              const err17 = {
                instancePath: instancePath + '/spec/allowedDestinations',
                schemaPath: '#/properties/spec/oneOf/1/properties/allowedDestinations/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err17];
              } else {
                vErrors.push(err17);
              }
              errors++;
            }
            const len0 = data7.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data8 = data7[i0];
              if (typeof data8 === 'string') {
                if (func1(data8) < 1) {
                  const err18 = {
                    instancePath: instancePath + '/spec/allowedDestinations/' + i0,
                    schemaPath:
                      '#/properties/spec/oneOf/1/properties/allowedDestinations/items/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err18];
                  } else {
                    vErrors.push(err18);
                  }
                  errors++;
                }
              } else {
                const err19 = {
                  instancePath: instancePath + '/spec/allowedDestinations/' + i0,
                  schemaPath: '#/properties/spec/oneOf/1/properties/allowedDestinations/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err19];
                } else {
                  vErrors.push(err19);
                }
                errors++;
              }
            }
            let i1 = data7.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data7[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err20 = {
                    instancePath: instancePath + '/spec/allowedDestinations',
                    schemaPath:
                      '#/properties/spec/oneOf/1/properties/allowedDestinations/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err20];
                  } else {
                    vErrors.push(err20);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/spec/allowedDestinations',
              schemaPath: '#/properties/spec/oneOf/1/properties/allowedDestinations/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
        if (data3.allowedMarkings !== undefined) {
          let data9 = data3.allowedMarkings;
          if (Array.isArray(data9)) {
            if (data9.length < 0) {
              const err22 = {
                instancePath: instancePath + '/spec/allowedMarkings',
                schemaPath: '#/properties/spec/oneOf/1/properties/allowedMarkings/minItems',
                keyword: 'minItems',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 items',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
            const len1 = data9.length;
            for (let i2 = 0; i2 < len1; i2++) {
              let data10 = data9[i2];
              if (typeof data10 === 'string') {
                if (!pattern5.test(data10)) {
                  const err23 = {
                    instancePath: instancePath + '/spec/allowedMarkings/' + i2,
                    schemaPath:
                      '#/properties/spec/oneOf/1/properties/allowedMarkings/items/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err23];
                  } else {
                    vErrors.push(err23);
                  }
                  errors++;
                }
              } else {
                const err24 = {
                  instancePath: instancePath + '/spec/allowedMarkings/' + i2,
                  schemaPath: '#/properties/spec/oneOf/1/properties/allowedMarkings/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err24];
                } else {
                  vErrors.push(err24);
                }
                errors++;
              }
            }
            let i3 = data9.length;
            let j1;
            if (i3 > 1) {
              const indices1 = {};
              for (; i3--;) {
                let item1 = data9[i3];
                if (typeof item1 !== 'string') {
                  continue;
                }
                if (typeof indices1[item1] == 'number') {
                  j1 = indices1[item1];
                  const err25 = {
                    instancePath: instancePath + '/spec/allowedMarkings',
                    schemaPath: '#/properties/spec/oneOf/1/properties/allowedMarkings/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i3, j: j1 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j1 +
                      ' and ' +
                      i3 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err25];
                  } else {
                    vErrors.push(err25);
                  }
                  errors++;
                  break;
                }
                indices1[item1] = i3;
              }
            }
          } else {
            const err26 = {
              instancePath: instancePath + '/spec/allowedMarkings',
              schemaPath: '#/properties/spec/oneOf/1/properties/allowedMarkings/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
      } else {
        const err27 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/1/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err27];
        } else {
          vErrors.push(err27);
        }
        errors++;
      }
      var _valid0 = _errs12 === errors;
      if (_valid0 && valid1) {
        valid1 = false;
        passing0 = [passing0, 1];
      } else {
        if (_valid0) {
          valid1 = true;
          passing0 = 1;
          if (props1 !== true) {
            props1 = true;
          }
        }
      }
      if (!valid1) {
        const err28 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf',
          keyword: 'oneOf',
          params: { passingSchemas: passing0 },
          message: 'must match exactly one schema in oneOf',
        };
        if (vErrors === null) {
          vErrors = [err28];
        } else {
          vErrors.push(err28);
        }
        errors++;
      } else {
        errors = _errs6;
        if (vErrors !== null) {
          if (_errs6) {
            vErrors.length = _errs6;
          } else {
            vErrors = null;
          }
        }
      }
    }
  } else {
    const err29 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err29];
    } else {
      vErrors.push(err29);
    }
    errors++;
  }
  validate154.errors = vErrors;
  return errors === 0;
}
validate154.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema53 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'SecretReference' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        binding: { type: 'string', minLength: 1 },
        consumers: {
          type: 'array',
          items: {
            type: 'string',
            pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
          },
          minItems: 1,
          uniqueItems: true,
        },
      },
      required: ['binding', 'consumers'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate158(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate158.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('SecretReference' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'SecretReference' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.binding === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'binding' },
            message: "must have required property '" + 'binding' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.consumers === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'consumers' },
            message: "must have required property '" + 'consumers' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'binding' || key1 === 'consumers')) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.binding !== undefined) {
          let data4 = data3.binding;
          if (typeof data4 === 'string') {
            if (func1(data4) < 1) {
              const err10 = {
                instancePath: instancePath + '/spec/binding',
                schemaPath: '#/properties/spec/properties/binding/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err10];
              } else {
                vErrors.push(err10);
              }
              errors++;
            }
          } else {
            const err11 = {
              instancePath: instancePath + '/spec/binding',
              schemaPath: '#/properties/spec/properties/binding/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.consumers !== undefined) {
          let data5 = data3.consumers;
          if (Array.isArray(data5)) {
            if (data5.length < 1) {
              const err12 = {
                instancePath: instancePath + '/spec/consumers',
                schemaPath: '#/properties/spec/properties/consumers/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
            const len0 = data5.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data6 = data5[i0];
              if (typeof data6 === 'string') {
                if (!pattern5.test(data6)) {
                  const err13 = {
                    instancePath: instancePath + '/spec/consumers/' + i0,
                    schemaPath: '#/properties/spec/properties/consumers/items/pattern',
                    keyword: 'pattern',
                    params: {
                      pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                    },
                    message:
                      'must match pattern "' +
                      '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                      '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err13];
                  } else {
                    vErrors.push(err13);
                  }
                  errors++;
                }
              } else {
                const err14 = {
                  instancePath: instancePath + '/spec/consumers/' + i0,
                  schemaPath: '#/properties/spec/properties/consumers/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err14];
                } else {
                  vErrors.push(err14);
                }
                errors++;
              }
            }
            let i1 = data5.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data5[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err15 = {
                    instancePath: instancePath + '/spec/consumers',
                    schemaPath: '#/properties/spec/properties/consumers/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err15];
                  } else {
                    vErrors.push(err15);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err16 = {
              instancePath: instancePath + '/spec/consumers',
              schemaPath: '#/properties/spec/properties/consumers/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err16];
            } else {
              vErrors.push(err16);
            }
            errors++;
          }
        }
      } else {
        const err17 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err17];
        } else {
          vErrors.push(err17);
        }
        errors++;
      }
    }
  } else {
    const err18 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err18];
    } else {
      vErrors.push(err18);
    }
    errors++;
  }
  validate158.errors = vErrors;
  return errors === 0;
}
validate158.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema54 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Marking' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        eligibilityGroup: { type: 'string', minLength: 1 },
        declassifierGroup: { type: 'string', minLength: 1 },
      },
      required: ['eligibilityGroup', 'declassifierGroup'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate161(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate161.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Marking' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Marking' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.eligibilityGroup === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'eligibilityGroup' },
            message: "must have required property '" + 'eligibilityGroup' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.declassifierGroup === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'declassifierGroup' },
            message: "must have required property '" + 'declassifierGroup' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'eligibilityGroup' || key1 === 'declassifierGroup')) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.eligibilityGroup !== undefined) {
          let data4 = data3.eligibilityGroup;
          if (typeof data4 === 'string') {
            if (func1(data4) < 1) {
              const err10 = {
                instancePath: instancePath + '/spec/eligibilityGroup',
                schemaPath: '#/properties/spec/properties/eligibilityGroup/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err10];
              } else {
                vErrors.push(err10);
              }
              errors++;
            }
          } else {
            const err11 = {
              instancePath: instancePath + '/spec/eligibilityGroup',
              schemaPath: '#/properties/spec/properties/eligibilityGroup/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.declassifierGroup !== undefined) {
          let data5 = data3.declassifierGroup;
          if (typeof data5 === 'string') {
            if (func1(data5) < 1) {
              const err12 = {
                instancePath: instancePath + '/spec/declassifierGroup',
                schemaPath: '#/properties/spec/properties/declassifierGroup/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/declassifierGroup',
              schemaPath: '#/properties/spec/properties/declassifierGroup/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
      } else {
        const err14 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err14];
        } else {
          vErrors.push(err14);
        }
        errors++;
      }
    }
  } else {
    const err15 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err15];
    } else {
      vErrors.push(err15);
    }
    errors++;
  }
  validate161.errors = vErrors;
  return errors === 0;
}
validate161.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema55 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'RestrictedView' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        dataset: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        policy: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
      },
      required: ['dataset', 'policy'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate164(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate164.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('RestrictedView' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'RestrictedView' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.dataset === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'dataset' },
            message: "must have required property '" + 'dataset' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.policy === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'policy' },
            message: "must have required property '" + 'policy' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'dataset' || key1 === 'policy')) {
            const err9 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err9];
            } else {
              vErrors.push(err9);
            }
            errors++;
          }
        }
        if (data3.dataset !== undefined) {
          let data4 = data3.dataset;
          if (typeof data4 === 'string') {
            if (!pattern5.test(data4)) {
              const err10 = {
                instancePath: instancePath + '/spec/dataset',
                schemaPath: '#/properties/spec/properties/dataset/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err10];
              } else {
                vErrors.push(err10);
              }
              errors++;
            }
          } else {
            const err11 = {
              instancePath: instancePath + '/spec/dataset',
              schemaPath: '#/properties/spec/properties/dataset/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.policy !== undefined) {
          let data5 = data3.policy;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err12 = {
                instancePath: instancePath + '/spec/policy',
                schemaPath: '#/properties/spec/properties/policy/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/policy',
              schemaPath: '#/properties/spec/properties/policy/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
      } else {
        const err14 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err14];
        } else {
          vErrors.push(err14);
        }
        errors++;
      }
    }
  } else {
    const err15 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err15];
    } else {
      vErrors.push(err15);
    }
    errors++;
  }
  validate164.errors = vErrors;
  return errors === 0;
}
validate164.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema56 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'Grant' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      type: 'object',
      properties: {
        subjects: {
          type: 'array',
          items: { type: 'string', minLength: 1 },
          minItems: 1,
          uniqueItems: true,
        },
        application: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        target: {
          type: 'string',
          pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
        },
        operations: {
          type: 'array',
          items: { type: 'string', minLength: 1 },
          minItems: 1,
          uniqueItems: true,
        },
        environment: { type: 'string', minLength: 1 },
      },
      required: ['subjects', 'application', 'target', 'operations', 'environment'],
      additionalProperties: false,
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate167(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate167.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('Grant' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'Grant' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.subjects === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'subjects' },
            message: "must have required property '" + 'subjects' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.application === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'application' },
            message: "must have required property '" + 'application' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.target === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'target' },
            message: "must have required property '" + 'target' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        if (data3.operations === undefined) {
          const err10 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'operations' },
            message: "must have required property '" + 'operations' + "'",
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        if (data3.environment === undefined) {
          const err11 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/required',
            keyword: 'required',
            params: { missingProperty: 'environment' },
            message: "must have required property '" + 'environment' + "'",
          };
          if (vErrors === null) {
            vErrors = [err11];
          } else {
            vErrors.push(err11);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(
            key1 === 'subjects' ||
            key1 === 'application' ||
            key1 === 'target' ||
            key1 === 'operations' ||
            key1 === 'environment'
          )) {
            const err12 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err12];
            } else {
              vErrors.push(err12);
            }
            errors++;
          }
        }
        if (data3.subjects !== undefined) {
          let data4 = data3.subjects;
          if (Array.isArray(data4)) {
            if (data4.length < 1) {
              const err13 = {
                instancePath: instancePath + '/spec/subjects',
                schemaPath: '#/properties/spec/properties/subjects/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            const len0 = data4.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data5 = data4[i0];
              if (typeof data5 === 'string') {
                if (func1(data5) < 1) {
                  const err14 = {
                    instancePath: instancePath + '/spec/subjects/' + i0,
                    schemaPath: '#/properties/spec/properties/subjects/items/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err14];
                  } else {
                    vErrors.push(err14);
                  }
                  errors++;
                }
              } else {
                const err15 = {
                  instancePath: instancePath + '/spec/subjects/' + i0,
                  schemaPath: '#/properties/spec/properties/subjects/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err15];
                } else {
                  vErrors.push(err15);
                }
                errors++;
              }
            }
            let i1 = data4.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data4[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err16 = {
                    instancePath: instancePath + '/spec/subjects',
                    schemaPath: '#/properties/spec/properties/subjects/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err16];
                  } else {
                    vErrors.push(err16);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err17 = {
              instancePath: instancePath + '/spec/subjects',
              schemaPath: '#/properties/spec/properties/subjects/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err17];
            } else {
              vErrors.push(err17);
            }
            errors++;
          }
        }
        if (data3.application !== undefined) {
          let data6 = data3.application;
          if (typeof data6 === 'string') {
            if (!pattern5.test(data6)) {
              const err18 = {
                instancePath: instancePath + '/spec/application',
                schemaPath: '#/properties/spec/properties/application/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err18];
              } else {
                vErrors.push(err18);
              }
              errors++;
            }
          } else {
            const err19 = {
              instancePath: instancePath + '/spec/application',
              schemaPath: '#/properties/spec/properties/application/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err19];
            } else {
              vErrors.push(err19);
            }
            errors++;
          }
        }
        if (data3.target !== undefined) {
          let data7 = data3.target;
          if (typeof data7 === 'string') {
            if (!pattern5.test(data7)) {
              const err20 = {
                instancePath: instancePath + '/spec/target',
                schemaPath: '#/properties/spec/properties/target/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err20];
              } else {
                vErrors.push(err20);
              }
              errors++;
            }
          } else {
            const err21 = {
              instancePath: instancePath + '/spec/target',
              schemaPath: '#/properties/spec/properties/target/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err21];
            } else {
              vErrors.push(err21);
            }
            errors++;
          }
        }
        if (data3.operations !== undefined) {
          let data8 = data3.operations;
          if (Array.isArray(data8)) {
            if (data8.length < 1) {
              const err22 = {
                instancePath: instancePath + '/spec/operations',
                schemaPath: '#/properties/spec/properties/operations/minItems',
                keyword: 'minItems',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 items',
              };
              if (vErrors === null) {
                vErrors = [err22];
              } else {
                vErrors.push(err22);
              }
              errors++;
            }
            const len1 = data8.length;
            for (let i2 = 0; i2 < len1; i2++) {
              let data9 = data8[i2];
              if (typeof data9 === 'string') {
                if (func1(data9) < 1) {
                  const err23 = {
                    instancePath: instancePath + '/spec/operations/' + i2,
                    schemaPath: '#/properties/spec/properties/operations/items/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err23];
                  } else {
                    vErrors.push(err23);
                  }
                  errors++;
                }
              } else {
                const err24 = {
                  instancePath: instancePath + '/spec/operations/' + i2,
                  schemaPath: '#/properties/spec/properties/operations/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err24];
                } else {
                  vErrors.push(err24);
                }
                errors++;
              }
            }
            let i3 = data8.length;
            let j1;
            if (i3 > 1) {
              const indices1 = {};
              for (; i3--;) {
                let item1 = data8[i3];
                if (typeof item1 !== 'string') {
                  continue;
                }
                if (typeof indices1[item1] == 'number') {
                  j1 = indices1[item1];
                  const err25 = {
                    instancePath: instancePath + '/spec/operations',
                    schemaPath: '#/properties/spec/properties/operations/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i3, j: j1 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j1 +
                      ' and ' +
                      i3 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err25];
                  } else {
                    vErrors.push(err25);
                  }
                  errors++;
                  break;
                }
                indices1[item1] = i3;
              }
            }
          } else {
            const err26 = {
              instancePath: instancePath + '/spec/operations',
              schemaPath: '#/properties/spec/properties/operations/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
        if (data3.environment !== undefined) {
          let data10 = data3.environment;
          if (typeof data10 === 'string') {
            if (func1(data10) < 1) {
              const err27 = {
                instancePath: instancePath + '/spec/environment',
                schemaPath: '#/properties/spec/properties/environment/minLength',
                keyword: 'minLength',
                params: { limit: 1 },
                message: 'must NOT have fewer than 1 characters',
              };
              if (vErrors === null) {
                vErrors = [err27];
              } else {
                vErrors.push(err27);
              }
              errors++;
            }
          } else {
            const err28 = {
              instancePath: instancePath + '/spec/environment',
              schemaPath: '#/properties/spec/properties/environment/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err28];
            } else {
              vErrors.push(err28);
            }
            errors++;
          }
        }
      } else {
        const err29 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err29];
        } else {
          vErrors.push(err29);
        }
        errors++;
      }
    }
  } else {
    const err30 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err30];
    } else {
      vErrors.push(err30);
    }
    errors++;
  }
  validate167.errors = vErrors;
  return errors === 0;
}
validate167.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
const schema57 = {
  type: 'object',
  properties: {
    apiVersion: { const: 'moria/v1alpha2' },
    kind: { const: 'ObjectSet' },
    metadata: { $ref: '#/$defs/Metadata' },
    spec: {
      oneOf: [
        {
          type: 'object',
          properties: {
            mode: { const: 'static' },
            objectType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            uids: {
              type: 'array',
              items: { type: 'string', format: 'uuid' },
              minItems: 0,
              uniqueItems: true,
            },
          },
          required: ['mode', 'objectType', 'uids'],
          additionalProperties: false,
        },
        {
          type: 'object',
          properties: {
            mode: { const: 'dynamic' },
            objectType: {
              type: 'string',
              pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
            },
            where: { $ref: '#/$defs/Expr' },
          },
          required: ['mode', 'objectType', 'where'],
          additionalProperties: false,
        },
      ],
    },
  },
  required: ['apiVersion', 'kind', 'metadata', 'spec'],
  additionalProperties: false,
};
function validate170(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate170.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.kind === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'kind' },
        message: "must have required property '" + 'kind' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.metadata === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'metadata' },
        message: "must have required property '" + 'metadata' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.spec === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'spec' },
        message: "must have required property '" + 'spec' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'apiVersion' || key0 === 'kind' || key0 === 'metadata' || key0 === 'spec')) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.kind !== undefined) {
      if ('ObjectSet' !== data.kind) {
        const err6 = {
          instancePath: instancePath + '/kind',
          schemaPath: '#/properties/kind/const',
          keyword: 'const',
          params: { allowedValue: 'ObjectSet' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err6];
        } else {
          vErrors.push(err6);
        }
        errors++;
      }
    }
    if (data.metadata !== undefined) {
      if (
        !validate56(data.metadata, {
          instancePath: instancePath + '/metadata',
          parentData: data,
          parentDataProperty: 'metadata',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate56.errors : vErrors.concat(validate56.errors);
        errors = vErrors.length;
      }
    }
    if (data.spec !== undefined) {
      let data3 = data.spec;
      const _errs6 = errors;
      let valid1 = false;
      let passing0 = null;
      const _errs7 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.mode === undefined) {
          const err7 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'mode' },
            message: "must have required property '" + 'mode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err7];
          } else {
            vErrors.push(err7);
          }
          errors++;
        }
        if (data3.objectType === undefined) {
          const err8 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'objectType' },
            message: "must have required property '" + 'objectType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
        if (data3.uids === undefined) {
          const err9 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/0/required',
            keyword: 'required',
            params: { missingProperty: 'uids' },
            message: "must have required property '" + 'uids' + "'",
          };
          if (vErrors === null) {
            vErrors = [err9];
          } else {
            vErrors.push(err9);
          }
          errors++;
        }
        for (const key1 in data3) {
          if (!(key1 === 'mode' || key1 === 'objectType' || key1 === 'uids')) {
            const err10 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/oneOf/0/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key1 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err10];
            } else {
              vErrors.push(err10);
            }
            errors++;
          }
        }
        if (data3.mode !== undefined) {
          if ('static' !== data3.mode) {
            const err11 = {
              instancePath: instancePath + '/spec/mode',
              schemaPath: '#/properties/spec/oneOf/0/properties/mode/const',
              keyword: 'const',
              params: { allowedValue: 'static' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err11];
            } else {
              vErrors.push(err11);
            }
            errors++;
          }
        }
        if (data3.objectType !== undefined) {
          let data5 = data3.objectType;
          if (typeof data5 === 'string') {
            if (!pattern5.test(data5)) {
              const err12 = {
                instancePath: instancePath + '/spec/objectType',
                schemaPath: '#/properties/spec/oneOf/0/properties/objectType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err12];
              } else {
                vErrors.push(err12);
              }
              errors++;
            }
          } else {
            const err13 = {
              instancePath: instancePath + '/spec/objectType',
              schemaPath: '#/properties/spec/oneOf/0/properties/objectType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err13];
            } else {
              vErrors.push(err13);
            }
            errors++;
          }
        }
        if (data3.uids !== undefined) {
          let data6 = data3.uids;
          if (Array.isArray(data6)) {
            if (data6.length < 0) {
              const err14 = {
                instancePath: instancePath + '/spec/uids',
                schemaPath: '#/properties/spec/oneOf/0/properties/uids/minItems',
                keyword: 'minItems',
                params: { limit: 0 },
                message: 'must NOT have fewer than 0 items',
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
            const len0 = data6.length;
            for (let i0 = 0; i0 < len0; i0++) {
              let data7 = data6[i0];
              if (typeof data7 === 'string') {
                if (!formats0.test(data7)) {
                  const err15 = {
                    instancePath: instancePath + '/spec/uids/' + i0,
                    schemaPath: '#/properties/spec/oneOf/0/properties/uids/items/format',
                    keyword: 'format',
                    params: { format: 'uuid' },
                    message: 'must match format "' + 'uuid' + '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err15];
                  } else {
                    vErrors.push(err15);
                  }
                  errors++;
                }
              } else {
                const err16 = {
                  instancePath: instancePath + '/spec/uids/' + i0,
                  schemaPath: '#/properties/spec/oneOf/0/properties/uids/items/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err16];
                } else {
                  vErrors.push(err16);
                }
                errors++;
              }
            }
            let i1 = data6.length;
            let j0;
            if (i1 > 1) {
              const indices0 = {};
              for (; i1--;) {
                let item0 = data6[i1];
                if (typeof item0 !== 'string') {
                  continue;
                }
                if (typeof indices0[item0] == 'number') {
                  j0 = indices0[item0];
                  const err17 = {
                    instancePath: instancePath + '/spec/uids',
                    schemaPath: '#/properties/spec/oneOf/0/properties/uids/uniqueItems',
                    keyword: 'uniqueItems',
                    params: { i: i1, j: j0 },
                    message:
                      'must NOT have duplicate items (items ## ' +
                      j0 +
                      ' and ' +
                      i1 +
                      ' are identical)',
                  };
                  if (vErrors === null) {
                    vErrors = [err17];
                  } else {
                    vErrors.push(err17);
                  }
                  errors++;
                  break;
                }
                indices0[item0] = i1;
              }
            }
          } else {
            const err18 = {
              instancePath: instancePath + '/spec/uids',
              schemaPath: '#/properties/spec/oneOf/0/properties/uids/type',
              keyword: 'type',
              params: { type: 'array' },
              message: 'must be array',
            };
            if (vErrors === null) {
              vErrors = [err18];
            } else {
              vErrors.push(err18);
            }
            errors++;
          }
        }
      } else {
        const err19 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/0/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err19];
        } else {
          vErrors.push(err19);
        }
        errors++;
      }
      var _valid0 = _errs7 === errors;
      if (_valid0) {
        valid1 = true;
        passing0 = 0;
        var props0 = true;
      }
      const _errs17 = errors;
      if (data3 && typeof data3 == 'object' && !Array.isArray(data3)) {
        if (data3.mode === undefined) {
          const err20 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'mode' },
            message: "must have required property '" + 'mode' + "'",
          };
          if (vErrors === null) {
            vErrors = [err20];
          } else {
            vErrors.push(err20);
          }
          errors++;
        }
        if (data3.objectType === undefined) {
          const err21 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'objectType' },
            message: "must have required property '" + 'objectType' + "'",
          };
          if (vErrors === null) {
            vErrors = [err21];
          } else {
            vErrors.push(err21);
          }
          errors++;
        }
        if (data3.where === undefined) {
          const err22 = {
            instancePath: instancePath + '/spec',
            schemaPath: '#/properties/spec/oneOf/1/required',
            keyword: 'required',
            params: { missingProperty: 'where' },
            message: "must have required property '" + 'where' + "'",
          };
          if (vErrors === null) {
            vErrors = [err22];
          } else {
            vErrors.push(err22);
          }
          errors++;
        }
        for (const key2 in data3) {
          if (!(key2 === 'mode' || key2 === 'objectType' || key2 === 'where')) {
            const err23 = {
              instancePath: instancePath + '/spec',
              schemaPath: '#/properties/spec/oneOf/1/additionalProperties',
              keyword: 'additionalProperties',
              params: { additionalProperty: key2 },
              message: 'must NOT have additional properties',
            };
            if (vErrors === null) {
              vErrors = [err23];
            } else {
              vErrors.push(err23);
            }
            errors++;
          }
        }
        if (data3.mode !== undefined) {
          if ('dynamic' !== data3.mode) {
            const err24 = {
              instancePath: instancePath + '/spec/mode',
              schemaPath: '#/properties/spec/oneOf/1/properties/mode/const',
              keyword: 'const',
              params: { allowedValue: 'dynamic' },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err24];
            } else {
              vErrors.push(err24);
            }
            errors++;
          }
        }
        if (data3.objectType !== undefined) {
          let data9 = data3.objectType;
          if (typeof data9 === 'string') {
            if (!pattern5.test(data9)) {
              const err25 = {
                instancePath: instancePath + '/spec/objectType',
                schemaPath: '#/properties/spec/oneOf/1/properties/objectType/pattern',
                keyword: 'pattern',
                params: {
                  pattern: '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$',
                },
                message:
                  'must match pattern "' +
                  '^[A-Za-z_][A-Za-z0-9_-]*/[A-Z][A-Za-z]*/[A-Za-z_][A-Za-z0-9_-]*$' +
                  '"',
              };
              if (vErrors === null) {
                vErrors = [err25];
              } else {
                vErrors.push(err25);
              }
              errors++;
            }
          } else {
            const err26 = {
              instancePath: instancePath + '/spec/objectType',
              schemaPath: '#/properties/spec/oneOf/1/properties/objectType/type',
              keyword: 'type',
              params: { type: 'string' },
              message: 'must be string',
            };
            if (vErrors === null) {
              vErrors = [err26];
            } else {
              vErrors.push(err26);
            }
            errors++;
          }
        }
        if (data3.where !== undefined) {
          if (
            !validate99(data3.where, {
              instancePath: instancePath + '/spec/where',
              parentData: data3,
              parentDataProperty: 'where',
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate99.errors : vErrors.concat(validate99.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err27 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf/1/type',
          keyword: 'type',
          params: { type: 'object' },
          message: 'must be object',
        };
        if (vErrors === null) {
          vErrors = [err27];
        } else {
          vErrors.push(err27);
        }
        errors++;
      }
      var _valid0 = _errs17 === errors;
      if (_valid0 && valid1) {
        valid1 = false;
        passing0 = [passing0, 1];
      } else {
        if (_valid0) {
          valid1 = true;
          passing0 = 1;
          if (props0 !== true) {
            props0 = true;
          }
        }
      }
      if (!valid1) {
        const err28 = {
          instancePath: instancePath + '/spec',
          schemaPath: '#/properties/spec/oneOf',
          keyword: 'oneOf',
          params: { passingSchemas: passing0 },
          message: 'must match exactly one schema in oneOf',
        };
        if (vErrors === null) {
          vErrors = [err28];
        } else {
          vErrors.push(err28);
        }
        errors++;
      } else {
        errors = _errs6;
        if (vErrors !== null) {
          if (_errs6) {
            vErrors.length = _errs6;
          } else {
            vErrors = null;
          }
        }
      }
    }
  } else {
    const err29 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err29];
    } else {
      vErrors.push(err29);
    }
    errors++;
  }
  validate170.errors = vErrors;
  return errors === 0;
}
validate170.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
function validate54(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate54.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (
    !validate55(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate55.errors : vErrors.concat(validate55.errors);
    errors = vErrors.length;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
    var props0 = true;
  }
  const _errs2 = errors;
  if (
    !validate59(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate59.errors : vErrors.concat(validate59.errors);
    errors = vErrors.length;
  }
  var _valid0 = _errs2 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true) {
        props0 = true;
      }
    }
    const _errs3 = errors;
    if (
      !validate75(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
    ) {
      vErrors = vErrors === null ? validate75.errors : vErrors.concat(validate75.errors);
      errors = vErrors.length;
    }
    var _valid0 = _errs3 === errors;
    if (_valid0 && valid0) {
      valid0 = false;
      passing0 = [passing0, 2];
    } else {
      if (_valid0) {
        valid0 = true;
        passing0 = 2;
        if (props0 !== true) {
          props0 = true;
        }
      }
      const _errs4 = errors;
      if (
        !validate82(data, {
          instancePath,
          parentData,
          parentDataProperty,
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate82.errors : vErrors.concat(validate82.errors);
        errors = vErrors.length;
      }
      var _valid0 = _errs4 === errors;
      if (_valid0 && valid0) {
        valid0 = false;
        passing0 = [passing0, 3];
      } else {
        if (_valid0) {
          valid0 = true;
          passing0 = 3;
          if (props0 !== true) {
            props0 = true;
          }
        }
        const _errs5 = errors;
        if (
          !validate86(data, {
            instancePath,
            parentData,
            parentDataProperty,
            rootData,
            dynamicAnchors,
          })
        ) {
          vErrors = vErrors === null ? validate86.errors : vErrors.concat(validate86.errors);
          errors = vErrors.length;
        }
        var _valid0 = _errs5 === errors;
        if (_valid0 && valid0) {
          valid0 = false;
          passing0 = [passing0, 4];
        } else {
          if (_valid0) {
            valid0 = true;
            passing0 = 4;
            if (props0 !== true) {
              props0 = true;
            }
          }
          const _errs6 = errors;
          if (
            !validate89(data, {
              instancePath,
              parentData,
              parentDataProperty,
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate89.errors : vErrors.concat(validate89.errors);
            errors = vErrors.length;
          }
          var _valid0 = _errs6 === errors;
          if (_valid0 && valid0) {
            valid0 = false;
            passing0 = [passing0, 5];
          } else {
            if (_valid0) {
              valid0 = true;
              passing0 = 5;
              if (props0 !== true) {
                props0 = true;
              }
            }
            const _errs7 = errors;
            if (
              !validate93(data, {
                instancePath,
                parentData,
                parentDataProperty,
                rootData,
                dynamicAnchors,
              })
            ) {
              vErrors = vErrors === null ? validate93.errors : vErrors.concat(validate93.errors);
              errors = vErrors.length;
            }
            var _valid0 = _errs7 === errors;
            if (_valid0 && valid0) {
              valid0 = false;
              passing0 = [passing0, 6];
            } else {
              if (_valid0) {
                valid0 = true;
                passing0 = 6;
                if (props0 !== true) {
                  props0 = true;
                }
              }
              const _errs8 = errors;
              if (
                !validate96(data, {
                  instancePath,
                  parentData,
                  parentDataProperty,
                  rootData,
                  dynamicAnchors,
                })
              ) {
                vErrors = vErrors === null ? validate96.errors : vErrors.concat(validate96.errors);
                errors = vErrors.length;
              }
              var _valid0 = _errs8 === errors;
              if (_valid0 && valid0) {
                valid0 = false;
                passing0 = [passing0, 7];
              } else {
                if (_valid0) {
                  valid0 = true;
                  passing0 = 7;
                  if (props0 !== true) {
                    props0 = true;
                  }
                }
                const _errs9 = errors;
                if (
                  !validate112(data, {
                    instancePath,
                    parentData,
                    parentDataProperty,
                    rootData,
                    dynamicAnchors,
                  })
                ) {
                  vErrors =
                    vErrors === null ? validate112.errors : vErrors.concat(validate112.errors);
                  errors = vErrors.length;
                }
                var _valid0 = _errs9 === errors;
                if (_valid0 && valid0) {
                  valid0 = false;
                  passing0 = [passing0, 8];
                } else {
                  if (_valid0) {
                    valid0 = true;
                    passing0 = 8;
                    if (props0 !== true) {
                      props0 = true;
                    }
                  }
                  const _errs10 = errors;
                  if (
                    !validate121(data, {
                      instancePath,
                      parentData,
                      parentDataProperty,
                      rootData,
                      dynamicAnchors,
                    })
                  ) {
                    vErrors =
                      vErrors === null ? validate121.errors : vErrors.concat(validate121.errors);
                    errors = vErrors.length;
                  }
                  var _valid0 = _errs10 === errors;
                  if (_valid0 && valid0) {
                    valid0 = false;
                    passing0 = [passing0, 9];
                  } else {
                    if (_valid0) {
                      valid0 = true;
                      passing0 = 9;
                      if (props0 !== true) {
                        props0 = true;
                      }
                    }
                    const _errs11 = errors;
                    if (
                      !validate127(data, {
                        instancePath,
                        parentData,
                        parentDataProperty,
                        rootData,
                        dynamicAnchors,
                      })
                    ) {
                      vErrors =
                        vErrors === null ? validate127.errors : vErrors.concat(validate127.errors);
                      errors = vErrors.length;
                    }
                    var _valid0 = _errs11 === errors;
                    if (_valid0 && valid0) {
                      valid0 = false;
                      passing0 = [passing0, 10];
                    } else {
                      if (_valid0) {
                        valid0 = true;
                        passing0 = 10;
                        if (props0 !== true) {
                          props0 = true;
                        }
                      }
                      const _errs12 = errors;
                      if (
                        !validate131(data, {
                          instancePath,
                          parentData,
                          parentDataProperty,
                          rootData,
                          dynamicAnchors,
                        })
                      ) {
                        vErrors =
                          vErrors === null
                            ? validate131.errors
                            : vErrors.concat(validate131.errors);
                        errors = vErrors.length;
                      }
                      var _valid0 = _errs12 === errors;
                      if (_valid0 && valid0) {
                        valid0 = false;
                        passing0 = [passing0, 11];
                      } else {
                        if (_valid0) {
                          valid0 = true;
                          passing0 = 11;
                          if (props0 !== true) {
                            props0 = true;
                          }
                        }
                        const _errs13 = errors;
                        if (
                          !validate138(data, {
                            instancePath,
                            parentData,
                            parentDataProperty,
                            rootData,
                            dynamicAnchors,
                          })
                        ) {
                          vErrors =
                            vErrors === null
                              ? validate138.errors
                              : vErrors.concat(validate138.errors);
                          errors = vErrors.length;
                        }
                        var _valid0 = _errs13 === errors;
                        if (_valid0 && valid0) {
                          valid0 = false;
                          passing0 = [passing0, 12];
                        } else {
                          if (_valid0) {
                            valid0 = true;
                            passing0 = 12;
                            if (props0 !== true) {
                              props0 = true;
                            }
                          }
                          const _errs14 = errors;
                          if (
                            !validate141(data, {
                              instancePath,
                              parentData,
                              parentDataProperty,
                              rootData,
                              dynamicAnchors,
                            })
                          ) {
                            vErrors =
                              vErrors === null
                                ? validate141.errors
                                : vErrors.concat(validate141.errors);
                            errors = vErrors.length;
                          }
                          var _valid0 = _errs14 === errors;
                          if (_valid0 && valid0) {
                            valid0 = false;
                            passing0 = [passing0, 13];
                          } else {
                            if (_valid0) {
                              valid0 = true;
                              passing0 = 13;
                              if (props0 !== true) {
                                props0 = true;
                              }
                            }
                            const _errs15 = errors;
                            if (
                              !validate144(data, {
                                instancePath,
                                parentData,
                                parentDataProperty,
                                rootData,
                                dynamicAnchors,
                              })
                            ) {
                              vErrors =
                                vErrors === null
                                  ? validate144.errors
                                  : vErrors.concat(validate144.errors);
                              errors = vErrors.length;
                            }
                            var _valid0 = _errs15 === errors;
                            if (_valid0 && valid0) {
                              valid0 = false;
                              passing0 = [passing0, 14];
                            } else {
                              if (_valid0) {
                                valid0 = true;
                                passing0 = 14;
                                if (props0 !== true) {
                                  props0 = true;
                                }
                              }
                              const _errs16 = errors;
                              if (
                                !validate147(data, {
                                  instancePath,
                                  parentData,
                                  parentDataProperty,
                                  rootData,
                                  dynamicAnchors,
                                })
                              ) {
                                vErrors =
                                  vErrors === null
                                    ? validate147.errors
                                    : vErrors.concat(validate147.errors);
                                errors = vErrors.length;
                              }
                              var _valid0 = _errs16 === errors;
                              if (_valid0 && valid0) {
                                valid0 = false;
                                passing0 = [passing0, 15];
                              } else {
                                if (_valid0) {
                                  valid0 = true;
                                  passing0 = 15;
                                  if (props0 !== true) {
                                    props0 = true;
                                  }
                                }
                                const _errs17 = errors;
                                if (
                                  !validate151(data, {
                                    instancePath,
                                    parentData,
                                    parentDataProperty,
                                    rootData,
                                    dynamicAnchors,
                                  })
                                ) {
                                  vErrors =
                                    vErrors === null
                                      ? validate151.errors
                                      : vErrors.concat(validate151.errors);
                                  errors = vErrors.length;
                                }
                                var _valid0 = _errs17 === errors;
                                if (_valid0 && valid0) {
                                  valid0 = false;
                                  passing0 = [passing0, 16];
                                } else {
                                  if (_valid0) {
                                    valid0 = true;
                                    passing0 = 16;
                                    if (props0 !== true) {
                                      props0 = true;
                                    }
                                  }
                                  const _errs18 = errors;
                                  if (
                                    !validate154(data, {
                                      instancePath,
                                      parentData,
                                      parentDataProperty,
                                      rootData,
                                      dynamicAnchors,
                                    })
                                  ) {
                                    vErrors =
                                      vErrors === null
                                        ? validate154.errors
                                        : vErrors.concat(validate154.errors);
                                    errors = vErrors.length;
                                  }
                                  var _valid0 = _errs18 === errors;
                                  if (_valid0 && valid0) {
                                    valid0 = false;
                                    passing0 = [passing0, 17];
                                  } else {
                                    if (_valid0) {
                                      valid0 = true;
                                      passing0 = 17;
                                      if (props0 !== true) {
                                        props0 = true;
                                      }
                                    }
                                    const _errs19 = errors;
                                    if (
                                      !validate158(data, {
                                        instancePath,
                                        parentData,
                                        parentDataProperty,
                                        rootData,
                                        dynamicAnchors,
                                      })
                                    ) {
                                      vErrors =
                                        vErrors === null
                                          ? validate158.errors
                                          : vErrors.concat(validate158.errors);
                                      errors = vErrors.length;
                                    }
                                    var _valid0 = _errs19 === errors;
                                    if (_valid0 && valid0) {
                                      valid0 = false;
                                      passing0 = [passing0, 18];
                                    } else {
                                      if (_valid0) {
                                        valid0 = true;
                                        passing0 = 18;
                                        if (props0 !== true) {
                                          props0 = true;
                                        }
                                      }
                                      const _errs20 = errors;
                                      if (
                                        !validate161(data, {
                                          instancePath,
                                          parentData,
                                          parentDataProperty,
                                          rootData,
                                          dynamicAnchors,
                                        })
                                      ) {
                                        vErrors =
                                          vErrors === null
                                            ? validate161.errors
                                            : vErrors.concat(validate161.errors);
                                        errors = vErrors.length;
                                      }
                                      var _valid0 = _errs20 === errors;
                                      if (_valid0 && valid0) {
                                        valid0 = false;
                                        passing0 = [passing0, 19];
                                      } else {
                                        if (_valid0) {
                                          valid0 = true;
                                          passing0 = 19;
                                          if (props0 !== true) {
                                            props0 = true;
                                          }
                                        }
                                        const _errs21 = errors;
                                        if (
                                          !validate164(data, {
                                            instancePath,
                                            parentData,
                                            parentDataProperty,
                                            rootData,
                                            dynamicAnchors,
                                          })
                                        ) {
                                          vErrors =
                                            vErrors === null
                                              ? validate164.errors
                                              : vErrors.concat(validate164.errors);
                                          errors = vErrors.length;
                                        }
                                        var _valid0 = _errs21 === errors;
                                        if (_valid0 && valid0) {
                                          valid0 = false;
                                          passing0 = [passing0, 20];
                                        } else {
                                          if (_valid0) {
                                            valid0 = true;
                                            passing0 = 20;
                                            if (props0 !== true) {
                                              props0 = true;
                                            }
                                          }
                                          const _errs22 = errors;
                                          if (
                                            !validate167(data, {
                                              instancePath,
                                              parentData,
                                              parentDataProperty,
                                              rootData,
                                              dynamicAnchors,
                                            })
                                          ) {
                                            vErrors =
                                              vErrors === null
                                                ? validate167.errors
                                                : vErrors.concat(validate167.errors);
                                            errors = vErrors.length;
                                          }
                                          var _valid0 = _errs22 === errors;
                                          if (_valid0 && valid0) {
                                            valid0 = false;
                                            passing0 = [passing0, 21];
                                          } else {
                                            if (_valid0) {
                                              valid0 = true;
                                              passing0 = 21;
                                              if (props0 !== true) {
                                                props0 = true;
                                              }
                                            }
                                            const _errs23 = errors;
                                            if (
                                              !validate170(data, {
                                                instancePath,
                                                parentData,
                                                parentDataProperty,
                                                rootData,
                                                dynamicAnchors,
                                              })
                                            ) {
                                              vErrors =
                                                vErrors === null
                                                  ? validate170.errors
                                                  : vErrors.concat(validate170.errors);
                                              errors = vErrors.length;
                                            }
                                            var _valid0 = _errs23 === errors;
                                            if (_valid0 && valid0) {
                                              valid0 = false;
                                              passing0 = [passing0, 22];
                                            } else {
                                              if (_valid0) {
                                                valid0 = true;
                                                passing0 = 22;
                                                if (props0 !== true) {
                                                  props0 = true;
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  if (!valid0) {
    const err0 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err0];
    } else {
      vErrors.push(err0);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate54.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate54.evaluated = { dynamicProps: true, dynamicItems: false };
function validate53(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  /*# sourceURL="https://spec.example.invalid/moria/v0.2/resources.schema.json" */ let vErrors =
    null;
  let errors = 0;
  const evaluated0 = validate53.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (
    !validate54(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate54.errors : vErrors.concat(validate54.errors);
    errors = vErrors.length;
  } else {
    var props0 = validate54.evaluated.props;
  }
  validate53.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate53.evaluated = { dynamicProps: true, dynamicItems: false };
function validate52(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  /*# sourceURL="https://spec.example.invalid/moria/v0.2/manifest.schema.json" */ let vErrors =
    null;
  let errors = 0;
  const evaluated0 = validate52.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.apiVersion === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'apiVersion' },
        message: "must have required property '" + 'apiVersion' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.projectId === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'projectId' },
        message: "must have required property '" + 'projectId' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.deploymentUnitId === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'deploymentUnitId' },
        message: "must have required property '" + 'deploymentUnitId' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    if (data.resources === undefined) {
      const err3 = {
        instancePath,
        schemaPath: '#/required',
        keyword: 'required',
        params: { missingProperty: 'resources' },
        message: "must have required property '" + 'resources' + "'",
      };
      if (vErrors === null) {
        vErrors = [err3];
      } else {
        vErrors.push(err3);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(
        key0 === 'apiVersion' ||
        key0 === 'projectId' ||
        key0 === 'deploymentUnitId' ||
        key0 === 'resources' ||
        key0 === 'dependencies'
      )) {
        const err4 = {
          instancePath,
          schemaPath: '#/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.apiVersion !== undefined) {
      if ('moria/v1alpha2' !== data.apiVersion) {
        const err5 = {
          instancePath: instancePath + '/apiVersion',
          schemaPath: '#/properties/apiVersion/const',
          keyword: 'const',
          params: { allowedValue: 'moria/v1alpha2' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.projectId !== undefined) {
      let data1 = data.projectId;
      if (typeof data1 === 'string') {
        if (!formats0.test(data1)) {
          const err6 = {
            instancePath: instancePath + '/projectId',
            schemaPath: '#/properties/projectId/format',
            keyword: 'format',
            params: { format: 'uuid' },
            message: 'must match format "' + 'uuid' + '"',
          };
          if (vErrors === null) {
            vErrors = [err6];
          } else {
            vErrors.push(err6);
          }
          errors++;
        }
      } else {
        const err7 = {
          instancePath: instancePath + '/projectId',
          schemaPath: '#/properties/projectId/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err7];
        } else {
          vErrors.push(err7);
        }
        errors++;
      }
    }
    if (data.deploymentUnitId !== undefined) {
      let data2 = data.deploymentUnitId;
      if (typeof data2 === 'string') {
        if (!formats0.test(data2)) {
          const err8 = {
            instancePath: instancePath + '/deploymentUnitId',
            schemaPath: '#/properties/deploymentUnitId/format',
            keyword: 'format',
            params: { format: 'uuid' },
            message: 'must match format "' + 'uuid' + '"',
          };
          if (vErrors === null) {
            vErrors = [err8];
          } else {
            vErrors.push(err8);
          }
          errors++;
        }
      } else {
        const err9 = {
          instancePath: instancePath + '/deploymentUnitId',
          schemaPath: '#/properties/deploymentUnitId/type',
          keyword: 'type',
          params: { type: 'string' },
          message: 'must be string',
        };
        if (vErrors === null) {
          vErrors = [err9];
        } else {
          vErrors.push(err9);
        }
        errors++;
      }
    }
    if (data.resources !== undefined) {
      let data3 = data.resources;
      if (Array.isArray(data3)) {
        if (data3.length < 1) {
          const err10 = {
            instancePath: instancePath + '/resources',
            schemaPath: '#/properties/resources/minItems',
            keyword: 'minItems',
            params: { limit: 1 },
            message: 'must NOT have fewer than 1 items',
          };
          if (vErrors === null) {
            vErrors = [err10];
          } else {
            vErrors.push(err10);
          }
          errors++;
        }
        const len0 = data3.length;
        for (let i0 = 0; i0 < len0; i0++) {
          if (
            !validate53(data3[i0], {
              instancePath: instancePath + '/resources/' + i0,
              parentData: data3,
              parentDataProperty: i0,
              rootData,
              dynamicAnchors,
            })
          ) {
            vErrors = vErrors === null ? validate53.errors : vErrors.concat(validate53.errors);
            errors = vErrors.length;
          }
        }
      } else {
        const err11 = {
          instancePath: instancePath + '/resources',
          schemaPath: '#/properties/resources/type',
          keyword: 'type',
          params: { type: 'array' },
          message: 'must be array',
        };
        if (vErrors === null) {
          vErrors = [err11];
        } else {
          vErrors.push(err11);
        }
        errors++;
      }
    }
    if (data.dependencies !== undefined) {
      let data5 = data.dependencies;
      if (Array.isArray(data5)) {
        if (data5.length < 0) {
          const err12 = {
            instancePath: instancePath + '/dependencies',
            schemaPath: '#/properties/dependencies/minItems',
            keyword: 'minItems',
            params: { limit: 0 },
            message: 'must NOT have fewer than 0 items',
          };
          if (vErrors === null) {
            vErrors = [err12];
          } else {
            vErrors.push(err12);
          }
          errors++;
        }
        const len1 = data5.length;
        for (let i1 = 0; i1 < len1; i1++) {
          let data6 = data5[i1];
          if (data6 && typeof data6 == 'object' && !Array.isArray(data6)) {
            if (data6.package === undefined) {
              const err13 = {
                instancePath: instancePath + '/dependencies/' + i1,
                schemaPath: '#/properties/dependencies/items/required',
                keyword: 'required',
                params: { missingProperty: 'package' },
                message: "must have required property '" + 'package' + "'",
              };
              if (vErrors === null) {
                vErrors = [err13];
              } else {
                vErrors.push(err13);
              }
              errors++;
            }
            if (data6.digest === undefined) {
              const err14 = {
                instancePath: instancePath + '/dependencies/' + i1,
                schemaPath: '#/properties/dependencies/items/required',
                keyword: 'required',
                params: { missingProperty: 'digest' },
                message: "must have required property '" + 'digest' + "'",
              };
              if (vErrors === null) {
                vErrors = [err14];
              } else {
                vErrors.push(err14);
              }
              errors++;
            }
            for (const key1 in data6) {
              if (!(key1 === 'package' || key1 === 'digest')) {
                const err15 = {
                  instancePath: instancePath + '/dependencies/' + i1,
                  schemaPath: '#/properties/dependencies/items/additionalProperties',
                  keyword: 'additionalProperties',
                  params: { additionalProperty: key1 },
                  message: 'must NOT have additional properties',
                };
                if (vErrors === null) {
                  vErrors = [err15];
                } else {
                  vErrors.push(err15);
                }
                errors++;
              }
            }
            if (data6.package !== undefined) {
              let data7 = data6.package;
              if (typeof data7 === 'string') {
                if (func1(data7) < 1) {
                  const err16 = {
                    instancePath: instancePath + '/dependencies/' + i1 + '/package',
                    schemaPath: '#/properties/dependencies/items/properties/package/minLength',
                    keyword: 'minLength',
                    params: { limit: 1 },
                    message: 'must NOT have fewer than 1 characters',
                  };
                  if (vErrors === null) {
                    vErrors = [err16];
                  } else {
                    vErrors.push(err16);
                  }
                  errors++;
                }
              } else {
                const err17 = {
                  instancePath: instancePath + '/dependencies/' + i1 + '/package',
                  schemaPath: '#/properties/dependencies/items/properties/package/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err17];
                } else {
                  vErrors.push(err17);
                }
                errors++;
              }
            }
            if (data6.digest !== undefined) {
              let data8 = data6.digest;
              if (typeof data8 === 'string') {
                if (!pattern11.test(data8)) {
                  const err18 = {
                    instancePath: instancePath + '/dependencies/' + i1 + '/digest',
                    schemaPath: '#/properties/dependencies/items/properties/digest/pattern',
                    keyword: 'pattern',
                    params: { pattern: '^sha256:[0-9a-f]{64}$' },
                    message: 'must match pattern "' + '^sha256:[0-9a-f]{64}$' + '"',
                  };
                  if (vErrors === null) {
                    vErrors = [err18];
                  } else {
                    vErrors.push(err18);
                  }
                  errors++;
                }
              } else {
                const err19 = {
                  instancePath: instancePath + '/dependencies/' + i1 + '/digest',
                  schemaPath: '#/properties/dependencies/items/properties/digest/type',
                  keyword: 'type',
                  params: { type: 'string' },
                  message: 'must be string',
                };
                if (vErrors === null) {
                  vErrors = [err19];
                } else {
                  vErrors.push(err19);
                }
                errors++;
              }
            }
          } else {
            const err20 = {
              instancePath: instancePath + '/dependencies/' + i1,
              schemaPath: '#/properties/dependencies/items/type',
              keyword: 'type',
              params: { type: 'object' },
              message: 'must be object',
            };
            if (vErrors === null) {
              vErrors = [err20];
            } else {
              vErrors.push(err20);
            }
            errors++;
          }
        }
      } else {
        const err21 = {
          instancePath: instancePath + '/dependencies',
          schemaPath: '#/properties/dependencies/type',
          keyword: 'type',
          params: { type: 'array' },
          message: 'must be array',
        };
        if (vErrors === null) {
          vErrors = [err21];
        } else {
          vErrors.push(err21);
        }
        errors++;
      }
    }
  } else {
    const err22 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err22];
    } else {
      vErrors.push(err22);
    }
    errors++;
  }
  validate52.errors = vErrors;
  return errors === 0;
}
validate52.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
export const validateAttributes = validate176;
const schema58 = {
  $id: 'https://spec.example.invalid/moria/compiler/principal-attributes',
  type: 'object',
  additionalProperties: {
    $ref: 'https://spec.example.invalid/moria/v0.2/resources.schema.json#/$defs/ValueType',
  },
};
function validate177(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  let vErrors = null;
  let errors = 0;
  const evaluated0 = validate177.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  const _errs0 = errors;
  let valid0 = false;
  let passing0 = null;
  const _errs1 = errors;
  if (
    !validate65(data, { instancePath, parentData, parentDataProperty, rootData, dynamicAnchors })
  ) {
    vErrors = vErrors === null ? validate65.errors : vErrors.concat(validate65.errors);
    errors = vErrors.length;
  } else {
    var props0 = validate65.evaluated.props;
  }
  var _valid0 = _errs1 === errors;
  if (_valid0) {
    valid0 = true;
    passing0 = 0;
  }
  const _errs2 = errors;
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    if (data.type === undefined) {
      const err0 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'type' },
        message: "must have required property '" + 'type' + "'",
      };
      if (vErrors === null) {
        vErrors = [err0];
      } else {
        vErrors.push(err0);
      }
      errors++;
    }
    if (data.nullable === undefined) {
      const err1 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'nullable' },
        message: "must have required property '" + 'nullable' + "'",
      };
      if (vErrors === null) {
        vErrors = [err1];
      } else {
        vErrors.push(err1);
      }
      errors++;
    }
    if (data.items === undefined) {
      const err2 = {
        instancePath,
        schemaPath: '#/oneOf/1/required',
        keyword: 'required',
        params: { missingProperty: 'items' },
        message: "must have required property '" + 'items' + "'",
      };
      if (vErrors === null) {
        vErrors = [err2];
      } else {
        vErrors.push(err2);
      }
      errors++;
    }
    for (const key0 in data) {
      if (!(key0 === 'type' || key0 === 'nullable' || key0 === 'items')) {
        const err3 = {
          instancePath,
          schemaPath: '#/oneOf/1/additionalProperties',
          keyword: 'additionalProperties',
          params: { additionalProperty: key0 },
          message: 'must NOT have additional properties',
        };
        if (vErrors === null) {
          vErrors = [err3];
        } else {
          vErrors.push(err3);
        }
        errors++;
      }
    }
    if (data.type !== undefined) {
      if ('array' !== data.type) {
        const err4 = {
          instancePath: instancePath + '/type',
          schemaPath: '#/oneOf/1/properties/type/const',
          keyword: 'const',
          params: { allowedValue: 'array' },
          message: 'must be equal to constant',
        };
        if (vErrors === null) {
          vErrors = [err4];
        } else {
          vErrors.push(err4);
        }
        errors++;
      }
    }
    if (data.nullable !== undefined) {
      if (typeof data.nullable !== 'boolean') {
        const err5 = {
          instancePath: instancePath + '/nullable',
          schemaPath: '#/oneOf/1/properties/nullable/type',
          keyword: 'type',
          params: { type: 'boolean' },
          message: 'must be boolean',
        };
        if (vErrors === null) {
          vErrors = [err5];
        } else {
          vErrors.push(err5);
        }
        errors++;
      }
    }
    if (data.items !== undefined) {
      let data2 = data.items;
      if (
        !validate65(data2, {
          instancePath: instancePath + '/items',
          parentData: data,
          parentDataProperty: 'items',
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate65.errors : vErrors.concat(validate65.errors);
        errors = vErrors.length;
      } else {
        var props1 = validate65.evaluated.props;
      }
      if (data2 && typeof data2 == 'object' && !Array.isArray(data2)) {
        if (data2.nullable !== undefined) {
          if (false !== data2.nullable) {
            const err6 = {
              instancePath: instancePath + '/items/nullable',
              schemaPath: '#/oneOf/1/properties/items/allOf/1/properties/nullable/const',
              keyword: 'const',
              params: { allowedValue: false },
              message: 'must be equal to constant',
            };
            if (vErrors === null) {
              vErrors = [err6];
            } else {
              vErrors.push(err6);
            }
            errors++;
          }
        }
      }
      if (props1 !== true) {
        props1 = props1 || {};
        props1.nullable = true;
      }
    }
  } else {
    const err7 = {
      instancePath,
      schemaPath: '#/oneOf/1/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err7];
    } else {
      vErrors.push(err7);
    }
    errors++;
  }
  var _valid0 = _errs2 === errors;
  if (_valid0 && valid0) {
    valid0 = false;
    passing0 = [passing0, 1];
  } else {
    if (_valid0) {
      valid0 = true;
      passing0 = 1;
      if (props0 !== true) {
        props0 = true;
      }
    }
  }
  if (!valid0) {
    const err8 = {
      instancePath,
      schemaPath: '#/oneOf',
      keyword: 'oneOf',
      params: { passingSchemas: passing0 },
      message: 'must match exactly one schema in oneOf',
    };
    if (vErrors === null) {
      vErrors = [err8];
    } else {
      vErrors.push(err8);
    }
    errors++;
  } else {
    errors = _errs0;
    if (vErrors !== null) {
      if (_errs0) {
        vErrors.length = _errs0;
      } else {
        vErrors = null;
      }
    }
  }
  validate177.errors = vErrors;
  evaluated0.props = props0;
  return errors === 0;
}
validate177.evaluated = { dynamicProps: true, dynamicItems: false };
function validate176(
  data,
  { instancePath = '', parentData, parentDataProperty, rootData = data, dynamicAnchors = {} } = {},
) {
  /*# sourceURL="https://spec.example.invalid/moria/compiler/principal-attributes" */ let vErrors =
    null;
  let errors = 0;
  const evaluated0 = validate176.evaluated;
  if (evaluated0.dynamicProps) {
    evaluated0.props = undefined;
  }
  if (evaluated0.dynamicItems) {
    evaluated0.items = undefined;
  }
  if (data && typeof data == 'object' && !Array.isArray(data)) {
    for (const key0 in data) {
      if (
        !validate177(data[key0], {
          instancePath: instancePath + '/' + key0.replace(/~/g, '~0').replace(/\//g, '~1'),
          parentData: data,
          parentDataProperty: key0,
          rootData,
          dynamicAnchors,
        })
      ) {
        vErrors = vErrors === null ? validate177.errors : vErrors.concat(validate177.errors);
        errors = vErrors.length;
      }
    }
  } else {
    const err0 = {
      instancePath,
      schemaPath: '#/type',
      keyword: 'type',
      params: { type: 'object' },
      message: 'must be object',
    };
    if (vErrors === null) {
      vErrors = [err0];
    } else {
      vErrors.push(err0);
    }
    errors++;
  }
  validate176.errors = vErrors;
  return errors === 0;
}
validate176.evaluated = { props: true, dynamicProps: false, dynamicItems: false };
