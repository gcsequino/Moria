import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import type { Diagnostic, Manifest, Resource, ValueType, Expr } from '../../contracts/types.js';
import { resourceRef, MoriaError } from '../../contracts/types.js';
import { compileExpression, type ExpressionTypes } from '../../authorization/src/expressions.js';

const require = createRequire(import.meta.url);
const Ajv = require('ajv/dist/2020').default;
const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: true });
require('ajv-formats')(ajv);
const schema = JSON.parse(
  readFileSync(new URL('../../contracts/schemas/resources.schema.json', import.meta.url), 'utf8'),
);
const manifestSchema = JSON.parse(
  readFileSync(new URL('../../contracts/schemas/manifest.schema.json', import.meta.url), 'utf8'),
);
ajv.addSchema(schema);
const validateShape = ajv.compile(manifestSchema);
const validateAttributes = ajv.compile({
  type: 'object',
  additionalProperties: { $ref: `${schema.$id}#/$defs/ValueType` },
});

/** Deterministic JSON, excluding no fields: fingerprints include policy/scopes. */
export function canonical(value: unknown): string {
  if (typeof value === 'number' && !Number.isFinite(value))
    throw new MoriaError('INVALID_JSON', 'Non-finite number');
  if (value === null || typeof value !== 'object') {
    const text = JSON.stringify(value);
    if (text === undefined) throw new MoriaError('INVALID_JSON', 'Value is not JSON');
    return text;
  }
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
    throw new MoriaError('INVALID_JSON', 'Expected a plain JSON object');
  return `{${Object.keys(value)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${canonical((value as any)[k])}`)
    .join(',')}}`;
}
export const fingerprint = (value: unknown): string =>
  `sha256:${createHash('sha256').update(canonical(value)).digest('hex')}`;

export interface CompilerContext {
  principalAttributes: Record<string, ValueType>;
}
export interface CompiledManifest {
  manifest: Manifest;
  fingerprint: string;
  order: string[];
  policyTypesChecked: boolean;
  attributeTypesFingerprint?: string;
}
export function validateManifest(input: unknown, context?: CompilerContext): Diagnostic[] {
  if (!validateShape(input))
    return (validateShape.errors ?? []).slice(0, 30).map((e: any) => ({
      code: 'SCHEMA_INVALID',
      path: e.instancePath || '/',
      message: `${e.keyword}: ${e.message}`,
    }));
  const m = input as Manifest;
  const diagnostics: Diagnostic[] = [];
  if (context && !validateAttributes(context.principalAttributes))
    return [
      {
        code: 'ATTRIBUTE_SCHEMA_INVALID',
        path: '/principalAttributes',
        message: 'Principal attribute types must use the declared value type schema',
      },
    ];
  const entries = new Map<string, Resource>();
  const namespaces = new Set(
    m.resources.filter((r) => r.kind === 'Namespace').map((r) => r.metadata.name),
  );
  const emit = (code: string, path: string, message: string) =>
    diagnostics.push({ code, path, message });
  if (m.dependencies?.length)
    emit(
      'DEPENDENCY_RESOLUTION_REQUIRED',
      '/dependencies',
      'External contract packages are not yet resolved; refusing partial validation',
    );
  for (const r of m.resources) {
    const key = resourceRef(r);
    if (entries.has(key)) emit('DUPLICATE_RESOURCE', key, 'Duplicate resource declaration');
    entries.set(key, r);
    if (
      r.kind === 'Namespace'
        ? r.metadata.namespace !== '_project'
        : !namespaces.has(r.metadata.namespace)
    )
      emit('NAMESPACE_INVALID', key, 'Resource must belong to a declared namespace');
  }
  const resolve = (value: string, kinds: string[], path: string): Resource | undefined => {
    const r = entries.get(value);
    if (!r) emit('UNRESOLVED_REFERENCE', path, `Unknown resource ${value}`);
    else if (kinds.length && !kinds.includes(r.kind)) {
      emit('REFERENCE_KIND', path, `Expected ${kinds.join('|')}, got ${r.kind}`);
      return undefined;
    }
    return r;
  };
  const checkTypes = (fields: Record<string, ValueType>, path: string) => {
    for (const [name, type] of Object.entries(fields)) {
      if (type.type === 'decimal' && type.scale! > type.precision!)
        emit('TYPE_INVALID', `${path}/${name}`, 'Decimal scale exceeds precision');
      if (type.type === 'array') {
        if (type.items?.nullable)
          emit('TYPE_INVALID', `${path}/${name}`, 'Array elements must be non-null');
        if (type.items) checkTypes({ item: type.items }, `${path}/${name}`);
      }
    }
  };
  const producers = new Map<string, string>();
  if (context) checkTypes(context.principalAttributes, '/principalAttributes');
  const graph = new Map<string, string[]>();
  for (const r of m.resources) {
    const key = resourceRef(r),
      s = r.spec;
    const ref = (value: string, kinds: string[]) => resolve(value, kinds, key);
    for (const mark of r.metadata.markings ?? []) ref(mark, ['Marking']);
    for (const name of ['columns', 'properties', 'inputs', 'outputs'])
      if (s[name] && !Array.isArray(s[name])) checkTypes(s[name], `${key}/${name}`);
    switch (r.kind) {
      case 'Dataset':
        for (const pk of s.primaryKey ?? [])
          if (!s.columns[pk] || s.columns[pk].nullable)
            emit('PRIMARY_KEY_INVALID', key, 'Keys must exist and be non-null');
        if (
          s.writeModes.some((x: string) => ['upsert', 'delete'].includes(x)) &&
          !s.primaryKey?.length
        )
          emit('PRIMARY_KEY_REQUIRED', key, 'Keyed writes require primaryKey');
        break;
      case 'Transform':
        for (const input of s.inputs) ref(input, ['Dataset']);
        for (const output of s.outputs) {
          ref(output, ['Dataset']);
          if (producers.has(output))
            emit('MULTIPLE_PRODUCERS', key, `${output} already has a transform producer`);
          producers.set(output, key);
        }
        graph.set(key, s.inputs);
        break;
      case 'RestrictedView': {
        ref(s.dataset, ['Dataset']);
        const p = ref(s.policy, ['Policy']);
        if (p && p.spec.type !== 'row')
          emit('POLICY_KIND', key, 'Restricted views require row policies');
        break;
      }
      case 'ObjectType': {
        const pk = s.properties[s.primaryKey];
        if (!pk || pk.type !== 'uid' || pk.nullable)
          emit('PRIMARY_KEY_INVALID', key, 'Object primary key must be a non-null UID');
        let source = ref(s.source.resource, ['Dataset', 'RestrictedView']);
        if (source?.kind === 'RestrictedView') source = ref(source.spec.dataset, ['Dataset']);
        if (
          canonical(Object.keys(s.source.mapping).sort()) !==
          canonical(Object.keys(s.properties).sort())
        )
          emit('MAPPING_INVALID', key, 'Mapping must cover every property exactly');
        if (source)
          for (const [prop, col] of Object.entries(s.source.mapping)) {
            const type = source.spec.columns[col as string];
            if (!type || !s.properties[prop] || canonical(type) !== canonical(s.properties[prop]))
              emit('MAPPING_INVALID', key, `Incompatible mapped property ${prop}`);
          }
        for (const [field, type] of [
          ['readPolicy', 'objectRead'],
          ['writePolicy', 'objectWrite'],
        ])
          if (s[field]) {
            const p = ref(s[field], ['Policy']);
            if (p && p.spec.type !== type) emit('POLICY_KIND', key, `Expected ${type}`);
          }
        for (const [prop, policies] of Object.entries(s.propertyPolicies ?? {})) {
          if (!s.properties[prop]) emit('PROPERTY_UNKNOWN', key, `Policy references ${prop}`);
          for (const [op, val] of Object.entries(policies as object)) {
            const p = ref(val, ['Policy']);
            if (p && p.spec.type !== (op === 'read' ? 'propertyRead' : 'propertyWrite'))
              emit('POLICY_KIND', key, 'Wrong property policy type');
          }
        }
        break;
      }
      case 'LinkType': {
        const a = ref(s.sourceType, ['ObjectType']),
          b = ref(s.targetType, ['ObjectType']);
        if (s.mode === 'mapped') {
          if (s.cardinality === 'manyToMany')
            emit('LINK_INVALID', key, 'Mapped many-to-many is unsupported');
          const x = a?.spec.properties[s.sourceProperty],
            y = b?.spec.properties[s.targetProperty];
          if (!x || !y || x.type !== y.type)
            emit('LINK_INVALID', key, 'Mapped link key types must match');
        } else if (s.sourceProperty || s.targetProperty)
          emit('LINK_INVALID', key, 'Action-managed links cannot have mapped columns');
        if (s.readPolicy) ref(s.readPolicy, ['Policy']);
        break;
      }
      case 'Action': {
        ref(s.targetType, ['ObjectType']);
        if (s.targetParameter && s.inputs[s.targetParameter]?.type !== 'uid')
          emit('ACTION_TARGET', key, 'Target parameter must be a declared UID input');
        if (!s.targetParameter && (s.function || s.edits?.some((e: any) => e.op !== 'create')))
          emit('ACTION_TARGET', key, 'Existing-object action needs a target parameter');
        if (s.function) {
          const fn = ref(s.function, ['Function']);
          if (fn && fn.spec.effect !== 'editProposal')
            emit('ACTION_FUNCTION', key, 'Action function must produce edit proposals');
        }
        for (const e of s.edits ?? []) {
          if (e.linkType) ref(e.linkType, ['LinkType']);
          const target = e.targetType ? ref(e.targetType, ['ObjectType']) : undefined;
          if (
            target &&
            e.property &&
            (!target.spec.properties[e.property] || target.spec.primaryKey === e.property)
          )
            emit('EDIT_INVALID', key, 'Cannot edit unknown property or UID');
          if (target && e.op === 'create' && target.spec.primaryKey in e.values)
            emit('EDIT_INVALID', key, 'Create cannot assign its own UID');
        }
        break;
      }
      case 'Application':
        for (const scope of s.scopes) ref(scope.target, []);
        if (s.frontend && s.mode !== 'browser')
          emit('APPLICATION_MODE', key, 'Frontend requires browser identity');
        break;
      case 'Connection': {
        const impl = ref(s.implementation, ['ConnectorImplementation']),
          out = ref(s.output, ['Dataset']);
        for (const sec of s.secrets) ref(sec, ['SecretReference']);
        if (out && !out.spec.writeModes.includes(s.mode))
          emit('WRITE_MODE', key, 'Output dataset does not allow this mode');
        if (impl) {
          try {
            if (!ajv.compile(impl.spec.configurationSchema)(s.configuration))
              emit('CONNECTOR_CONFIG', key, 'Configuration does not satisfy implementation schema');
          } catch {
            emit('CONNECTOR_SCHEMA', key, 'Invalid connector configuration schema');
          }
        }
        break;
      }
      case 'ModelProvider': {
        const p = ref(s.destinationPolicy, ['Policy']);
        if (p && p.spec.type !== 'modelDestination')
          emit('POLICY_KIND', key, 'Expected destination policy');
        if (s.secret) ref(s.secret, ['SecretReference']);
        break;
      }
      case 'Model':
        ref(s.provider, ['ModelProvider']);
        if (s.capabilities.includes('embeddings') && !s.embeddingDimensions)
          emit('MODEL_INVALID', key, 'Embedding model needs dimensions');
        if (!s.mutable && !s.versionLabel)
          emit('MODEL_INVALID', key, 'Immutable model needs a version');
        break;
      case 'Agent':
        ref(s.application, ['Application']);
        ref(s.adapter, ['AgentAdapter']);
        ref(s.model, ['Model']);
        break;
      case 'RetrievalCollection': {
        const src = ref(s.source, ['ObjectType']),
          model = ref(s.embeddingModel, ['Model']);
        if (src?.spec.properties[s.textProperty]?.type !== 'string')
          emit('RETRIEVAL_INVALID', key, 'Retrieval property must be text');
        if (model && !model.spec.capabilities.includes('embeddings'))
          emit('RETRIEVAL_INVALID', key, 'Model must support embeddings');
        if (s.chunkOverlap >= s.chunkSize)
          emit('RETRIEVAL_INVALID', key, 'Chunk overlap must be smaller than size');
        break;
      }
      case 'EvaluationSuite':
        ref(s.agent, ['Agent']);
        ref(s.fixtures, ['Dataset']);
        break;
      case 'Automation':
        ref(s.application, ['Application']);
        ref(s.target, [
          (
            {
              build: 'Transform',
              sync: 'Connection',
              invokeAgent: 'Agent',
              evaluate: 'EvaluationSuite',
            } as any
          )[s.operation],
        ]);
        break;
      case 'Grant':
        ref(s.application, ['Application']);
        ref(s.target, []);
        break;
      case 'SecretReference':
        for (const x of s.consumers) ref(x, []);
        break;
      case 'ObjectSet':
        ref(s.objectType, ['ObjectType']);
        break;
      case 'Policy':
        if (s.type === 'modelDestination') for (const x of s.allowedMarkings) ref(x, ['Marking']);
        break;
    }
  }
  const done = new Set<string>();
  function visit(node: string, active: Set<string>) {
    if (active.has(node)) {
      emit('TRANSFORM_CYCLE', node, 'Transform graph must be acyclic');
      return;
    }
    if (done.has(node)) return;
    for (const input of graph.get(node) ?? []) {
      const p = producers.get(input);
      if (p) visit(p, new Set([...active, node]));
    }
    done.add(node);
  }
  for (const node of graph.keys()) visit(node, new Set());
  if (context) {
    const attached = new Set<string>();
    const expression = (expr: Expr, types: ExpressionTypes, path: string) => {
      try {
        compileExpression(expr, { ...types, attributes: context.principalAttributes });
      } catch (error) {
        emit('POLICY_INVALID', path, error instanceof Error ? error.message : 'Invalid expression');
      }
    };
    const policy = (
      reference: string | undefined,
      fields: Record<string, ValueType>,
      path: string,
    ) => {
      if (!reference) return;
      const resource = entries.get(reference);
      if (resource?.kind === 'Policy' && resource.spec.expression) {
        attached.add(reference);
        expression(resource.spec.expression, { fields }, path);
      }
    };
    for (const resource of m.resources) {
      const key = resourceRef(resource),
        s = resource.spec;
      if (resource.kind === 'RestrictedView')
        policy(s.policy, entries.get(s.dataset)?.spec.columns ?? {}, key);
      if (resource.kind === 'ObjectType') {
        policy(s.readPolicy, s.properties, key);
        policy(s.writePolicy, s.properties, key);
        for (const policies of Object.values(s.propertyPolicies ?? {}) as Record<
          string,
          string
        >[]) {
          policy(policies.read, {}, key);
          policy(policies.write, {}, key);
        }
      }
      if (resource.kind === 'Action')
        expression(
          s.condition,
          { fields: entries.get(s.targetType)?.spec.properties ?? {}, parameters: s.inputs },
          key,
        );
      if (resource.kind === 'LinkType') policy(s.readPolicy, {}, key);
    }
    for (const resource of m.resources)
      if (
        resource.kind === 'Policy' &&
        resource.spec.expression &&
        !attached.has(resourceRef(resource))
      ) {
        if (['propertyRead', 'propertyWrite'].includes(resource.spec.type))
          expression(resource.spec.expression, {}, resourceRef(resource));
        else
          emit(
            'POLICY_CONTEXT_REQUIRED',
            resourceRef(resource),
            'Policy must be bound to a resource to validate its field types',
          );
      }
  }
  return diagnostics;
}
export function compileManifest(input: unknown, context?: CompilerContext): CompiledManifest {
  const diagnostics = validateManifest(input, context);
  if (diagnostics.length)
    throw new MoriaError('VALIDATION_FAILED', 'Manifest validation failed', diagnostics);
  const manifest = input as Manifest;
  return {
    manifest,
    fingerprint: fingerprint(manifest),
    order: manifest.resources.map(resourceRef).sort(),
    policyTypesChecked: !!context,
    ...(context ? { attributeTypesFingerprint: fingerprint(context.principalAttributes) } : {}),
  };
}
