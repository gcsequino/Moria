import { MoriaError, type Expr, type Value, type ValueType } from '../../contracts/types.js';
import { authorize, type Identity } from './access.js';
import { compileExpression } from './expressions.js';

interface Row {
  source: Record<string, Value>;
  effective: Record<string, Value>;
}
export interface ObjectPolicies {
  resource: string;
  markings: string[];
  properties: Record<string, ValueType>;
  sourceColumns: Record<string, ValueType>;
  attributes: Record<string, ValueType>;
  row?: Expr;
  objectRead?: Expr;
  propertyRead?: Record<string, Expr>;
}

/** Compile once per trusted policy revision; current identity is supplied per read. */
export function objectReader(policy: ObjectPolicies) {
  const row =
    policy.row &&
    compileExpression(policy.row, { fields: policy.sourceColumns, attributes: policy.attributes });
  const object =
    policy.objectRead &&
    compileExpression(policy.objectRead, {
      fields: policy.properties,
      attributes: policy.attributes,
    });
  const properties = Object.fromEntries(
    Object.entries(policy.propertyRead ?? {}).map(([key, expression]) => {
      if (!Object.hasOwn(policy.properties, key))
        throw new MoriaError('POLICY_INVALID', 'Unknown protected property');
      return [key, compileExpression(expression, { attributes: policy.attributes })];
    }),
  );
  return {
    prepare(
      identity: Identity,
      attributes: Record<string, Value>,
      projection?: string[],
      predicateProperties: string[] = [],
      delegatedAttributes: Record<string, Value>[] = [],
    ) {
      authorize(identity, policy.resource, 'read', policy.markings);
      let depth = 0;
      for (let delegate = identity.delegated; delegate; delegate = delegate.delegated) depth++;
      if (depth !== delegatedAttributes.length)
        throw new MoriaError('FORBIDDEN', 'Current delegated attributes are required');
      const contexts = [attributes, ...delegatedAttributes];
      const permitted = (name: string) =>
        !Object.hasOwn(properties, name) ||
        contexts.every((attributes) => properties[name].allows({ attributes }));
      // This happens before inspecting any row, including an empty result set.
      for (const name of [...(projection ?? []), ...predicateProperties]) {
        if (!Object.hasOwn(policy.properties, name))
          throw new MoriaError('PROPERTY_UNKNOWN', 'Unknown property');
        if (!permitted(name))
          throw new MoriaError('PROPERTY_FORBIDDEN', 'Property is not accessible');
      }
      const selected = projection ?? Object.keys(policy.properties).filter(permitted);
      return (pinned: Row | null, current: Row | null): Record<string, Value> | null => {
        if (!pinned || !current) return null;
        for (const state of [pinned, current])
          for (const attributes of contexts) {
            if (row && !row.allows({ fields: state.source, attributes })) return null;
            if (object && !object.allows({ fields: state.effective, attributes })) return null;
          }
        return Object.fromEntries(
          selected
            .filter((name) => Object.hasOwn(pinned.effective, name))
            .map((name) => [name, pinned.effective[name]]),
        );
      };
    },
  };
}
