import { MoriaError } from '../../contracts/types.js';
import { recordAccess } from './flow.js';

export interface Identity {
  principal: string;
  application: string;
  environment: string;
  grantedOperations: Set<string>;
  applicationScopes: Set<string>;
  markings: Set<string>;
  delegated?: Identity;
  disabled?: boolean;
}
export function authorize(
  identity: Identity,
  resource: string,
  operation: string,
  markings: Iterable<string> = [],
): void {
  const labels = [...markings];
  const permission = `${resource}:${operation}`;
  if (
    identity.disabled ||
    !identity.grantedOperations.has(permission) ||
    !identity.applicationScopes.has(permission)
  )
    throw new MoriaError('FORBIDDEN', 'Operation is not authorized');
  for (const marking of labels)
    if (!identity.markings.has(marking))
      throw new MoriaError('FORBIDDEN', 'Operation is not authorized');
  if (identity.delegated) {
    if (identity.environment !== identity.delegated.environment)
      throw new MoriaError('FORBIDDEN', 'Delegation environment differs');
    authorize(identity.delegated, resource, operation, labels);
  }
  recordAccess(identity.environment, resource, operation, labels);
}
export function authorizeDestination(
  identity: Identity,
  labels: Set<string>,
  policy: { allowedDestinations: string[]; allowedMarkings: string[] } | undefined,
  destination: string,
): void {
  if (!policy?.allowedDestinations.includes(destination))
    throw new MoriaError('DESTINATION_FORBIDDEN', 'Model destination is not allowed');
  for (const label of labels)
    if (!identity.markings.has(label) || !policy.allowedMarkings.includes(label))
      throw new MoriaError('DESTINATION_FORBIDDEN', 'Model destination is not allowed');
  if (identity.delegated) authorizeDestination(identity.delegated, labels, policy, destination);
}
