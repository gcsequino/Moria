import type { PoolClient } from 'pg';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { SessionContext } from '../../ontology/src/service.js';
import { accessMarkings } from '../../../packages/authorization/src/flow.js';

export interface ExecutionConstraint {
  id: string;
  fence: string;
  proposal?: boolean;
}

/** Trusted service composition only; never accepted from a request body. */
export async function executionGuard(
  client: PoolClient,
  session: SessionContext,
  effect: 'read' | 'write' | 'proposal' = 'read',
  destination: { environment: string; marking: string }[] = [],
  verifyReads = true,
) {
  if (!session.execution) return [];
  const row = (
    await client.query(
      `SELECT markings,reads,definition->'spec'->>'effect' AS effect FROM moria.function_invocations
     WHERE id=$1 AND fence=$2 AND environment=$3 AND context->'session'->>'principal'=$4
     AND context->'session'->>'application'=$5 AND status='running'
     AND lease_until>clock_timestamp() AND deadline>clock_timestamp() FOR UPDATE`,
      [
        session.execution.id,
        session.execution.fence,
        session.environment,
        session.principal,
        session.application,
      ],
    )
  ).rows[0];
  if (!row) throw new MoriaError('FUNCTION_WORKER_LOST', 'Invocation is no longer active');
  if (effect !== 'read') {
    if (row.effect !== 'readWrite' && !(effect === 'proposal' && row.effect === 'editProposal'))
      throw new MoriaError('FUNCTION_EFFECT', 'Function effect does not allow this mutation');
    if (
      [...row.markings, ...accessMarkings()].some(
        (label: { environment: string; marking: string }) =>
          !destination.some(
            (target) =>
              target.environment === label.environment && target.marking === label.marking,
          ),
      )
    )
      throw new MoriaError('MARKING_DOWNGRADE', 'Destination omits invocation markings');
    if (verifyReads && !session.execution.proposal) {
      const environment = (
        await client.query('SELECT revision,security_epoch FROM moria.environments WHERE id=$1', [
          session.environment,
        ])
      ).rows[0];
      const validate = async (reads: any[], depth: number) => {
        if (depth >= 8)
          throw new MoriaError('FUNCTION_RECURSION', 'Invocation evidence depth exceeded');
        for (const read of reads) {
          if (
            read.epoch !== environment.security_epoch ||
            (!read.filesystem && !read.invocation && read.revision !== environment.revision)
          )
            throw new MoriaError(
              'FUNCTION_INPUT_CHANGED',
              'Invocation read changed before mutation',
            );
          if (read.invocation && !read.invocation.metadata) {
            const child = (
              await client.query(
                'SELECT reads FROM moria.function_invocations WHERE id=$1 AND environment=$2',
                [read.invocation.id, session.environment],
              )
            ).rows[0];
            if (!child)
              throw new MoriaError(
                'FUNCTION_INPUT_CHANGED',
                'Nested invocation evidence disappeared',
              );
            await validate(child.reads, depth + 1);
          }
          if (
            read.filesystem &&
            read.instanceEpoch !==
              (await client.query('SELECT security_epoch FROM moria.instance_state')).rows[0]
                .security_epoch
          )
            throw new MoriaError(
              'FUNCTION_INPUT_CHANGED',
              'Filesystem authority changed before mutation',
            );
          for (const file of read.files ?? []) {
            const current = (
              await client.query(
                'SELECT revision::text FROM moria.fs_entries WHERE id=$1 AND NOT deleted',
                [file.id],
              )
            ).rows[0];
            if (current?.revision !== file.revision)
              throw new MoriaError(
                'FUNCTION_INPUT_CHANGED',
                'Filesystem read changed before mutation',
              );
          }
        }
      };
      await validate(row.reads, 0);
    }
  }
  return row.markings as { environment: string; marking: string }[];
}
