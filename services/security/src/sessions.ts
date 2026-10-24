import { randomBytes, createHash } from 'node:crypto';
import { Authority } from './authority.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { SessionContext } from '../../ontology/src/service.js';
const hash = (token: string) => createHash('sha256').update(token).digest('hex');

/** Opaque short-lived workload credentials. Mint/revoke are trusted operator
 * methods, not public login endpoints. Human login must use OIDC separately. */
export class WorkloadSessions {
  constructor(readonly authority: Authority) {}
  async mint(
    context: SessionContext,
    actor: string,
  ): Promise<{ token: string; expiresAt: string }> {
    const state = await this.authority.current(
      context.environment,
      context.principal,
      context.application,
    );
    return this.authority.catalog.transaction(async (c) => {
      await c.query('SELECT 1 FROM moria.environments WHERE id=$1 FOR UPDATE', [
        context.environment,
      ]);
      await this.authority.assertCurrent(
        c,
        context.environment,
        context.principal,
        context.application,
        state.epoch,
      );
      const principal = (
        await c.query('SELECT kind FROM moria.principals WHERE environment=$1 AND principal=$2', [
          context.environment,
          context.principal,
        ])
      ).rows[0];
      if (principal.kind !== 'workload')
        throw new MoriaError('AUTH_METHOD_INVALID', 'Human sessions require OIDC');
      const token = randomBytes(32).toString('base64url');
      const result = await c.query(
        `INSERT INTO moria.workload_sessions(token_hash,environment,principal,application,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '15 minutes') RETURNING expires_at`,
        [hash(token), context.environment, context.principal, context.application],
      );
      await c.query(
        'INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,$3,$4,$5)',
        [context.environment, actor, 'mintWorkloadSession', context.principal, state.epoch],
      );
      return { token, expiresAt: result.rows[0].expires_at.toISOString() };
    }, context.environment);
  }
  async authenticate(header: string | undefined): Promise<SessionContext> {
    const match = /^Bearer ([A-Za-z0-9_-]{43})$/.exec(header ?? '');
    if (!match) throw new MoriaError('UNAUTHENTICATED', 'A workload bearer session is required');
    const sessionHash = hash(match[1]);
    const result = await this.authority.catalog.pool.query(
      'SELECT environment,principal,application FROM moria.workload_sessions WHERE token_hash=$1 AND NOT disabled AND expires_at>clock_timestamp()',
      [sessionHash],
    );
    if (!result.rowCount) throw new MoriaError('UNAUTHENTICATED', 'Session expired or was revoked');
    return { ...result.rows[0], sessionHash };
  }
  async revoke(context: SessionContext, actor: string): Promise<void> {
    if (!context.sessionHash) throw new MoriaError('UNAUTHENTICATED', 'Session is required');
    await this.authority.catalog.transaction(async (c) => {
      const result = await c.query(
        'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch',
        [context.environment],
      );
      await c.query(
        'UPDATE moria.workload_sessions SET disabled=true WHERE token_hash=$1 AND environment=$2',
        [context.sessionHash, context.environment],
      );
      await c.query(
        'INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,$3,$4,$5)',
        [
          context.environment,
          actor,
          'revokeWorkloadSession',
          context.principal,
          result.rows[0].security_epoch,
        ],
      );
    }, context.environment);
  }
}
