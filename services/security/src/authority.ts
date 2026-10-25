import type { PoolClient } from 'pg';
import { Catalog } from '../../catalog/src/catalog.js';
import type { Identity } from '../../../packages/authorization/src/access.js';
import { MoriaError, type Value } from '../../../packages/contracts/types.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';

export interface PrincipalState {
  enabled: boolean;
  kind: 'human' | 'workload';
  attributes: Record<string, Value>;
  markings: string[];
}
export interface SecuritySnapshot {
  epoch: string;
  identity: Identity;
  attributes: Record<string, Value>;
}

/** Trusted administration/synchronization interface. Not exposed to application
 * deployments. An authenticated API must establish the caller, not accept it
 * from request JSON. IdP verification and group synchronization are pending. */
export class Authority {
  constructor(readonly catalog: Catalog) {}
  async migrate(): Promise<void> {
    await this.catalog.migrate();
  }
  private async change(
    environment: string,
    actor: string,
    operation: string,
    target: string,
    apply: (c: PoolClient) => Promise<unknown>,
  ): Promise<string> {
    return this.catalog.transaction(async (c) => {
      const env = await c.query(
        'SELECT security_epoch FROM moria.environments WHERE id=$1 FOR UPDATE',
        [environment],
      );
      if (!env.rowCount) throw new MoriaError('ENVIRONMENT_UNKNOWN', 'Unknown environment');
      if ((await apply(c)) === false) return env.rows[0].security_epoch;
      const epoch = (
        await c.query(
          'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch',
          [environment],
        )
      ).rows[0].security_epoch;
      await c.query(
        'INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,$3,$4,$5)',
        [environment, actor, operation, target, epoch],
      );
      return epoch;
    }, environment);
  }
  async syncPrincipal(
    environment: string,
    actor: string,
    principal: string,
    state: PrincipalState,
  ): Promise<string> {
    return this.change(environment, actor, 'syncPrincipal', principal, async (c) => {
      const previous = (
        await c.query(
          'SELECT enabled,kind,attributes,markings FROM moria.principals WHERE environment=$1 AND principal=$2',
          [environment, principal],
        )
      ).rows[0];
      await c.query(
        `INSERT INTO moria.principals VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()) ON CONFLICT(environment,principal) DO UPDATE SET enabled=EXCLUDED.enabled,kind=EXCLUDED.kind,attributes=EXCLUDED.attributes,markings=EXCLUDED.markings,synced_at=EXCLUDED.synced_at`,
        [
          environment,
          principal,
          state.enabled,
          state.kind,
          JSON.stringify(state.attributes),
          state.markings,
        ],
      );
      return !previous || fingerprint(previous) !== fingerprint(state);
    });
  }
  async setApplication(
    environment: string,
    actor: string,
    application: string,
    operations: string[],
    enabled = true,
  ): Promise<string> {
    return this.change(environment, actor, 'setApplication', application, (c) =>
      c.query(
        `INSERT INTO moria.application_scopes VALUES($1,$2,$3,$4) ON CONFLICT(environment,application) DO UPDATE SET operations=EXCLUDED.operations,enabled=EXCLUDED.enabled`,
        [environment, application, operations, enabled],
      ),
    );
  }
  async setGrants(
    environment: string,
    actor: string,
    principal: string,
    application: string,
    operations: string[],
  ): Promise<string> {
    return this.change(environment, actor, 'setGrants', `${principal}/${application}`, (c) =>
      c.query(
        `INSERT INTO moria.permissions VALUES($1,$2,$3,$4) ON CONFLICT(environment,principal,application) DO UPDATE SET operations=EXCLUDED.operations`,
        [environment, principal, application, operations],
      ),
    );
  }
  /** One statement gives a coherent current policy epoch, identity and scopes. */
  async assertCurrent(
    c: PoolClient,
    environment: string,
    principal: string,
    application: string,
    epoch: string,
    sessionHash?: string,
  ): Promise<void> {
    const result = await c.query(
      `SELECT e.security_epoch,p.enabled,p.kind,p.synced_at>clock_timestamp()-interval '60 seconds' AS fresh,a.enabled AS app_enabled FROM moria.environments e JOIN moria.principals p ON p.environment=e.id AND p.principal=$2 JOIN moria.application_scopes a ON a.environment=e.id AND a.application=$3 WHERE e.id=$1`,
      [environment, principal, application],
    );
    const row = result.rows[0];
    if (!row || row.security_epoch !== epoch || !row.enabled || !row.app_enabled)
      throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
    if (row.kind === 'human' && !row.fresh)
      throw new MoriaError('IDENTITY_STALE', 'Identity synchronization is stale');
    if (sessionHash) {
      const session = await c.query(
        `SELECT 1 FROM moria.workload_sessions WHERE token_hash=$1 AND environment=$2 AND principal=$3 AND application=$4 AND NOT disabled AND expires_at>clock_timestamp() AND $5='workload'
        UNION ALL SELECT 1 FROM moria.browser_sessions s LEFT JOIN moria.auth_providers ap ON ap.environment=s.environment AND ap.application=s.application AND ap.provider=s.provider
        WHERE s.token_hash=$1 AND s.environment=$2 AND s.principal=$3 AND s.application=$4 AND NOT s.disabled AND s.expires_at>clock_timestamp()
        AND ((s.provider IS NULL AND $5='workload') OR (s.provider IS NOT NULL AND $5='human' AND ap.enabled AND ap.revision=s.revision AND s.verified_at>clock_timestamp()-interval '60 seconds'))`,
        [sessionHash, environment, principal, application, row.kind],
      );
      if (!session.rowCount)
        throw new MoriaError('UNAUTHENTICATED', 'Session expired or was revoked');
    }
  }
  async current(
    environment: string,
    principal: string,
    application: string,
  ): Promise<SecuritySnapshot> {
    const result = await this.catalog.pool.query(
      `SELECT e.security_epoch,p.enabled,p.kind,p.attributes,p.markings,
      p.synced_at>clock_timestamp()-interval '60 seconds' AS fresh,a.enabled AS app_enabled,a.operations AS scopes,g.operations AS grants
      FROM moria.environments e JOIN moria.principals p ON p.environment=e.id AND p.principal=$2
      JOIN moria.application_scopes a ON a.environment=e.id AND a.application=$3
      LEFT JOIN moria.permissions g ON g.environment=e.id AND g.principal=p.principal AND g.application=a.application
      WHERE e.id=$1`,
      [environment, principal, application],
    );
    const row = result.rows[0];
    if (!row || !row.enabled || !row.app_enabled)
      throw new MoriaError('FORBIDDEN', 'Identity or application is unavailable');
    if (row.kind === 'human' && !row.fresh)
      throw new MoriaError('IDENTITY_STALE', 'Identity synchronization is stale');
    return {
      epoch: row.security_epoch,
      attributes: row.attributes,
      identity: {
        environment,
        principal,
        application,
        markings: new Set(row.markings),
        grantedOperations: new Set(row.grants ?? []),
        applicationScopes: new Set(row.scopes),
      },
    };
  }
}
