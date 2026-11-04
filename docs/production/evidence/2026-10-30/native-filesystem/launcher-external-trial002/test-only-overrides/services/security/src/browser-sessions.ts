import { createHash, randomBytes, randomUUID, createCipheriv, createDecipheriv } from 'node:crypto';
import type { PoolClient } from 'pg';
import { Authority } from './authority.js';
import { OidcAdapter, type OidcProvider } from './oidc.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { SessionContext } from '../../ontology/src/service.js';
import type { Value } from '../../../packages/contracts/types.js';

export const digest = (value: string) => createHash('sha256').update(value).digest('hex');
export const secret = () => randomBytes(32).toString('base64url');
const denied = () =>
  new MoriaError('UNAUTHENTICATED', 'Sign in again. Your session is unavailable or expired.');

type BrowserIdentityRow = {
  token_hash: string;
  environment: string;
  application: string;
  principal: string;
  provider: string;
  revision: string;
  subject: string;
  email: string;
  access_token: string;
  verified_at: Date;
  verified_anchor: string;
  expires_at: Date;
};

/** Persistent, app-bound sessions; this service is trusted and is never shipped to React. */
export class BrowserSessions {
  readonly providers: Map<string, OidcAdapter>;
  constructor(
    readonly authority: Authority,
    readonly binding: {
      environment: string;
      application: string;
      origin: string;
      authenticationOrigin?: string;
      entryPath: string;
      initialOperations: string[];
      recoveryPrincipal: string;
      initialPrincipal?: { attributes: Record<string, Value>; markings: string[] };
    },
    providers: OidcProvider[],
    private readonly encryptionKey: Buffer,
    testHttp = false,
  ) {
    if (binding.authenticationOrigin) {
      const target = new URL(binding.origin),
        issuer = new URL(binding.authenticationOrigin);
      if (
        issuer.origin !== binding.authenticationOrigin ||
        target.origin !== binding.origin ||
        issuer.hostname !== target.hostname ||
        issuer.protocol !== target.protocol ||
        !['http:', 'https:'].includes(issuer.protocol) ||
        issuer.username ||
        issuer.password
      )
        throw new MoriaError(
          'AUTH_CONFIG_INVALID',
          'Authentication and application origins must use the same exact host and scheme.',
        );
    }
    this.providers = new Map(providers.map((p) => [p.id, new OidcAdapter(p, testHttp)]));
  }
  private revision(adapter: OidcAdapter) {
    return digest(JSON.stringify(adapter.provider));
  }
  private seal(value: string) {
    const iv = randomBytes(12),
      cipher = createCipheriv('aes-256-gcm', this.encryptionKey, iv);
    return Buffer.concat([iv, cipher.update(value), cipher.final(), cipher.getAuthTag()]).toString(
      'base64',
    );
  }
  private unseal(value: string) {
    const bytes = Buffer.from(value, 'base64');
    const cipher = createDecipheriv('aes-256-gcm', this.encryptionKey, bytes.subarray(0, 12));
    cipher.setAuthTag(bytes.subarray(-16));
    return Buffer.concat([cipher.update(bytes.subarray(12, -16)), cipher.final()]).toString();
  }
  private async audit(c: PoolClient, operation: string, principal: string) {
    const epoch = (
      await c.query(
        'UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=$1 RETURNING security_epoch',
        [this.binding.environment],
      )
    ).rows[0].security_epoch;
    await c.query(
      'INSERT INTO moria.security_audit(environment,actor,operation,target,epoch) VALUES($1,$2,$3,$4,$5)',
      [this.binding.environment, principal, operation, this.binding.application, epoch],
    );
  }
  async initialize() {
    await this.authority.migrate();
    const { environment, application } = this.binding;
    await this.authority.catalog.transaction(async (c) => {
      const previous = (
        await c.query(
          'SELECT provider,revision,enabled FROM moria.auth_providers WHERE environment=$1 AND application=$2',
          [environment, application],
        )
      ).rows;
      let changed = false;
      for (const old of previous) {
        const adapter = this.providers.get(old.provider);
        if (old.enabled && (!adapter || old.revision !== this.revision(adapter))) {
          changed = true;
          await c.query(
            'UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE environment=$1 AND application=$2 AND provider=$3',
            [environment, application, old.provider],
          );
        }
      }
      await c.query(
        'UPDATE moria.auth_providers SET enabled=false WHERE environment=$1 AND application=$2',
        [environment, application],
      );
      for (const [id, adapter] of this.providers) {
        if (
          !previous.some(
            (p) => p.provider === id && p.enabled && p.revision === this.revision(adapter),
          )
        )
          changed = true;
        await c.query(
          `INSERT INTO moria.auth_providers VALUES($1,$2,$3,$4,true)
          ON CONFLICT(environment,application,provider) DO UPDATE SET revision=EXCLUDED.revision,enabled=true`,
          [environment, application, id, this.revision(adapter)],
        );
      }
      if (changed) await this.audit(c, 'configureAuthProviders', 'local-operator');
      await c.query('DELETE FROM moria.auth_flows WHERE expires_at<clock_timestamp()');
      await c.query(
        "DELETE FROM moria.browser_sessions s WHERE expires_at<clock_timestamp() AND NOT EXISTS(SELECT 1 FROM moria.runtime_job_delegations d WHERE d.origin_kind='session' AND d.origin_session_hash=s.token_hash)",
      );
    }, environment);
  }
  private async provider(id: string) {
    const adapter = this.providers.get(id);
    if (!adapter) throw denied();
    const result = await this.authority.catalog.pool.query(
      `SELECT 1 FROM moria.auth_providers WHERE environment=$1 AND application=$2 AND provider=$3 AND enabled AND revision=$4`,
      [this.binding.environment, this.binding.application, id, this.revision(adapter)],
    );
    if (!result.rowCount) throw denied();
    return adapter;
  }
  private async refreshIdentity(row: BrowserIdentityRow, acceptedExecution?: string) {
    const adapter = await this.provider(row.provider);
    if (row.revision !== this.revision(adapter) || !adapter.provider.emails.includes(row.email))
      throw denied();
    const client = await this.authority.catalog.pool.connect(),
      key = `moria/oidc-verify/${row.token_hash}`;
    const locks: string[] = [];
    let locked = false,
      began = false;
    const valid = async () => {
      const found = await client.query(
        `SELECT 1 FROM moria.browser_sessions s
        JOIN moria.principals p USING(environment,principal)
        JOIN moria.application_scopes a USING(environment,application)
        JOIN moria.auth_providers provider ON provider.environment=s.environment AND provider.application=s.application AND provider.provider=s.provider
        LEFT JOIN moria.instance_principal_bindings b ON b.environment=p.environment AND b.principal=p.principal
        LEFT JOIN moria.instance_principals i ON i.id=b.instance_id
        WHERE s.token_hash=$1 AND s.environment=$2 AND s.application=$3 AND s.principal=$4
          AND s.provider=$5 AND s.revision=$6 AND s.subject=$7 AND s.email=$8 AND s.access_token=$9
          AND NOT s.disabled AND p.enabled AND p.kind='human' AND a.enabled
          AND (b.instance_id IS NULL OR (i.enabled AND i.kind=p.kind))
          AND provider.enabled AND provider.revision=s.revision
          AND ($10::boolean OR s.expires_at>clock_timestamp())`,
        [
          row.token_hash,
          this.binding.environment,
          this.binding.application,
          row.principal,
          row.provider,
          row.revision,
          row.subject,
          row.email,
          row.access_token,
          acceptedExecution !== undefined,
        ],
      );
      if (!found.rowCount) throw denied();
      await client.query('SELECT moria.runtime_environment_cell_check($1)', [
        this.binding.environment,
      ]);
      if (acceptedExecution) {
        const proof = (
          await client.query(
            "SELECT moria.runtime_job_delegation_guard(e.id) AS proof FROM moria.runtime_executions e WHERE e.id=$1 AND e.status IN('queued','running','preparing') AND e.deadline>clock_timestamp()",
            [acceptedExecution],
          )
        ).rows[0]?.proof;
        if (
          !proof ||
          proof.originSessionHash !== row.token_hash ||
          proof.environment !== row.environment ||
          proof.principal !== row.principal ||
          proof.application !== row.application
        )
          throw denied();
      }
    };
    try {
      locked = (
        await client.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS locked', [key])
      ).rows[0].locked;
      if (!locked) throw denied();
      await valid();
      const profile = await adapter.profile(this.unseal(row.access_token), row.subject);
      if (profile.email !== row.email || !adapter.provider.emails.includes(profile.email))
        throw denied();
      await client.query("SET lock_timeout='5s'");
      for (const resource of [
        `moria/environment/${this.binding.environment}`,
        'moria/filesystem',
      ]) {
        await client.query('SELECT pg_advisory_lock(hashtextextended($1,0))', [resource]);
        locks.push(resource);
      }
      await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
      began = true;
      await valid();
      const updated = await client.query(
        'UPDATE moria.browser_sessions SET verified_at=clock_timestamp() WHERE token_hash=$1 AND verified_at::text=$2 RETURNING principal',
        [row.token_hash, row.verified_anchor],
      );
      if (!updated.rowCount) throw denied();
      const principal = await client.query(
        "UPDATE moria.principals SET synced_at=clock_timestamp() WHERE environment=$1 AND principal=$2 AND enabled AND kind='human' RETURNING principal",
        [row.environment, row.principal],
      );
      if (!principal.rowCount) throw denied();
      await client.query('COMMIT');
      began = false;
    } finally {
      let discard: Error | undefined;
      try {
        if (began) await client.query('ROLLBACK');
        for (const resource of locks.reverse())
          await client.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [resource]);
        if (locked) await client.query('SELECT pg_advisory_unlock(hashtextextended($1,0))', [key]);
        await client.query('RESET lock_timeout');
      } catch (error) {
        discard = error instanceof Error ? error : new Error('Identity verifier cleanup failed');
        throw discard;
      } finally {
        client.release(discard);
      }
    }
  }
  /** Bounded IdP evidence refresh for actual accepted human jobs; never renews session TTL. */
  startAcceptedRefresh() {
    let stopping = false,
      active: Promise<void> | undefined;
    const tick = async () => {
      const rows = (
        await this.authority.catalog.pool.query<BrowserIdentityRow & { execution: string }>(
          `SELECT s.*,s.verified_at::text AS verified_anchor,e.id AS execution FROM moria.browser_sessions s
        JOIN LATERAL (SELECT d.execution FROM moria.runtime_job_delegations d JOIN moria.runtime_executions e ON e.id=d.execution
          WHERE d.origin_kind='session' AND d.origin_session_hash=s.token_hash AND e.status IN('queued','running','preparing') AND e.deadline>clock_timestamp()
          ORDER BY e.created_at LIMIT 1) accepted ON true JOIN moria.runtime_executions e ON e.id=accepted.execution
        WHERE s.environment=$1 AND s.application=$2 AND NOT s.disabled AND s.provider IS NOT NULL AND s.verified_at<clock_timestamp()-interval '30 seconds'
        ORDER BY s.verified_at,s.token_hash LIMIT 16`,
          [this.binding.environment, this.binding.application],
        )
      ).rows;
      for (const row of rows) {
        if (stopping) break;
        try {
          await this.refreshIdentity(row, row.execution);
        } catch {
          /* Failed verification leaves freshness unchanged. */
        }
      }
    };
    const timer = setInterval(() => {
      if (!active && !stopping) {
        active = tick()
          .catch(() => {})
          .finally(() => {
            active = undefined;
          });
      }
    }, 10000);
    timer.unref();
    return async () => {
      stopping = true;
      clearInterval(timer);
      await active;
    };
  }
  async start(provider: string) {
    const adapter = await this.provider(provider);
    const state = secret(),
      browser = secret(),
      nonce = secret(),
      verifier = secret();
    const location = await adapter.start(
      `${this.binding.authenticationOrigin ?? this.binding.origin}/api/auth/callback`,
      state,
      nonce,
      verifier,
    );
    await this.authority.catalog.pool.query(
      `INSERT INTO moria.auth_flows(state_hash,environment,application,provider,revision,browser_hash,nonce,verifier,expires_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '5 minutes')`,
      [
        digest(state),
        this.binding.environment,
        this.binding.application,
        provider,
        this.revision(adapter),
        digest(browser),
        nonce,
        verifier,
      ],
    );
    return { location, browser };
  }
  async finish(url: URL, browser: string) {
    const { environment, application } = this.binding;
    const flow = (
      await this.authority.catalog.pool.query(
        `DELETE FROM moria.auth_flows WHERE state_hash=$1 AND browser_hash=$2
      AND environment=$3 AND application=$4 AND expires_at>clock_timestamp() AND provider IS NOT NULL RETURNING *`,
        [digest(url.searchParams.get('state') ?? ''), digest(browser), environment, application],
      )
    ).rows[0];
    if (!flow) throw denied();
    const adapter = await this.provider(flow.provider);
    if (flow.revision !== this.revision(adapter)) throw denied();
    let user;
    try {
      user = await adapter.finish(url, url.searchParams.get('state')!, flow.nonce, flow.verifier);
    } catch {
      throw new MoriaError(
        'UNAUTHENTICATED',
        'The identity provider could not verify this sign-in. Please try again.',
      );
    }
    if (!adapter.provider.emails.includes(user.email))
      throw new MoriaError('FORBIDDEN', 'This account has not been invited to this application.');
    const token = secret();
    await this.authority.catalog.transaction(async (c) => {
      const enabled = await c.query(
        'SELECT 1 FROM moria.auth_providers WHERE environment=$1 AND application=$2 AND provider=$3 AND revision=$4 AND enabled',
        [environment, application, flow.provider, flow.revision],
      );
      if (!enabled.rowCount) throw denied();
      // Invitations bind once to an issuer+subject, never merge accounts by email.
      const invite = (
        await c.query(
          'SELECT subject FROM moria.auth_invitations WHERE environment=$1 AND application=$2 AND issuer=$3 AND email=$4',
          [environment, application, adapter.provider.issuer, user.email],
        )
      ).rows[0];
      if (invite && invite.subject !== user.subject)
        throw new MoriaError('FORBIDDEN', 'This invitation is already bound to another identity.');
      let identity = (
        await c.query(
          'SELECT principal FROM moria.external_identities WHERE environment=$1 AND issuer=$2 AND subject=$3',
          [environment, adapter.provider.issuer, user.subject],
        )
      ).rows[0];
      if (!identity) {
        identity = { principal: `user-${randomUUID()}` };
        await c.query(
          `INSERT INTO moria.principals VALUES($1,$2,true,'human',$3,$4,clock_timestamp())`,
          [
            environment,
            identity.principal,
            JSON.stringify(this.binding.initialPrincipal?.attributes ?? {}),
            this.binding.initialPrincipal?.markings ?? [],
          ],
        );
        await c.query('INSERT INTO moria.external_identities VALUES($1,$2,$3,$4)', [
          environment,
          adapter.provider.issuer,
          user.subject,
          identity.principal,
        ]);
      }
      if (!invite) {
        await c.query('INSERT INTO moria.auth_invitations VALUES($1,$2,$3,$4,$5)', [
          environment,
          application,
          adapter.provider.issuer,
          user.email,
          user.subject,
        ]);
        // Do not restore a grant that an operator has already removed.
        await c.query('INSERT INTO moria.permissions VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING', [
          environment,
          identity.principal,
          application,
          this.binding.initialOperations,
        ]);
      }
      const fresh = await c.query(
        `UPDATE moria.principals SET synced_at=clock_timestamp() WHERE environment=$1 AND principal=$2 AND enabled AND kind='human' RETURNING principal`,
        [environment, identity.principal],
      );
      if (!fresh.rowCount) throw new MoriaError('FORBIDDEN', 'This identity has been disabled.');
      await c.query(
        `INSERT INTO moria.browser_sessions(token_hash,environment,principal,application,provider,revision,subject,email,display_name,access_token,verified_at,expires_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp(),clock_timestamp()+$11*interval '1 second')`,
        [
          digest(token),
          environment,
          identity.principal,
          application,
          flow.provider,
          flow.revision,
          user.subject,
          user.email,
          user.name,
          this.seal(user.accessToken),
          Math.max(1, user.seconds),
        ],
      );
      await this.audit(c, 'login', identity.principal);
    }, environment);
    return token;
  }
  async authenticate(header: string | undefined): Promise<SessionContext> {
    const token = /^Bearer ([A-Za-z0-9_-]{43})$/.exec(header ?? '')?.[1];
    if (!token) throw denied();
    const sessionHash = digest(token),
      { environment, application } = this.binding;
    const row = (
      await this.authority.catalog.pool.query(
        `SELECT s.*,s.verified_at::text AS verified_anchor FROM moria.browser_sessions s JOIN moria.principals p USING(environment,principal)
      WHERE s.token_hash=$1 AND s.environment=$2 AND s.application=$3 AND NOT s.disabled AND p.enabled AND s.expires_at>clock_timestamp()`,
        [sessionHash, environment, application],
      )
    ).rows[0];
    if (!row) throw denied();
    if (row.provider) {
      const adapter = await this.provider(row.provider);
      if (row.revision !== this.revision(adapter) || !adapter.provider.emails.includes(row.email))
        throw denied();
      if (Date.now() - row.verified_at.getTime() > 30000) {
        try {
          await this.refreshIdentity(row);
        } catch {
          throw new MoriaError(
            'UNAUTHENTICATED',
            'Identity refresh failed. Sign in again when your provider is available.',
          );
        }
      }
    }
    const state = await this.authority.current(environment, row.principal, application);
    await this.authority.catalog.transaction(
      (c) =>
        this.authority.assertCurrent(
          c,
          environment,
          row.principal,
          application,
          state.epoch,
          sessionHash,
        ),
      environment,
    );
    return { environment, principal: row.principal, application, sessionHash };
  }
  async describe(token: string) {
    const context = await this.authenticate(`Bearer ${token}`);
    const row = (
      await this.authority.catalog.pool.query(
        'SELECT display_name,provider,expires_at FROM moria.browser_sessions WHERE token_hash=$1',
        [context.sessionHash],
      )
    ).rows[0];
    return {
      principal: context.principal,
      name: row.display_name,
      recovery: row.provider === null,
      expiresAt: row.expires_at,
    };
  }
  async logout(token: string) {
    await this.authority.catalog.transaction(async (c) => {
      const result = await c.query(
        'UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE token_hash=$1 AND environment=$2 AND application=$3 AND NOT disabled RETURNING principal',
        [digest(token), this.binding.environment, this.binding.application],
      );
      if (result.rowCount) await this.audit(c, 'logout', result.rows[0].principal);
    }, this.binding.environment);
  }
  async recoveryTicket() {
    const ticket = secret();
    await this.authority.catalog.pool.query(
      `INSERT INTO moria.auth_flows(state_hash,environment,application,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '5 minutes')`,
      [digest(ticket), this.binding.environment, this.binding.application],
    );
    return ticket;
  }
  async recover(ticket: string) {
    const { environment, application, recoveryPrincipal } = this.binding,
      token = secret();
    const state = await this.authority.current(environment, recoveryPrincipal, application);
    await this.authority.catalog.transaction(async (c) => {
      const result = await c.query(
        `DELETE FROM moria.auth_flows WHERE state_hash=$1 AND environment=$2 AND application=$3 AND provider IS NULL AND expires_at>clock_timestamp() RETURNING state_hash`,
        [digest(ticket), environment, application],
      );
      if (!result.rowCount) throw denied();
      await this.authority.assertCurrent(
        c,
        environment,
        recoveryPrincipal,
        application,
        state.epoch,
      );
      const principal = (
        await c.query('SELECT kind FROM moria.principals WHERE environment=$1 AND principal=$2', [
          environment,
          recoveryPrincipal,
        ])
      ).rows[0];
      if (principal.kind !== 'workload') throw denied();
      await c.query(
        `INSERT INTO moria.browser_sessions(token_hash,environment,principal,application,display_name,verified_at,expires_at)
        VALUES($1,$2,$3,$4,'Local recovery',clock_timestamp(),clock_timestamp()+interval '15 minutes')`,
        [digest(token), environment, recoveryPrincipal, application],
      );
      await this.audit(c, 'localRecoveryLogin', recoveryPrincipal);
    }, environment);
    return token;
  }
}
