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

/** Persistent, app-bound sessions; this service is trusted and is never shipped to React. */
export class BrowserSessions {
  readonly providers: Map<string, OidcAdapter>;
  constructor(
    readonly authority: Authority,
    readonly binding: {
      environment: string;
      application: string;
      origin: string;
      entryPath: string;
      initialOperations: string[];
      recoveryPrincipal: string;
      initialPrincipal?: { attributes: Record<string, Value>; markings: string[] };
    },
    providers: OidcProvider[],
    private readonly encryptionKey: Buffer,
    testHttp = false,
  ) {
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
      await c.query('DELETE FROM moria.browser_sessions WHERE expires_at<clock_timestamp()');
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
  async start(provider: string) {
    const adapter = await this.provider(provider);
    const state = secret(),
      browser = secret(),
      nonce = secret(),
      verifier = secret();
    const location = await adapter.start(
      `${this.binding.origin}/api/auth/callback`,
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
        `SELECT s.* FROM moria.browser_sessions s JOIN moria.principals p USING(environment,principal)
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
          const profile = await adapter.profile(this.unseal(row.access_token), row.subject);
          if (profile.email !== row.email) throw denied();
        } catch {
          throw new MoriaError(
            'UNAUTHENTICATED',
            'Identity refresh failed. Sign in again when your provider is available.',
          );
        }
        await this.authority.catalog.transaction(async (c) => {
          const session = await c.query(
            `UPDATE moria.browser_sessions SET verified_at=clock_timestamp() WHERE token_hash=$1 AND NOT disabled AND expires_at>clock_timestamp() RETURNING principal`,
            [sessionHash],
          );
          if (!session.rowCount) throw denied();
          await c.query(
            'UPDATE moria.principals SET synced_at=clock_timestamp() WHERE environment=$1 AND principal=$2 AND enabled',
            [environment, row.principal],
          );
        }, environment);
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
