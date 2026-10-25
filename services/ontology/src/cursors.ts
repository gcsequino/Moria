import { randomBytes, createHash } from 'node:crypto';
import { Catalog } from '../../catalog/src/catalog.js';
import { fingerprint } from '../../../packages/compiler/src/index.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import type { PoolClient } from 'pg';

/** Identity and epoch must come from the current trusted security authority. */
export interface CursorBinding {
  environment: string;
  principal: string;
  application: string;
  delegation: string[];
  contract: string;
  securityEpoch: string;
  query: unknown;
}
export interface ReadPin {
  revision: string;
  currentRevision: string;
  after: unknown;
  expiresAt: string;
}
const digest = (token: string) => createHash('sha256').update(token).digest('hex');

export class CursorStore {
  constructor(readonly catalog: Catalog) {}
  async migrate(): Promise<void> {
    await this.catalog.migrate();
  }
  private binding(binding: CursorBinding): string {
    const { securityEpoch, ...rest } = binding;
    return fingerprint(rest);
  }
  async issue(binding: CursorBinding, after: unknown): Promise<{ token: string; pin: ReadPin }> {
    return this.catalog.transaction(async (c) => {
      const env = await c.query(
        'SELECT revision,security_epoch FROM moria.environments WHERE id=$1 FOR UPDATE',
        [binding.environment],
      );
      if (!env.rowCount || env.rows[0].security_epoch !== binding.securityEpoch)
        throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
      const token = randomBytes(32).toString('base64url');
      const result = await c.query(
        `INSERT INTO moria.read_cursors VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()+interval '15 minutes') RETURNING expires_at`,
        [
          digest(token),
          binding.environment,
          this.binding(binding),
          binding.securityEpoch,
          env.rows[0].revision,
          JSON.stringify(after),
        ],
      );
      return {
        token,
        pin: {
          revision: env.rows[0].revision,
          currentRevision: env.rows[0].revision,
          after,
          expiresAt: result.rows[0].expires_at.toISOString(),
        },
      };
    }, binding.environment);
  }
  async resume(token: string, binding: CursorBinding): Promise<ReadPin> {
    return this.catalog.transaction(
      (c) => this.resumeIn(c, token, binding),
      binding.environment,
      'repeatable-read',
      'shared',
    );
  }
  /** Reuses a caller's protected read transaction; no transaction spans pages. */
  async resumeIn(c: PoolClient, token: string, binding: CursorBinding): Promise<ReadPin> {
    const env = await c.query(
      'SELECT revision,security_epoch FROM moria.environments WHERE id=$1 FOR SHARE',
      [binding.environment],
    );
    if (!env.rowCount || env.rows[0].security_epoch !== binding.securityEpoch)
      throw new MoriaError('AUTHORIZATION_CHANGED', 'Current authorization must be rechecked');
    const result = await c.query(
      'SELECT *,expires_at<=clock_timestamp() AS expired FROM moria.read_cursors WHERE token_hash=$1',
      [digest(token)],
    );
    const cursor = result.rows[0];
    if (!cursor || cursor.binding_hash !== this.binding(binding))
      throw new MoriaError('CURSOR_INVALID', 'Cursor does not belong to this query and identity');
    if (cursor.expired) throw new MoriaError('CURSOR_EXPIRED', 'Cursor lifetime expired');
    if (cursor.security_epoch !== binding.securityEpoch)
      throw new MoriaError('CURSOR_INVALIDATED', 'Authorization changed since the first page');
    return {
      revision: cursor.revision,
      currentRevision: env.rows[0].revision,
      after: cursor.position,
      expiresAt: cursor.expires_at.toISOString(),
    };
  }
  /** Internal: pin must have been established in the same protected transaction. */
  async saveIn(
    c: PoolClient,
    binding: CursorBinding,
    pin: Pick<ReadPin, 'revision' | 'expiresAt'>,
    after: unknown,
  ): Promise<string> {
    const token = randomBytes(32).toString('base64url');
    const result = await c.query(
      'INSERT INTO moria.read_cursors SELECT $1,$2,$3,$4,$5,$6,$7::timestamptz WHERE $7::timestamptz>clock_timestamp() RETURNING token_hash',
      [
        digest(token),
        binding.environment,
        this.binding(binding),
        binding.securityEpoch,
        pin.revision,
        JSON.stringify(after),
        pin.expiresAt,
      ],
    );
    if (!result.rowCount) throw new MoriaError('CURSOR_EXPIRED', 'Cursor lifetime expired');
    return token;
  }
  /** Each next-page token preserves the original pin and deadline. */
  async advance(token: string, binding: CursorBinding, after: unknown): Promise<string> {
    return this.catalog.transaction(
      async (c) => this.saveIn(c, binding, await this.resumeIn(c, token, binding), after),
      binding.environment,
      'repeatable-read',
      'shared',
    );
  }
}
