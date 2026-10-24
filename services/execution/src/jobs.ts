import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { Catalog } from '../../catalog/src/catalog.js';
import { MoriaError } from '../../../packages/contracts/types.js';

export type JobContext = {
  id: string;
  input: unknown;
  guard: (client?: PoolClient) => Promise<void>;
  phase: (phase: string) => Promise<void>;
  complete: (client: PoolClient, result: unknown) => Promise<void>;
};
export type JobHandler = (job: JobContext) => Promise<unknown>;
type Guard = (client: PoolClient) => Promise<void>;

/** Trusted, fixed-handler execution queue. No arbitrary code or URLs supplied by callers. */
export class Jobs {
  private stopped = false;
  private timer?: ReturnType<typeof setTimeout>;
  private active?: Promise<void>;
  constructor(
    readonly catalog: Catalog,
    readonly handlers: Record<string, JobHandler>,
  ) {}
  async migrate() {
    await this.catalog.pool.query(`
      CREATE TABLE IF NOT EXISTS moria.jobs (
        id uuid PRIMARY KEY, kind text NOT NULL, input jsonb NOT NULL,
        request_key text NOT NULL UNIQUE, requested_by text NOT NULL,
        status text NOT NULL DEFAULT 'queued', phase text NOT NULL DEFAULT 'Queued',
        attempt integer NOT NULL DEFAULT 0, fence uuid,
        available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        heartbeat_at timestamptz, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), error text, result jsonb
      );
      CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_kind ON moria.jobs(kind)
        WHERE status IN ('queued','running','retrying');
      CREATE TABLE IF NOT EXISTS moria.job_attempts (
        job_id uuid NOT NULL REFERENCES moria.jobs(id), attempt integer NOT NULL,
        status text NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        finished_at timestamptz, error text, PRIMARY KEY(job_id,attempt)
      );
      CREATE TABLE IF NOT EXISTS moria.job_requests (
        request_key text PRIMARY KEY, job_id uuid NOT NULL REFERENCES moria.jobs(id)
      );
      INSERT INTO moria.job_requests(request_key,job_id) SELECT request_key,id FROM moria.jobs ON CONFLICT DO NOTHING;
      CREATE TABLE IF NOT EXISTS moria.job_schedules (
        kind text PRIMARY KEY, enabled boolean NOT NULL DEFAULT false,
        interval_minutes integer NOT NULL, next_run timestamptz NOT NULL DEFAULT clock_timestamp()
      );
    `);
    for (const kind of Object.keys(this.handlers))
      await this.catalog.pool.query(
        'INSERT INTO moria.job_schedules(kind,interval_minutes) VALUES($1,$2) ON CONFLICT DO NOTHING',
        [kind, kind === 'weather' ? 60 : 360],
      );
  }
  private known(kind: string) {
    if (!Object.hasOwn(this.handlers, kind))
      throw new MoriaError('INPUT_INVALID', 'Unknown workload.');
  }
  async enqueue(kind: string, input: unknown, requestKey: string, actor: string, check?: Guard) {
    this.known(kind);
    if (!/^[a-zA-Z0-9:/_.-]{1,180}$/.test(requestKey))
      throw new MoriaError('INPUT_INVALID', 'A bounded request key is required.');
    return this.catalog.transaction(async (c) => {
      await check?.(c);
      await c.query("SELECT pg_advisory_xact_lock(hashtextextended('moria/jobs/enqueue',0))");
      const old = (
        await c.query(
          'SELECT j.* FROM moria.job_requests r JOIN moria.jobs j ON j.id=r.job_id WHERE r.request_key=$1',
          [requestKey],
        )
      ).rows[0];
      if (old) {
        if (old.kind !== kind || JSON.stringify(old.input) !== JSON.stringify(input)) {
          // jsonb canonical ordering differs from input; compare through PostgreSQL.
          const same = (
            await c.query('SELECT input=$2::jsonb AS same FROM moria.jobs WHERE id=$1', [
              old.id,
              JSON.stringify(input),
            ])
          ).rows[0].same;
          if (old.kind !== kind || !same)
            throw new MoriaError(
              'IDEMPOTENCY_CONFLICT',
              'Request key was used with different input.',
            );
        }
        return this.publicJob(old);
      }
      const active = (
        await c.query(
          "SELECT * FROM moria.jobs WHERE kind=$1 AND status IN ('queued','running','retrying')",
          [kind],
        )
      ).rows[0];
      if (active) {
        const same = (
          await c.query('SELECT input=$2::jsonb AS same FROM moria.jobs WHERE id=$1', [
            active.id,
            JSON.stringify(input),
          ])
        ).rows[0].same;
        if (!same)
          throw new MoriaError(
            'WORKLOAD_BUSY',
            'This workload already has an active job with different input.',
          );
        await c.query('INSERT INTO moria.job_requests(request_key,job_id) VALUES($1,$2)', [
          requestKey,
          active.id,
        ]);
        return this.publicJob(active);
      }
      const row = (
        await c.query(
          'INSERT INTO moria.jobs(id,kind,input,request_key,requested_by) VALUES($1,$2,$3,$4,$5) RETURNING *',
          [randomUUID(), kind, JSON.stringify(input), requestKey, actor],
        )
      ).rows[0];
      await c.query('INSERT INTO moria.job_requests(request_key,job_id) VALUES($1,$2)', [
        requestKey,
        row.id,
      ]);
      return this.publicJob(row);
    });
  }
  private publicJob(row: any) {
    return {
      id: row.id,
      kind: row.kind,
      status: row.status,
      phase: row.phase,
      attempt: row.attempt,
      created_at: row.created_at,
      updated_at: row.updated_at,
      error: row.error,
      requested_by: row.requested_by,
      result: row.result === null ? null : JSON.stringify(row.result),
    };
  }
  async inspect() {
    const jobs = (
      await this.catalog.pool.query('SELECT * FROM moria.jobs ORDER BY created_at DESC LIMIT 60')
    ).rows.map((r) => this.publicJob(r));
    const schedules = (
      await this.catalog.pool.query('SELECT * FROM moria.job_schedules ORDER BY kind')
    ).rows;
    return { jobs, schedules };
  }
  async schedule(kind: string, enabled: boolean, minutes: number, check?: Guard) {
    this.known(kind);
    if (
      typeof enabled !== 'boolean' ||
      !Number.isInteger(minutes) ||
      minutes < 60 ||
      minutes > 10080
    )
      throw new MoriaError('INPUT_INVALID', 'Schedule must be 60–10080 minutes.');
    return this.catalog.transaction(async (c) => {
      await check?.(c);
      return (
        await c.query(
          'UPDATE moria.job_schedules SET enabled=$2,interval_minutes=$3,next_run=clock_timestamp() WHERE kind=$1 RETURNING *',
          [kind, enabled, minutes],
        )
      ).rows[0];
    });
  }
  async retry(id: string, check?: Guard) {
    if (!/^[0-9a-f-]{36}$/i.test(id)) throw new MoriaError('INPUT_INVALID', 'Invalid job ID.');
    return this.catalog.transaction(async (c) => {
      await check?.(c);
      await c.query("SELECT pg_advisory_xact_lock(hashtextextended('moria/jobs/enqueue',0))");
      const active = await c.query(
        "SELECT 1 FROM moria.jobs WHERE kind=(SELECT kind FROM moria.jobs WHERE id=$1) AND status IN ('queued','running','retrying')",
        [id],
      );
      if (active.rowCount)
        throw new MoriaError('INPUT_INVALID', 'This workload already has an active job.');
      const row = (
        await c.query(
          "UPDATE moria.jobs SET status='queued',attempt=0,phase='Operator retry',error=NULL,available_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1 AND status='failed' RETURNING *",
          [id],
        )
      ).rows[0];
      if (!row) throw new MoriaError('INPUT_INVALID', 'Only a failed job can be retried.');
      // Preserve attempts across manual retries: numbering comes from attempt history on claim.
      return this.publicJob(row);
    });
  }
  async tick() {
    const lock = await this.catalog.pool.connect();
    let acquired = false,
      lost = false;
    const onError = () => {
      lost = true;
    };
    lock.on('error', onError);
    try {
      acquired = (
        await lock.query(
          "SELECT pg_try_advisory_lock(hashtextextended('moria/execution/supervisor',0)) AS ok",
        )
      ).rows[0].ok;
      if (!acquired || this.stopped) return;
      const kinds = Object.keys(this.handlers);
      // Owning the process lock proves the former supervisor no longer owns its connection.
      await this.catalog.transaction(async (c) => {
        await c.query(
          "UPDATE moria.job_attempts SET status='interrupted',finished_at=clock_timestamp(),error='Worker stopped before acknowledgment' WHERE status='running' AND job_id IN (SELECT id FROM moria.jobs WHERE kind=ANY($1::text[]))",
          [kinds],
        );
        await c.query(
          "UPDATE moria.jobs SET status='retrying',fence=NULL,phase='Recovering interrupted attempt',available_at=clock_timestamp() WHERE status='running' AND kind=ANY($1::text[])",
          [kinds],
        );
      });
      const due = (
        await this.catalog.pool.query(
          'SELECT * FROM moria.job_schedules WHERE enabled AND next_run<=clock_timestamp() AND kind=ANY($1::text[])',
          [kinds],
        )
      ).rows;
      for (const schedule of due) {
        try {
          await this.enqueue(
            schedule.kind,
            {},
            `schedule:${schedule.kind}:${new Date(schedule.next_run).toISOString()}`,
            'schedule',
          );
        } catch (error) {
          // A configured manual refresh takes precedence over the default scheduled input.
          if (!(error instanceof MoriaError) || error.code !== 'WORKLOAD_BUSY') throw error;
        }
        // Coalesce missed intervals. No restart storm of old forecasts.
        await this.catalog.pool.query(
          "UPDATE moria.job_schedules SET next_run=clock_timestamp()+interval_minutes*interval '1 minute' WHERE kind=$1",
          [schedule.kind],
        );
      }
      const fence = randomUUID();
      const row = await this.catalog.transaction(async (c) => {
        const candidate = (
          await c.query(
            "SELECT * FROM moria.jobs WHERE status IN ('queued','retrying') AND available_at<=clock_timestamp() AND kind=ANY($1::text[]) ORDER BY created_at LIMIT 1 FOR UPDATE",
            [kinds],
          )
        ).rows[0];
        if (!candidate) return;
        const attempt = Number(
          (
            await c.query(
              'SELECT COALESCE(max(attempt),0)+1 AS n FROM moria.job_attempts WHERE job_id=$1',
              [candidate.id],
            )
          ).rows[0].n,
        );
        await c.query(
          "INSERT INTO moria.job_attempts(job_id,attempt,status) VALUES($1,$2,'running')",
          [candidate.id, attempt],
        );
        const claimed = (
          await c.query(
            "UPDATE moria.jobs SET status='running',attempt=attempt+1,fence=$2,phase='Starting',heartbeat_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1 RETURNING *",
            [candidate.id, fence],
          )
        ).rows[0];
        return { ...claimed, historyAttempt: attempt };
      });
      if (!row) return;
      const deadline = Date.now() + 30 * 60 * 1000;
      const guard = async (c?: PoolClient) => {
        if (lost || Date.now() > deadline)
          throw new MoriaError('LEASE_LOST', 'Execution lost its worker or exceeded its deadline.');
        const result = await (c ?? this.catalog.pool).query(
          "SELECT 1 FROM moria.jobs WHERE id=$1 AND fence=$2 AND status='running' AND heartbeat_at>clock_timestamp()-interval '90 seconds' FOR UPDATE",
          [row.id, fence],
        );
        if (!result.rowCount)
          throw new MoriaError('LEASE_LOST', 'Execution fence is no longer current.');
      };
      let renewing = false;
      const heartbeat = setInterval(() => {
        if (renewing) return;
        renewing = true;
        void lock
          .query(
            "UPDATE moria.jobs SET heartbeat_at=clock_timestamp() WHERE id=$1 AND fence=$2 AND status='running'",
            [row.id, fence],
          )
          .catch(() => {
            lost = true;
          })
          .finally(() => {
            renewing = false;
          });
      }, 10000);
      const complete = async (c: PoolClient, result: unknown) => {
        await guard(c);
        await c.query(
          "UPDATE moria.jobs SET status='succeeded',phase='Published',result=$3,error=NULL,updated_at=clock_timestamp() WHERE id=$1 AND fence=$2",
          [row.id, fence, JSON.stringify(result)],
        );
        await c.query(
          "UPDATE moria.job_attempts SET status='succeeded',finished_at=clock_timestamp() WHERE job_id=$1 AND status='running'",
          [row.id],
        );
      };
      try {
        const result = await this.handlers[row.kind]({
          id: row.id,
          input: row.input,
          guard,
          complete,
          phase: async (phase) => {
            await guard();
            await this.catalog.pool.query(
              'UPDATE moria.jobs SET phase=$3,updated_at=clock_timestamp() WHERE id=$1 AND fence=$2',
              [row.id, fence, phase],
            );
          },
        });
        // Handlers normally commit completion atomically with the served head.
        const current = (
          await this.catalog.pool.query('SELECT status FROM moria.jobs WHERE id=$1', [row.id])
        ).rows[0];
        if (current.status === 'running')
          await this.catalog.transaction((c) => complete(c, result));
      } catch (error) {
        const message =
          error instanceof MoriaError
            ? `${error.code}: ${error.message}`
            : 'Execution failed; prior published data remains available.';
        const permanent =
          error instanceof MoriaError &&
          [
            'FORBIDDEN',
            'INPUT_INVALID',
            'AUTHORIZATION_CHANGED',
            'IDENTITY_STALE',
            'UNAUTHENTICATED',
          ].includes(error.code);
        await this.catalog.transaction(async (c) => {
          const changed = await c.query(
            "UPDATE moria.jobs SET status=$3,phase=$4,error=$5,available_at=clock_timestamp()+interval '60 seconds',updated_at=clock_timestamp() WHERE id=$1 AND fence=$2 AND status='running'",
            [
              row.id,
              fence,
              permanent || row.attempt >= 3 ? 'failed' : 'retrying',
              permanent || row.attempt >= 3 ? 'Needs attention' : 'Retry in one minute',
              message,
            ],
          );
          if (changed.rowCount)
            await c.query(
              "UPDATE moria.job_attempts SET status='failed',finished_at=clock_timestamp(),error=$2 WHERE job_id=$1 AND attempt=$3 AND status='running'",
              [row.id, message, row.historyAttempt],
            );
        });
      } finally {
        clearInterval(heartbeat);
      }
    } finally {
      lock.removeListener('error', onError);
      if (acquired && !lost)
        await lock
          .query("SELECT pg_advisory_unlock(hashtextextended('moria/execution/supervisor',0))")
          .catch(() => {
            lost = true;
          });
      lock.release(lost ? new Error('Supervisor connection lost') : undefined);
    }
  }
  start() {
    this.stopped = false;
    const run = () => {
      this.active = this.tick()
        .catch((e) =>
          console.error(
            'Execution supervisor failed:',
            e instanceof MoriaError ? e.code : 'database unavailable',
          ),
        )
        .finally(() => {
          this.active = undefined;
          if (!this.stopped) this.timer = setTimeout(run, 2000);
        });
    };
    run();
  }
  async stop() {
    this.stopped = true;
    clearTimeout(this.timer);
    await this.active;
  }
}
