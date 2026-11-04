import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createServer, request } from 'node:http';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import {
  PutBucketVersioningCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { fingerprint } from '../packages/compiler/src/index.js';
import { Authority } from '../services/security/src/authority.js';
import { WorkloadSessions } from '../services/security/src/sessions.js';
import { database } from './fixtures/catalog-storage/database.js';
import { startS3 } from './fixtures/storage-s3/server.js';
import { assertNativeArtifacts } from './fixtures/native-common/artifact.js';

test(
  'operator filesystem GC permanently retires abandoned uploads, honors the grace interval and deletes only an exact qualified S3 version',
  { timeout: 120000 },
  async (t) => {
    const binary = process.env.MORIA_RUNTIME_BINARY ?? '/usr/local/bin/moria-runtime';
    await assertNativeArtifacts(binary, []);
    const db = await database(),
      s3 = await startS3(),
      children: ChildProcess[] = [];
    const sockets = new Set<import('node:net').Socket>();
    let completed!: () => void, deleting!: () => void, proceed!: () => void;
    const complete = new Promise<void>((resolve) => (completed = resolve)),
      beforeDelete = new Promise<void>((resolve) => (deleting = resolve)),
      releaseDelete = new Promise<void>((resolve) => (proceed = resolve));
    const upstream = new URL(s3.endpoint);
    const proxy = createServer((incoming, outgoing) => {
      const forward = () => {
        const sent = request(
          {
            hostname: upstream.hostname,
            port: upstream.port,
            path: incoming.url,
            method: incoming.method,
            headers: incoming.headers,
          },
          (response) => {
            if (
              incoming.method === 'POST' &&
              new URL(incoming.url!, s3.endpoint).searchParams.has('uploadId') &&
              response.statusCode === 200
            ) {
              response.resume();
              response.on('end', completed);
              return;
            }
            outgoing.writeHead(response.statusCode!, response.headers);
            response.pipe(outgoing);
          },
        );
        sent.on('error', () => outgoing.destroy());
        incoming.pipe(sent);
      };
      if (
        incoming.method === 'DELETE' &&
        new URL(incoming.url!, s3.endpoint).searchParams.has('versionId')
      ) {
        deleting();
        void releaseDelete.then(forward);
      } else forward();
    });
    proxy.on('connection', (socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
    await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
    const endpoint = `http://127.0.0.1:${(proxy.address() as { port: number }).port}`;
    t.after(async () => {
      proceed();
      for (const child of children) child.kill('SIGKILL');
      await Promise.all(
        children.map((child) =>
          child.exitCode !== null || child.signalCode !== null
            ? Promise.resolve()
            : new Promise<void>((resolve) => child.once('exit', () => resolve())),
        ),
      );
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => proxy.close(() => resolve()));
      await s3.close();
      await db.close();
    });
    await s3.client.send(
      new PutBucketVersioningCommand({
        Bucket: s3.bucket,
        VersioningConfiguration: { Status: 'Enabled' },
      }),
    );
    const environment = randomUUID(),
      application = 'filesystem-gc',
      principal = 'owner',
      operations = [
        'filesystem:read',
        'filesystem:list',
        'filesystem:create',
        'filesystem:write',
        'instance:manage',
      ];
    await db.catalog.ensureEnvironment(environment);
    const authority = new Authority(db.catalog);
    await authority.syncPrincipal(environment, 'fixture', principal, {
      kind: 'workload',
      enabled: true,
      attributes: {},
      markings: [],
    });
    await authority.setApplication(environment, 'fixture', application, operations);
    await authority.setGrants(environment, 'fixture', principal, application, operations);
    const token = (
      await new WorkloadSessions(authority).mint({ environment, principal, application }, 'fixture')
    ).token;
    const role = 'filesystem_gc_' + randomUUID().replaceAll('-', ''),
      password = randomUUID();
    await db.pool.query(
      `CREATE ROLE ${role} LOGIN PASSWORD '${password}';GRANT USAGE ON SCHEMA moria TO ${role};GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role};GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA moria TO ${role}`,
    );
    const connection = (user: string, secret: string) =>
      `postgresql://${user}:${secret}@127.0.0.1:${db.pool.options.port}/postgres`;
    const operator = connection(db.pool.options.user!, String(db.pool.options.password)),
      runtime = connection(role, password);
    const namespace = { warehouseId: 'filesystem', tableId: 'content', revision: 'v1' };
    const physical = {
      backend: 's3',
      endpoint: new URL(endpoint).href,
      bucket: s3.bucket,
      region: 'us-east-1',
      prefix: 'gc-files',
    };
    const env = {
      PATH: process.env.PATH,
      MORIA_INDEXING_DATABASE_SECRET_REF: 'GC_DATABASE',
      GC_DATABASE: operator,
      MORIA_INDEXING_DATABASE_TLS: 'disable',
      MORIA_NATIVE_FILESYSTEM_STORAGE_REVISION: 'v1',
      GC_ACCESS: s3.credentials.accessKeyId,
      GC_SECRET: s3.credentials.secretAccessKey,
      MORIA_NATIVE_STORAGE_BINDINGS: JSON.stringify([
        {
          namespace,
          provider: {
            kind: 's3',
            binding: {
              warehouseId: 'filesystem',
              tableId: 'content',
              endpoint,
              bucket: s3.bucket,
              prefix: 'gc-files',
              region: 'us-east-1',
              accessKeySecretRef: 'GC_ACCESS',
              secretKeySecretRef: 'GC_SECRET',
              sessionTokenSecretRef: null,
              limits: { maxObjectBytes: 64 * 1024 * 1024, chunkBytes: 1024 * 1024 },
            },
          },
        },
      ]),
    };
    const command = (args: string[], body: unknown, dsn = operator) =>
      new Promise<unknown>((resolve, reject) => {
        const child = spawn(binary, args, {
          env: { ...env, GC_DATABASE: dsn },
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        children.push(child);
        let output = '',
          errors = '';
        child.stdout.on('data', (bytes) => (output += bytes));
        child.stderr.on('data', (bytes) => (errors += bytes));
        child.once('exit', (code) =>
          code === 0 ? resolve(JSON.parse(output)) : reject(new Error(errors)),
        );
        child.stdin.end(JSON.stringify(body));
      });
    await promisify(execFile)(binary, ['filesystem', 'configure-storage'], { env:{...env,GC_DATABASE:runtime} });
    assert.equal(
      (
        await db.pool.query(
          "SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content'",
        )
      ).rows[0].binding_hash,
      fingerprint(physical),
    );
    const start = async () => {
      const child = spawn(binary, ['serve'], {
        env: { ...env, GC_DATABASE: runtime, MORIA_NATIVE_LISTEN: '127.0.0.1:0' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      let output = '',
        errors = '';
      child.stdout!.on('data', (bytes) => (output += bytes));
      child.stderr!.on('data', (bytes) => (errors += bytes));
      for (let i = 0; i < 1000 && !output.includes('\"event\":\"serving\"'); i++) {
        assert.equal(child.exitCode, null, errors);
        await delay(10);
      }
      const line = output.split('\n').find((line) => line.includes('\"event\":\"serving\"'));
      assert(line, errors + output);
      return { child, url: `http://${JSON.parse(line).address}` };
    };
    const first = await start();
    const homeResponse = await fetch(first.url + '/api/management/v1/fs/home', {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(homeResponse.status, 200, await homeResponse.clone().text());
    const home = (await homeResponse.json()).home;
    const payload = Buffer.alloc(2 * 1024 * 1024, 71),
      key = randomUUID(),
      path = `/api/management/v1/fs/entries/${home.id}/files?expectedRevision=${home.revision}&name=abandoned.bin`;
    const headers = {
      authorization: `Bearer ${token}`,
      'content-type': 'application/octet-stream',
      'x-moria-size': String(payload.length),
      'x-moria-digest': 'sha256:' + createHash('sha256').update(payload).digest('hex'),
      'idempotency-key': key,
    };
    const upload = fetch(first.url + path, { method: 'POST', headers, body: payload }).then(
      async (response) => {
        throw new Error(
          'Unexpected upload response ' + response.status + ' ' + (await response.text()),
        );
      },
      () => undefined,
    );
    await Promise.race([
      complete,
      upload.then(() => {
        throw new Error('Upload did not reach provider completion');
      }),
    ]);
    first.child.kill('SIGKILL');
    await new Promise<void>((resolve) => first.child.once('exit', () => resolve()));
    await upload;
    const issued = (
      await db.pool.query('SELECT * FROM moria.native_filesystem_uploads WHERE request_key=$1', [
        key,
      ])
    ).rows[0];
    assert.equal(issued.state, 'writing');
    assert.equal(issued.receipt, null);
    const objectKey = `gc-files/filesystem/content/objects/${issued.location.objectId}`;
    const version = (
      await s3.client.send(new HeadObjectCommand({ Bucket: s3.bucket, Key: objectKey }))
    ).VersionId;
    assert(version && version !== 'null');
    await db.pool.query(
      "UPDATE moria.native_filesystem_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
      [issued.id],
    );
    const policyRequest = {
      requestKey: 'uncommitted-fs',
      namespace,
      minimumAgeSeconds: 86400,
      recheckSeconds: 30,
    };
    await assert.rejects(
      command(['filesystem-retention', 'approve'], policyRequest, runtime),
      /GC_OPERATOR_REQUIRED/,
    );
    const policy = (await command(['filesystem-retention', 'approve'], policyRequest)) as {
      policy: string;
    };
    assert.equal(
      (
        (await command(['filesystem-retention', 'plan'], {
          policy: policy.policy,
          after: null,
          limit: 64,
        })) as { planned: number }
      ).planned,
      1,
    );
    assert.equal(
      (
        (await command(['filesystem-retention', 'once'], {
          policy: policy.policy,
          owner: randomUUID(),
          timeSeconds: 15,
        })) as { claimed: boolean }
      ).claimed,
      false,
    );
    const next = await start();
    const resumed = await fetch(next.url + path, { method: 'POST', headers, body: payload });
    assert.equal(resumed.status, 410, await resumed.clone().text());
    assert.equal((await resumed.json()).error.code, 'UPLOAD_RETIRED');
    await assert.rejects(
      db.pool.query(
        "UPDATE moria.native_filesystem_uploads SET lease_until=clock_timestamp()+interval '1 hour' WHERE id=$1",
        [issued.id],
      ),
      /UPLOAD_RETIRED/,
    );
    // Explicitly age the isolated proof journal; this does not claim an elapsed-day soak test.
    await db.pool.query(
      "UPDATE moria.native_filesystem_gc_candidates SET first_unreferenced=clock_timestamp()-interval '25 hours' WHERE upload=$1",
      [issued.id],
    );
    // A retained historical version is an independent reference even though no current file exists.
    await db.pool.query(
      "INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot) VALUES($1,999,jsonb_build_object('content',jsonb_build_object('location',$2::jsonb)))",
      [home.id, issued.location],
    );
    const blocked = (await command(['filesystem-retention', 'once'], {
      policy: policy.policy,
      owner: randomUUID(),
      timeSeconds: 15,
    })) as { claimed: boolean; referencePresent: boolean };
    assert.equal(blocked.claimed, false);
    assert.equal(blocked.referencePresent, true);
    assert.equal(
      (
        await db.pool.query(
          'SELECT first_unreferenced FROM moria.native_filesystem_gc_candidates WHERE upload=$1',
          [issued.id],
        )
      ).rows[0].first_unreferenced,
      null,
    );
    await db.pool.query('DELETE FROM moria.fs_entry_versions WHERE entry_id=$1 AND revision=999', [
      home.id,
    ]);
    await command(['filesystem-retention', 'plan'], {
      policy: policy.policy,
      after: null,
      limit: 64,
    });
    assert.equal(
      (
        (await command(['filesystem-retention', 'once'], {
          policy: policy.policy,
          owner: randomUUID(),
          timeSeconds: 15,
        })) as { claimed: boolean }
      ).claimed,
      false,
    );
    // Re-age the new unreferenced interval only after proving removal of the retained reference.
    await db.pool.query(
      "UPDATE moria.native_filesystem_gc_candidates SET first_unreferenced=clock_timestamp()-interval '25 hours' WHERE upload=$1",
      [issued.id],
    );
    const collected = command(['filesystem-retention', 'once'], {
      policy: policy.policy,
      owner: randomUUID(),
      timeSeconds: 15,
    });
    await Promise.race([
      beforeDelete,
      collected.then(() => {
        throw new Error('Exact-version deletion did not run');
      }),
    ]);
    const replacement = await s3.client.send(
      new PutObjectCommand({
        Bucket: s3.bucket,
        Key: objectKey,
        Body: 'newer independently retained version',
      }),
    );
    proceed();
    const result = (await collected) as { removed: boolean; version: { token: string } };
    assert.equal(result.removed, true);
    assert.match(result.version.token, /^s3v1\//);
    await assert.rejects(
      s3.client.send(
        new HeadObjectCommand({ Bucket: s3.bucket, Key: objectKey, VersionId: version }),
      ),
      (error) =>
        (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404,
    );
    assert.equal(
      (await s3.client.send(new HeadObjectCommand({ Bucket: s3.bucket, Key: objectKey })))
        .VersionId,
      replacement.VersionId,
    );
    assert.equal(
      (
        await db.pool.query('SELECT count(*)::int n FROM moria.fs_mutations WHERE request_key=$1', [
          key,
        ])
      ).rows[0].n,
      0,
    );
    assert.equal(
      (
        await db.pool.query(
          'SELECT count(*)::int n FROM moria.native_filesystem_upload_tombstones WHERE upload=$1',
          [issued.id],
        )
      ).rows[0].n,
      1,
    );
    const proof = (
      await db.pool.query(
        'SELECT status,last_removed_version FROM moria.native_filesystem_gc_candidates WHERE upload=$1',
        [issued.id],
      )
    ).rows[0];
    assert.equal(proof.status, 'watching');
    assert.deepEqual(proof.last_removed_version, result.version);
  },
);
