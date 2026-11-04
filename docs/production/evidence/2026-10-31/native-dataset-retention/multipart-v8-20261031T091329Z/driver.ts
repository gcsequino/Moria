import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createServer, request } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import pg from 'pg';
import {
  CreateMultipartUploadCommand,
  ListPartsCommand,
  AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import { managedRetentionDataset } from '/work/tests/fixtures/native-retention/dataset.js';
import {
  ageClosedDatasetHistory,
  ageClosedUnclaimedCandidates,
} from '/work/tests/fixtures/native-retention/history.js';

test(
  'native Dataset abandoned multipart cleanup uses exact issued keys across lost ACKs and late provider effects',
  { timeout: 180000 },
  async (t) => {
    let phase: 'pass' | 'part' | 'initiate' | 'before-initiate' = 'pass';
    let signal = () => {};
    const deferred: { release: () => void; held: boolean } = {
      release: () => {
        throw new Error('No actual delayed provider request');
      },
      held: false,
    };
    let providerUpload = '';
    const sockets = new Set<import('node:net').Socket>();
    const fixture = await managedRetentionDataset({
      wrapS3: async (s3) => {
        const upstream = new URL(s3.endpoint);
        const proxy = createServer((incoming, outgoing) => {
          const url = new URL(incoming.url!, s3.endpoint);
          const selected = url.pathname.includes('dataset_multipart_');
          const parts: Buffer[] = [];
          let size = 0;
          incoming.on('data', (part) => {
            size += part.length;
            if (size > 10 * 1024 * 1024) {
              incoming.destroy();
              return;
            }
            parts.push(part);
          });
          incoming.on('end', () => {
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
                  const held =
                    selected &&
                    response.statusCode === 200 &&
                    ((phase === 'part' &&
                      incoming.method === 'PUT' &&
                      url.searchParams.has('partNumber')) ||
                      (phase === 'initiate' &&
                        incoming.method === 'POST' &&
                        url.searchParams.has('uploads')));
                  if (held) {
                    const chunks: Buffer[] = [];
                    response.on('data', (bytes) => chunks.push(bytes));
                    response.on('end', () => {
                      const xml = Buffer.concat(chunks).toString();
                      providerUpload =
                        xml.match(/<UploadId>([^<]+)<\/UploadId>/)?.[1] ??
                        url.searchParams.get('uploadId') ??
                        '';
                      signal();
                    });
                    return;
                  }
                  if (outgoing.destroyed) {
                    response.resume();
                    response.on('end', () => signal());
                    return;
                  }
                  outgoing.writeHead(response.statusCode!, response.headers);
                  response.pipe(outgoing);
                },
              );
              sent.on('error', () => outgoing.destroy());
              sent.end(Buffer.concat(parts));
            };
            if (
              selected &&
              phase === 'before-initiate' &&
              incoming.method === 'POST' &&
              url.searchParams.has('uploads')
            ) {
              deferred.release = forward;
              deferred.held = true;
              signal();
            } else forward();
          });
        });
        proxy.on('connection', (socket) => {
          sockets.add(socket);
          socket.on('close', () => sockets.delete(socket));
        });
        await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
        return {
          ...s3,
          endpoint: `http://127.0.0.1:${(proxy.address() as { port: number }).port}`,
          close: async () => {
            for (const socket of sockets) socket.destroy();
            await new Promise<void>((resolve) => proxy.close(() => resolve()));
            await s3.close();
          },
        };
      },
    });
    t.after(fixture.close);
    const rows = Array.from({ length: 64 }, (_, i) => ({
      id: randomUUID(),
      name: 'multipart-' + i,
      tags: ['typed'],
    }));
    await fixture.publish(rows);
    const raw = new pg.Client({ connectionString: fixture.nativeEnv.RETENTION_DATABASE });
    await raw.connect();
    try {
      const producer = (
        await fixture.db.pool.query('SELECT * FROM moria.runtime_dataset_producers LIMIT 1')
      ).rows[0];
      assert(producer);
      await assert.rejects(
        raw.query(
          'INSERT INTO moria.runtime_dataset_producers(scope,execution,owner,fence,incarnation,namespace,physical_fingerprint) VALUES($1,$2,$3,$4,$5,$6,$7)',
          [
            'forged-producer',
            producer.execution,
            randomUUID(),
            1,
            producer.incarnation,
            producer.namespace,
            producer.physical_fingerprint,
          ],
        ),
        /GC_OBJECT_DENIED/,
      );
      await assert.rejects(
        raw.query(
          'INSERT INTO moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id,producer,expected_size,expected_sha256) VALUES($1,$2,$3,$4,$5,$6,$7)',
          [
            producer.physical_fingerprint,
            fixture.namespace.warehouseId,
            fixture.namespace.tableId,
            'forged-object',
            producer.scope,
            0,
            'sha256:' + '0'.repeat(64),
          ],
        ),
        /GC_OBJECT_DENIED/,
      );
    } finally {
      await raw.end();
    }

    const policy = await fixture.command(
      ['dataset-retention', 'approve'],
      {
        requestKey: 'multipart-policy',
        environment: fixture.environment,
        dataset: fixture.dataset,
        retainSeconds: 86400,
        recheckSeconds: 30,
      },
      fixture.operatorEnv,
    );
    let sibling: { Key: string; UploadId: string } | undefined;
    t.after(async () => {
      if (sibling)
        await fixture.s3.client.send(
          new AbortMultipartUploadCommand({ Bucket: fixture.s3.bucket, ...sibling }),
        );
    });
    for (const selectedPhase of ['part', 'initiate', 'before-initiate'] as const) {
      phase = selectedPhase;
      providerUpload = '';
      deferred.held = false;
      const reached = new Promise<void>((resolve) => (signal = resolve));
      const head = (await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0];
      const binding = await fixture.bind([], { [fixture.dataset]: head.revision });
      const child = spawn(
        fixture.harness,
        ['--ignored', '--exact', 'catalog_publication', '--nocapture', '--test-threads=1'],
        {
          env: {
            ...fixture.nativeEnv,
            MORIA_NATIVE_CATALOG_FIXTURE: JSON.stringify({
              root: fixture.db.root,
              token: fixture.token,
              binding,
              mode: 'multipart-crash',
              snapshots: [fixture.snapshot],
            }),
          },
          stdio: ['pipe', 'pipe', 'pipe'],
        },
      );
      let out = '',
        errors = '';
      child.stdout.on('data', (part) => (out = (out + part).slice(-65536)));
      child.stderr.on('data', (part) => (errors = (errors + part).slice(-65536)));
      const exited = new Promise<number | null>((resolve) => child.once('exit', resolve));
      try {
        await Promise.race([
          reached,
          exited.then((code) => {
            throw new Error(
              'Producer exited before provider barrier ' + code + ' ' + errors + ' ' + out,
            );
          }),
          delay(15000).then(() => {
            throw new Error('Provider barrier deadline ' + errors + ' ' + out);
          }),
        ]);
        const line = out
          .split('\n')
          .find((line) => line.includes('"event":"dataset_multipart_intent"'));
        assert(line, out);
        const intent = JSON.parse(line.slice(line.indexOf('{')));
        const issued = (
          await fixture.db.pool.query(
            'SELECT o.*,p.tombstoned_at FROM moria.runtime_dataset_issued_objects o JOIN moria.runtime_dataset_producers p ON p.scope=o.producer WHERE o.object_id=$1',
            [intent.object.objectId],
          )
        ).rows[0];
        assert(issued);
        assert.equal(issued.tombstoned_at, null);
        const journal = (
          await fixture.db.pool.query(
            'SELECT upload_id FROM moria.runtime_dataset_multipart_uploads WHERE object_id=$1',
            [intent.object.objectId],
          )
        ).rows;
        if (selectedPhase === 'part') assert.equal(journal[0]?.upload_id, providerUpload);
        else assert.equal(journal.length, 0);
        child.kill('SIGKILL');
        await exited;
        await fixture.command(['execution', 'cancel'], { id: intent.execution });
        const Key = `${fixture.datasetStorage.prefix}/${fixture.namespace.warehouseId}/${fixture.namespace.tableId}/objects/${intent.object.objectId}`;
        if (!sibling) {
          const foreign = await fixture.s3.client.send(
            new CreateMultipartUploadCommand({ Bucket: fixture.s3.bucket, Key: Key + '-sibling' }),
          );
          assert(foreign.UploadId);
          sibling = { Key: Key + '-sibling', UploadId: foreign.UploadId };
        }
        if (selectedPhase !== 'before-initiate')
          assert.equal(
            (
              await fixture.s3.client.send(
                new ListPartsCommand({ Bucket: fixture.s3.bucket, Key, UploadId: providerUpload }),
              )
            ).$metadata.httpStatusCode,
            200,
          );
        await ageClosedDatasetHistory(fixture);
        const plan = await fixture.command(
          ['dataset-retention', 'plan'],
          {
            policy: policy.policy,
            requestKey: 'multipart-' + selectedPhase,
            after: null,
            limit: 256,
          },
          fixture.operatorEnv,
        );
        assert(plan.added >= 1);
        const young=(await fixture.db.pool.query('SELECT id,attempts,orphaned_at,not_before FROM moria.runtime_dataset_gc_candidates WHERE object_id=$1',[intent.object.objectId])).rows[0];assert(young);
        assert.equal(young.attempts,0);assert(young.not_before.getTime()-young.orphaned_at.getTime()>=86400000);
        const early=await fixture.command(['dataset-retention','once'],{policy:policy.policy,owner:randomUUID(),timeSeconds:30},fixture.operatorEnv);assert.notEqual(early.claim,young.id);
        assert.equal((await fixture.db.pool.query('SELECT attempts FROM moria.runtime_dataset_gc_candidates WHERE id=$1',[young.id])).rows[0].attempts,0);
        await ageClosedUnclaimedCandidates(fixture, policy.policy);
        const candidate = (
          await fixture.db.pool.query(
            'SELECT id,status FROM moria.runtime_dataset_gc_candidates WHERE object_id=$1',
            [intent.object.objectId],
          )
        ).rows[0];
        assert(candidate);
        let result: any;
        for (let pass = 0; pass < 64; pass++) {
          result = await fixture.command(
            ['dataset-retention', 'once'],
            { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
            fixture.operatorEnv,
          );
          assert.equal(result.claimed, true);
          if (result.claim === candidate.id) break;
        }
        assert.equal(result.claim, candidate.id);
        candidate.status = (
          await fixture.db.pool.query(
            'SELECT status FROM moria.runtime_dataset_gc_candidates WHERE id=$1',
            [candidate.id],
          )
        ).rows[0].status;
        assert.equal(candidate.status, 'watching');
        if (selectedPhase === 'before-initiate') {
          assert.equal(result.multipartAborted, 0);
          assert(deferred.held);
          phase = 'initiate';
          const created = new Promise<void>((resolve) => (signal = resolve));
          deferred.release();
          await created;
          assert(providerUpload);
          assert.equal(
            (
              await fixture.s3.client.send(
                new ListPartsCommand({ Bucket: fixture.s3.bucket, Key, UploadId: providerUpload }),
              )
            ).$metadata.httpStatusCode,
            200,
          );
          // This is a real wall-clock watcher interval, independent of the synthetic pre-aged closed DATA above.
          await delay(30100);
          for (let pass = 0; pass < 4; pass++) {
            result = await fixture.command(
              ['dataset-retention', 'once'],
              { policy: policy.policy, owner: randomUUID(), timeSeconds: 30 },
              fixture.operatorEnv,
            );
            if (result.claim === candidate.id) break;
          }
          assert.equal(result.claim, candidate.id);
        }
        assert.equal(result.multipartAborted, 1);
        await assert.rejects(
          fixture.s3.client.send(
            new ListPartsCommand({ Bucket: fixture.s3.bucket, Key, UploadId: providerUpload }),
          ),
          /NoSuchUpload/,
        );
        assert.equal(
          (
            await fixture.s3.client.send(
              new ListPartsCommand({ Bucket: fixture.s3.bucket, ...sibling }),
            )
          ).$metadata.httpStatusCode,
          200,
        );
        const receipts = (
          await fixture.db.pool.query(
            'SELECT upload_id,result FROM moria.runtime_dataset_gc_multipart WHERE candidate=$1',
            [candidate.id],
          )
        ).rows;
        assert(
          receipts.some((row) => row.upload_id === providerUpload && row.result === 'aborted'),
        );
        t.diagnostic(
          JSON.stringify({
            phase: selectedPhase,
            exactMultipartAborted: true,
            siblingPreserved: true,
            actualProducer: intent.execution,
            receipt: receipts,
          }),
        );
      } finally {
        child.kill('SIGKILL');
        await exited;
        phase = 'pass';
      }
    }
    await fixture.read((await fixture.db.catalog.heads(fixture.environment, [fixture.dataset]))[0]);
  },
);
