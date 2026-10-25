import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  ListMultipartUploadsCommand,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { startS3 } from './fixtures/storage-s3/server.js';
import { createS3Storage } from '../services/storage/src/s3.js';
import { createAuthorizedS3Storage } from '../services/storage/src/authority.js';
import { proxyS3 } from './fixtures/storage-s3/proxy.js';
import { logical, limits, source, code, collect } from './fixtures/storage-contract/helpers.js';

test(
  'real S3 prerequisite: conditional multipart finalization is atomic and create-only',
  { timeout: 30000 },
  async () => {
    const server = await startS3();
    try {
      const { client, bucket: Bucket } = server;
      const Key = 'conditional-proof';
      const prepare = async (value: string) => {
        const { UploadId } = await client.send(new CreateMultipartUploadCommand({ Bucket, Key }));
        assert.ok(UploadId);
        const { ETag } = await client.send(
          new UploadPartCommand({ Bucket, Key, UploadId, PartNumber: 1, Body: Buffer.from(value) }),
        );
        return {
          Bucket,
          Key,
          UploadId,
          MultipartUpload: { Parts: [{ PartNumber: 1, ETag }] },
          IfNoneMatch: '*',
        };
      };
      const uploads = await Promise.all([prepare('first'), prepare('second')]);
      const results = await Promise.allSettled(
        uploads.map((input) => client.send(new CompleteMultipartUploadCommand(input))),
      );
      assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
      const loser = results.findIndex((result) => result.status === 'rejected');
      const result = results[loser];
      assert.equal(result?.status, 'rejected');
      if (result?.status === 'rejected') {
        assert.equal(result.reason.$metadata.httpStatusCode, 412);
      }
      const object = await client.send(new GetObjectCommand({ Bucket, Key }));
      assert.equal(await object.Body!.transformToString(), loser === 0 ? 'second' : 'first');
      const later = await prepare('overwrite');
      await assert.rejects(
        client.send(new CompleteMultipartUploadCommand(later)),
        (error: any) => error.$metadata.httpStatusCode === 412,
      );
      for (const upload of [uploads[loser]!, later]) {
        await client.send(new AbortMultipartUploadCommand(upload));
      }
      const pending = await client.send(new ListMultipartUploadsCommand({ Bucket }));
      assert.equal(pending.Uploads?.length ?? 0, 0);
      const preserved = await client.send(new GetObjectCommand({ Bucket, Key }));
      assert.equal(await preserved.Body!.transformToString(), loser === 0 ? 'second' : 'first');
    } finally {
      await server.close();
    }
  },
);

test(
  'S3 trusted endpoint, bucket and prefix cannot be supplied through logical locations',
  { timeout: 30000 },
  async () => {
    const server = await startS3();
    try {
      const binding = {
        ...limits,
        ...server,
        credentials: { ...server.credentials },
        prefix: 'tenant',
        region: 'us-east-1',
      };
      for (const endpoint of [
        'http://example.com',
        `${server.endpoint}/escape`,
        `${server.endpoint}?bucket=other`,
        `http://user:password@127.0.0.1`,
        'file:///tmp/object',
      ]) {
        await assert.rejects(createS3Storage({ ...binding, endpoint }), code('INVALID_INPUT'));
      }
      for (const prefix of ['../escape', '/absolute', 'a//b', 'a/%2f', 'a/..', 'a\\b']) {
        await assert.rejects(createS3Storage({ ...binding, prefix }), code('INVALID_INPUT'));
      }
      for (const bucket of ['../escape', 'a/b', 'https://other', 'bucket.other']) {
        await assert.rejects(createS3Storage({ ...binding, bucket }), code('INVALID_INPUT'));
      }
      const storage = await createS3Storage(binding);
      await assert.rejects(
        storage.read({ ...logical, endpoint: server.endpoint } as typeof logical),
        code('INVALID_INPUT'),
      );
      const wrong = await createS3Storage({
        ...binding,
        credentials: { accessKeyId: 'synthetic-invalid', secretAccessKey: 'synthetic-invalid' },
      });
      await assert.rejects(wrong.read(logical), code('IO_ERROR'));
      binding.prefix = 'changed';
      binding.credentials.accessKeyId = 'changed';
      // Configuration copies preserve the original binding and credential values.
      await storage.write(logical, source([[1]]), { size: 1 });
      assert.deepEqual(await collect(storage), [1]);
      const response = await server.client.send(
        new GetObjectCommand({
          Bucket: server.bucket,
          Key: 'tenant/warehouse/table/objects/object',
        }),
      );
      assert.deepEqual(await response.Body!.transformToByteArray(), new Uint8Array([1]));
    } finally {
      await server.close();
    }
  },
);

test(
  'S3 multipart buffering is bounded by one part and abort removes uploaded parts',
  { timeout: 30000 },
  async () => {
    const server = await startS3();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let reached!: () => void;
    const uploaded = new Promise<void>((resolve) => {
      reached = resolve;
    });
    const partSizes: number[] = [];
    const proxy = await proxyS3(server.endpoint, async (request, status) => {
      if (request.method === 'PUT' && request.url?.includes('partNumber=')) {
        assert.equal(status, 200);
        partSizes.push(Number(request.headers['content-length']));
        if (partSizes.length === 1) {
          reached();
          await blocked;
        }
      }
    });
    try {
      const chunkBytes = 1024 * 1024;
      const storage = await createS3Storage({
        ...limits,
        ...server,
        endpoint: proxy.endpoint,
        prefix: '',
        region: 'us-east-1',
        maxObjectBytes: 20 * chunkBytes,
        chunkBytes,
      });
      let pulls = 0;
      let cancelled = false;
      const input = new ReadableStream<Uint8Array>(
        {
          pull(value) {
            pulls++;
            if (pulls <= 12) {
              value.enqueue(new Uint8Array(chunkBytes).fill(pulls));
            } else value.close();
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const abort = new AbortController();
      const write = storage.write(logical, input, { size: 12 * chunkBytes, signal: abort.signal });
      const rejected = assert.rejects(write, code('CANCELLED'));
      await uploaded;
      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      assert.equal(pulls, 5, 'no sixth source chunk before first part acknowledgment');
      assert.deepEqual(partSizes, [5 * chunkBytes]);
      abort.abort();
      release();
      await rejected;
      assert.equal(cancelled, true);
      assert.equal(input.locked, false);
      const pending = await server.client.send(
        new ListMultipartUploadsCommand({ Bucket: server.bucket }),
      );
      assert.equal(pending.Uploads?.length ?? 0, 0);
      await assert.rejects(storage.read(logical), code('NOT_FOUND'));
      let sent = 0;
      const success = new ReadableStream<Uint8Array>(
        {
          pull(value) {
            if (sent++ < 12) {
              value.enqueue(new Uint8Array(chunkBytes).fill(7));
            } else value.close();
          },
        },
        { highWaterMark: 0 },
      );
      await storage.write(logical, success, { size: 12 * chunkBytes });
      assert.deepEqual(
        partSizes,
        [5, 5, 5, 2].map((size) => size * chunkBytes),
      );
      let total = 0;
      for await (const chunk of await storage.read(logical)) {
        assert.ok(chunk.length <= chunkBytes);
        assert.equal(
          chunk.every((value) => value === 7),
          true,
        );
        total += chunk.length;
      }
      assert.equal(total, 12 * chunkBytes);
    } finally {
      release();
      await proxy.close();
      await server.close();
    }
  },
);

test(
  'S3 rechecks authority after part upload and before completion; reads validate range responses',
  { timeout: 30000 },
  async () => {
    const server = await startS3();
    let allow = true;
    let revokeUpload = true;
    let corruptRange = false;
    let revokeRead = false;
    let gets = 0;
    let completions = 0;
    const proxy = await proxyS3(server.endpoint, async (request, _status, response) => {
      if (request.method === 'PUT' && request.url?.includes('partNumber=') && revokeUpload) {
        allow = false;
      }
      if (request.method === 'POST' && request.url?.includes('uploadId=')) {
        completions++;
      }
      if (request.method === 'GET') {
        gets++;
        if (revokeRead) {
          allow = false;
        }
        if (corruptRange) {
          response.headers['content-range'] = 'bytes 0-100/101';
        }
      }
    });
    try {
      const binding = {
        ...limits,
        ...server,
        endpoint: proxy.endpoint,
        prefix: '',
        region: 'us-east-1',
      };
      const storage = await createAuthorizedS3Storage(
        binding,
        { location: logical, operations: ['read', 'create'] },
        {
          async checkCurrent() {
            return allow;
          },
        },
      );
      await assert.rejects(
        storage.write(logical, source([[1, 2, 3, 4]]), { size: 4 }),
        code('AUTHORITY_DENIED'),
      );
      assert.equal(completions, 0);
      assert.equal(
        (await server.client.send(new ListMultipartUploadsCommand({ Bucket: server.bucket })))
          .Uploads?.length ?? 0,
        0,
      );
      revokeUpload = false;
      allow = true;
      await storage.write(
        logical,
        source([
          [1, 2, 3, 4],
          [5, 6, 7, 8],
        ]),
        { size: 8 },
      );
      const reader = (await storage.read(logical)).getReader();
      assert.equal(gets, 0);
      await reader.read();
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(gets, 1, 'no speculative GET');
      revokeRead = true;
      await assert.rejects(reader.read(), code('AUTHORITY_DENIED'));
      allow = true;
      revokeRead = false;
      corruptRange = true;
      const malformed = (await storage.read(logical)).getReader();
      await assert.rejects(malformed.read(), code('IO_ERROR'));
    } finally {
      await proxy.close();
      await server.close();
    }
  },
);

test(
  'S3 requests never load ambient configuration or credential files',
  { timeout: 30000 },
  async () => {
    await promisify(execFile)(
      process.execPath,
      ['--import', 'tsx', new URL('./fixtures/storage-s3/no-profile.ts', import.meta.url).pathname],
      { timeout: 20000 },
    );
  },
);
