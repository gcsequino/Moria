import { s3ClientDefaults } from '../../../services/storage/src/s3-client.js';
import { createReadStream } from 'node:fs';
import { Agent } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';
import { randomBytes, createHash } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';
import { S3Client, CreateBucketCommand } from '@aws-sdk/client-s3';

export async function startS3() {
  const binary = process.env.MORIA_TEST_S3_BINARY ?? '/usr/local/bin/minio';
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(binary)) {
    digest.update(chunk);
  }
  if (digest.digest('hex') !== '7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f') {
    throw new Error('Required isolated MinIO binary checksum mismatch');
  }
  const root = await mkdtemp(join(tmpdir(), 'moria-s3-'));
  const reservation = createServer();
  await new Promise<void>((resolve) => reservation.listen(0, '127.0.0.1', resolve));
  const address = reservation.address();
  if (!address || typeof address === 'string') {
    throw new Error('Port reservation failed');
  }
  const port = address.port;
  await new Promise<void>((resolve) => reservation.close(() => resolve()));
  const credentials = {
    accessKeyId: randomBytes(12).toString('hex'),
    secretAccessKey: randomBytes(24).toString('hex'),
  };
  const endpoint = `http://127.0.0.1:${port}`;
  const child = spawn(binary, ['server', root, '--address', `127.0.0.1:${port}`], {
    env: {
      PATH: process.env.PATH,
      MINIO_ROOT_USER: credentials.accessKeyId,
      MINIO_ROOT_PASSWORD: credentials.secretAccessKey,
      MINIO_BROWSER: 'off',
    },
    stdio: 'ignore',
  });
  let failed = false;
  child.on('error', () => {
    failed = true;
  });
  const exited = new Promise<void>((resolve) => {
    child.once('exit', () => resolve());
    child.once('error', () => resolve());
  });
  const client = new S3Client({
    ...s3ClientDefaults,
    endpoint,
    region: 'us-east-1',
    credentials,
    requestHandler: { httpAgent: new Agent({ keepAlive: false }) },
  });
  const close = async () => {
    client.destroy();
    child.kill('SIGKILL');
    await exited;
    await rm(root, { recursive: true, force: true });
  };
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      if (failed || child.exitCode !== null) {
        break;
      }
      try {
        const response = await fetch(`${endpoint}/minio/health/ready`, {
          signal: AbortSignal.timeout(500),
        });
        if (response.ok) {
          ready = true;
          break;
        }
      } catch {
        /* Wait for isolated server startup. */
      }
      await setTimeout(100);
    }
    if (!ready) {
      throw new Error(
        'Required isolated MinIO server unavailable; provision the pinned test binary',
      );
    }
    const bucket = 'moria-synthetic';
    for (let attempt = 0; ; attempt++) {
      try {
        await client.send(new CreateBucketCommand({ Bucket: bucket }));
        break;
      } catch (error) {
        if (attempt >= 50 || (error as { name?: string }).name !== 'XMinioServerNotInitialized') {
          throw error;
        }
        await setTimeout(100);
      }
    }
    return { client, endpoint, credentials, bucket, close };
  } catch (error) {
    await close();
    throw error;
  }
}
