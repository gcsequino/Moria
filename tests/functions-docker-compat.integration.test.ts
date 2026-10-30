import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import http, { type IncomingMessage, type RequestOptions } from 'node:http';
import { syncBuiltinESMExports } from 'node:module';
import test from 'node:test';
import { functionImage } from './fixtures/functions/image.js';
import { functionPlatform } from './fixtures/functions/platform.js';

test(
  'Docker CPU accounting supports older daemon APIs without weakening limits',
  { timeout: 60000 },
  async (t) => {
    const image = await functionImage();
    t.after(image.close);
    const p = await functionPlatform(image.artifact);
    t.after(p.close);
    const invoke = async (value = 3) => {
      const accepted = await p.functions.invoke(p.owner.context, p.resourceId, randomUUID(), {
        mode: 'normal',
        value,
      });
      return p.functions.wait(p.owner.context, accepted.id);
    };
    const request = http.request;
    let probes = 0;
    const transport = t.mock.method(http, 'request', ((
      options: RequestOptions,
      listener: (response: IncomingMessage) => void,
    ) => {
      const path = options.path ?? '';
      if (options.socketPath === '/var/run/docker.sock' && path.includes('/stats?')) {
        probes++;
        const version = /^\/v(\d+)\.(\d+)\//.exec(path);
        const unsupported = version && (Number(version[1]) > 1 || Number(version[2]) > 44);
        // Real stats use API 1.44; too-new requests receive a real daemon rejection.
        return request(
          {
            ...options,
            path: unsupported
              ? path.replace(/^\/v[^/]+/, '/v999.0')
              : version
                ? path
                : `/v1.44${path}`,
          },
          listener,
        );
      }
      return request(options, listener);
    }) as typeof http.request);
    syncBuiltinESMExports();
    t.after(() => {
      transport.mock.restore();
      syncBuiltinESMExports();
    });
    const result = await invoke(41);
    assert.equal(result.status, 'succeeded', JSON.stringify(result));
    assert.deepEqual(result.result, { value: 42 });
    await p.publish({
      ...p.declaration,
      spec: { ...p.declaration.spec, limits: { ...p.declaration.spec.limits, cpuMillis: 1 } },
    });
    const exhausted = await invoke();
    assert.equal(exhausted.status, 'failed');
    assert.equal(exhausted.error?.code, 'FUNCTION_CPU_LIMIT');
    assert.ok(probes >= 2, 'both invocations must use real daemon CPU measurements');
  },
);
