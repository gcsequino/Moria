import { randomUUID } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Functions } from '../../../services/functions/src/service.js';
import type { FilesystemContext } from '../../../services/security/src/filesystem.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { body } from './server.js';

export function functionsHandler(functions: Functions) {
  return async (request: IncomingMessage, response: ServerResponse, context: FilesystemContext) => {
    const url = new URL(request.url ?? '/', 'http://localhost'),
      parts = url.pathname.split('/').filter(Boolean);
    if (
      parts.slice(0, 2).join('/') !== 'api/v1' ||
      !['functions', 'invocations'].includes(parts[2])
    )
      return false;
    const correlationId = randomUUID();
    let invocationId: string | undefined;
    const send = (status: number, value: unknown) => {
      response.writeHead(status, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
        'x-content-type-options': 'nosniff',
      });
      response.end(JSON.stringify(value));
    };
    try {
      await functions.runtime.management.ensureHome(context);
      if (
        [...url.searchParams.keys()].some(
          (key) => key !== 'after' || url.searchParams.getAll(key).length > 1,
        )
      )
        throw new MoriaError('INPUT_INVALID', 'Unsupported query parameters');
      const id = parts[3];
      if (
        parts[2] === 'functions' &&
        parts[4] === 'invoke' &&
        parts.length === 5 &&
        request.method === 'POST' &&
        !url.search
      ) {
        const value = await body(request),
          key = request.headers['idempotency-key'];
        if (
          !value ||
          typeof value !== 'object' ||
          Array.isArray(value) ||
          !Object.hasOwn(value, 'input') ||
          Object.keys(value).some((key) => !['input', 'wait'].includes(key)) ||
          (value.wait !== undefined && typeof value.wait !== 'boolean') ||
          typeof key !== 'string'
        )
          throw new MoriaError(
            'INPUT_INVALID',
            'Expected input, optional wait and Idempotency-Key',
          );
        let result = await functions.invoke(context, id, key, value.input);
        invocationId = result.id;
        response.setHeader('location', `/api/v1/invocations/${result.id}`);
        if (value.wait !== false) result = await functions.wait(context, result.id);
        send(['accepted', 'running'].includes(result.status) ? 202 : 200, result);
        return true;
      }
      if (
        parts[2] === 'invocations' &&
        parts.length === 4 &&
        request.method === 'GET' &&
        !url.search
      ) {
        send(200, await functions.get(context, id));
        return true;
      }
      if (
        parts[2] === 'invocations' &&
        parts[4] === 'events' &&
        parts.length === 5 &&
        request.method === 'GET'
      ) {
        send(200, {
          events: await functions.events(context, id, url.searchParams.get('after') ?? '0'),
        });
        return true;
      }
      if (
        parts[2] === 'invocations' &&
        parts[4] === 'cancel' &&
        parts.length === 5 &&
        request.method === 'POST' &&
        !url.search
      ) {
        const value = await body(request);
        if (
          !value ||
          typeof value !== 'object' ||
          Array.isArray(value) ||
          Object.keys(value).length
        )
          throw new MoriaError('INPUT_INVALID', 'Expected an empty object');
        await functions.cancel(context, id);
        send(200, { id, cancelRequested: true });
        return true;
      }
      throw new MoriaError('NOT_FOUND', 'Function endpoint not found');
    } catch (error) {
      const known = error instanceof MoriaError,
        code = known ? error.code : 'INTERNAL';
      const status =
        code === 'UNAUTHENTICATED'
          ? 401
          : code === 'FORBIDDEN'
            ? 403
            : code === 'NOT_FOUND'
              ? 404
              : /EXPIRED/.test(code)
                ? 410
                : /CONFLICT|CHANGED/.test(code)
                  ? 409
                  : /BUSY/.test(code)
                    ? 503
                    : code === 'UNSUPPORTED_CAPABILITY'
                      ? 422
                      : code === 'INTERNAL'
                        ? 500
                        : 400;
      request.resume();
      send(status, {
        error: {
          code,
          message: known ? error.message : 'Function request failed',
          correlationId,
          ...(invocationId ? { details: { invocationId } } : {}),
        },
      });
      return true;
    }
  };
}
