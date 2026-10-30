import { randomUUID } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { body } from './server.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import { StorageError } from '../../../services/storage/src/locations.js';
import type { FilesystemRuntime } from '../../../services/filesystem/src/local.js';
import type { FilesystemContext } from '../../../services/security/src/filesystem.js';

function input(value: unknown, required: string[], optional: string[] = []): Record<string, any> {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    required.some((key) => !Object.hasOwn(value, key)) ||
    Object.keys(value).some((key) => ![...required, ...optional].includes(key))
  )
    throw new MoriaError('INPUT_INVALID', 'Unexpected or missing request fields');
  return value as Record<string, any>;
}
function key(request: IncomingMessage): string {
  const value = request.headers['idempotency-key'];
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$/.test(value))
    throw new MoriaError('IDEMPOTENCY_KEY_INVALID', 'Idempotency-Key is required');
  return value;
}
function query(url: URL, allowed: string[]) {
  if (
    [...url.searchParams.keys()].some(
      (name) => !allowed.includes(name) || url.searchParams.getAll(name).length !== 1,
    )
  )
    throw new MoriaError('INPUT_INVALID', 'Unsupported or duplicate query parameter');
}

/** Authentication/origin enforcement belongs to the host; all operations also reauthorize in the service. */
export function filesystemHandler(runtime: FilesystemRuntime) {
  return async (request: IncomingMessage, response: ServerResponse, context: FilesystemContext) => {
    const correlationId = randomUUID();
    const send = (status: number, value: unknown) => {
      response.writeHead(status, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
        'x-content-type-options': 'nosniff',
      });
      response.end(JSON.stringify(value));
    };
    try {
      const url = new URL(request.url ?? '/', 'http://localhost');
      const path = url.pathname.replace(/^\/api\/management\//, '/management/');
      const parts = path.split('/').filter(Boolean);
      if (parts.slice(0, 3).join('/') !== 'management/v1/fs') return false;
      const home = await runtime.management.ensureHome(context);
      if (parts.length === 4 && parts[3] === 'principals' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['id', 'enabled']);
        send(200, await runtime.management.setEnabled(context, value.id, value.enabled));
        return true;
      }
      if (parts.length === 4 && parts[3] === 'collect' && request.method === 'POST') {
        query(url, []);
        input(await body(request), []);
        send(200, await runtime.filesystem.collect(context));
        return true;
      }
      if (parts.length === 4 && parts[3] === 'home' && request.method === 'GET') {
        query(url, []);
        send(200, { ...home, home: await runtime.filesystem.get(context, home.home.id) });
        return true;
      }
      if (parts.length === 4 && parts[3] === 'resolve' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['path']);
        send(200, await runtime.filesystem.resolve(context, value.path));
        return true;
      }
      if (parts.length === 4 && parts[3] === 'resources' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['reference'], ['projectId']);
        send(200, await runtime.source.resource(context, value.reference, value.projectId));
        return true;
      }
      if (parts.length === 4 && parts[3] === 'changes' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['changes']);
        send(200, await runtime.filesystem.apply(context, key(request), value.changes));
        return true;
      }
      if (parts.length === 4 && parts[3] === 'copy' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), [
          'id',
          'expectedRevision',
          'parentId',
          'expectedParentRevision',
          'name',
        ]);
        send(
          200,
          await runtime.filesystem.copy(
            context,
            key(request),
            value as {
              id: string;
              expectedRevision: string;
              parentId: string;
              expectedParentRevision: string;
              name: string;
            },
          ),
        );
        return true;
      }
      if (parts[3] === 'entries' && parts[4] && parts.length <= 6) {
        const id = parts[4];
        if (parts.length === 5 && request.method === 'GET') {
          query(url, []);
          send(200, await runtime.filesystem.get(context, id));
          return true;
        }
        if (parts[5] === 'children' && request.method === 'GET') {
          query(url, ['cursor']);
          send(
            200,
            await runtime.filesystem.list(context, id, url.searchParams.get('cursor') ?? undefined),
          );
          return true;
        }
        if (parts[5] === 'role' && request.method === 'GET') {
          query(url, []);
          send(200, await runtime.management.role(context, id));
          return true;
        }
        if (parts[5] === 'history' && request.method === 'GET') {
          query(url, ['before']);
          send(
            200,
            await runtime.filesystem.history(
              context,
              id,
              url.searchParams.get('before') ?? undefined,
            ),
          );
          return true;
        }
        if (parts[5] === 'policy') {
          query(url, []);
          if (request.method === 'GET') {
            send(200, await runtime.management.policy(context, id));
            return true;
          }
          if (request.method === 'POST') {
            const value = input(await body(request), ['expectedRevision', 'policy']);
            send(
              200,
              await runtime.management.setPolicy(context, id, value.expectedRevision, value.policy),
            );
            return true;
          }
        }
        const controller = new AbortController();
        request.once('aborted', () => controller.abort());
        response.once('close', () => {
          if (!response.writableFinished) controller.abort();
        });
        const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(30000)]);
        if (parts[5] === 'content' && request.method === 'GET') {
          query(url, ['revision']);
          const file = await runtime.filesystem.readFile(
            context,
            id,
            url.searchParams.get('revision') ?? undefined,
            signal,
          );
          const filename = encodeURIComponent(file.entry.name).replace(
            /['()*]/g,
            (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
          );
          response.writeHead(200, {
            'content-type': file.entry.content!.mediaType,
            'x-moria-size': String(file.entry.content!.size),
            'content-disposition': `attachment; filename*=UTF-8''${filename}`,
            'cache-control': 'no-store',
            'x-content-type-options': 'nosniff',
            'content-security-policy': 'sandbox',
            'x-moria-digest': file.entry.content!.digest,
            'x-moria-revision': file.entry.revision,
          });
          await pipeline(
            Readable.fromWeb(file.stream as import('node:stream/web').ReadableStream),
            response,
            { signal },
          );
          return true;
        }
        if ((parts[5] === 'files' || parts[5] === 'content') && request.method === 'POST') {
          query(url, parts[5] === 'files' ? ['name', 'expectedRevision'] : ['expectedRevision']);
          const sizeHeader = request.headers['x-moria-size'] ?? request.headers['content-length'];
          if (typeof sizeHeader !== 'string' || !/^(0|[1-9][0-9]*)$/.test(sizeHeader))
            throw new MoriaError('INPUT_INVALID', 'A file byte count is required');
          const digest = request.headers['x-moria-digest'],
            type = request.headers['content-type'];
          if (typeof digest !== 'string' || typeof type !== 'string')
            throw new MoriaError('INPUT_INVALID', 'A content digest and media type are required');
          const content = {
            size: Number(sizeHeader),
            digest,
            mediaType: type.split(';')[0].trim().toLowerCase(),
          };
          const change =
            parts[5] === 'files'
              ? {
                  op: 'create' as const,
                  parentId: id,
                  expectedParentRevision: url.searchParams.get('expectedRevision')!,
                  name: url.searchParams.get('name')!,
                  content,
                }
              : {
                  op: 'write' as const,
                  id,
                  expectedRevision: url.searchParams.get('expectedRevision')!,
                  content,
                };
          send(
            200,
            await runtime.filesystem.writeFile(
              context,
              key(request),
              change,
              Readable.toWeb(request) as ReadableStream<Uint8Array>,
              signal,
            ),
          );
          return true;
        }
        if (parts[5] === 'version' && request.method === 'GET') {
          query(url, ['revision']);
          send(
            200,
            await runtime.filesystem.version(context, id, url.searchParams.get('revision')!),
          );
          return true;
        }
      }
      if (parts.length === 4 && parts[3] === 'projects' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['parentId', 'name', 'expectedRevision']);
        send(
          200,
          await runtime.management.createProject(
            context,
            value.parentId,
            value.name,
            value.expectedRevision,
          ),
        );
        return true;
      }
      if (parts.length === 4 && parts[3] === 'groups' && request.method === 'POST') {
        query(url, []);
        const value = input(await body(request), ['name', 'groupKey', 'expectedRevision']);
        send(
          200,
          await runtime.management.createGroup(
            context,
            value.name,
            value.groupKey,
            value.expectedRevision,
          ),
        );
        return true;
      }
      if (parts[3] === 'sources' && parts[4] && parts.length === 6 && request.method === 'POST') {
        if (parts[5] === 'retire') {
          query(url, []);
          const value = input(await body(request), ['expectedRevision']);
          send(200, await runtime.source.retire(context, parts[4], value.expectedRevision));
          return true;
        }
        query(url, []);
        const id = parts[4];
        if (parts[5] === 'register') {
          const value = input(await body(request), ['kind', 'expectedRevision']);
          send(200, await runtime.source.register(context, id, value.expectedRevision, value.kind));
          return true;
        }
        if (parts[5] === 'checkout') {
          input(await body(request), []);
          send(200, await runtime.source.checkout(context, id));
          return true;
        }
        if (parts[5] === 'publish') {
          const value = input(await body(request), ['expectedSourceDigest'], ['exports']);
          send(
            200,
            await runtime.source.publish(context, id, value.expectedSourceDigest, value.exports),
          );
          return true;
        }
        if (parts[5] === 'plan') {
          const value = input(await body(request), ['digest'], ['deletions']);
          send(200, await runtime.source.plan(context, id, value.digest, value.deletions));
          return true;
        }
      }
      if (
        parts[3] === 'releases' &&
        parts[4] === 'read' &&
        parts.length === 5 &&
        request.method === 'POST'
      ) {
        query(url, []);
        const value = input(await body(request), ['digest']);
        send(200, await runtime.source.getRelease(context, value.digest));
        return true;
      }
      if (
        parts[3] === 'plans' &&
        parts[4] &&
        parts[5] === 'apply' &&
        parts.length === 6 &&
        request.method === 'POST'
      ) {
        query(url, []);
        input(await body(request), []);
        send(200, await runtime.source.apply(context, parts[4]));
        return true;
      }
      throw new MoriaError('NOT_FOUND', 'Filesystem endpoint not found');
    } catch (error) {
      if (response.headersSent) {
        response.destroy();
        return true;
      }
      const code = (error as { code?: string }).code;
      const known = error instanceof MoriaError || error instanceof StorageError;
      const safeCode = known && code ? code : code === '22P02' ? 'INPUT_INVALID' : 'INTERNAL';
      const status =
        safeCode === 'UNAUTHENTICATED'
          ? 401
          : ['FORBIDDEN', 'AUTHORITY_DENIED', 'MARKING_DOWNGRADE', 'FUNCTION_EFFECT'].includes(
                safeCode,
              )
            ? 403
            : safeCode === 'NOT_FOUND'
              ? 404
              : /CONFLICT|AUTHORIZATION_CHANGED/.test(safeCode)
                ? 409
                : /EXPIRED/.test(safeCode)
                  ? 410
                  : safeCode === 'LIMIT_EXCEEDED'
                    ? 413
                    : safeCode === 'UNSUPPORTED_CAPABILITY'
                      ? 422
                      : /INVALID|MISMATCH/.test(safeCode)
                        ? 400
                        : 500;
      send(status, {
        error: {
          code: safeCode,
          message: known ? (error as Error).message : 'Request failed',
          correlationId,
        },
      });
      return true;
    }
  };
}
