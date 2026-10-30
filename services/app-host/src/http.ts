import type { IncomingMessage, ServerResponse } from 'node:http';
import { extname } from 'node:path';
import { appHeaders } from '../../http/src/server.js';
import { AppHost, safeFile } from './store.js';

const types: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8',
  '.wasm': 'application/wasm',
};
/** Mount one app per origin. Backend routing and auth are supplied separately. */
export async function serveApp(
  host: AppHost,
  app: string,
  request: IncomingMessage,
  response: ServerResponse,
  entryPath = '/',
) {
  if (request.method !== 'GET' && request.method !== 'HEAD') return false;
  const url = new URL(request.url ?? '/', 'http://localhost');
  const path = url.pathname;
  if (path.startsWith('/api/') || path.startsWith('/management/') || path.startsWith('/local/'))
    return false;
  appHeaders(response);
  try {
    if (path === '/' && entryPath !== '/') {
      response.writeHead(302, {
        location: `${entryPath}${url.search}`,
        'cache-control': 'no-store',
      });
      response.end();
      return true;
    }
    const match = /^\/releases\/([a-f0-9]{64})\/(.*)$/.exec(path);
    const stable =
      !path.startsWith('/releases/') &&
      path.startsWith(entryPath) &&
      /^[a-zA-Z0-9_/-]*$/.test(path.slice(entryPath.length));
    if (!match && !stable) {
      response.writeHead(404);
      response.end();
      return true;
    }
    // Keep the page address readable; its base pins every asset to this release.
    const version = match ? match[1] : (await host.active(app)).current;
    let name = match ? decodeURIComponent(match[2] || 'index.html') : 'index.html';
    if (!name.includes('.') && /^[a-zA-Z0-9_/-]+$/.test(name) && !name.includes('..'))
      name = 'index.html';
    if (!safeFile(name)) {
      response.writeHead(404);
      response.end();
      return true;
    }
    let bytes = await host.file(app, version, name);
    if (name.endsWith('.html'))
      bytes = Buffer.from(
        bytes
          .toString('utf8')
          .replace(/<head(?:\s[^>]*)?>/i, (match) => `${match}<base href="/releases/${version}/">`),
      );
    response.writeHead(200, {
      'content-type': types[extname(name)],
      'cache-control': name.endsWith('.html') ? 'no-store' : 'public,max-age=31536000,immutable',
      'x-moria-app': app,
      'x-moria-version': version,
    });
    response.end(request.method === 'HEAD' ? undefined : bytes);
  } catch (error) {
    const code = (error as { code?: string }).code;
    response.writeHead(code === 'NOT_FOUND' || code === 'ENOENT' ? 404 : 503, {
      'content-type': 'text/plain; charset=utf-8',
      'cache-control': 'no-store',
    });
    response.end(
      `Application unavailable. The operator can inspect or restore app ${app} using the Moria CLI.`,
    );
  }
  return true;
}
