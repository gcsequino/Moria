import type { IncomingMessage, ServerResponse } from 'node:http';
import { BrowserSessions, digest } from '../../security/src/browser-sessions.js';
import {
  body,
  sendJson,
  cliKeyMatches,
  requireBrowserOrigin,
  requireRecoveryAllowed,
} from '../../http/src/server.js';
import { MoriaError } from '../../../packages/contracts/types.js';

export class AppAuth {
  readonly cookieName: string;
  constructor(
    readonly sessions: BrowserSessions,
    private readonly recoveryKey: string,
    readonly allowRecovery = true,
  ) {
    this.cookieName = `moria_${digest(sessions.binding.origin + '/' + sessions.binding.application).slice(0, 16)}`;
  }
  cookie(req: IncomingMessage, name = this.cookieName) {
    const values = (req.headers.cookie ?? '')
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.startsWith(`${name}=`));
    return values.length === 1 ? values[0].slice(name.length + 1) : '';
  }
  private setCookie(res: ServerResponse, value: string, flow = false) {
    const name = this.cookieName + (flow ? '_flow' : '');
    const secure = this.sessions.binding.origin.startsWith('https:') ? '; Secure' : '';
    res.setHeader(
      'set-cookie',
      `${name}=${value}; HttpOnly; SameSite=Lax; Path=/api/; Max-Age=${value ? (flow ? 300 : 3600) : 0}${secure}`,
    );
  }
  async handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const url = new URL(req.url ?? '/', this.sessions.binding.origin);
    if (!url.pathname.startsWith('/api/auth/')) return false;
    requireRecoveryAllowed(url.pathname, this.allowRecovery);
    res.setHeader('cache-control', 'no-store');
    res.setHeader('referrer-policy', 'no-referrer');
    const send = (status: number, value: unknown) => sendJson(res, status, value);
    const token = this.cookie(req);
    requireBrowserOrigin(req, this.sessions.binding.origin, false, this.allowRecovery);
    if (url.pathname === '/api/auth/session' && req.method === 'GET') {
      let user = null;
      if (token) {
        try {
          user = await this.sessions.describe(token);
        } catch (e) {
          if (
            !(e instanceof MoriaError) ||
            !['UNAUTHENTICATED', 'FORBIDDEN', 'IDENTITY_STALE'].includes(e.code)
          )
            throw e;
          this.setCookie(res, '');
        }
      }
      send(200, {
        user,
        providers: [...this.sessions.providers.values()].map(({ provider }) => ({
          id: provider.id,
          label: provider.label,
        })),
      });
    } else if (url.pathname === '/api/auth/login' && req.method === 'POST') {
      const input = await body(req);
      const flow = await this.sessions.start(input?.provider);
      this.setCookie(res, flow.browser, true);
      send(200, { url: flow.location });
    } else if (url.pathname === '/api/auth/callback' && req.method === 'GET') {
      try {
        const next = await this.sessions.finish(url, this.cookie(req, this.cookieName + '_flow'));
        if (token) await this.sessions.logout(token);
        this.setCookie(res, next);
        res.writeHead(303, { location: this.sessions.binding.entryPath });
      } catch {
        res.writeHead(303, { location: `${this.sessions.binding.entryPath}?auth=failed` });
      }
      res.end();
    } else if (url.pathname === '/api/auth/logout' && req.method === 'POST') {
      await this.sessions.logout(token);
      this.setCookie(res, '');
      send(200, { signedOut: true });
    } else if (url.pathname === '/api/auth/recovery-ticket' && req.method === 'POST') {
      if (!cliKeyMatches(req, 'x-moria-app-key', this.recoveryKey))
        throw new MoriaError('UNAUTHENTICATED', 'Local CLI credentials are required.');
      const ticket = await this.sessions.recoveryTicket();
      send(200, {
        url: `${this.sessions.binding.origin}${this.sessions.binding.entryPath}#recovery=${ticket}`,
        expiresIn: 300,
      });
    } else if (url.pathname === '/api/auth/recover' && req.method === 'POST') {
      const input = await body(req);
      if (typeof input?.ticket !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(input.ticket))
        throw new MoriaError('UNAUTHENTICATED', 'Invalid recovery link.');
      const next = await this.sessions.recover(input.ticket);
      if (token) await this.sessions.logout(token);
      this.setCookie(res, next);
      send(200, { recovered: true });
    } else send(404, { error: { code: 'NOT_FOUND', message: 'Auth endpoint not found.' } });
    req.resume();
    return true;
  }
}
