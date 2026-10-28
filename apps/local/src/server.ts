import { readWorkflow } from '../../../services/lineage/src/workflow.js';
import type { Jobs } from '../../../services/execution/src/jobs.js';
import { readLineage } from '../../../services/lineage/src/service.js';
import { configInput } from '../../../services/snotel/src/connector.js';
import {
  createHttpServer,
  browserProfile,
  dispatchHttp,
  sendJson,
} from '../../../services/http/src/server.js';
import { AppHost } from '../../../services/app-host/src/store.js';
import { localAppHost } from '../../../services/app-host/src/bootstrap.js';
import { serveApp } from '../../../services/app-host/src/http.js';
import { AppAuth } from '../../../services/app-host/src/auth.js';
import type { PoolClient } from 'pg';
import type { SnowService } from '../../../services/snotel/src/service.js';
import { createApi, body } from '../../api/src/server.js';
import { MoriaError } from '../../../packages/contracts/types.js';
import {
  editAction,
  fieldActions,
  resetAction,
  profiles,
  type Profile,
  type Workspace,
} from './workspace.js';

export function createLocalServer(
  workspace: Workspace,
  accessKey: string,
  appHost: AppHost = localAppHost(),
  snow?: SnowService,
  auth?: AppAuth,
  publicOrigin?: string,
  jobs?: Jobs,
  options: { serveManagement?: boolean } = {},
) {
  const profile = browserProfile('management', accessKey, auth, publicOrigin);
  const routes = {
    objects: new Map([['assets', workspace.service]]),
    actions: new Map([
      ...fieldActions.map(
        (action): [string, { service: Workspace['service']; action: typeof action }] => [
          action.resource,
          { service: workspace.service, action },
        ],
      ),
      ['edit-asset', { service: workspace.service, action: editAction }],
      ['reset-asset', { service: workspace.service, action: resetAction }],
    ]),
  };
  const api = createApi(workspace.sessions, routes);
  const adminService = workspace.service.requiring([{ resource: 'instance', operation: 'manage' }]);
  const browserApi = auth
    ? createApi(auth.sessions, {
        objects: new Map([['assets', adminService]]),
        actions: new Map(
          [...routes.actions].map(([name, route]) => [name, { ...route, service: adminService }]),
        ),
      })
    : undefined;
  const server = createHttpServer(profile, async (request, response, { origin, cli }) => {
    const send = (status: number, value: unknown) => sendJson(response, status, value);
    const originalPath = new URL(request.url ?? '/', origin).pathname;
    const path = originalPath.replace(/^\/api\/management\//, '/management/');
    if (auth && (await auth.handle(request, response))) return;
    if (options.serveManagement !== false) {
      if (await serveApp(appHost, 'management', request, response)) return;
    } else if (!['/api/', '/management/', '/local/'].some((prefix) => path.startsWith(prefix))) {
      request.resume();
      send(404, { error: { code: 'NOT_FOUND', message: 'Endpoint not found.' } });
      return;
    }
    const executionRoute = async (actor: string, check: (c?: PoolClient) => Promise<void>) => {
      if (!jobs) return false;
      if (path === '/management/v1/jobs' && request.method === 'GET') {
        const result = await jobs.inspect();
        await check();
        send(200, { ...result, registeredKinds: Object.keys(jobs.handlers) });
        return true;
      }
      if (path === '/management/v1/lineage' && request.method === 'GET') {
        const result = await readLineage(jobs.catalog);
        const workflow = await readWorkflow(appHost);
        const nodes = new Map(result.nodes.map((n) => [n.id, n]));
        for (const node of workflow.nodes) if (!nodes.has(node.id)) nodes.set(node.id, node);
        result.nodes = [...nodes.values()];
        result.edges.push(...workflow.edges);
        result.limitations.push(...workflow.limitations);
        await check();
        send(200, result);
        return true;
      }
      const workload = /^\/management\/v1\/jobs\/([a-z][a-z0-9-]*)\/(enqueue|schedule)$/.exec(path);
      if (workload && request.method === 'POST') {
        const input = (await body(request)) as Record<string, unknown>;
        if (!input || typeof input !== 'object' || Array.isArray(input))
          throw new MoriaError('INPUT_INVALID', 'Expected workload parameters.');
        if (workload[2] === 'enqueue') {
          if (
            Object.keys(input).some((key) => !['requestKey', 'input'].includes(key)) ||
            typeof input.requestKey !== 'string'
          )
            throw new MoriaError('INPUT_INVALID', 'Expected requestKey and optional input.');
          send(
            202,
            await jobs.enqueue(
              workload[1],
              input.input === undefined ? {} : input.input,
              input.requestKey,
              actor,
              check,
            ),
          );
        } else {
          if (
            Object.keys(input).some((key) => !['enabled', 'intervalMinutes'].includes(key)) ||
            typeof input.enabled !== 'boolean' ||
            typeof input.intervalMinutes !== 'number'
          )
            throw new MoriaError('INPUT_INVALID', 'Expected enabled and intervalMinutes.');
          send(200, await jobs.schedule(workload[1], input.enabled, input.intervalMinutes, check));
        }
        return true;
      }
      const legacyKind = /^\/management\/v1\/(snotel|weather)\//.exec(path)?.[1];
      // Standalone legacy fixtures may explicitly compose their old handlers.
      if (legacyKind && !snow && !Object.hasOwn(jobs.handlers, legacyKind))
        throw new MoriaError('INPUT_INVALID', 'The bundled Snow/weather POC is retired.');
      const syncKind =
        path === '/management/v1/snotel/sync'
          ? 'snotel'
          : path === '/management/v1/weather/sync'
            ? 'weather'
            : undefined;
      if (syncKind && request.method === 'POST') {
        const input = (await body(request)) as { requestKey?: string; config?: unknown };
        if (
          !input ||
          typeof input !== 'object' ||
          Array.isArray(input) ||
          Object.keys(input).some((k) => !['requestKey', 'config'].includes(k))
        )
          throw new MoriaError('INPUT_INVALID', 'Expected requestKey and optional SNOTEL config.');
        if (syncKind === 'weather' && input.config !== undefined)
          throw new MoriaError('INPUT_INVALID', 'Weather locations are server configured.');
        const config = input.config === undefined ? {} : configInput(input.config);
        send(202, await jobs.enqueue(syncKind, config, input.requestKey ?? '', actor, check));
        return true;
      }
      const scheduleKind =
        path === '/management/v1/snotel/schedule'
          ? 'snotel'
          : path === '/management/v1/weather/schedule'
            ? 'weather'
            : undefined;
      if (scheduleKind && request.method === 'POST') {
        const input = (await body(request)) as { enabled: boolean; intervalMinutes: number };
        if (!input || Object.keys(input).some((k) => !['enabled', 'intervalMinutes'].includes(k)))
          throw new MoriaError('INPUT_INVALID', 'Expected enabled and intervalMinutes.');
        send(200, await jobs.schedule(scheduleKind, input.enabled, input.intervalMinutes, check));
        return true;
      }
      if (path === '/management/v1/jobs/retry' && request.method === 'POST') {
        const input = (await body(request)) as { id: string };
        if (!input || typeof input.id !== 'string' || Object.keys(input).some((k) => k !== 'id'))
          throw new MoriaError('INPUT_INVALID', 'Expected job ID.');
        if (!/^[0-9a-f-]{36}$/i.test(input.id))
          throw new MoriaError('INPUT_INVALID', 'Invalid job ID.');
        send(200, await jobs.retry(input.id, check));
        return true;
      }
      return false;
    };
    if (auth && !cli) {
      const bearer = `Bearer ${auth.cookie(request)}`;
      const resolveSession = () => auth.sessions.authenticate(bearer);
      const session = await resolveSession();
      let check = await workspace.managementGuard(session);
      const guard = async (c?: PoolClient) => {
        if (c) await check(c);
        else check = await workspace.managementGuard(await resolveSession());
      };
      if (await executionRoute(session.principal, guard)) return;
      if (path === '/management/v1/workspace' && request.method === 'GET') {
        const status = await workspace.status();
        await guard();
        send(200, { ...status, principal: session.principal, administrator: true });
      } else if (path === '/management/v1/apps' && request.method === 'GET') {
        const apps = await appHost.list();
        await guard();
        send(200, apps);
      } else if (path === '/management/v1/datasets/assets/import' && request.method === 'POST') {
        send(200, await workspace.ingest(session, await body(request), resolveSession));
      } else if (path === '/management/v1/snotel/status' && request.method === 'GET' && snow) {
        const status = await snow.status();
        await guard();
        send(200, status);
      } else if (path === '/management/v1/snotel/sync' && request.method === 'POST' && snow) {
        send(200, await snow.sync(await body(request), undefined, guard));
      } else if (path.startsWith('/api/v1/')) {
        request.headers.authorization = bearer;
        await dispatchHttp(browserApi!, request, response);
      } else send(404, { error: { code: 'NOT_FOUND', message: 'Endpoint not found.' } });
      return;
    }
    if (!cli)
      throw new MoriaError('UNAUTHENTICATED', 'Open the workspace link printed by pnpm start.');
    const profile = auth ? 'operator' : (request.headers['x-moria-profile'] ?? 'operator');
    if (auth) await workspace.requireManagement('operator');
    if (typeof profile !== 'string' || !profiles.includes(profile as Profile))
      throw new MoriaError('INPUT_INVALID', 'Unknown demo identity.');
    if (jobs) {
      const check = async (c?: PoolClient) => {
        const guard = await workspace.managementGuard(profile as Profile);
        if (c) await guard(c);
      };
      if (await executionRoute(profile as string, check)) return;
    }
    if (path.startsWith('/management/v1/snotel/') && snow) {
      await workspace.requireManagement(profile as Profile);
      if (path === '/management/v1/snotel/sync' && request.method === 'POST')
        send(200, await snow.sync(await body(request)));
      else if (path === '/management/v1/snotel/status' && request.method === 'GET')
        send(200, await snow.status());
      else send(404, { error: { message: 'Endpoint not found.' } });
    } else if (path === '/management/v1/apps' && request.method === 'GET') {
      await workspace.requireManagement(profile as Profile);
      send(200, await appHost.list());
    } else if (
      ['/local/status', '/management/v1/workspace'].includes(path) &&
      request.method === 'GET'
    ) {
      send(200, { ...(await workspace.status()), profile });
    } else if (
      ['/local/import', '/management/v1/datasets/assets/import'].includes(path) &&
      request.method === 'POST'
    ) {
      const input = await body(request);
      send(200, await workspace.ingest(profile as Profile, input));
    } else if (path.startsWith('/api/v1/')) {
      request.headers.authorization = `Bearer ${await workspace.token(profile as Profile)}`;
      await dispatchHttp(api, request, response);
    } else send(404, { error: { message: 'Endpoint not found.' } });
  });
  return server;
}
