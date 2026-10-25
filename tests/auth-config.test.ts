import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { authCommand } from '../apps/cli/src/auth.js';
import { loadAuthConfig } from '../services/security/src/auth-config.js';

test('Google CLI setup validates redirects, references private credentials and supports offline recovery', async () => {
  const directory = resolve('.local', `auth-config-${randomUUID()}`);
  await mkdir(directory, { recursive: true });
  const setup = await authCommand(['setup'], { directory });
  assert.equal(
    setup.authorizedRedirectUri,
    `http://127.0.0.1:${process.env.MORIA_PORT ?? 3210}/api/auth/callback`,
  );
  assert.ok(setup.instructions.some((step: string) => step.includes('grant-admin')));
  const file = resolve(directory, 'download.json');
  const web = {
    client_id: 'client',
    client_secret: 'private-test-secret',
    redirect_uris: ['http://wrong.test'],
  };
  await writeFile(file, JSON.stringify({ web }));
  await assert.rejects(
    authCommand(['configure-google'], { directory, config: file, email: 'reader@gmail.com' }),
    /redirect URI/,
  );
  web.redirect_uris = [setup.authorizedRedirectUri];
  await writeFile(file, JSON.stringify({ web }));
  const configured = await authCommand(['configure-google'], {
    directory,
    config: file,
    email: 'Reader@gmail.com',
  });
  assert.ok(!JSON.stringify(configured).includes(web.client_secret));
  const path = resolve(directory, 'auth', 'providers.json');
  assert.ok(!(await readFile(path, 'utf8')).includes(web.client_secret));
  const first = await loadAuthConfig(directory),
    second = await loadAuthConfig(directory);
  assert.deepEqual(first.key, second.key);
  assert.deepEqual(first.providers[0].emails, ['reader@gmail.com']);
  assert.equal(first.providers[0].clientSecret, web.client_secret);
  assert.deepEqual(first.managementProviders, []);
  await authCommand(['grant-admin'], { directory, email: 'Admin@gmail.com' });
  const adminConfig = await loadAuthConfig(directory);
  assert.deepEqual(adminConfig.providers[0].emails, ['reader@gmail.com']);
  assert.deepEqual(adminConfig.managementProviders[0].emails, ['admin@gmail.com']);
  await authCommand(['configure-google'], { directory, config: file, email: 'reader@gmail.com' });
  assert.deepEqual((await loadAuthConfig(directory)).managementProviders[0].emails, [
    'admin@gmail.com',
  ]);
  await writeFile(path, '{broken');
  await authCommand(['disable'], { directory });
  assert.deepEqual((await loadAuthConfig(directory)).providers, []);
});

test('management validates all enabled declarations but reads only required provider credentials', async () => {
  const directory = resolve('.local', `auth-selection-${randomUUID()}`);
  const auth = resolve(directory, 'auth');
  await mkdir(auth, { recursive: true });
  const provider = {
    id: 'unused',
    label: 'Unused',
    issuer: 'https://identity.example.test',
    credentialsFile: 'unavailable.json',
    emails: ['reader@example.test'],
  };
  const save = async (providers: unknown[]) =>
    writeFile(resolve(auth, 'providers.json'), JSON.stringify({ providers }));
  await save([provider]);
  assert.deepEqual((await loadAuthConfig(directory, 'management')).managementProviders, []);
  await assert.rejects(loadAuthConfig(directory), /credential JSON/);
  await save([{ ...provider, managementEmails: ['admin@example.test'] }]);
  await assert.rejects(loadAuthConfig(directory, 'management'), /credential JSON/);
  await save([{ ...provider, issuer: 'http://identity.example.test' }]);
  await assert.rejects(loadAuthConfig(directory, 'management'), /HTTPS/);
  await save([{ ...provider, managementEmails: ['invalid'] }]);
  await assert.rejects(loadAuthConfig(directory, 'management'), /Invalid OIDC/);
  await save([provider, provider]);
  await assert.rejects(loadAuthConfig(directory, 'management'), /Duplicate/);
  await writeFile(
    resolve(auth, 'required.json'),
    JSON.stringify({ client_id: 'synthetic', client_secret: 'synthetic' }),
  );
  await save([
    provider,
    {
      ...provider,
      id: 'required',
      credentialsFile: 'required.json',
      managementEmails: ['ADMIN@example.test'],
    },
  ]);
  assert.deepEqual(
    (await loadAuthConfig(directory, 'management')).managementProviders.map((p) => [
      p.id,
      p.emails,
    ]),
    [['required', ['admin@example.test']]],
  );
});
