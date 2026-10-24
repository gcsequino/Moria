import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { randomBytes } from 'node:crypto';
import type { OidcProvider } from './oidc.js';

export async function loadAuthConfig(
  directory: string,
): Promise<{ providers: OidcProvider[]; managementProviders: OidcProvider[]; key: Buffer }> {
  const authDir = resolve(directory, 'auth');
  await mkdir(authDir, { recursive: true });
  const keyPath = resolve(authDir, 'session-key');
  try {
    await writeFile(keyPath, randomBytes(32), { flag: 'wx', mode: 0o600 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
  }
  const key = await readFile(keyPath);
  if (key.length !== 32)
    throw new Error('Invalid auth/session-key. Restore it or revoke sessions before replacing it.');
  let config;
  try {
    config = JSON.parse(await readFile(resolve(authDir, 'providers.json'), 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT')
      return { providers: [], managementProviders: [], key };
    throw new Error(
      'Invalid auth/providers.json. CLI recovery remains available after correcting the file.',
    );
  }
  if (!Array.isArray(config.providers)) throw new Error('Expected a providers array.');
  const providers: OidcProvider[] = [];
  const managementProviders: OidcProvider[] = [];
  for (const entry of config.providers) {
    if (entry.enabled === false) continue;
    if (
      !/^[a-z][a-z0-9-]{0,39}$/.test(entry.id) ||
      typeof entry.label !== 'string' ||
      typeof entry.issuer !== 'string' ||
      typeof entry.credentialsFile !== 'string' ||
      !Array.isArray(entry.emails) ||
      !entry.emails.every((s: unknown) => typeof s === 'string' && /^[^\s@]+@[^\s@]+$/.test(s)) ||
      (entry.managementEmails !== undefined &&
        (!Array.isArray(entry.managementEmails) ||
          !entry.managementEmails.every(
            (s: unknown) => typeof s === 'string' && /^[^\s@]+@[^\s@]+$/.test(s),
          )))
    )
      throw new Error('Invalid OIDC provider configuration.');
    const issuer = new URL(entry.issuer);
    if (
      issuer.protocol !== 'https:' ||
      issuer.username ||
      issuer.password ||
      issuer.search ||
      issuer.hash
    )
      throw new Error('Provider issuer must be an HTTPS issuer URL.');
    let secret;
    try {
      secret = JSON.parse(await readFile(resolve(authDir, entry.credentialsFile), 'utf8'));
    } catch {
      throw new Error('Could not read provider credential JSON. Check its local path and format.');
    }
    const credentials = secret.web ?? secret;
    if (
      typeof credentials.client_id !== 'string' ||
      !credentials.client_id ||
      typeof credentials.client_secret !== 'string' ||
      !credentials.client_secret
    )
      throw new Error(
        'Credentials must contain client_id and client_secret (Google Web application JSON is supported).',
      );
    if (providers.some((p) => p.id === entry.id)) throw new Error('Duplicate provider id.');
    providers.push({
      id: entry.id,
      label: entry.label,
      issuer: entry.issuer,
      clientId: credentials.client_id,
      clientSecret: credentials.client_secret,
      emails: entry.emails.map((email: string) => email.toLowerCase()),
    });
    if (entry.managementEmails?.length)
      managementProviders.push({
        ...providers.at(-1)!,
        emails: entry.managementEmails.map((email: string) => email.toLowerCase()),
      });
  }
  return { providers, managementProviders, key };
}
