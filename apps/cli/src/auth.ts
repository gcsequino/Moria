import { mkdir, readFile, writeFile, rename } from 'node:fs/promises';
import { resolve } from 'node:path';
import { MoriaError } from '../../../packages/contracts/types.js';

export async function authCommand(
  args: string[],
  options: { directory?: string; config?: string; email?: string; app?: string },
) {
  const [command, ...extra] = args;
  if (
    extra.length ||
    !['setup', 'configure-google', 'grant-admin', 'recover', 'disable'].includes(command)
  )
    throw new MoriaError(
      'USAGE',
      'auth setup | configure-google --config downloaded-client.json --email you@gmail.com | grant-admin --email you@gmail.com | recover [--app management|snotel] | disable',
    );
  const directory = resolve(options.directory ?? process.env.MORIA_LOCAL_DIR ?? '.local/workspace');
  if (options.app && !['management', 'snotel'].includes(options.app))
    throw new MoriaError('USAGE', 'App must be management or snotel.');
  const management = options.app === 'management';
  const port = Number(
    management ? (process.env.MORIA_PORT ?? 3210) : (process.env.MORIA_SNOTEL_PORT ?? 3211),
  );
  if (!Number.isInteger(port) || port < 1024 || port > 65535)
    throw new MoriaError('USAGE', 'Invalid SNOTEL port.');
  const origin = `http://127.0.0.1:${port}`,
    callback = `${origin}/api/auth/callback`;
  const authDir = resolve(directory, 'auth'),
    configPath = resolve(authDir, 'providers.json');
  if (command === 'recover') {
    const key = management
      ? JSON.parse(await readFile(resolve(directory, 'credentials.json'), 'utf8')).accessKey
      : (await readFile(resolve(directory, 'snow-access-key'), 'utf8')).trim();
    const response = await fetch(`${origin}/api/auth/recovery-ticket`, {
      method: 'POST',
      headers: { 'x-moria-app-key': key },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok)
      throw new MoriaError('RECOVERY_FAILED', 'Start Moria locally, then retry recovery.');
    return response.json();
  }
  if (command === 'setup')
    return {
      console: 'https://console.cloud.google.com/auth/clients',
      clientType: 'Web application',
      authorizedRedirectUri: callback,
      scopes: ['openid', 'email', 'profile'],
      instructions: [
        'Choose a Google Cloud project. Configure Google Auth Platform branding and audience; while in Testing add your Gmail or Workspace account as a test user.',
        `Create an OAuth Web application client with this exact authorized redirect URI: ${callback}`,
        'Download the client JSON to a private local folder. Do not paste the client secret into chat or commit it.',
        'Run: pnpm moria auth configure-google --config "C:\\path\\client.json" --email "you@gmail.com"',
        'Restart pnpm start, then open the snow app and choose Continue with Google.',
      ],
      configPath,
      recovery: 'pnpm moria auth recover',
    };
  await mkdir(authDir, { recursive: true });
  if (command === 'disable') {
    // Offline escape hatch, including malformed config. Preserve the old file.
    try {
      await rename(configPath, `${configPath}.disabled-${Date.now()}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
    return {
      disabled: true,
      next: 'Restart pnpm start, then run pnpm moria auth recover. Existing provider sessions will be invalidated.',
    };
  }
  if (command === 'grant-admin') {
    if (!options.email || !/^[^\s@]+@[^\s@]+$/.test(options.email))
      throw new MoriaError(
        'USAGE',
        'Provide --email for the explicit Google administrator invitation.',
      );
    const config = JSON.parse(await readFile(configPath, 'utf8'));
    const google = config.providers?.find((p: { id: string }) => p.id === 'google');
    if (!google) throw new MoriaError('INPUT_INVALID', 'Configure the Google provider first.');
    google.managementEmails = [
      ...new Set([...(google.managementEmails ?? []), options.email.toLowerCase()]),
    ];
    await writeFile(configPath, JSON.stringify(config, null, 2), { mode: 0o600 });
    return {
      administratorInvitation: options.email.toLowerCase(),
      callback: `http://127.0.0.1:${process.env.MORIA_PORT ?? 3210}/api/auth/callback`,
      next: 'Add the management callback to the existing Google Web client, then restart Moria. First management sign-in grants explicit admin operations; returning sign-ins do not restore revoked grants.',
    };
  }
  if (!options.config || !options.email || !/^[^\s@]+@[^\s@]+$/.test(options.email))
    throw new MoriaError(
      'USAGE',
      'Provide --config downloaded-client.json and --email your-account.',
    );
  const credentialsFile = resolve(options.config);
  let credentials;
  try {
    credentials = JSON.parse(await readFile(credentialsFile, 'utf8')).web;
  } catch {
    throw new MoriaError('INPUT_INVALID', 'Could not read Google Web application client JSON.');
  }
  if (
    !credentials ||
    typeof credentials.client_id !== 'string' ||
    typeof credentials.client_secret !== 'string' ||
    !Array.isArray(credentials.redirect_uris) ||
    !credentials.redirect_uris.includes(callback)
  )
    throw new MoriaError(
      'INPUT_INVALID',
      `Use a Google Web application client with redirect URI ${callback}.`,
    );
  let config: { providers: { id: string; [key: string]: unknown }[] } = { providers: [] };
  try {
    config = JSON.parse(await readFile(configPath, 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  if (!Array.isArray(config.providers))
    throw new MoriaError('INPUT_INVALID', 'Invalid providers file. Use auth disable to recover.');
  const existing = config.providers.find((p) => p.id === 'google');
  config.providers = config.providers.filter((p) => p.id !== 'google');
  config.providers.push({
    id: 'google',
    label: 'Google',
    issuer: 'https://accounts.google.com',
    credentialsFile,
    emails: [options.email.toLowerCase()],
    enabled: true,
    ...(existing?.managementEmails ? { managementEmails: existing.managementEmails } : {}),
  });
  await writeFile(configPath, JSON.stringify(config, null, 2), { mode: 0o600 });
  return {
    configured: 'google',
    invitedEmail: options.email,
    callback,
    next: 'Restart pnpm start, then open /snow/. Credentials remain in your local file.',
  };
}
