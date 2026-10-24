/** Explicit trusted origins behind a TLS reverse proxy. Never infer them from forwarded headers. */
export function publicHosting(env: NodeJS.ProcessEnv) {
  const entries: { app: 'management' | 'snotel'; origin: string; port: number }[] = [];
  for (const [app, name, portName, fallback] of [
    ['management', 'MORIA_PUBLIC_ORIGIN', 'MORIA_PUBLIC_PORT', 3212],
    ['snotel', 'MORIA_SNOW_PUBLIC_ORIGIN', 'MORIA_SNOW_PUBLIC_PORT', 3213],
  ] as const) {
    const origin = env[name];
    if (!origin) continue;
    const url = new URL(origin);
    if (url.protocol !== 'https:' || url.origin !== origin || url.username || url.password)
      throw new Error(`${name} must be an exact HTTPS origin without a path or trailing slash.`);
    const port = Number(env[portName] ?? fallback);
    if (!Number.isInteger(port) || port < 1024 || port > 65535)
      throw new Error(`${portName} must be a valid unprivileged port.`);
    entries.push({ app, origin, port });
  }
  if (entries.length === 2 && entries[0].origin === entries[1].origin)
    throw new Error('Management and snow must have separate public origins.');
  const ports = [
    Number(env.MORIA_PORT ?? 3210),
    Number(env.MORIA_SNOTEL_PORT ?? 3211),
    Number(env.MORIA_DB_PORT ?? 55432),
    ...entries.map((e) => e.port),
  ];
  if (new Set(ports).size !== ports.length) throw new Error('Listener ports must differ.');
  return entries;
}
