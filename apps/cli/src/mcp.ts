import { MoriaError } from '../../../packages/contracts/types.js';

export async function mcpCommand(args: string[], values: { url?: string; cursor?: string }) {
  if (args.length !== 1 || !['config', 'discover', 'tools'].includes(args[0]))
    throw new MoriaError(
      'USAGE',
      'moria mcp config|discover|tools [--url https://instance.example] [--cursor token]',
    );
  const url = new URL(
    values.url ?? process.env.MORIA_URL ?? `http://127.0.0.1:${process.env.MORIA_PORT ?? '3210'}`,
  );
  if (
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash ||
    !['http:', 'https:'].includes(url.protocol) ||
    (url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))
  )
    throw new MoriaError(
      'USAGE',
      'Use an HTTPS instance origin, or HTTP on loopback, without credentials or a path',
    );
  const endpoint = `${url.origin}/mcp`,
    protocolVersion = '2026-07-28';
  if (args[0] === 'config')
    return {
      transport: 'streamable-http',
      url: endpoint,
      protocolVersion,
      credentialEnv: 'MORIA_API_TOKEN',
    };
  const token = process.env.MORIA_API_TOKEN;
  if (!token || /[\r\n]/.test(token))
    throw new MoriaError('USAGE', 'MCP requires a dedicated MORIA_API_TOKEN');
  const method = args[0] === 'discover' ? 'server/discover' : 'tools/list';
  const response = await fetch(endpoint, {
    method: 'POST',
    signal: AbortSignal.timeout(30000),
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
      accept: 'application/json, text/event-stream',
      'mcp-protocol-version': protocolVersion,
      'mcp-method': method,
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method,
      params: {
        ...(values.cursor ? { cursor: values.cursor } : {}),
        _meta: {
          'io.modelcontextprotocol/protocolVersion': protocolVersion,
          'io.modelcontextprotocol/clientCapabilities': {},
        },
      },
    }),
  });
  const reader = response.body?.getReader();
  if (!reader) throw new MoriaError('MCP_REQUEST_FAILED', 'MCP response body is missing');
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > 8 * 1024 * 1024 + 65536) {
        await reader.cancel();
        throw new MoriaError('RESOURCE_LIMIT', 'MCP response exceeds its byte limit');
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  const value = JSON.parse(new TextDecoder().decode(bytes));
  if (!response.ok || value.error)
    throw new MoriaError('MCP_REQUEST_FAILED', value.error?.message ?? 'MCP request failed');
  return value.result;
}
