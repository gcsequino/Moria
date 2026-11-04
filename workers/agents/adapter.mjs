export const description = Object.freeze({
  abiVersion: 'agent/v1',
  framework: 'langgraph-js',
  capabilities: ['events', 'cancel'],
});

const program = await import('/agent/program.mjs');
export const createOsdk = program.createOsdk;

/** Guest adapter only; identity, tools, model dispatch and persistence remain native. */
export default async function start(input, context) {
  if (typeof program.createGraph !== 'function') throw new Error('Agent graph export required');
  const model = Object.freeze({
    async chat(request, logicalId) {
      if (typeof logicalId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$/.test(logicalId))
        throw new Error('Stable model call ID required');
      const response = await context.platform.fetch('/api/v1/agent/model', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': logicalId },
        body: JSON.stringify(request),
      });
      if (!response.ok) throw new Error('Agent model call failed');
      return response.json();
    },
  });
  const graph = await program.createGraph({ ...context, executionId: context.invocationId, model });
  if (!graph || typeof graph.invoke !== 'function')
    throw new Error('Compiled agent graph required');
  context.log('agent graph started');
  const result = await graph.invoke(input, { signal: context.signal, recursionLimit: 24 });
  context.signal.throwIfAborted();
  if (typeof program.result !== 'function') throw new Error('Agent result export required');
  return program.result(result);
}
