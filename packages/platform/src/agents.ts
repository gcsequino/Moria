import type { AgentExecution, AgentEvent } from '../../contracts/agents.js';
import { VALUE_VERSION } from '../../contracts/value-types.js';
import { checkedCancellation } from './execution.js';
import { ApiError, type Transport, type ActionOptions, type RequestOptions } from './transport.js';

/** Domain lifecycle over the authenticated persistent native API. */
export function agentsClient(transport: Transport) {
  const checked = (value: AgentExecution) => {
    if (value.valueVersion !== VALUE_VERSION)
      throw new ApiError(502, 'UNSUPPORTED_VALUE_VERSION', 'Unsupported agent value version');
    return value;
  };
  const execution = (id: string) => `/api/v1/agent-executions/${encodeURIComponent(id)}`;
  return {
    invoke: (resource: string, input: unknown, settings: ActionOptions & { wait?: boolean }) =>
      transport
        .request<AgentExecution>(
          `/api/v1/agents/${encodeURIComponent(resource)}/executions`,
          'POST',
          { input, ...(settings.wait === undefined ? {} : { wait: settings.wait }) },
          settings,
        )
        .then(checked),
    get: (id: string, settings?: RequestOptions) =>
      transport.request<AgentExecution>(execution(id), 'GET', undefined, settings).then(checked),
    events: (id: string, after = '0', settings?: RequestOptions) =>
      transport.request<{ events: AgentEvent[]; after: string }>(
        `${execution(id)}/events?${new URLSearchParams({ after })}`,
        'GET',
        undefined,
        settings,
      ),
    cancel: (id: string, settings?: RequestOptions) =>
      transport
        .request<unknown>(`${execution(id)}/cancel`, 'POST', {}, settings)
        .then((value) => checkedCancellation(id, value)),
  };
}
