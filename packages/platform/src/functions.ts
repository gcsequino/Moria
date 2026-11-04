import type { Transport, RequestOptions, ActionOptions } from './transport.js';
import { ApiError } from './transport.js';
import { VALUE_VERSION } from '../../contracts/value-types.js';
import type { FunctionInvocation, FunctionEvent } from '../../contracts/functions.js';
import { checkedCancellation } from './execution.js';

export function functionsClient(transport: Transport) {
  const checked = (value: FunctionInvocation) => {
    if (value.valueVersion !== VALUE_VERSION)
      throw new ApiError(502, 'UNSUPPORTED_VALUE_VERSION', 'Unsupported invocation value version');
    return value;
  };
  const invocation = (id: string) => `/api/v1/invocations/${encodeURIComponent(id)}`;
  return {
    invoke: (resourceId: string, input: unknown, settings: ActionOptions & { wait?: boolean }) =>
      transport
        .request<FunctionInvocation>(
          `/api/v1/functions/${encodeURIComponent(resourceId)}/invoke`,
          'POST',
          { input, ...(settings.wait === undefined ? {} : { wait: settings.wait }) },
          settings,
        )
        .then(checked),
    get: (id: string, settings?: RequestOptions) =>
      transport
        .request<FunctionInvocation>(invocation(id), 'GET', undefined, settings)
        .then(checked),
    events: (id: string, after = '0', settings?: RequestOptions) =>
      transport.request<{ events: FunctionEvent[] }>(
        `${invocation(id)}/events?${new URLSearchParams({ after })}`,
        'GET',
        undefined,
        settings,
      ),
    cancel: (id: string, settings?: RequestOptions) =>
      transport
        .request<unknown>(`${invocation(id)}/cancel`, 'POST', {}, settings)
        .then((value) => checkedCancellation(id, value)),
  };
}
