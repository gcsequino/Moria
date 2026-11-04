import type { PipelineExecution, PipelineEvent } from '../../contracts/pipelines.js';
import { VALUE_VERSION } from '../../contracts/value-types.js';
import { checkedCancellation } from './execution.js';
import { ApiError, type Transport, type ActionOptions, type RequestOptions } from './transport.js';

export function pipelinesClient(transport: Transport) {
  const checked = (value: PipelineExecution) => {
    if (value.valueVersion !== VALUE_VERSION)
      throw new ApiError(502, 'UNSUPPORTED_VALUE_VERSION', 'Unsupported pipeline value version');
    return value;
  };
  const execution = (id: string) => `/api/v1/pipeline-executions/${encodeURIComponent(id)}`;
  return {
    invoke: (resource: string, settings: ActionOptions & { wait?: boolean }) =>
      transport
        .request<PipelineExecution>(
          `/api/v1/pipelines/${encodeURIComponent(resource)}/executions`,
          'POST',
          { ...(settings.wait === undefined ? {} : { wait: settings.wait }) },
          settings,
        )
        .then(checked),
    get: (id: string, settings?: RequestOptions) =>
      transport.request<PipelineExecution>(execution(id), 'GET', undefined, settings).then(checked),
    events: (id: string, after = '0', settings?: RequestOptions) =>
      transport.request<{ events: PipelineEvent[]; after: string }>(
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
