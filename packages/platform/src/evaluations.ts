import type {
  EvaluationRun,
  EvaluationCase,
  EvaluationEvent,
  EvaluationComparison,
} from '../../contracts/evaluations.js';
import { VALUE_VERSION } from '../../contracts/value-types.js';
import { checkedCancellation } from './execution.js';
import { ApiError, type Transport, type ActionOptions, type RequestOptions } from './transport.js';

/** Authenticated lifecycle over the persistent native API. */
export function evaluationsClient(transport: Transport) {
  const checked = (value: EvaluationRun) => {
    if (value.valueVersion !== VALUE_VERSION)
      throw new ApiError(502, 'UNSUPPORTED_VALUE_VERSION', 'Unsupported evaluation value version');
    return value;
  };
  const run = (id: string) => `/api/v1/evaluation-runs/${encodeURIComponent(id)}`;
  return {
    invoke: (resource: string, settings: ActionOptions & { wait?: boolean }) =>
      transport
        .request<EvaluationRun>(
          `/api/v1/evaluations/${encodeURIComponent(resource)}/runs`,
          'POST',
          { ...(settings.wait === undefined ? {} : { wait: settings.wait }) },
          settings,
        )
        .then(checked),
    get: (id: string, settings?: RequestOptions) =>
      transport.request<EvaluationRun>(run(id), 'GET', undefined, settings).then(checked),
    cases: (id: string, after?: string, settings?: RequestOptions) =>
      transport.request<{ cases: EvaluationCase[]; after: string | null }>(
        `${run(id)}/cases${after === undefined ? '' : `?${new URLSearchParams({ after })}`}`,
        'GET',
        undefined,
        settings,
      ),
    events: (id: string, after = '0', settings?: RequestOptions) =>
      transport.request<{ events: EvaluationEvent[]; after: string }>(
        `${run(id)}/events?${new URLSearchParams({ after })}`,
        'GET',
        undefined,
        settings,
      ),
    compare: (id: string, other: string, settings?: RequestOptions) =>
      transport.request<EvaluationComparison>(
        `${run(id)}/compare?${new URLSearchParams({ other })}`,
        'GET',
        undefined,
        settings,
      ),
    cancel: (id: string, settings?: RequestOptions) =>
      transport
        .request<unknown>(`${run(id)}/cancel`, 'POST', {}, settings)
        .then((value) => checkedCancellation(id, value)),
  };
}
