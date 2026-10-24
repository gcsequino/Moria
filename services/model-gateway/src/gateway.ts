import {
  authorize,
  authorizeDestination,
  type Identity,
} from '../../../packages/authorization/src/access.js';
import { MoriaError } from '../../../packages/contracts/types.js';

export interface ModelRoute {
  resource: string;
  model: string;
  endpoint: string;
  destination: string;
  secret?: string;
}
export interface ModelAuthorization {
  identity: Identity;
  policy: { allowedDestinations: string[]; allowedMarkings: string[] };
}
export interface ChatRequest {
  messages: { role: 'system' | 'user' | 'assistant'; content: string }[];
  maxTokens: number;
}
/** Route, labels and authorization resolver are trusted supervisor inputs, never
 * agent-supplied credentials or authorization claims. No streaming/tools yet. */
export class ModelGateway {
  constructor(
    readonly route: ModelRoute,
    readonly current: () => Promise<ModelAuthorization>,
  ) {}
  private async check(labels: Set<string>): Promise<void> {
    const state = await this.current();
    authorize(state.identity, this.route.resource, 'invoke', labels);
    authorizeDestination(state.identity, labels, state.policy, this.route.destination);
  }
  async chat(
    request: ChatRequest,
    labels: Set<string>,
  ): Promise<{ content: string; finishReason: string }> {
    const envelope = structuredClone(request),
      effectiveLabels = new Set(labels);
    if (
      !Number.isInteger(envelope.maxTokens) ||
      envelope.maxTokens < 1 ||
      envelope.maxTokens > 4096 ||
      !envelope.messages.length ||
      envelope.messages.length > 100 ||
      envelope.messages.some(
        (m) => !['system', 'user', 'assistant'].includes(m.role) || typeof m.content !== 'string',
      ) ||
      Buffer.byteLength(JSON.stringify(envelope)) > 256 * 1024
    )
      throw new MoriaError('MODEL_REQUEST_INVALID', 'Request exceeds gateway limits');
    await this.check(effectiveLabels);
    let response: Response;
    try {
      response = await fetch(
        new URL(
          'chat/completions',
          this.route.endpoint.endsWith('/') ? this.route.endpoint : this.route.endpoint + '/',
        ),
        {
          method: 'POST',
          redirect: 'error',
          signal: AbortSignal.timeout(60000),
          headers: {
            'content-type': 'application/json',
            ...(this.route.secret ? { authorization: `Bearer ${this.route.secret}` } : {}),
          },
          body: JSON.stringify({
            model: this.route.model,
            messages: envelope.messages,
            max_tokens: envelope.maxTokens,
            stream: false,
            temperature: 0,
          }),
        },
      );
    } catch {
      throw new MoriaError('MODEL_UNAVAILABLE', 'Model request failed or timed out');
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new MoriaError('MODEL_UNAVAILABLE', `Model returned HTTP ${response.status}`);
    }
    const reader = response.body?.getReader();
    if (!reader) throw new MoriaError('MODEL_PROTOCOL', 'Model returned no body');
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > 1024 * 1024) {
          await reader.cancel();
          throw new MoriaError('MODEL_PROTOCOL', 'Model response exceeded limit');
        }
        chunks.push(value);
      }
    } catch (error) {
      if (error instanceof MoriaError) throw error;
      throw new MoriaError('MODEL_INCOMPLETE', 'Model response was interrupted');
    }
    let result: any;
    try {
      result = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      throw new MoriaError('MODEL_PROTOCOL', 'Model returned invalid JSON');
    }
    const choice = result.choices?.[0];
    if (typeof choice?.message?.content !== 'string' || choice.finish_reason !== 'stop')
      throw new MoriaError('MODEL_INCOMPLETE', 'Model did not complete a text response');
    // A revoke during inference blocks release of the result.
    await this.check(effectiveLabels);
    return { content: choice.message.content, finishReason: choice.finish_reason };
  }
}
