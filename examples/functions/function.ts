import type { FunctionContext } from '../../packages/platform/src/function-context.js';

export default async function increment(input: { value: number }, context: FunctionContext) {
  context.signal.throwIfAborted();
  context.log('Calculating increment');
  return { value: input.value + 1 };
}
