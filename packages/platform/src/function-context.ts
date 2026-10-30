/** The generated ontology client's factory is exported as createOsdk by the function module. */
export interface FunctionContext<Osdk = unknown> {
  valueVersion: 1;
  invocationId: string;
  effect: 'readOnly' | 'readWrite' | 'editProposal';
  signal: AbortSignal;
  log(...values: unknown[]): void;
  osdk: Osdk;
  platform: { fetch: typeof fetch };
}
