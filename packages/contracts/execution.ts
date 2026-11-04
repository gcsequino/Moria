/** A durable control request; lifecycle and protected data require separate inspection. */
export interface CancellationReceipt {
  id: string;
  cancelRequested: true;
}
