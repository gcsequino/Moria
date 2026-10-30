export class MoriaError extends Error {
  code: string;
  details: unknown;
  constructor(code: string, message: string, details: unknown = undefined) {
    super(message);
    this.name = 'MoriaError';
    this.code = code;
    this.details = details;
  }
}
