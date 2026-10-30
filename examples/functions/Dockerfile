FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS runtime
WORKDIR /source
COPY scripts/build-function-runtime.mjs scripts/
COPY packages/contracts/errors.ts packages/contracts/value-types.ts packages/contracts/values.ts packages/contracts/
COPY workers/functions/runner.mjs workers/functions/
RUN node scripts/build-function-runtime.mjs /runtime

FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5
COPY --from=runtime /runtime /function/runtime
COPY examples/functions/function.ts /function/function.ts
CMD ["node", "/function/runtime/runner.mjs", "/function/function.ts"]
