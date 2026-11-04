FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5
WORKDIR /agent
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY adapter.mjs ./adapter.mjs
COPY runtime/ /agent/runtime/
USER 65532:65532
CMD ["node", "/agent/runtime/runner.mjs", "/agent/adapter.mjs"]
