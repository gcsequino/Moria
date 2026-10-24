# Test harness only, not a production deployment image.
FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv libicu72 libxml2 libreadline8 libxslt1.1 && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm@11.25.0 && mkdir /app && chown node:node /app
WORKDIR /app
USER node
COPY --chown=node:node package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY --chown=node:node workers/iceberg/requirements.txt workers/iceberg/constraints.txt workers/iceberg/
COPY --chown=node:node requirements-dev.txt ./
COPY --chown=node:node docs/spec/schemas/requirements.txt docs/spec/schemas/
RUN python3 -m venv .venv && .venv/bin/python -m pip install --no-cache-dir -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
COPY --chown=node:node . .
CMD ["pnpm", "check"]
