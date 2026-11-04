FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS agent-dependencies
WORKDIR /agent
COPY workers/agents/package.json workers/agents/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
FROM python:3.11-slim-bookworm@sha256:061c190f3d5815b341a18dfe8a45771f8ba68c15a607ea56997fde3009ac4eec AS engine-dependencies
COPY workers/data-engine/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /tmp/requirements.lock
FROM sha256:dc770d0c567c4df10c71fde080829ab7bdc6935602325c353c522108eaff3344
COPY --from=agent-dependencies /agent/ /opt/moria-agent/
COPY --from=engine-dependencies /usr/local/lib/python3.11/site-packages /opt/moria-engine/site-packages
COPY workers/data-engine/requirements.lock /opt/moria-engine/requirements.lock
