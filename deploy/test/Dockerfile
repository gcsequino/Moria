# Test harness only, not a production deployment image.
FROM docker:cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb AS docker-cli
FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv libicu72 libxml2 libreadline8 libxslt1.1 && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm@11.25.0 && mkdir /app && chown node:node /app
# Isolated S3 conformance server only: MinIO RELEASE.2025-09-07T16-13-09Z, AGPLv3.
# Download occurs at image build, never during the offline quality gate.
RUN node --input-type=module -e "\
    import {writeFileSync} from 'node:fs'; \
    import {createHash} from 'node:crypto'; \
    if (process.arch !== 'x64') { \
        throw new Error('Pinned S3 test server requires linux-amd64'); \
    } \
    const response = await fetch('https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio_20250907161309.0.0_amd64.deb'); \
    if (!response.ok) { \
        throw new Error('S3 prerequisite download failed'); \
    } \
    const bytes = Buffer.from(await response.arrayBuffer()); \
    if (createHash('sha256').update(bytes).digest('hex') !== 'eeda08f699f6592d1b868ac8bda864ae2cacdb5ee1b888663366e8c8ff566249') { \
        throw new Error('S3 prerequisite checksum mismatch'); \
    } \
    writeFileSync('/tmp/minio.deb', bytes);" \
    && dpkg-deb -x /tmp/minio.deb /tmp/minio-dist \
    && install -m 0755 /tmp/minio-dist/usr/local/bin/minio /usr/local/bin/minio \
    && rm -rf /tmp/minio.deb /tmp/minio-dist
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
