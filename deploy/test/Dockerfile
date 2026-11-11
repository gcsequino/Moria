# Test harness only, not a production deployment image.
FROM rust:bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS indexing-toolchain
RUN rustup component add rustfmt clippy
FROM indexing-toolchain AS benchmark-lint
WORKDIR /bench/rust
COPY benchmarks/ontology/rust/ ./
RUN rustc --version > /bench/rust-version.txt \
    && cargo fmt --check \
    && cargo clippy --release --locked --all-targets -j 2 -- -D clippy::perf
FROM indexing-toolchain AS indexing-build
RUN useradd --create-home --uid 1000 builder && mkdir /build && chown builder:builder /build
ENV CARGO_HOME=/home/builder/.cargo
WORKDIR /build/services/runtime
COPY --chown=builder:builder services/runtime/ ./
COPY --chown=builder:builder packages/contracts/*-conformance.json /build/packages/contracts/
COPY --chown=builder:builder packages/contracts/schemas/ /build/packages/contracts/schemas/
COPY --chown=builder:builder tests/fixtures/ /build/tests/fixtures/
USER builder
RUN --mount=type=cache,target=/home/builder/.cargo/registry,uid=1000,gid=1000 \
    --mount=type=cache,target=/build/services/runtime/target,uid=1000,gid=1000 \
    cargo fmt --check \
    && cargo clippy --release --locked --all-targets -j 2 -- -D clippy::perf \
    && cargo test --release --locked --lib --test native_policy --test native_storage \
         --test native_transport --test native_catalog --test native_sandbox --test native_retention \
         --no-run -j 2 --message-format=json > /tmp/indexing-artifacts.json \
    && mkdir /build/out \
    && for binary in moria-indexing moria-runtime; do \
         executable="$(grep -F "\"name\":\"$binary\"" /tmp/indexing-artifacts.json | sed -n 's/^.*"executable":"\([^"]*\)".*$/\1/p')" \
         && test -x "$executable" && cp "$executable" "/build/out/$binary" || exit 1; \
       done \
    && test_binary="$(sed -n '/"kind":\["lib"\]/s/^.*"executable":"\([^"]*\)".*$/\1/p' /tmp/indexing-artifacts.json)" \
    && test -x "$test_binary" && cp "$test_binary" /build/out/moria-indexing-tests \
    && for harness in policy storage transport catalog sandbox retention; do \
         test_binary="$(grep -F "\"name\":\"native_$harness\"" /tmp/indexing-artifacts.json | sed -n 's/^.*"executable":"\([^"]*\)".*$/\1/p')" \
         && test -x "$test_binary" \
         && cp "$test_binary" "/build/out/moria-native-$harness-test" || exit 1; \
       done
FROM python:3.11-slim-bookworm@sha256:061c190f3d5815b341a18dfe8a45771f8ba68c15a607ea56997fde3009ac4eec AS mcp-client
COPY deploy/test/mcp-client/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /tmp/requirements.lock
FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS agent-dependencies
WORKDIR /agent
COPY workers/agents/package.json workers/agents/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
FROM python:3.11-slim-bookworm@sha256:061c190f3d5815b341a18dfe8a45771f8ba68c15a607ea56997fde3009ac4eec AS engine-dependencies
COPY workers/data-engine/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /tmp/requirements.lock
FROM docker:cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb AS docker-cli
FROM pgvector/pgvector@sha256:2358fcba361ed2233a5ed81b5fe4ca779ccb304120ce531a3bf51c0ed7e2bc11 AS vector-extension
FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS static-check
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=mcp-client /usr/local/lib/python3.11/site-packages /opt/moria-mcp-client
COPY --from=agent-dependencies /agent/ /opt/moria-agent/
COPY --from=engine-dependencies /usr/local/lib/python3.11/site-packages /opt/moria-engine/site-packages
COPY workers/data-engine/requirements.lock /opt/moria-engine/requirements.lock
COPY --from=vector-extension /usr/share/doc/postgresql-client-18/copyright /opt/moria-postgresql/client-COPYRIGHT
COPY --from=vector-extension /usr/share/doc/libpq5/copyright /opt/moria-postgresql/libpq-COPYRIGHT
COPY --from=vector-extension /usr/lib/postgresql/18/bin/pg_dump /usr/local/bin/pg_dump
COPY --from=vector-extension /usr/lib/postgresql/18/bin/pg_restore /usr/local/bin/pg_restore
COPY --from=vector-extension /usr/lib/x86_64-linux-gnu/libpq.so.5* /usr/lib/x86_64-linux-gnu/
COPY --from=vector-extension /usr/lib/postgresql/18/lib/vector.so /opt/moria-pgvector/vector.so
COPY --from=vector-extension /usr/share/postgresql/18/extension/vector* /opt/moria-pgvector/extension/
COPY deploy/test/licenses/pgvector-LICENSE /opt/moria-pgvector/LICENSE
ENV MORIA_POLICY_TEST_BINARY=/usr/local/bin/moria-native-policy-test \
    MORIA_STORAGE_TEST_BINARY=/usr/local/bin/moria-native-storage-test \
    MORIA_RPC_TEST_BINARY=/usr/local/bin/moria-native-transport-test \
    MORIA_CATALOG_TEST_BINARY=/usr/local/bin/moria-native-catalog-test \
    MORIA_NATIVE_SANDBOX_TEST_BINARY=/usr/local/bin/moria-native-sandbox-test \
    MORIA_NATIVE_RETENTION_TEST_BINARY=/usr/local/bin/moria-native-retention-test
RUN apt-get update && apt-get install -y --no-install-recommends libldap-2.5-0 libgssapi-krb5-2 liblz4-1 libzstd1 python3-venv libicu72 libxml2 libreadline8 libxslt1.1 openssl && rm -rf /var/lib/apt/lists/*
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
COPY --chown=node:node scripts/install-test-extensions.mjs scripts/
RUN node scripts/install-test-extensions.mjs
COPY --chown=node:node workers/iceberg/requirements.txt workers/iceberg/constraints.txt workers/iceberg/
COPY --chown=node:node requirements-dev.txt ./
COPY --chown=node:node docs/spec/schemas/requirements.txt docs/spec/schemas/
RUN python3 -m venv .venv && .venv/bin/python -m pip install --no-cache-dir -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
COPY --chown=node:node . .
FROM static-check AS quality
COPY --from=indexing-build /build/out/moria-indexing /usr/local/bin/moria-indexing
COPY --from=indexing-build /build/out/moria-indexing-tests /usr/local/bin/moria-indexing-tests
COPY --from=indexing-build /build/out/moria-runtime /usr/local/bin/moria-runtime
COPY --from=indexing-build /build/out/moria-native-*-test /usr/local/bin/
CMD ["pnpm", "check"]
