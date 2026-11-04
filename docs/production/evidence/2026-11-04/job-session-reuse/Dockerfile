FROM moria-test:workflow-20261104
USER root
RUN rm -rf /app/services/indexing
USER node
COPY --chown=node:node source/ /app/
COPY --chown=node:node --chmod=0555 artifacts/ /usr/local/bin/
ENV MORIA_POLICY_TEST_BINARY=/usr/local/bin/moria-native-policy-test \
    MORIA_STORAGE_TEST_BINARY=/usr/local/bin/moria-native-storage-test \
    MORIA_RPC_TEST_BINARY=/usr/local/bin/moria-native-transport-test \
    MORIA_CATALOG_TEST_BINARY=/usr/local/bin/moria-native-catalog-test \
    MORIA_NATIVE_SANDBOX_TEST_BINARY=/usr/local/bin/moria-native-sandbox-test \
    MORIA_NATIVE_RETENTION_TEST_BINARY=/usr/local/bin/moria-native-retention-test
RUN pnpm install --offline --frozen-lockfile
