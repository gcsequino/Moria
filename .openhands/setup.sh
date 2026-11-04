#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ "$(uname -s)" != "Linux" ]; then
  echo "Linux is required. Use a Linux container or WSL2 Linux filesystem." >&2
  exit 1
fi

if [ "$(id -u)" -eq 0 ]; then
  echo "Use a non-root Linux user; embedded PostgreSQL cannot run as root." >&2
  exit 1
fi
node -e 'if (Number(process.versions.node.split(".")[0]) !== 24) { console.error("Node 24 is required for the supported development environment."); process.exit(1); }'
if [ "$(pnpm --version)" != "11.25.0" ]; then
  echo "Install pnpm 11.25.0 before running setup." >&2
  exit 1
fi
# Provision native libraries and matching artifacts using deploy/test/Dockerfile.
for binary in \
  "${MORIA_INDEXING_BINARY:-/usr/local/bin/moria-indexing}" \
  "${MORIA_INDEXING_TEST_BINARY:-/usr/local/bin/moria-indexing-tests}" \
  "${MORIA_NATIVE_BINARY:-/usr/local/bin/moria-runtime}" \
  "${MORIA_POLICY_TEST_BINARY:-/usr/local/bin/moria-native-policy-test}" \
  "${MORIA_STORAGE_TEST_BINARY:-/usr/local/bin/moria-native-storage-test}" \
  "${MORIA_RPC_TEST_BINARY:-/usr/local/bin/moria-native-transport-test}" \
  "${MORIA_CATALOG_TEST_BINARY:-/usr/local/bin/moria-native-catalog-test}" \
  "${MORIA_NATIVE_SANDBOX_TEST_BINARY:-/usr/local/bin/moria-native-sandbox-test}"; do
  if [ ! -x "$binary" ]; then
    echo "Matching native runtime/test artifacts are required; deploy/test/Dockerfile supplies them." >&2
    exit 1
  fi
done
python3 -m venv .venv
.venv/bin/python -m pip install -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
pnpm install --frozen-lockfile
node scripts/install-test-extensions.mjs
