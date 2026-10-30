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
# Provision OS libraries using deploy/test/Dockerfile before running this script.
if [ ! -x "${MORIA_INDEXING_BINARY:-/usr/local/bin/moria-indexing}" ] || [ ! -x "${MORIA_INDEXING_TEST_BINARY:-/usr/local/bin/moria-indexing-tests}" ]; then
  echo "Build the Rust indexing runtime/tests and set MORIA_INDEXING_BINARY and MORIA_INDEXING_TEST_BINARY; deploy/test/Dockerfile supplies both." >&2
  exit 1
fi
python3 -m venv .venv
.venv/bin/python -m pip install -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
pnpm install --frozen-lockfile
