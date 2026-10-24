#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

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
python3 -m venv .venv
.venv/bin/python -m pip install -r workers/iceberg/requirements.txt -r requirements-dev.txt -r docs/spec/schemas/requirements.txt
pnpm install --frozen-lockfile
