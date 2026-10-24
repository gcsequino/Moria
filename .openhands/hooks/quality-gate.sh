#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
if ! pnpm check >&2; then
  echo '{"decision":"deny","reason":"pnpm check failed. Fix the failure and rerun the complete quality gate."}'
  exit 2
fi
