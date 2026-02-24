#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js is required to run the macOS Content Manager launcher."
  echo "Install Node.js (LTS), then run this launcher again."
  exit 1
fi

exec node "$SCRIPT_DIR/content-manager-mac.mjs"
