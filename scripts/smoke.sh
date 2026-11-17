#!/usr/bin/env bash
# Runs the Compose smoke checks from the repository root.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/smoke.py
