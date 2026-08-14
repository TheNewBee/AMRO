#!/usr/bin/env bash
# Build artifacts and bring up Kafka + backend + frontend.
# Usage: scripts/up.sh [docker compose up args...]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
# ponytail: skip tests on boot; scripts/e2e-stack.sh is the stack check
mvn -f backend/pom.xml -q -DskipTests package
npm --prefix frontend ci
npm --prefix frontend run build
exec docker compose up --build "$@"
