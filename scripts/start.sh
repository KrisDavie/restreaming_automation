#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# Restreaming Automation – Start API server
# Ctrl+C stops it.
# ────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

API_PID=""

cleanup() {
    echo ""
    echo -e "${YELLOW}Stopping services…${NC}"
    [[ -n "$API_PID" ]] && kill "$API_PID" 2>/dev/null && wait "$API_PID" 2>/dev/null || true
    echo -e "${GREEN}All services stopped.${NC}"
}
trap cleanup EXIT INT TERM

# Activate venv
if [[ -f "venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
fi

# ── Start Python API backend ──────────────────────────────────
echo -e "${GREEN}${BOLD}Starting Python API server…${NC}"
python -m src &
API_PID=$!

cat <<EOF

${BOLD}Services started:${NC}
  - API Backend  → http://localhost:8008        (PID: $API_PID)
  - Dashboard    → http://localhost:8008/dashboard
  - API Docs     → http://localhost:8008/docs

Press Ctrl+C to stop.
EOF

wait "$API_PID" 2>/dev/null || true
