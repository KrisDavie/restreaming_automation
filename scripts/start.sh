#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# Restreaming Automation – Start all services
# Launches the Python API backend and NodeCG side by side.
# Ctrl+C stops both.
# ────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# PIDs we need to clean up
API_PID=""
NODECG_PID=""

cleanup() {
    echo ""
    echo -e "${YELLOW}Stopping services…${NC}"
    [[ -n "$API_PID" ]]    && kill "$API_PID"    2>/dev/null && wait "$API_PID"    2>/dev/null || true
    [[ -n "$NODECG_PID" ]] && kill "$NODECG_PID" 2>/dev/null && wait "$NODECG_PID" 2>/dev/null || true
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

# ── Start NodeCG ──────────────────────────────────────────────
if [[ -f "nodecg/index.js" ]]; then
    echo -e "${GREEN}${BOLD}Starting NodeCG…${NC}"
    (cd nodecg && node index.js) &
    NODECG_PID=$!
else
    echo -e "${YELLOW}NodeCG not found – skipping (run scripts/setup.sh first)${NC}"
fi

cat <<EOF

${BOLD}Services started:${NC}
  - API Backend  → http://localhost:8008        (PID: $API_PID)
  - Dashboard    → http://localhost:8008/dashboard
  - API Docs     → http://localhost:8008/docs
EOF

if [[ -n "$NODECG_PID" ]]; then
    echo "  - NodeCG       → http://localhost:9090        (PID: $NODECG_PID)"
fi

echo ""
echo "Press Ctrl+C to stop all services."
echo ""

# Wait for either process to exit
wait -n "$API_PID" ${NODECG_PID:+"$NODECG_PID"} 2>/dev/null || true
