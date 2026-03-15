#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# Restreaming Automation – Linux Setup Script
# Tested on CachyOS / Arch Linux. Run from the project root.
# ────────────────────────────────────────────────────────────────
set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}${BOLD}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}${BOLD}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}${BOLD}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}${BOLD}[ERR]${NC}   $*"; }

echo -e "${BOLD}=== Restreaming Automation – Linux Setup ===${NC}"

# ── 0. Detect package manager ──────────────────────────────────
PM=""
if command -v pacman &>/dev/null; then
    PM="pacman"
elif command -v apt &>/dev/null; then
    PM="apt"
elif command -v dnf &>/dev/null; then
    PM="dnf"
fi

# ── 1. Check / install prerequisites ──────────────────────────
info "Checking prerequisites…"

MISSING=()
command -v python3 &>/dev/null || MISSING+=("python")
command -v ffmpeg  &>/dev/null || MISSING+=("ffmpeg")
command -v git     &>/dev/null || MISSING+=("git")

if [[ ${#MISSING[@]} -gt 0 ]]; then
    warn "Missing packages: ${MISSING[*]}"

    if [[ "$PM" == "pacman" ]]; then
        info "Installing via pacman…"
        sudo pacman -Sy --needed --noconfirm "${MISSING[@]}"
    elif [[ "$PM" == "apt" ]]; then
        info "Installing via apt…"
        sudo apt update && sudo apt install -y "${MISSING[@]}"
    elif [[ "$PM" == "dnf" ]]; then
        info "Installing via dnf…"
        sudo dnf install -y "${MISSING[@]}"
    else
        err "Unknown package manager. Please install manually: ${MISSING[*]}"
        exit 1
    fi
fi

# Streamlink (pip package, not always in distro repos)
if ! command -v streamlink &>/dev/null; then
    if [[ "$PM" == "pacman" ]]; then
        # Available in Arch community repo
        sudo pacman -S --needed --noconfirm streamlink
    else
        warn "Streamlink not found – installing via pip…"
        pip3 install --user streamlink
    fi
fi

ok "All prerequisites satisfied."

# ── 2. Python virtual environment ──────────────────────────────
info "[1/2] Setting up Python virtual environment…"
if [[ ! -f "venv/bin/activate" ]]; then
    # Remove any leftover partial venv from a previous failed attempt
    rm -rf venv
    # On Debian/Ubuntu python3-venv (ensurepip) may be missing
    if ! python3 -m ensurepip --version &>/dev/null; then
        if [[ "$PM" == "apt" ]]; then
            warn "ensurepip not available – installing python3-venv…"
            sudo apt install -y python3-venv
        else
            err "python3 ensurepip module is missing. Install the python3-venv package for your distro."
            exit 1
        fi
    fi
    python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
ok "Python environment ready."

# ── 3. Environment file ───────────────────────────────────────
info "[2/2] Environment configuration…"
if [[ ! -f ".env" ]]; then
    cp .env.example .env
    warn "Created .env from .env.example – edit it with your OBS WebSocket password."
else
    ok ".env already exists, skipping."
fi

# ── 4. Make scripts executable ─────────────────────────────────
chmod +x scripts/*.sh 2>/dev/null || true

echo ""
echo -e "${GREEN}${BOLD}=== Setup complete! ===${NC}"
cat <<'EOF'

Next steps:
  1. Edit .env with your OBS WebSocket password
  2. Place template images (hearts.png etc.) in ./templates/
  3. Start the server:    ./scripts/start.sh
     Or manually:         source venv/bin/activate && python -m src
  4. Open dashboard:      http://localhost:8008/dashboard

EOF
