#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# Install systemd services for production deployment.
# Run as root or with sudo.
# Usage:  sudo ./scripts/install-systemd.sh [username]
# ────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_USER="${1:-$(logname 2>/dev/null || echo "$SUDO_USER")}"

if [[ -z "$SERVICE_USER" ]]; then
    echo "Usage: sudo $0 <username>"
    exit 1
fi

echo "Installing systemd services for user: $SERVICE_USER"
echo "Project directory: $PROJECT_DIR"

# Adjust WorkingDirectory in service files to match actual project path
for svc in "$SCRIPT_DIR/systemd/"*.service; do
    filename=$(basename "$svc")
    sed "s|/opt/restreaming_automation|$PROJECT_DIR|g" "$svc" \
        > "/etc/systemd/system/$filename"
    echo "  Installed: $filename"
done

systemctl daemon-reload

echo ""
echo "Services installed. To enable and start:"
echo "  sudo systemctl enable --now restream-api@${SERVICE_USER}"
echo "  sudo systemctl enable --now restream-nodecg@${SERVICE_USER}"
echo ""
echo "To check status:"
echo "  systemctl status restream-api@${SERVICE_USER}"
echo "  systemctl status restream-nodecg@${SERVICE_USER}"
echo ""
echo "To view logs:"
echo "  journalctl -u restream-api@${SERVICE_USER} -f"
echo "  journalctl -u restream-nodecg@${SERVICE_USER} -f"
