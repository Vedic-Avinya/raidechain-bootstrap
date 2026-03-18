#!/bin/bash
# deploy.sh — Quick deploy on the VM. Run after copying the new binary.
# Usage: sudo bash deploy.sh [/path/to/new/binary]
# Default: /tmp/bootstrap-new
set -e

NEW_BINARY="${1:-/tmp/bootstrap-new}"
BINARY_PATH="/opt/ridechain-bootstrap/bootstrap"
SERVICE="ridechain-bootstrap"
USER="ridechain"

if [ ! -f "$NEW_BINARY" ]; then
  echo "ERROR: $NEW_BINARY not found."
  echo "Build it first:"
  echo "  cd /tmp/bootstrap && GOFLAGS='-mod=mod' go build -ldflags='-s -w' -o /tmp/bootstrap-new ./cmd/main.go"
  exit 1
fi

echo "[deploy] Stopping $SERVICE..."
systemctl stop "$SERVICE" 2>/dev/null || true

echo "[deploy] Backing up current binary..."
cp "$BINARY_PATH" "${BINARY_PATH}.bak" 2>/dev/null || true

echo "[deploy] Installing new binary..."
mv "$NEW_BINARY" "$BINARY_PATH"
chmod +x "$BINARY_PATH"
chown "$USER:$USER" "$BINARY_PATH"

echo "[deploy] Starting $SERVICE..."
systemctl start "$SERVICE"
sleep 3

echo "[deploy] Status:"
systemctl status "$SERVICE" --no-pager || true

echo ""
echo "[deploy] Health check:"
curl -sf http://127.0.0.1:4005/discover 2>/dev/null && echo " OK" || echo " (may need a few more seconds)"
echo ""
echo "[deploy] Done! Tail logs: sudo journalctl -u $SERVICE -f"
