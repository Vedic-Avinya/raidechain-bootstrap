#!/bin/bash
# install-binary-from-gcs.sh — On the VM: download bootstrap from GCS and start the service.
# Usage: sudo bash install-binary-from-gcs.sh [gs://bucket/object]
# Default: gs://ridechain-bootstrap/bootstrap
set -e
GCS_PATH="${1:-gs://ridechain-bootstrap/bootstrap}"
BINARY_PATH="/opt/ridechain-bootstrap/bootstrap"
SERVICE_USER="ridechain"

echo "[install] Stopping service..."
systemctl stop ridechain-bootstrap.service 2>/dev/null || true

echo "[install] Downloading from $GCS_PATH ..."
if ! command -v gsutil &>/dev/null; then
  echo "[install] gsutil not found. Installing Google Cloud SDK (minimal)..."
  curl -sSL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz | tar -xz -C /tmp
  export PATH="/tmp/google-cloud-sdk/bin:$PATH"
fi
gsutil -q cp "$GCS_PATH" "$BINARY_PATH"

echo "[install] Setting ownership and permissions..."
chown "$SERVICE_USER:$SERVICE_USER" "$BINARY_PATH"
chmod 755 "$BINARY_PATH"

echo "[install] Starting service..."
systemctl start ridechain-bootstrap.service
sleep 2
systemctl status ridechain-bootstrap.service --no-pager

echo ""
echo "[install] Done. Test with: curl -s -X POST http://127.0.0.1:4005/register -H 'Content-Type: application/json' -d '{\"peerId\":\"test\"}'"
