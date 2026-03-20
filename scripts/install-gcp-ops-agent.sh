#!/usr/bin/env bash
# Install Google Cloud Ops Agent on a GCP Compute Engine VM (Ubuntu/Debian).
# Logs from systemd unit ridechain-bootstrap.service can be forwarded to Cloud Logging
# after you merge bootstrap/config/gcp-ops-agent-ridechain-snippet.yaml into the agent config.
#
# Usage (on the VM):
#   sudo bash scripts/install-gcp-ops-agent.sh
#
# IAM: The VM's service account needs roles/logging.logWriter and roles/monitoring.metricWriter
#      (default GCE service account often has these when Ops Agent is used).

set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root: sudo bash $0"
  exit 1
fi

echo "[ops-agent] Installing Google Cloud Ops Agent (official repo)..."
curl -sS https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh | bash -
apt-get update -y
apt-get install -y google-cloud-ops-agent

echo "[ops-agent] Restarting agent..."
systemctl restart google-cloud-ops-agent.service
systemctl status google-cloud-ops-agent.service --no-pager || true

echo ""
echo "Next steps:"
echo "  1. Merge logging receivers from:"
echo "       bootstrap/config/gcp-ops-agent-ridechain-snippet.yaml"
echo "     into /etc/google-cloud-ops-agent/config.yaml"
echo "     (see bootstrap/docs/GCP_CLOUD_LOGGING_AND_DEPLOY.md)"
echo "  2. sudo systemctl restart google-cloud-ops-agent"
echo "  3. GCP Console → Logging → Logs Explorer → resource.type=\"gce_instance\""
