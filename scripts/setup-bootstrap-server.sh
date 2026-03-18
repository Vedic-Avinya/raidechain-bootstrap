#!/bin/bash
# setup-bootstrap-server.sh — GCP startup script for bootstrap server (Ubuntu 22.04)
# Installs Redis, Caddy, and the RideChain bootstrap binary; configures systemd.
# Use as instance startup script or run once to prepare a custom image.
#
# Instance metadata (optional):
#   bootstrap_binary_url   — HTTPS or gs:// URL to download the bootstrap binary (recommended). If unset and bootstrap_repo unset, defaults to gs://ridechain-bootstrap-mumbai/bootstrap.
#   bootstrap_repo         — Git repo URL to clone and build from (e.g. https://github.com/you/chain)
#   bootstrap_repo_ref     — Branch/tag (default: main)
#   bootstrap_domain       — Domain for Caddy TLS (e.g. bootstrap.ridechain.in). If unset, Caddy serves HTTP on 80 only.
#   redis_maxmemory        — Redis maxmemory (default: 256mb)
set -e
export DEBIAN_FRONTEND=noninteractive

BOOTSTRAP_USER=ridechain
BOOTSTRAP_HOME=/opt/ridechain-bootstrap
BINARY_PATH="${BOOTSTRAP_HOME}/bootstrap"
REDIS_MAXMEMORY=256mb

# Read GCP instance metadata (if available)
get_metadata() {
  local key="$1"
  curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" 2>/dev/null || true
}

BINARY_URL=$(get_metadata bootstrap_binary_url)
REPO_URL=$(get_metadata bootstrap_repo)
# Default to Mumbai bucket when no URL/repo metadata is set (so startup script runs without custom metadata)
if [ -z "$BINARY_URL" ] && [ -z "$REPO_URL" ]; then
  BINARY_URL=gs://ridechain-bootstrap-mumbai/bootstrap
fi
REPO_REF=$(get_metadata bootstrap_repo_ref)
REPO_REF=${REPO_REF:-main}
CADDY_DOMAIN=$(get_metadata bootstrap_domain)
REDIS_MEM=$(get_metadata redis_maxmemory)
REDIS_MEM=${REDIS_MEM:-$REDIS_MAXMEMORY}

log() { echo "[setup-bootstrap] $*"; }

# --- Redis ---
log "Installing Redis..."
apt-get update -qq
apt-get install -y -qq redis-server
mkdir -p /etc/redis
if ! grep -q '^maxmemory ' /etc/redis/redis.conf 2>/dev/null; then
  echo "maxmemory ${REDIS_MEM}" >> /etc/redis/redis.conf
  echo "maxmemory-policy allkeys-lru" >> /etc/redis/redis.conf
fi
systemctl enable redis-server
systemctl start redis-server
log "Redis installed and started."

# --- Caddy ---
log "Installing Caddy..."
apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
apt-get update -qq
apt-get install -y -qq caddy
mkdir -p /etc/caddy /var/lib/caddy /var/log/caddy
chown -R caddy:caddy /var/lib/caddy /var/log/caddy

# Use :80 so Caddy accepts any Host (localhost, bootstrap.ridechain.in, or IP). Avoids 404 when curling localhost or when Cloudflare forwards.
if [ -n "$CADDY_DOMAIN" ]; then
  CADDY_SITE="${CADDY_DOMAIN}, localhost"
  CADDY_TLS_LINE="tls { protocols tls1.2 tls1.3 }"
else
  CADDY_SITE=":80"
  CADDY_TLS_LINE=""
fi

# Write Caddyfile. Put block bodies on new lines so "Unexpected next token after '{'" is avoided.
{
  echo "# Bootstrap: /rider->4003, /driver->4004, rest->4005. Use :80 behind Cloudflare."
  echo "${CADDY_SITE} {"
  [ -n "$CADDY_TLS_LINE" ] && echo "    $CADDY_TLS_LINE"
  echo '    handle /rider* {'
  echo '        reverse_proxy localhost:4003'
  echo '    }'
  echo '    handle /driver* {'
  echo '        reverse_proxy localhost:4004'
  echo '    }'
  echo '    handle {'
  echo '        reverse_proxy localhost:4005'
  echo '    }'
  echo '}'
} > /etc/caddy/Caddyfile

systemctl enable caddy
if ! systemctl start caddy 2>/dev/null; then
  log "Caddy start failed. Try: sudo systemctl status caddy; journalctl -xeu caddy.service"
  log "Common fix: ensure /etc/caddy/Caddyfile has no empty blocks. Example minimal Caddyfile:"
  log "  :80 { reverse_proxy localhost:4005 }"
else
  log "Caddy installed and started."
fi

# --- Bootstrap binary ---
log "Preparing bootstrap binary..."
id "$BOOTSTRAP_USER" &>/dev/null || useradd -r -s /bin/false -d "$BOOTSTRAP_HOME" "$BOOTSTRAP_USER"
mkdir -p "$BOOTSTRAP_HOME"
chown -R "$BOOTSTRAP_USER:$BOOTSTRAP_USER" "$BOOTSTRAP_HOME"

if [ -n "$BINARY_URL" ]; then
  log "Downloading binary from $BINARY_URL"
  if [[ "$BINARY_URL" == gs://* ]]; then
    apt-get install -y -qq google-cloud-sdk 2>/dev/null || true
    gsutil -q cp "$BINARY_URL" "$BINARY_PATH"
  else
    curl -sfL -o "$BINARY_PATH" "$BINARY_URL"
  fi
  chmod +x "$BINARY_PATH"
  chown "$BOOTSTRAP_USER:$BOOTSTRAP_USER" "$BINARY_PATH"
elif [ -n "$REPO_URL" ]; then
  log "Building from repo $REPO_URL (ref $REPO_REF)"
  apt-get install -y -qq git
  install_go() {
    local v=1.23.2
    curl -sfL "https://go.dev/dl/go${v}.linux-amd64.tar.gz" | tar -C /usr/local -xzf -
    export PATH="/usr/local/go/bin:$PATH"
  }
  command -v go >/dev/null || install_go
  export PATH="/usr/local/go/bin:${PATH}"
  SRC=/tmp/ridechain-src
  rm -rf "$SRC"
  git clone --depth 1 --branch "$REPO_REF" "$REPO_URL" "$SRC"
  (cd "$SRC" && go build -o "$BINARY_PATH" ./services/bootstrap/cmd)
  rm -rf "$SRC"
  chmod +x "$BINARY_PATH"
  chown "$BOOTSTRAP_USER:$BOOTSTRAP_USER" "$BINARY_PATH"
else
  log "No bootstrap_binary_url or bootstrap_repo metadata set on this VM; skipping binary download."
  log "Set VM metadata bootstrap_binary_url to e.g. gs://YOUR_BUCKET/bootstrap (and grant the VM's service account Storage Object Viewer on the bucket), then re-run this script; or copy the binary to $BINARY_PATH manually and run: systemctl start ridechain-bootstrap"
  touch "$BINARY_PATH"
  chmod +x "$BINARY_PATH"
fi

# Bootstrap env (Redis local)
cat > /etc/ridechain-bootstrap.env << ENVEOF
REDIS_URL=redis://localhost:6379
BOOTSTRAP_GOSSIPSUB_TOPIC=/ridechain/in/p2p/v1
ENVEOF
chmod 640 /etc/ridechain-bootstrap.env

# systemd unit
cat > /etc/systemd/system/ridechain-bootstrap.service << UNITEOF
[Unit]
Description=RideChain Bootstrap (libp2p + HTTP API + bridges)
After=network.target redis-server.service
Wants=redis-server.service

[Service]
Type=simple
User=$BOOTSTRAP_USER
WorkingDirectory=$BOOTSTRAP_HOME
EnvironmentFile=/etc/ridechain-bootstrap.env
ExecStart=$BINARY_PATH
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNITEOF

systemctl daemon-reload
systemctl enable ridechain-bootstrap.service
# Only start if binary exists and is executable (non-zero size)
if [ -s "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
  systemctl start ridechain-bootstrap.service
  log "Bootstrap service started."
else
  log "Bootstrap binary not present or empty. Install binary at $BINARY_PATH and run: systemctl start ridechain-bootstrap"
fi

log "Setup complete. Caddy: 80/443 -> localhost:4005; Redis: localhost:6379; Bootstrap: 4001-4005."
