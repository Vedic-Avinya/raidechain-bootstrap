#!/usr/bin/env bash
# =============================================================================
# RideChain Bootstrap — BASIC Setup (e2-micro friendly, no Docker)
# =============================================================================
# Installs: Go 1.24 + Redis + Caddy + Bootstrap binary + systemd service
# Skips:    Docker, Prometheus, Grafana  (add later with setup-gcp.sh)
#
# Usage — run on the GCP VM as root:
#   sudo bash scripts/setup-gcp-basic.sh
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[[ "$(id -u)" -eq 0 ]] || error "Run as root: sudo bash $0"

# Detect where the script lives (works when run via: sudo bash scripts/setup-gcp-basic.sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BIN_PATH="/opt/ridechain/bootstrap"
ENV_FILE="/etc/ridechain/.env"
SERVICE_USER="ridechain"
GO_VERSION="1.24.0"
ARCH=$(dpkg --print-architecture 2>/dev/null || echo "amd64")

# =============================================================================
# 1. System Packages
# =============================================================================
info "Updating packages..."
apt-get update -qq
apt-get install -y -qq \
  build-essential git curl wget ca-certificates gnupg \
  redis-server ufw htop jq lsb-release apt-transport-https \
  software-properties-common

# =============================================================================
# 2. Install Go 1.24
# =============================================================================
if ! command -v go &>/dev/null || ! go version 2>/dev/null | grep -q "go${GO_VERSION}"; then
  info "Installing Go ${GO_VERSION}..."
  GO_TAR="go${GO_VERSION}.linux-${ARCH}.tar.gz"
  wget -q "https://go.dev/dl/${GO_TAR}" -O /tmp/${GO_TAR}
  rm -rf /usr/local/go
  tar -C /usr/local -xzf /tmp/${GO_TAR}
  rm -f /tmp/${GO_TAR}
  echo 'export PATH=$PATH:/usr/local/go/bin' > /etc/profile.d/go.sh
fi
export PATH=$PATH:/usr/local/go/bin
info "Go: $(go version)"

# =============================================================================
# 3. Install Caddy
# =============================================================================
if ! command -v caddy &>/dev/null; then
  info "Installing Caddy..."
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    | tee /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq
  apt-get install -y -qq caddy
fi
info "Caddy: $(caddy version)"

# =============================================================================
# 4. Create service user + directories
# =============================================================================
if ! id "$SERVICE_USER" &>/dev/null; then
  info "Creating user: $SERVICE_USER"
  useradd --system --shell /usr/sbin/nologin \
    --home-dir /opt/ridechain --create-home "$SERVICE_USER"
fi
mkdir -p /opt/ridechain /etc/ridechain /var/log/ridechain
chown -R "$SERVICE_USER:$SERVICE_USER" /opt/ridechain /var/log/ridechain

# =============================================================================
# 5. Build bootstrap binary
# =============================================================================
info "Building bootstrap binary from $REPO_DIR ..."
cd "$REPO_DIR"
GOFLAGS="-mod=mod" go build \
  -ldflags="-s -w" \
  -o "$BIN_PATH" \
  ./cmd/main.go
chmod +x "$BIN_PATH"
chown "$SERVICE_USER:$SERVICE_USER" "$BIN_PATH"
info "Binary: $BIN_PATH"

# =============================================================================
# 6. Configure Redis (lightweight — 128 MB cap for e2-micro)
# =============================================================================
info "Configuring Redis..."
cat > /etc/redis/redis.conf << 'EOF'
bind 127.0.0.1
port 6379
maxmemory 128mb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
appendonly yes
dir /var/lib/redis
protected-mode yes
EOF
systemctl enable --now redis-server
info "Redis running on 127.0.0.1:6379"

# =============================================================================
# 7. Create /etc/ridechain/.env (only if not already present)
# =============================================================================
if [[ ! -f "$ENV_FILE" ]]; then
  info "Creating $ENV_FILE ..."
  IDENTITY_SEED=$(openssl rand -hex 32)
  cat > "$ENV_FILE" << ENVEOF
# ── Identity ──────────────────────────────────────────────────────────────────
BOOTSTRAP_IDENTITY_SEED=${IDENTITY_SEED}
BOOTSTRAP_ENV=dev

# ── Ports ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_PORT=4001
BOOTSTRAP_WS_PORT=4002
BOOTSTRAP_RIDER_BRIDGE_PORT=4003
BOOTSTRAP_DRIVER_BRIDGE_PORT=4004
BOOTSTRAP_HTTP_PORT=4005
# Metrics disabled in basic setup (no Prometheus)
# BOOTSTRAP_METRICS_PORT=9090

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL=redis://127.0.0.1:6379

# ── Rider Bridge ──────────────────────────────────────────────────────────────
MAX_RIDER_CONNECTIONS=500
# RIDER_ALLOWED_ORIGINS=https://bootstrap.ridechain.in

# ── Firebase FCM ──────────────────────────────────────────────────────────────
# After uploading firebase-sa.json, uncomment this:
# GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json

# ── Firebase Analytics ────────────────────────────────────────────────────────
# FIREBASE_MEASUREMENT_ID=G-7W4MJWQE02
# FIREBASE_ANALYTICS_SECRET=<your_secret>

# ── HTTP API Rate Limiting ────────────────────────────────────────────────────
BOOTSTRAP_API_RATE_LIMIT=60
BOOTSTRAP_API_BURST=10
ENVEOF

  chmod 600 "$ENV_FILE"
  chown root:root "$ENV_FILE"
  warn "TODO: Edit $ENV_FILE before starting → sudo nano $ENV_FILE"
else
  info ".env already exists — skipping generation"
fi

# =============================================================================
# 8. systemd service
# =============================================================================
info "Installing systemd service..."
cat > /etc/systemd/system/ridechain-bootstrap.service << SVCEOF
[Unit]
Description=RideChain Bootstrap Node
After=network.target redis-server.service
Wants=redis-server.service

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=/opt/ridechain
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_PATH}
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ridechain

NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ReadWritePaths=/var/log/ridechain

LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable ridechain-bootstrap
info "Service installed (not started yet — configure .env first)"

# =============================================================================
# 9. UFW Firewall
# =============================================================================
info "Configuring firewall..."
ufw --force reset >/dev/null
ufw default deny incoming >/dev/null
ufw default allow outgoing >/dev/null
ufw allow ssh
ufw allow 80/tcp   comment "HTTP (Caddy TLS challenge)"
ufw allow 443/tcp  comment "HTTPS (Caddy)"
ufw allow 4001/tcp comment "libp2p TCP"
ufw allow 4001/udp comment "libp2p QUIC"
ufw --force enable >/dev/null
info "Firewall: SSH + 80 + 443 + 4001 open. Ports 4003-4005 internal only."

# =============================================================================
# 10. Caddyfile template
# =============================================================================
CADDYFILE="/etc/caddy/Caddyfile"
if [[ ! -f "${CADDYFILE}.orig" ]]; then
  cp "$CADDYFILE" "${CADDYFILE}.orig" 2>/dev/null || true
fi

cat > "$CADDYFILE" << 'CADDYEOF'
# ── EDIT: replace bootstrap.ridechain.in with your actual domain ──────────────
bootstrap.ridechain.in {

    # WebSocket rider bridge  →  ws://localhost:4003
    reverse_proxy /rider localhost:4003 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
    }
    reverse_proxy /health localhost:4003

    # HTTP API  →  http://localhost:4005
    reverse_proxy localhost:4005 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
        header_up X-Forwarded-For {http.request.header.CF-Connecting-IP}
    }

    log {
        output file /var/log/caddy/bootstrap.log {
            roll_size 50mb
            roll_keep 3
        }
    }
}
CADDYEOF

mkdir -p /var/log/caddy
systemctl enable caddy
info "Caddyfile written: $CADDYFILE"

# =============================================================================
# Done — print next steps
# =============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RideChain Basic Setup Complete — 3 steps left:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  STEP A — Edit .env (add your domain + Firebase keys):"
echo "    sudo nano /etc/ridechain/.env"
echo "    → Uncomment RIDER_ALLOWED_ORIGINS and set your domain"
echo "    → Uncomment + fill FIREBASE_MEASUREMENT_ID / FIREBASE_ANALYTICS_SECRET"
echo "    → Uncomment GOOGLE_APPLICATION_CREDENTIALS after uploading firebase-sa.json"
echo ""
echo "  STEP B — Upload firebase-sa.json (in GCP SSH browser):"
echo "    1. Click the gear icon ⚙ at top-right of the SSH window"
echo "    2. Choose 'Upload file' → select your firebase-sa.json"
echo "    3. Then run:"
echo "       sudo mv ~/firebase-sa.json /etc/ridechain/firebase-sa.json"
echo "       sudo chmod 600 /etc/ridechain/firebase-sa.json"
echo "       sudo chown ridechain:ridechain /etc/ridechain/firebase-sa.json"
echo ""
echo "  STEP C — Start everything:"
echo "    sudo systemctl start ridechain-bootstrap"
echo "    sudo systemctl start caddy"
echo "    sudo journalctl -u ridechain-bootstrap -f   # watch logs"
echo ""
echo "  TEST (from your Mac after DNS is set):"
echo "    curl https://bootstrap.ridechain.in/register -X POST \\"
echo "      -H 'Content-Type: application/json' -d '{\"peerId\":\"test1\"}'"
echo "    wscat -c 'wss://bootstrap.ridechain.in/rider?city=mumbai'"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
