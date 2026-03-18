#!/usr/bin/env bash
# =============================================================================
# RideChain Bootstrap Node — GCP VM Full Setup Script
# =============================================================================
# Usage (run as root or via sudo):
#   curl -fsSL https://raw.githubusercontent.com/YOUR_ORG/ridechain/main/bootstrap/scripts/setup-gcp.sh | bash
# Or copy to VM and run:
#   chmod +x setup-gcp.sh && sudo ./setup-gcp.sh
#
# What this script does:
#   1. Installs Go 1.24, Redis, Docker (for Prometheus + Grafana)
#   2. Builds the bootstrap binary
#   3. Creates /etc/ridechain/.env from your answers
#   4. Installs systemd service (ridechain-bootstrap)
#   5. Installs systemd service (ridechain-redis)
#   6. Starts Prometheus + Grafana via Docker Compose
#   7. Prints next steps
#
# Prerequisites (do before running):
#   - GCP VM e2-standard-2+ (2 vCPU, 8 GB RAM), Ubuntu 22.04 LTS
#   - Static external IP attached
#   - Firewall rules open (see docs/PRODUCTION_SETUP.md)
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[[ "$(id -u)" -eq 0 ]] || error "Run as root: sudo $0"

RIDECHAIN_DIR="/opt/ridechain"
BIN_PATH="$RIDECHAIN_DIR/bootstrap"
ENV_FILE="/etc/ridechain/.env"
SERVICE_USER="ridechain"
GO_VERSION="1.24.0"
ARCH=$(dpkg --print-architecture)

# =============================================================================
# 1. System Packages
# =============================================================================
info "Updating package list..."
apt-get update -qq

info "Installing dependencies..."
apt-get install -y -qq \
  build-essential git curl wget ca-certificates gnupg \
  redis-server ufw htop jq unzip lsb-release apt-transport-https \
  software-properties-common

# =============================================================================
# 2. Install Go 1.24
# =============================================================================
if ! command -v go &>/dev/null || [[ "$(go version | awk '{print $3}')" != "go${GO_VERSION}" ]]; then
  info "Installing Go ${GO_VERSION}..."
  GO_TAR="go${GO_VERSION}.linux-${ARCH}.tar.gz"
  wget -q "https://go.dev/dl/${GO_TAR}" -O /tmp/${GO_TAR}
  rm -rf /usr/local/go
  tar -C /usr/local -xzf /tmp/${GO_TAR}
  rm /tmp/${GO_TAR}
  echo 'export PATH=$PATH:/usr/local/go/bin' > /etc/profile.d/go.sh
  export PATH=$PATH:/usr/local/go/bin
fi
info "Go version: $(go version)"

# =============================================================================
# 3. Install Docker (for Prometheus + Grafana)
# =============================================================================
if ! command -v docker &>/dev/null; then
  info "Installing Docker..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
  echo "deb [arch=${ARCH} signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
  systemctl enable --now docker
fi
info "Docker version: $(docker --version)"

# =============================================================================
# 4. Create service user + directories
# =============================================================================
if ! id "$SERVICE_USER" &>/dev/null; then
  info "Creating service user: $SERVICE_USER"
  useradd --system --shell /usr/sbin/nologin --home-dir "$RIDECHAIN_DIR" --create-home "$SERVICE_USER"
fi
mkdir -p "$RIDECHAIN_DIR" /etc/ridechain /var/log/ridechain
chown -R "$SERVICE_USER:$SERVICE_USER" "$RIDECHAIN_DIR" /var/log/ridechain

# =============================================================================
# 5. Build bootstrap binary
# =============================================================================
info "Cloning / building bootstrap..."
REPO_DIR="/tmp/ridechain-build"
if [[ -d "$REPO_DIR" ]]; then
  git -C "$REPO_DIR" pull --ff-only
else
  # If running from local copy, build in place
  REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
  warn "Using local source at $REPO_DIR"
fi

export PATH=$PATH:/usr/local/go/bin
cd "$REPO_DIR"
GOFLAGS="-mod=mod" go build -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" \
  -o "$BIN_PATH" ./cmd/main.go
chmod +x "$BIN_PATH"
chown "$SERVICE_USER:$SERVICE_USER" "$BIN_PATH"
info "Binary built: $BIN_PATH"

# =============================================================================
# 6. Configure .env (interactive or use defaults)
# =============================================================================
if [[ ! -f "$ENV_FILE" ]]; then
  info "Creating $ENV_FILE (you can edit it afterwards)"

  # Generate identity seed
  IDENTITY_SEED=$(openssl rand -hex 32)

  cat > "$ENV_FILE" << ENVEOF
# ── Identity ──────────────────────────────────────────────────────────────────
BOOTSTRAP_IDENTITY_SEED=${IDENTITY_SEED}
BOOTSTRAP_ENV=prod

# ── Ports ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_PORT=4001
BOOTSTRAP_WS_PORT=4002
BOOTSTRAP_RIDER_BRIDGE_PORT=4003
BOOTSTRAP_DRIVER_BRIDGE_PORT=4004
BOOTSTRAP_HTTP_PORT=4005
BOOTSTRAP_METRICS_PORT=9090

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL=redis://127.0.0.1:6379

# ── Rider Bridge ──────────────────────────────────────────────────────────────
MAX_RIDER_CONNECTIONS=5000
RIDER_ALLOWED_ORIGINS=https://app.ridechain.in,https://ridechain.in

# ── Firebase FCM ──────────────────────────────────────────────────────────────
# GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json

# ── Firebase Analytics ────────────────────────────────────────────────────────
# FIREBASE_MEASUREMENT_ID=G-XXXXXXXXXX   # Set directly on server — NEVER commit real values here
# FIREBASE_ANALYTICS_SECRET=xxxx         # Set directly on server — NEVER commit real values here

# ── HTTP API Rate Limiting ────────────────────────────────────────────────────
BOOTSTRAP_API_RATE_LIMIT=100
BOOTSTRAP_API_BURST=20

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL=info
ENVEOF

  chmod 600 "$ENV_FILE"
  chown root:root "$ENV_FILE"
  warn "IMPORTANT: Edit $ENV_FILE before starting the service!"
  warn "  - Set RIDER_ALLOWED_ORIGINS to your domain(s)"
  warn "  - Set GOOGLE_APPLICATION_CREDENTIALS if using FCM"
  warn "  - Set FIREBASE_MEASUREMENT_ID + FIREBASE_ANALYTICS_SECRET for analytics"
else
  info "Env file already exists at $ENV_FILE (skipping generation)"
fi

# =============================================================================
# 7. Configure Redis
# =============================================================================
info "Configuring Redis..."
cat > /etc/redis/redis.conf << 'REDISEOF'
bind 127.0.0.1
port 6379
maxmemory 512mb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
appendonly yes
appendfilename "ridechain.aof"
dir /var/lib/redis
protected-mode yes
REDISEOF
systemctl enable --now redis-server
info "Redis running on 127.0.0.1:6379"

# =============================================================================
# 8. systemd service for bootstrap
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
WorkingDirectory=${RIDECHAIN_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_PATH}
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ridechain-bootstrap

# Security hardening
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ReadWritePaths=/var/log/ridechain

# File descriptor limits (5000 WebSocket conns + overhead)
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable ridechain-bootstrap
info "Service installed. Start with: systemctl start ridechain-bootstrap"

# =============================================================================
# 9. UFW Firewall
# =============================================================================
info "Configuring firewall (UFW)..."
ufw --force reset >/dev/null
ufw default deny incoming >/dev/null
ufw default allow outgoing >/dev/null
ufw allow ssh
ufw allow 80/tcp   comment "HTTP (Caddy)"
ufw allow 443/tcp  comment "HTTPS (Caddy)"
ufw allow 4001/tcp comment "libp2p TCP"
ufw allow 4001/udp comment "libp2p QUIC"
ufw allow 4002/tcp comment "WebSocket (Caddy proxies to this)"
ufw allow 9090/tcp comment "Prometheus metrics (restrict to monitoring VM)"
# Internal ports 4003-4005 should NOT be exposed — Caddy proxies them
ufw --force enable >/dev/null
info "Firewall configured"

# =============================================================================
# 10. Prometheus + Grafana via Docker Compose
# =============================================================================
MONITORING_DIR="/opt/ridechain-monitoring"
mkdir -p "$MONITORING_DIR/prometheus" "$MONITORING_DIR/grafana"

cat > "$MONITORING_DIR/prometheus/prometheus.yml" << 'PROMEOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: ridechain_bootstrap
    static_configs:
      - targets: ["host.docker.internal:9090"]
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        replacement: "bootstrap-prod"
PROMEOF

cat > "$MONITORING_DIR/docker-compose.yml" << 'DCEOF'
version: "3.8"
services:
  prometheus:
    image: prom/prometheus:latest
    container_name: ridechain-prometheus
    restart: unless-stopped
    ports:
      - "9091:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-lifecycle"
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:latest
    container_name: ridechain-grafana
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=changeme_in_production
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_SERVER_ROOT_URL=https://grafana.ridechain.in
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  prometheus_data:
  grafana_data:
DCEOF

cd "$MONITORING_DIR"
docker compose up -d
info "Prometheus → http://YOUR_VM_IP:9091"
info "Grafana    → http://YOUR_VM_IP:3000 (admin / changeme_in_production)"

# =============================================================================
# 11. Install Caddy
# =============================================================================
if ! command -v caddy &>/dev/null; then
  info "Installing Caddy..."
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq
  apt-get install -y -qq caddy
fi

# Generate Caddyfile template
if [[ ! -f /etc/caddy/Caddyfile.ridechain ]]; then
cat > /etc/caddy/Caddyfile.ridechain << 'CADDYEOF'
# ── Rider WebSocket Bridge ─────────────────────────────────────────────────────
# wss://ws.ridechain.in/rider  →  ws://localhost:4003/rider
ws.ridechain.in {
    reverse_proxy /rider     localhost:4003
    reverse_proxy /health    localhost:4003
    log { output file /var/log/caddy/ws.log }
}

# ── HTTP API ───────────────────────────────────────────────────────────────────
# https://api.ridechain.in  →  http://localhost:4005
api.ridechain.in {
    reverse_proxy localhost:4005
    log { output file /var/log/caddy/api.log }

    # Cloudflare real IP passthrough
    header_up X-Real-IP {http.request.header.CF-Connecting-IP}
}

# ── Grafana (optional — restrict to your IP or use VPN) ───────────────────────
# grafana.ridechain.in {
#     reverse_proxy localhost:3000
# }
CADDYEOF
  warn "Caddyfile template at /etc/caddy/Caddyfile.ridechain"
  warn "Review and copy to /etc/caddy/Caddyfile, then: systemctl reload caddy"
fi

# =============================================================================
# Done
# =============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RideChain Bootstrap — Setup Complete"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  NEXT STEPS (in order):"
echo ""
echo "  1. Edit environment file:"
echo "     sudo nano ${ENV_FILE}"
echo "     → Set RIDER_ALLOWED_ORIGINS, FCM credentials, Analytics keys"
echo ""
echo "  2. Upload Firebase service account key:"
echo "     scp serviceAccountKey.json user@VM:/etc/ridechain/firebase-sa.json"
echo "     sudo chmod 600 /etc/ridechain/firebase-sa.json"
echo "     sudo chown ridechain:ridechain /etc/ridechain/firebase-sa.json"
echo ""
echo "  3. Start the bootstrap service:"
echo "     sudo systemctl start ridechain-bootstrap"
echo "     sudo systemctl status ridechain-bootstrap"
echo "     sudo journalctl -u ridechain-bootstrap -f"
echo ""
echo "  4. Configure Caddy:"
echo "     sudo cp /etc/caddy/Caddyfile.ridechain /etc/caddy/Caddyfile"
echo "     sudo systemctl reload caddy"
echo ""
echo "  5. Point Cloudflare DNS:"
echo "     A  ws.ridechain.in   → YOUR_VM_IP   (Proxied ON)"
echo "     A  api.ridechain.in  → YOUR_VM_IP   (Proxied ON)"
echo ""
echo "  6. Grafana dashboard:"
echo "     http://YOUR_VM_IP:3000  (admin / changeme_in_production)"
echo "     → Add Prometheus data source: http://prometheus:9090"
echo "     → Import dashboard JSON from docs/monitoring/grafana-dashboard.json"
echo ""
echo "  Ports summary:"
echo "    4001  libp2p TCP/QUIC (public — GCP firewall)"
echo "    4003  Rider WS bridge (internal — Caddy proxies)"
echo "    4005  HTTP API        (internal — Caddy proxies)"
echo "    9090  Prometheus metrics (internal)"
echo "    9091  Prometheus UI   (Docker)"
echo "    3000  Grafana UI      (Docker)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
