# Graceful Deployment Plan — RideChain Bootstrap on GCP + Cloudflare + Caddy

> **Goal:** Zero-downtime deployments. Active WebSocket connections (riders/drivers) are not dropped. New code rolls in cleanly. Rollback takes < 2 minutes.

---

## Architecture Context

```
Internet → Cloudflare (proxy ON) → GCP Static IP → Caddy (:80/:443) → Bootstrap Process
                                                                              │
                                                                         Redis (local or Memorystore)
```

The bootstrap is a **stateful** service — WebSocket connections are pinned to a process. This means:
- Rolling updates to the same process = connection disruption
- Strategy: **Blue/Green** (safest for WebSocket), with **graceful drain** on the old instance

---

## Strategy: Blue/Green on a Single GCP VM (Current Setup)

Since you run one VM today, blue/green means running **two processes simultaneously** during deploy, then shifting traffic.

```
Before:  Cloudflare → Caddy → Bootstrap-OLD (port 4003/4004/4005)
During:  Cloudflare → Caddy → Bootstrap-NEW (port 4013/4014/4015) [warming up]
After:   Cloudflare → Caddy → Bootstrap-NEW (port 4003/4004/4005)  [OLD draining]
```

---

## Step-by-Step Deployment

### Step 1 — Build the Binary Locally

```bash
# On your dev machine (monorepo root)
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -ldflags="-s -w -X main.Version=$(git rev-parse --short HEAD)" \
  -o bootstrap-$(git rev-parse --short HEAD) \
  ./services/bootstrap/cmd/main.go

# Upload to GCS (version-tagged)
gsutil cp bootstrap-$(git rev-parse --short HEAD) gs://ridechain-bootstrap/releases/
gsutil cp bootstrap-$(git rev-parse --short HEAD) gs://ridechain-bootstrap/bootstrap-latest
```

---

### Step 2 — SSH into GCP VM and Download New Binary

```bash
ssh user@bootstrap.ridechain.in

# Download the new binary to a staging path
sudo gsutil cp gs://ridechain-bootstrap/bootstrap-latest /opt/ridechain-bootstrap/bootstrap-new
sudo chmod 755 /opt/ridechain-bootstrap/bootstrap-new
sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap-new

# Verify it starts (smoke test — use different ports to avoid conflict)
sudo -u ridechain BOOTSTRAP_PORT=4011 BOOTSTRAP_WS_PORT=4012 \
  BOOTSTRAP_RIDER_BRIDGE_PORT=4013 BOOTSTRAP_DRIVER_BRIDGE_PORT=4014 \
  BOOTSTRAP_HTTP_PORT=4015 \
  /opt/ridechain-bootstrap/bootstrap-new &
NEW_PID=$!
sleep 3

# Health check new binary on shadow ports
curl -sf http://localhost:4015/health && echo "NEW binary healthy" || echo "FAILED"
curl -sf http://localhost:4013/health && echo "Rider bridge healthy" || echo "FAILED"
curl -sf http://localhost:4014/health && echo "Driver bridge healthy" || echo "FAILED"

# Kill the smoke-test instance
kill $NEW_PID
```

---

### Step 3 — Swap the Binary (Atomic)

```bash
# Atomic swap: old becomes .prev, new becomes active
sudo systemctl stop ridechain-bootstrap        # sends SIGTERM → graceful shutdown (5s)
sudo cp /opt/ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap.prev
sudo mv /opt/ridechain-bootstrap/bootstrap-new /opt/ridechain-bootstrap/bootstrap

# Start new binary
sudo systemctl start ridechain-bootstrap
sleep 3

# Verify new process is up
sudo systemctl is-active ridechain-bootstrap
curl -sf http://localhost:4005/health && echo "API healthy" || echo "FAILED — rollback!"
curl -sf http://localhost:4003/health && echo "Rider bridge healthy" || echo "FAILED — rollback!"
curl -sf http://localhost:4004/health && echo "Driver bridge healthy" || echo "FAILED — rollback!"
```

---

### Step 4 — Verify via Cloudflare

```bash
# From your local machine
curl -sf https://bootstrap.ridechain.in/health
# Expected: {"status":"ok","rider_connected":true|false,"rider_count":N}

# Check logs on VM for errors
sudo journalctl -u ridechain-bootstrap.service -n 30 --no-pager | grep -E "ERROR|WARN|step"
```

---

### Step 5 — Rollback (if Step 3/4 fails)

```bash
# < 2 minutes to rollback
sudo systemctl stop ridechain-bootstrap
sudo cp /opt/ridechain-bootstrap/bootstrap.prev /opt/ridechain-bootstrap/bootstrap
sudo systemctl start ridechain-bootstrap
sleep 3
curl -sf http://localhost:4005/health && echo "ROLLBACK OK"
```

---

## Graceful Shutdown — What Happens to Active Connections

The bootstrap already handles `SIGTERM` correctly via `signal.NotifyContext`:

```go
// cmd/main.go:56
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
```

On `systemctl stop` (sends SIGTERM):
1. `ctx.Done()` fires → both bridge `Run()` loops exit
2. `server.Shutdown(5s context)` → HTTP server stops accepting new WS upgrades
3. All connected riders/drivers receive WebSocket `CloseGoingAway` frame
4. Mobile apps see the close frame and **automatically reconnect** to the new process

**Client reconnect logic (required in apps):**
```kotlin
// Android / rider app — auto-reconnect on close
webSocket.onClosing { code, reason ->
    if (code == 1001) { // GoingAway
        reconnectWithBackoff(initialDelay = 1.second, maxDelay = 30.seconds)
    }
}
```

> **Note:** During the ~3–5s shutdown window, new connections are rejected (503). Cloudflare will retry on failure if you enable **Retry on 5xx** in Cloudflare → Rules → Page Rules.

---

## Systemd Unit (Production-Hardened)

Edit `/etc/systemd/system/ridechain-bootstrap.service`:

```ini
[Unit]
Description=RideChain Bootstrap Node
After=network-online.target redis-server.service
Wants=network-online.target
Requires=redis-server.service

[Service]
Type=simple
User=ridechain
Group=ridechain
WorkingDirectory=/opt/ridechain-bootstrap
EnvironmentFile=/etc/ridechain-bootstrap.env
ExecStart=/opt/ridechain-bootstrap/bootstrap
ExecStop=/bin/kill -TERM $MAINPID

# Graceful shutdown: wait up to 30s for connections to drain
TimeoutStopSec=30s
KillMode=mixed
KillSignal=SIGTERM
FinalKillSignal=SIGKILL

# Auto-restart on crash (but not on clean exit)
Restart=on-failure
RestartSec=5s
StartLimitInterval=60s
StartLimitBurst=3

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/ridechain-bootstrap /var/log/ridechain

# Resource limits
LimitNOFILE=65536       # Max open file descriptors (= max WS connections)
LimitNPROC=4096         # Max goroutines/threads

[Install]
WantedBy=multi-user.target
```

```bash
# Apply changes
sudo systemctl daemon-reload
sudo systemctl enable ridechain-bootstrap
```

---

## /etc/ridechain-bootstrap.env (Production)

```bash
# Identity — generate once: GENERATE_SEED=true ./bootstrap
BOOTSTRAP_IDENTITY_SEED=<64_hex_chars_from_generate_seed>

# Ports
BOOTSTRAP_PORT=4001
BOOTSTRAP_WS_PORT=4002
BOOTSTRAP_RIDER_BRIDGE_PORT=4003
BOOTSTRAP_DRIVER_BRIDGE_PORT=4004
BOOTSTRAP_HTTP_PORT=4005

# Topic
BOOTSTRAP_GOSSIPSUB_TOPIC=/ridechain/prod/in/p2p/v1

# Redis — use Memorystore internal IP or Upstash TLS URL
REDIS_URL=redis://:yourpassword@10.0.0.x:6379

# FCM — use ADC on GCP (no key file needed if VM service account has Firebase role)
# Or set GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain-bootstrap/firebase-sa.json

# Logging
LOG_LEVEL=info

# Dev mode guard — NEVER set this in production
# BOOTSTRAP_DEV_MODE=true
```

---

## Cloudflare Settings for Zero-Downtime Deploy

| Setting | Value | Why |
|---------|-------|-----|
| **WebSocket** | Enabled (Network → WebSockets) | Required for rider/driver WS |
| **Proxy status** | Proxied (orange cloud) | DDoS + edge TLS |
| **SSL/TLS mode** | Full (not Flexible) | Caddy has valid cert; Full prevents MITM between CF and Caddy |
| **Always Online** | Disabled | Not applicable for WebSocket APIs |
| **Retry on failure** | Enable via Page Rule: `bootstrap.ridechain.in/*` → "Error rules: Retry on 5xx" | Auto-retry during 5s shutdown window |
| **Health check** | Cloudflare → Traffic → Health Checks → `GET /health` on port 80 | Alerts you when bootstrap is down |

---

## CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/deploy-bootstrap.yml
name: Deploy Bootstrap

on:
  push:
    branches: [main]
    paths: ['services/bootstrap/**']

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version: '1.24'

      - name: Build binary
        working-directory: services/bootstrap
        run: |
          GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
            go build -ldflags="-s -w -X main.Version=${{ github.sha }}" \
            -o bootstrap-${{ github.sha }} ./cmd/main.go

      - name: Upload to GCS
        uses: google-github-actions/upload-cloud-storage@v2
        with:
          path: services/bootstrap/bootstrap-${{ github.sha }}
          destination: ridechain-bootstrap/releases/

      - name: Deploy to GCP VM
        uses: google-github-actions/ssh-compute@v1
        with:
          instance_name: bootstrap-vm
          zone: asia-south1-a
          ssh_private_key: ${{ secrets.GCP_SSH_KEY }}
          command: |
            set -e
            gsutil cp gs://ridechain-bootstrap/releases/bootstrap-${{ github.sha }} /tmp/bootstrap-new
            chmod 755 /tmp/bootstrap-new
            # Smoke test on shadow port
            BOOTSTRAP_HTTP_PORT=4099 /tmp/bootstrap-new &
            SMOKE_PID=$!
            sleep 3
            curl -sf http://localhost:4099/health || (kill $SMOKE_PID; echo "Smoke test FAILED"; exit 1)
            kill $SMOKE_PID
            # Atomic swap + restart
            sudo systemctl stop ridechain-bootstrap
            sudo cp /opt/ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap.prev
            sudo mv /tmp/bootstrap-new /opt/ridechain-bootstrap/bootstrap
            sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
            sudo systemctl start ridechain-bootstrap
            sleep 5
            curl -sf http://localhost:4005/health || (sudo systemctl stop ridechain-bootstrap; sudo cp /opt/ridechain-bootstrap/bootstrap.prev /opt/ridechain-bootstrap/bootstrap; sudo systemctl start ridechain-bootstrap; echo "ROLLBACK triggered"; exit 1)
            echo "Deploy SUCCESS: ${{ github.sha }}"

      - name: Notify Slack on failure
        if: failure()
        uses: 8398a7/action-slack@v3
        with:
          status: failure
          text: "Bootstrap deploy FAILED for ${{ github.sha }} — auto-rollback triggered"
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```

---

## Future: Multi-Instance Deployment (When You Scale)

Once you have 2+ bootstrap VMs behind a load balancer:

```
Cloudflare → GCP L4 LB (TCP, sticky by source IP)
                 │
        ┌────────┴────────┐
   Bootstrap-1       Bootstrap-2
   (Rolling update: update one at a time, wait for health check)
```

**Rolling update script (2 instances):**
```bash
# Update Bootstrap-2 first (50% traffic)
./deploy.sh bootstrap-2
# Wait for health check pass
gcloud compute backend-services get-health bootstrap-lb-backend

# Update Bootstrap-1
./deploy.sh bootstrap-1
```

WebSocket sticky sessions require L4 (TCP) load balancing — source IP hash, not round-robin.

---

## Quick Reference Checklist

```
Before every deploy:
[ ] Run go test ./... locally
[ ] Build for linux/amd64
[ ] Upload to GCS with SHA tag
[ ] Check current server health: curl https://bootstrap.ridechain.in/health

During deploy (on VM):
[ ] Download new binary to /tmp
[ ] Smoke test on shadow port 4099
[ ] systemctl stop (5s graceful drain)
[ ] Atomic binary swap
[ ] systemctl start
[ ] Wait 5s
[ ] Health check all 3 ports
[ ] Check logs: journalctl -u ridechain-bootstrap -n 20

After deploy:
[ ] Verify via Cloudflare: curl https://bootstrap.ridechain.in/health
[ ] Check app connects (open rider/driver app)
[ ] Monitor logs for 5 minutes: journalctl -u ridechain-bootstrap -f
[ ] Keep bootstrap.prev for 1 hour before deleting
```
