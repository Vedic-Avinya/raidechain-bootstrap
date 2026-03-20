# RideChain Bootstrap — Complete Setup + Deploy Guide

> **This guide takes you from zero to a live production server.**
> Follow every step in order. Estimated time: ~45 minutes.

---

## Which VM to Use?

| Machine           | RAM  | Cost/mo | Use for                                       |
|-------------------|------|---------|-----------------------------------------------|
| **e2-micro**      | 1 GB | ~$6     | ❌ NOT enough — will OOM with Redis + Docker   |
| **e2-small**      | 2 GB | ~$14    | ✅ OK for early testing, <50 users, no Grafana |
| **e2-medium**     | 4 GB | ~$27    | ✅ Good for MVP, up to ~500 users              |
| **e2-standard-2** | 8 GB | ~$49    | ✅ Production, up to 5,000 users               |

**Your current e2-micro is not enough for production.** The Go server + Redis + Docker (Prometheus +
Grafana) together need ~2.5 GB RAM.

**Upgrade path:**

- Testing right now → use e2-small, skip Grafana/Docker
- Going live → upgrade to e2-medium or e2-standard-2

To upgrade without losing data:

```bash
# GCP Console → Compute Engine → VM Instances → click your VM → Edit → Machine type
# OR via gcloud (stop VM first):
gcloud compute instances stop YOUR_VM_NAME --zone=asia-south1-a
gcloud compute instances set-machine-type YOUR_VM_NAME \
  --machine-type=e2-standard-2 --zone=asia-south1-a
gcloud compute instances start YOUR_VM_NAME --zone=asia-south1-a
```

---

## PART 1 — GCP Setup (Do this in GCP Console)

### Step 1.1 — Reserve a Static External IP

1. GCP Console → **VPC network → External IP addresses**
2. Click **Reserve Static Address**
3. Name: `ridechain-bootstrap-ip`
4. Region: `asia-south1` (Mumbai)
5. Click **Reserve** — note down the IP address (you'll need it for Cloudflare)

### Step 1.2 — Create the VM

If you're upgrading your e2-micro, stop it first and change the machine type (see above).

If creating fresh:

```bash
gcloud compute instances create ridechain-bootstrap \
  --zone=asia-south1-a \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --address=ridechain-bootstrap-ip \
  --tags=ridechain-bootstrap \
  --scopes=cloud-platform
```

### Step 1.3 — GCP Firewall Rules

Run these in GCP Cloud Shell or your terminal (one time only):

```bash
# Allow libp2p TCP (other bootstrap nodes connect here)
gcloud compute firewall-rules create allow-libp2p-tcp \
  --direction=INGRESS --action=ALLOW --rules=tcp:4001 \
  --target-tags=ridechain-bootstrap

# Allow libp2p QUIC (UDP)
gcloud compute firewall-rules create allow-libp2p-quic \
  --direction=INGRESS --action=ALLOW --rules=udp:4001 \
  --target-tags=ridechain-bootstrap

# Allow HTTP + HTTPS (Caddy handles TLS termination)
gcloud compute firewall-rules create allow-http-https \
  --direction=INGRESS --action=ALLOW --rules=tcp:80,tcp:443 \
  --target-tags=ridechain-bootstrap
```

> **Do NOT open ports 4003, 4004, 4005** — Caddy proxies them internally only.
> **Do NOT open port 9090** unless you have a separate monitoring VM.

---

## PART 2 — SSH Into the VM and Run Setup Script

### Step 2.1 — SSH into the VM

```bash
# From your terminal:
gcloud compute ssh ridechain-bootstrap --zone=asia-south1-a

# OR use GCP Console → Compute Engine → SSH button
```

### Step 2.2 — Upload your code to the VM

**Option A — from your Mac terminal (run OUTSIDE the VM):**

```bash
# Copy the bootstrap folder to the VM
gcloud compute scp --recurse \
  /Users/umashankar.pathak/Documents/Learn_Node/ride/bootstrap \
  ridechain-bootstrap:/tmp/ridechain-bootstrap \
  --zone=asia-south1-a
```

**Option B — git clone on the VM (if repo is on GitHub):**

```bash
# On the VM:
git clone https://github.com/YOUR_ORG/ridechain.git /tmp/ridechain
```

### Step 2.3 — Run the setup script (on the VM)

```bash
# On the VM — run setup script
cd /tmp/ridechain-bootstrap   # or /tmp/ridechain/bootstrap

sudo bash scripts/setup-gcp.sh
```

This script does everything automatically:

- Installs Go 1.24, Redis, Docker, Caddy
- Builds the bootstrap binary
- Creates `/etc/ridechain/.env` with a fresh identity seed
- Sets up systemd service
- Configures UFW firewall
- Starts Prometheus + Grafana containers
- **Does NOT start the bootstrap service yet** (you configure .env first)

Wait for it to finish (~5-10 min). At the end you'll see `Setup Complete` with next steps.

---

## PART 3 — Configure .env (The Most Important Step)

The setup script created `/etc/ridechain/.env` with safe defaults. You only need to fill in 4–5
values.

### Step 3.1 — Open the .env file

```bash
sudo nano /etc/ridechain/.env
```

You'll see something like this. **Only change the lines marked with ← CHANGE:**

```bash
# ── Identity ──────────────────────────────────────────────────────────────────
BOOTSTRAP_IDENTITY_SEED=a3f9...  ← already generated, leave it
BOOTSTRAP_ENV=prod               ← leave as prod

# ── Ports ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_PORT=4001              ← leave
BOOTSTRAP_WS_PORT=4002           ← leave
BOOTSTRAP_RIDER_BRIDGE_PORT=4003 ← leave
BOOTSTRAP_DRIVER_BRIDGE_PORT=4004 ← leave
BOOTSTRAP_HTTP_PORT=4005         ← leave
BOOTSTRAP_METRICS_PORT=9090      ← leave

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL=redis://127.0.0.1:6379 ← leave (Redis runs locally)

# ── Rider Bridge ──────────────────────────────────────────────────────────────
MAX_RIDER_CONNECTIONS=5000       ← leave
RIDER_ALLOWED_ORIGINS=https://app.ridechain.in,https://ridechain.in  ← CHANGE to your domain(s)

# ── Firebase FCM ──────────────────────────────────────────────────────────────
# GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json  ← uncomment AFTER uploading key

# ── Firebase Analytics ────────────────────────────────────────────────────────
FIREBASE_MEASUREMENT_ID=G-7W4MJWQE02          ← ADD your real value here
FIREBASE_ANALYTICS_SECRET=0ZclyhQSQgironqnTxG-og  ← ADD your real value here

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL=info                   ← leave
```

**In nano:**

- Arrow keys to move, type to edit
- `Ctrl+O` then `Enter` to save
- `Ctrl+X` to exit

### Step 3.2 — Upload Firebase service account key (for FCM)

**On your Mac** (not the VM), run:

```bash
# Replace the path with where you downloaded the key from Firebase Console
gcloud compute scp ~/Downloads/your-firebase-sa-key.json \
  ridechain-bootstrap:/tmp/firebase-sa.json \
  --zone=asia-south1-a
```

**Back on the VM:**

```bash
sudo mv /tmp/firebase-sa.json /etc/ridechain/firebase-sa.json
sudo chmod 600 /etc/ridechain/firebase-sa.json
sudo chown ridechain:ridechain /etc/ridechain/firebase-sa.json
```

Then uncomment this line in `/etc/ridechain/.env`:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json
```

### Step 3.3 — Verify the .env looks correct

```bash
sudo cat /etc/ridechain/.env
```

---

## PART 4 — Configure Caddy (Reverse Proxy + TLS)

### Step 4.1 — Create the Caddyfile

```bash
sudo nano /etc/caddy/Caddyfile
```

Paste this exactly (replace `ridechain.in` with your actual domain):

```caddyfile
# WebSocket bridge for riders
ws.ridechain.in {
    reverse_proxy /rider  localhost:4003 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
    }
    reverse_proxy /health localhost:4003
}

# HTTP API (register, discover, search)
api.ridechain.in {
    reverse_proxy localhost:4005 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
        header_up X-Forwarded-For {http.request.header.CF-Connecting-IP}
    }
}
```

Save: `Ctrl+O`, Enter, `Ctrl+X`

### Step 4.2 — Start Caddy

```bash
sudo systemctl enable caddy
sudo systemctl start caddy
sudo systemctl status caddy   # should say "active (running)"
```

> Caddy automatically gets a free Let's Encrypt TLS certificate for your domains.
> It will fail to get certs until Cloudflare DNS is pointing to this VM (Part 5).

---

## PART 5 — Cloudflare DNS Setup

### Step 5.1 — Add DNS Records

Go to **Cloudflare Dashboard → ridechain.in → DNS → Records → Add record**

Add these **4 records** (use your actual VM IP):

| Type | Name        | IPv4 address | Proxy status             |
|------|-------------|--------------|--------------------------|
| A    | `ws`        | `YOUR_VM_IP` | ✅ Proxied (orange cloud) |
| A    | `api`       | `YOUR_VM_IP` | ✅ Proxied (orange cloud) |
| A    | `bootstrap` | `YOUR_VM_IP` | ⚠️ DNS only (grey cloud) |
| A    | `grafana`   | `YOUR_VM_IP` | ✅ Proxied (optional)     |

> `bootstrap.ridechain.in` must be DNS only (not proxied) because libp2p uses TCP/UDP 4001, which
> Cloudflare doesn't proxy.

### Step 5.2 — Enable WebSockets

Cloudflare Dashboard → **Network** → **WebSockets** → Toggle to **ON**

### Step 5.3 — Set SSL/TLS Mode

Cloudflare Dashboard → **SSL/TLS → Overview** → Select **Full** (not Flexible, not Full Strict yet)

> Use Full until Caddy successfully gets a cert, then switch to Full Strict.

### Step 5.4 — Disable Cloudflare Cache for API

Cloudflare Dashboard → **Rules → Cache Rules → Create rule**:

- Rule name: `No cache for API and WS`
- When: `Hostname equals api.ridechain.in OR Hostname equals ws.ridechain.in`
- Then: Cache Eligibility → **Bypass cache**

---

## PART 6 — Start the Server

### Step 6.1 — Start bootstrap service

```bash
sudo systemctl start ridechain-bootstrap
```

### Step 6.2 — Watch logs (stay here for 1 minute)

```bash
sudo journalctl -u ridechain-bootstrap -f
```

You should see lines like:

```json
{
  "level": "INFO",
  "msg": "step",
  "n": 1,
  "msg": "bootstrap starting"
}
{
  "level": "INFO",
  "msg": "step",
  "n": 9,
  "msg": "redis store connected"
}
{
  "level": "INFO",
  "msg": "bootstrap_ready",
  "peer_id": "12D3Koo...",
  "tcp_port": "4001"
}
{
  "level": "INFO",
  "msg": "metrics",
  "msg": "prometheus endpoint listening",
  "port": "9090"
}
{
  "level": "INFO",
  "msg": "http_api",
  "msg": "listening",
  "port": "4005"
}
```

If you see `redis connection failed` — check Redis is running:

```bash
redis-cli ping   # should print: PONG
sudo systemctl status redis-server
```

### Step 6.3 — Test locally (on the VM)

```bash
# Test HTTP API
curl http://localhost:4005/register \
  -X POST -H 'Content-Type: application/json' \
  -d '{"peerId":"test1","displayName":"Test User"}'
# Expected: {"status":"ok","peerId":"test1"}

# Test metrics
curl http://localhost:9090/metrics | grep ridechain_riders
# Expected: ridechain_riders_connected 0

# Test health
curl http://localhost:9090/healthz
# Expected: ok
```

### Step 6.4 — Test from outside (on your Mac)

Wait 2-3 minutes after DNS propagation, then:

```bash
# Test API via Cloudflare
curl https://api.ridechain.in/register \
  -X POST -H 'Content-Type: application/json' \
  -d '{"peerId":"test1","displayName":"Test User"}'

# Test WebSocket (install wscat: npm install -g wscat)
wscat -c "wss://ws.ridechain.in/rider?city=mumbai"
# Should connect. Type: {"type":"peer_online","peer_id":"test1","city":"mumbai"}
```

---

## PART 7 — Grafana Dashboard

### Step 7.1 — Access Grafana

Go to `http://YOUR_VM_IP:3000` in your browser (not via domain — direct IP).

- Username: `admin`
- Password: `changeme_in_production`

**Change the password immediately:**
Profile (bottom left) → Change Password

### Step 7.2 — Check Prometheus data source

Left sidebar → **Connections → Data sources**

You should see **Prometheus** already configured (auto-provisioned). Click it and click **Save &
Test** — should show green "Data source connected".

### Step 7.3 — Create your first dashboard

1. Left sidebar → **Dashboards → New → New dashboard**
2. Click **Add visualization**
3. Select **Prometheus** as data source
4. In the query box, type: `ridechain_riders_connected`
5. Click **Run queries**
6. Top right: change from **Graph** to **Stat** panel type
7. Title: "Active Riders"
8. **Save dashboard** (Ctrl+S), name it "RideChain Overview"

**Add these panels one by one:**

| Panel title          | Query                                                             | Visualization |
|----------------------|-------------------------------------------------------------------|---------------|
| Active Riders        | `ridechain_riders_connected`                                      | Stat          |
| Messages/sec         | `rate(ridechain_messages_relayed_total[1m])`                      | Time series   |
| Dropped (rate limit) | `rate(ridechain_messages_dropped_total{reason="rate_limit"}[1m])` | Time series   |
| Geo Topics Active    | `ridechain_geo_topics_active`                                     | Gauge         |
| FCM Success Rate     | `rate(ridechain_fcm_pushes_total{result="success"}[5m])`          | Stat          |
| Nearby Broadcasts    | `rate(ridechain_nearby_broadcasts_total[1m]) * 60`                | Time series   |

---

## PART 8 — Re-deploy (After Code Changes)

Every time you change code and want to update the server:

**On your Mac (in the bootstrap folder):**

```bash
# Build Linux binary
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  GOFLAGS="-mod=mod" \
  go build -ldflags="-s -w" -o bootstrap-new ./cmd/main.go

# Copy to VM
gcloud compute scp bootstrap-new \
  ridechain-bootstrap:/tmp/bootstrap-new \
  --zone=asia-south1-a
```

**On the VM:**

```bash
# Backup current, swap in new, restart
sudo cp /opt/ridechain/bootstrap /opt/ridechain/bootstrap.bak
sudo mv /tmp/bootstrap-new /opt/ridechain/bootstrap
sudo chmod +x /opt/ridechain/bootstrap
sudo chown ridechain:ridechain /opt/ridechain/bootstrap
sudo systemctl restart ridechain-bootstrap

# Watch logs for 30 seconds
sudo journalctl -u ridechain-bootstrap -f

# If something is wrong — rollback in 30 seconds:
# sudo cp /opt/ridechain/bootstrap.bak /opt/ridechain/bootstrap
# sudo systemctl restart ridechain-bootstrap
```

---

## PART 9 — Useful Commands (Cheat Sheet)

```bash
# Service management
sudo systemctl start   ridechain-bootstrap
sudo systemctl stop    ridechain-bootstrap
sudo systemctl restart ridechain-bootstrap
sudo systemctl status  ridechain-bootstrap

# Logs
sudo journalctl -u ridechain-bootstrap -f          # live logs
sudo journalctl -u ridechain-bootstrap -n 50       # last 50 lines
sudo journalctl -u ridechain-bootstrap --since "5 min ago"

# Edit config
sudo nano /etc/ridechain/.env                      # edit env vars
sudo systemctl restart ridechain-bootstrap         # apply changes

# Redis
redis-cli ping                                     # should print PONG
redis-cli info keyspace                            # shows stored peers
redis-cli keys "peer:meta:*" | head -10            # list registered peers

# Metrics
curl http://localhost:9090/metrics | grep ridechain

# Docker (Prometheus + Grafana)
cd /opt/ridechain-monitoring
docker compose ps                                  # check running
docker compose logs prometheus                     # prometheus logs
docker compose restart grafana                     # restart grafana

# Caddy
sudo systemctl status caddy
sudo caddy validate --config /etc/caddy/Caddyfile  # check for errors
sudo systemctl reload caddy                        # reload after Caddyfile edit
```

---

## PART 10 — Checklist Before Going Live

```
VM:
[ ] Machine type is e2-medium or larger (NOT e2-micro)
[ ] Static IP assigned
[ ] Firewall: tcp/udp 4001 open, 80/443 open, 4003-4005 CLOSED externally

Server (SSH in, verify each):
[ ] sudo systemctl status ridechain-bootstrap  → active (running)
[ ] sudo systemctl status redis-server          → active (running)
[ ] sudo systemctl status caddy                 → active (running)
[ ] curl http://localhost:4005/register -X POST -H 'Content-Type:application/json' -d '{"peerId":"x"}' → {"status":"ok"}
[ ] sudo cat /etc/ridechain/.env | grep BOOTSTRAP_ENV  → prod
[ ] sudo cat /etc/ridechain/.env | grep BOOTSTRAP_IDENTITY_SEED  → 64-char hex value

.env values set:
[ ] BOOTSTRAP_ENV=prod
[ ] RIDER_ALLOWED_ORIGINS=https://app.ridechain.in,https://ridechain.in
[ ] FIREBASE_MEASUREMENT_ID=G-7W4MJWQE02
[ ] FIREBASE_ANALYTICS_SECRET=<your secret>
[ ] GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json  (if using FCM)

Cloudflare:
[ ] A record: ws.ridechain.in → VM_IP (Proxied ON)
[ ] A record: api.ridechain.in → VM_IP (Proxied ON)
[ ] A record: bootstrap.ridechain.in → VM_IP (DNS only)
[ ] WebSockets: Enabled (Network tab)
[ ] SSL/TLS mode: Full

From your Mac (test after DNS propagates):
[ ] curl https://api.ridechain.in/register -X POST -H 'Content-Type:application/json' -d '{"peerId":"x"}' → {"status":"ok"}
[ ] wscat -c "wss://ws.ridechain.in/rider?city=mumbai"  → connects
```

---

## PART 11 — Blue/Green Deploy (Future: Zero Downtime)

> Skip this for now. Use Part 8 re-deploy. Come back here when you have real users and can't afford
> 5-second downtime.

The bootstrap is **stateful** — WebSocket connections are pinned to the process. Strategy: run new
binary on shadow ports, verify, then atomic swap.

```
Before:  Caddy → Bootstrap on :4003/:4004/:4005
During:  Caddy → Bootstrap-NEW on :4013/:4014/:4015  [smoke test]
After:   Caddy → Bootstrap-NEW promoted to :4003/:4004/:4005
```

```bash
# On VM: smoke-test new binary on shadow ports
sudo -u ridechain \
  BOOTSTRAP_RIDER_BRIDGE_PORT=4013 \
  BOOTSTRAP_DRIVER_BRIDGE_PORT=4014 \
  BOOTSTRAP_HTTP_PORT=4015 \
  /opt/ridechain/bootstrap &
NEW_PID=$!
sleep 3
curl -sf http://localhost:4015/register && echo "OK" || echo "FAILED"
kill $NEW_PID

# Then atomic swap (5s downtime):
sudo systemctl stop ridechain-bootstrap
sudo cp /opt/ridechain/bootstrap /opt/ridechain/bootstrap.prev
sudo mv /opt/ridechain/bootstrap-new /opt/ridechain/bootstrap
sudo systemctl start ridechain-bootstrap
```

**On SIGTERM, the bootstrap gracefully:**

1. Stops accepting new WebSocket upgrades
2. Sends `CloseGoingAway` frame to all connected riders
3. Mobile apps auto-reconnect (implement `onClosing` with backoff in Android/iOS)

---

## Cloudflare Settings Reference

| Setting       | Where               | Value                                    |
|---------------|---------------------|------------------------------------------|
| WebSockets    | Network             | **Enabled**                              |
| SSL/TLS mode  | SSL/TLS → Overview  | **Full**                                 |
| Cache for API | Rules → Cache Rules | **Bypass** for api.ridechain.in          |
| Cache for WS  | Rules → Cache Rules | **Bypass** for ws.ridechain.in           |
| Always Online | Speed               | Disabled                                 |
| Retry on 5xx  | Rules → Page Rules  | Enable for `*.ridechain.in/*` (optional) |

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

Since you run one VM today, blue/green means running **two processes simultaneously** during deploy,
then shifting traffic.

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

> **Note:** During the ~3–5s shutdown window, new connections are rejected (503). Cloudflare will
> retry on failure if you enable **Retry on 5xx** in Cloudflare → Rules → Page Rules.

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

| Setting              | Value                                                                          | Why                                                           |
|----------------------|--------------------------------------------------------------------------------|---------------------------------------------------------------|
| **WebSocket**        | Enabled (Network → WebSockets)                                                 | Required for rider/driver WS                                  |
| **Proxy status**     | Proxied (orange cloud)                                                         | DDoS + edge TLS                                               |
| **SSL/TLS mode**     | Full (not Flexible)                                                            | Caddy has valid cert; Full prevents MITM between CF and Caddy |
| **Always Online**    | Disabled                                                                       | Not applicable for WebSocket APIs                             |
| **Retry on failure** | Enable via Page Rule: `bootstrap.ridechain.in/*` → "Error rules: Retry on 5xx" | Auto-retry during 5s shutdown window                          |
| **Health check**     | Cloudflare → Traffic → Health Checks → `GET /health` on port 80                | Alerts you when bootstrap is down                             |

---

## CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/deploy-bootstrap.yml
name: Deploy Bootstrap

on:
  push:
    branches: [ main ]
    paths: [ 'services/bootstrap/**' ]

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

```
sudo mkdir -p /etc/ridechain
echo "BOOTSTRAP_IDENTITY_SEED=$(openssl rand -hex 32)" | sudo tee /etc/ridechain/.env
sudo nano /etc/ridechain/.env   # then add the rest of the lines


sudo nano /etc/ridechain/.env
BOOTSTRAP_IDENTITY_SEED=$(openssl rand -hex 32)
BOOTSTRAP_ENV=prod

BOOTSTRAP_PORT=4001
BOOTSTRAP_WS_PORT=4002
BOOTSTRAP_RIDER_BRIDGE_PORT=4003
BOOTSTRAP_DRIVER_BRIDGE_PORT=4004
BOOTSTRAP_HTTP_PORT=4005

REDIS_URL=redis://127.0.0.1:6379

MAX_RIDER_CONNECTIONS=500
RIDER_ALLOWED_ORIGINS=https://bootstrap.ridechain.in

FIREBASE_MEASUREMENT_ID=G-7W4MJWQE02
FIREBASE_ANALYTICS_SECRET=0ZclyhQSQgironqnTxG-og

BOOTSTRAP_API_RATE_LIMIT=60
BOOTSTRAP_API_BURST=10

LOG_LEVEL=info


sudo apt-get update -qq && sudo apt-get install -y git
git clone https://github.com/Vedic-Avinya/raidechain-bootstrap.git /tmp/bootstrap
cd /tmp/bootstrap


export PATH=$PATH:/usr/local/go/bin

git clone https://github.com/Vedic-Avinya/raidechain-bootstrap.git /tmp/bootstrap
# (if already cloned: cd /tmp/bootstrap && git pull)

# Install Go 1.24
wget -q https://go.dev/dl/go1.24.0.linux-amd64.tar.gz -O /tmp/go.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go.tar.gz
rm /tmp/go.tar.gz

# Add to PATH (current session)
export PATH=$PATH:/usr/local/go/bin

# Verify
go version

cd /tmp/bootstrap
GOFLAGS="-mod=mod" go build -ldflags="-s -w" -o /tmp/bootstrap-new ./cmd/main.go

sudo cp /opt/ridechain/bootstrap /opt/ridechain/bootstrap.bak
sudo mv /tmp/bootstrap-new /opt/ridechain/bootstrap
sudo chmod +x /opt/ridechain/bootstrap
sudo chown ridechain:ridechain /opt/ridechain/bootstrap
sudo systemctl restart ridechain-bootstrap

sudo journalctl -u ridechain-bootstrap -f
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS="-mod=mod" go build -ldflags="-s -w" -o bootstrap-new ./cmd/main.go && gcloud compute scp bootstrap-new instance-20260317-201243:/tmp/bootstrap-new --zone=asia-south1-a --project=ridechain-90ebd && gcloud compute ssh instance-20260317-201243 --zone=asia-south1-a --project=ridechain-90ebd --command='set -e; sudo cp /opt/ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap.bak || true; sudo mv /tmp/bootstrap-new /opt/ridechain-bootstrap/bootstrap; sudo chmod +x /opt/ridechain-bootstrap/bootstrap; sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap; sudo systemctl restart ridechain-bootstrap; sleep 3; echo "=== systemctl ==="; sudo systemctl status ridechain-bootstrap --no-pager; echo; echo "=== recent logs ==="; sudo journalctl -u ridechain-bootstrap -n 30 --no-pager'
```