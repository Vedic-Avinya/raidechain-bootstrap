# RideChain Bootstrap — Production Setup Guide

> **Stack:** GCP Compute Engine → Caddy (TLS) → Bootstrap Node → Redis | Cloudflare DNS | Prometheus + Grafana

---

## Prerequisites

| What | Spec |
|------|------|
| GCP VM | e2-standard-2 (2 vCPU, 8 GB RAM), Ubuntu 22.04 LTS |
| Static IP | Reserve via GCP → VPC → External IP addresses |
| Domain | ridechain.in (managed in Cloudflare) |
| Firebase project | for FCM + Analytics |

---

## Part 1 — Firebase Setup

### 1A. Firebase Analytics (GA4)

You **DO** need to create this on Firebase. Here's how:

1. Go to [https://console.firebase.google.com](https://console.firebase.google.com)
2. Open your **ridechain** project (or create one linked to `ridechain.in`)
3. **Left sidebar → Analytics → Dashboard**
   - If Analytics is not enabled, click "Enable Google Analytics" and select/create a GA4 property
4. Get the **Measurement ID** and **API Secret**:
   - Firebase Console → Analytics → (click gear icon) → **Manage Google Analytics** link
   - Opens GA4 Admin at analytics.google.com
   - **Admin → Data Streams → Web stream (ridechain.in)**
   - Note `Measurement ID` (format: `G-XXXXXXXXXX`)
   - Scroll to **Measurement Protocol API secrets** → **Create** → note the `Secret value`
5. Set in GCP (see Part 2 below):
   ```
   FIREBASE_MEASUREMENT_ID=G-XXXXXXXXXX
   FIREBASE_ANALYTICS_SECRET=<secret_value>
   ```

> These are **server-side** only — never expose in client code. GA4 Measurement Protocol events appear in GA4 → Realtime and Events reports (delay ~24h for processed data).

### 1B. Firebase FCM (Cloud Messaging)

1. Firebase Console → **Project Settings → Service accounts**
2. Click **Generate new private key** → download JSON
3. Upload to your GCP VM:
   ```bash
   scp ~/Downloads/firebase-sa.json USER@VM_IP:/tmp/firebase-sa.json
   ssh USER@VM_IP "sudo mv /tmp/firebase-sa.json /etc/ridechain/firebase-sa.json && \
     sudo chmod 600 /etc/ridechain/firebase-sa.json && \
     sudo chown ridechain:ridechain /etc/ridechain/firebase-sa.json"
   ```
4. Set in `/etc/ridechain/.env`:
   ```
   GOOGLE_APPLICATION_CREDENTIALS=/etc/ridechain/firebase-sa.json
   ```

---

## Part 2 — GCP VM Setup

### 2A. Create the VM

```bash
gcloud compute instances create ridechain-bootstrap-prod \
  --zone=asia-south1-a \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --address=STATIC_IP_NAME \
  --tags=ridechain-bootstrap
```

### 2B. GCP Firewall Rules

```bash
# libp2p TCP
gcloud compute firewall-rules create ridechain-libp2p-tcp \
  --direction=INGRESS --priority=1000 --network=default \
  --action=ALLOW --rules=tcp:4001 \
  --target-tags=ridechain-bootstrap

# libp2p QUIC (UDP)
gcloud compute firewall-rules create ridechain-libp2p-quic \
  --direction=INGRESS --priority=1000 --network=default \
  --action=ALLOW --rules=udp:4001 \
  --target-tags=ridechain-bootstrap

# HTTP + HTTPS (Caddy handles TLS)
gcloud compute firewall-rules create ridechain-http-https \
  --direction=INGRESS --priority=1000 --network=default \
  --action=ALLOW --rules=tcp:80,tcp:443 \
  --target-tags=ridechain-bootstrap

# Prometheus metrics (RESTRICT to your monitoring IP or VPN)
gcloud compute firewall-rules create ridechain-metrics \
  --direction=INGRESS --priority=1000 --network=default \
  --action=ALLOW --rules=tcp:9090 \
  --source-ranges=YOUR_MONITORING_IP/32 \
  --target-tags=ridechain-bootstrap

# SSH (restrict to your IP)
gcloud compute firewall-rules create ridechain-ssh \
  --direction=INGRESS --priority=1000 --network=default \
  --action=ALLOW --rules=tcp:22 \
  --source-ranges=YOUR_IP/32 \
  --target-tags=ridechain-bootstrap
```

> **NEVER open ports 4003, 4004, 4005 in GCP firewall** — Caddy proxies them. Only Caddy on the same machine talks to them.

### 2C. Run the setup script

```bash
# SSH into VM
gcloud compute ssh ridechain-bootstrap-prod --zone=asia-south1-a

# Clone repo and run setup
git clone https://github.com/YOUR_ORG/ridechain.git /tmp/ridechain
cd /tmp/ridechain/bootstrap
sudo bash scripts/setup-gcp.sh
```

### 2D. Set environment variables in GCP Secret Manager (recommended)

Instead of editing `.env` directly, use GCP Secret Manager + Secret Manager addon:

```bash
# Create secrets
echo -n "YOUR_IDENTITY_SEED" | gcloud secrets create ridechain-identity-seed --data-file=-
echo -n "G-XXXXXXXXXX"       | gcloud secrets create ridechain-ga-measurement-id --data-file=-
echo -n "your-secret-value"  | gcloud secrets create ridechain-ga-api-secret --data-file=-

# Grant VM service account access
gcloud secrets add-iam-policy-binding ridechain-identity-seed \
  --member="serviceAccount:$(gcloud iam service-accounts list --format='value(email)' | head -1)" \
  --role=roles/secretmanager.secretAccessor
```

Or simply edit `/etc/ridechain/.env` after running the setup script:

```bash
sudo nano /etc/ridechain/.env
```

---

## Part 3 — Cloudflare DNS Setup

### 3A. Add DNS records

Go to **Cloudflare → ridechain.in → DNS**:

| Type | Name | Content | Proxy | TTL |
|------|------|---------|-------|-----|
| A | `ws` | `YOUR_GCP_VM_IP` | ✅ Proxied | Auto |
| A | `api` | `YOUR_GCP_VM_IP` | ✅ Proxied | Auto |
| A | `bootstrap` | `YOUR_GCP_VM_IP` | ⚠️ DNS only | Auto |

> - `ws.ridechain.in` — WebSocket bridge (proxied through Cloudflare CDN)
> - `api.ridechain.in` — HTTP API (proxied)
> - `bootstrap.ridechain.in` — raw libp2p TCP/UDP 4001 (**DNS only, not proxied** — Cloudflare doesn't proxy UDP/TCP other than 80/443)

### 3B. Cloudflare Settings

**SSL/TLS → Overview:** Set to **Full (strict)**

**Network → WebSockets:** Enable ✅ (critical for rider bridge)

**Speed → Optimization → HTTP/3 (with QUIC):** Enable ✅

**Security → WAF Rules** (add custom rule):
```
# Block if request path is /rider and no valid Origin header
(http.request.uri.path eq "/rider" and not http.request.headers["origin"] contains "ridechain.in")
```

**Page Rules** (or Transform Rules):
- Cache Level: Bypass for `api.ridechain.in/*`
- Cache Level: Bypass for `ws.ridechain.in/*`

---

## Part 4 — Caddy Configuration

After `setup-gcp.sh` runs, copy and start Caddy:

```bash
sudo cp /etc/caddy/Caddyfile.ridechain /etc/caddy/Caddyfile
sudo systemctl reload caddy
sudo systemctl status caddy
```

Full Caddyfile (edit `/etc/caddy/Caddyfile`):

```caddyfile
# WebSocket rider bridge
ws.ridechain.in {
    # Forward WebSocket upgrade + all WS traffic to rider bridge
    reverse_proxy /rider  localhost:4003 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
        transport http {
            dial_timeout 5s
        }
    }
    reverse_proxy /health localhost:4003

    log {
        output file /var/log/caddy/ws.log {
            roll_size 100mb
            roll_keep 5
        }
    }
}

# HTTP API
api.ridechain.in {
    reverse_proxy localhost:4005 {
        header_up X-Real-IP {http.request.header.CF-Connecting-IP}
        header_up X-Forwarded-For {http.request.header.CF-Connecting-IP}
    }

    log {
        output file /var/log/caddy/api.log {
            roll_size 100mb
            roll_keep 5
        }
    }
}
```

---

## Part 5 — Grafana Setup (Step by Step)

### 5A. Start Prometheus + Grafana

```bash
cd /opt/ridechain-monitoring
docker compose up -d
docker compose ps   # verify both running
```

### 5B. Access Grafana

Open `http://YOUR_VM_IP:3000` in your browser.
- Username: `admin`
- Password: `changeme_in_production` ← **change immediately**

To change password:
1. Grafana → Profile (top right) → Change Password

### 5C. Add Prometheus Data Source (auto-provisioned)

The `grafana/provisioning/datasources/prometheus.yml` file in this repo auto-provisions it when Grafana starts. Verify at:
**Grafana → Connections → Data sources → Prometheus** — should show "Data source connected and labels found."

If not auto-provisioned, add manually:
1. **Grafana → Connections → Data sources → Add data source**
2. Choose **Prometheus**
3. URL: `http://prometheus:9090`
4. Click **Save & Test**

### 5D. Import RideChain Dashboard

1. **Grafana → Dashboards → Import**
2. Click **Upload JSON file** → select `docs/monitoring/grafana-dashboard.json`
3. Select Prometheus as data source → **Import**

Or create panels manually with these queries:

```promql
# Active riders
ridechain_riders_connected

# Message rate (last 5 min)
rate(ridechain_messages_relayed_total[5m])

# Dropped messages (rate limit hits)
rate(ridechain_messages_dropped_total{reason="rate_limit"}[5m])

# FCM push success rate
rate(ridechain_fcm_pushes_total{result="success"}[5m]) /
  rate(ridechain_fcm_pushes_total[5m])

# Active geo topics (shows geographic spread)
ridechain_geo_topics_active

# Nearby broadcasts per minute
rate(ridechain_nearby_broadcasts_total[1m]) * 60

# Message size p99
histogram_quantile(0.99, rate(ridechain_message_size_bytes_bucket[5m]))
```

### 5E. (Optional) Expose Grafana via Caddy + Cloudflare

Add to `/etc/caddy/Caddyfile`:
```caddyfile
grafana.ridechain.in {
    # Restrict to your IP or use Cloudflare Access for auth
    reverse_proxy localhost:3000
}
```

Then add DNS record: `grafana.ridechain.in → YOUR_VM_IP` (Proxied)

---

## Part 6 — Service Management

```bash
# Start / stop / restart
sudo systemctl start   ridechain-bootstrap
sudo systemctl stop    ridechain-bootstrap
sudo systemctl restart ridechain-bootstrap
sudo systemctl status  ridechain-bootstrap

# Live logs
sudo journalctl -u ridechain-bootstrap -f

# Check metrics
curl http://localhost:9090/metrics | grep ridechain_riders

# Redis check
redis-cli ping           # → PONG
redis-cli info keyspace  # → shows keys

# Force reload Caddy (after Caddyfile edit)
sudo systemctl reload caddy
```

---

## Part 7 — Graceful Deploy (Zero Downtime)

```bash
# 1. Build new binary on your laptop
GOFLAGS="-mod=mod" GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o bootstrap-new ./cmd/main.go

# 2. Copy to VM
scp bootstrap-new USER@VM_IP:/tmp/bootstrap-new

# 3. On VM: atomic swap + restart
ssh USER@VM_IP << 'EOF'
  sudo cp /opt/ridechain/bootstrap /opt/ridechain/bootstrap.bak
  sudo mv /tmp/bootstrap-new /opt/ridechain/bootstrap
  sudo chmod +x /opt/ridechain/bootstrap
  sudo chown ridechain:ridechain /opt/ridechain/bootstrap
  sudo systemctl restart ridechain-bootstrap
  sleep 3
  sudo systemctl status ridechain-bootstrap
EOF

# 4. Rollback if needed
ssh USER@VM_IP "sudo cp /opt/ridechain/bootstrap.bak /opt/ridechain/bootstrap && \
  sudo systemctl restart ridechain-bootstrap"
```

---

## Part 8 — Android Native P2P

See `docs/ANDROID_P2P.md` for a full architecture walkthrough.

**Short answer:** Yes, you can do native P2P on Android now. Two approaches:

### Option A — WebSocket to Bootstrap (current, works today)
Android app connects via WebSocket to `wss://ws.ridechain.in/rider?city=mumbai`.
- Geohash location updates via `PUT https://api.ridechain.in/register/lat-lng`
- Nearby messages via WS message type `nearby_ping`
- 1:1 chat via DM message type with `target_peer_id`

### Option B — Native libp2p on Android (true P2P)
Use `go-libp2p` compiled to Android via `gomobile`:
```bash
gomobile bind -target=android \
  github.com/libp2p/go-libp2p \
  github.com/libp2p/go-libp2p-pubsub
```
Android app gets its own libp2p peer ID, connects directly to bootstrap node for peer discovery, then communicates peer-to-peer via QUIC.
- **Pros:** True P2P, no server needed for messaging after peer discovery
- **Cons:** ~15 MB AAR, battery impact, QUIC may be blocked on some networks

### Option C — WebRTC Data Channels (recommended for mobile chat)
- Bootstrap node provides signaling (SDP exchange via WS)
- Peers establish direct WebRTC Data Channels for chat
- Works through NAT, battery efficient, widely supported
- Add `github.com/pion/webrtc/v3` (already in go.mod as a transitive dep)

**Recommended path:** Start with Option A now (zero changes needed), plan Option C for v2.

---

## Checklist: Production Ready

- [ ] VM created with static IP in `asia-south1` (Mumbai) or `asia-south2` (Delhi)
- [ ] GCP firewall: 4001 TCP/UDP open, 80/443 open, 4003-4005 CLOSED externally
- [ ] `setup-gcp.sh` run successfully
- [ ] `/etc/ridechain/.env` configured (identity seed, origins, FCM, analytics)
- [ ] Firebase SA key at `/etc/ridechain/firebase-sa.json` (chmod 600)
- [ ] `ridechain-bootstrap.service` running (`systemctl status`)
- [ ] Caddy running with correct Caddyfile (`systemctl status caddy`)
- [ ] Cloudflare DNS: `ws.ridechain.in` and `api.ridechain.in` pointing to VM
- [ ] Cloudflare: WebSockets enabled, SSL Full Strict
- [ ] Prometheus scraping `localhost:9090/metrics` (check: `curl localhost:9090/metrics | head`)
- [ ] Grafana accessible, data source connected, dashboard imported
- [ ] `BOOTSTRAP_ENV=prod` set in `.env`
- [ ] `BOOTSTRAP_IDENTITY_SEED` is a fresh 64-hex-char secret (not dev seed)
- [ ] Test: `wscat -c wss://ws.ridechain.in/rider?city=mumbai`
- [ ] Test: `curl https://api.ridechain.in/register -d '{"peerId":"test1",...}'`
