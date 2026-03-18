# Bootstrap Services — Start, Caddyfile, Restart & Status

> Quick runbook: for a **new GCP instance**, do **Section 0** (static IP + firewall) first, then run **setup-bootstrap-server.sh**, then Caddyfile and start/restart. Otherwise use sections 1–5 as needed.

---

## 0. New GCP instance — do these first (static IP, firewall, DNS)

If you created a **new** VM, the default external IP is **ephemeral** and can change (e.g. on restart). That breaks Cloudflare DNS and the app. Do these in order:

**Step 1 — Reserve a static external IP**

1. GCP Console → **VPC network** → **IP addresses** (or **Compute Engine** → **IP addresses**).
2. Find your VM’s external IP in the list. If **Type** is **Ephemeral**:
   - Click the IP (or the three dots → **View details**).
   - Click **Promote to static** (or **Reserve**).
   - Name it (e.g. `bootstrap-static`) → **Reserve**.
3. If the IP is not attached to the VM: **Compute Engine** → **VM instances** → your VM → **Edit** → **Networking** → under **External IP** select the static IP you reserved → **Save**.

**Step 2 — Open firewall (tcp 80, 443)**

1. GCP Console → **VPC network** → **Firewall** → **+ CREATE FIREWALL RULE**.
2. **Name:** e.g. `allow-bootstrap-http`.
3. **Network:** default (or your VPC).
4. **Direction:** Ingress.
5. **Targets:** All instances in the network (or add a tag to the VM and choose “Specified target tags”).
6. **Source IPv4 ranges:** `0.0.0.0/0`.
7. **Protocols and ports:** **tcp** → `80, 443` (add `4003, 4004, 4005` if you need direct access).
8. **Create**.

**Step 3 — Point Cloudflare DNS to the static IP**

1. Cloudflare → **DNS** → **Records**.
2. Edit the **A** record for your bootstrap host (e.g. `bootstrap` or `bootstrap.ridechain.in`).
3. **Value:** set to the **static** external IP from Step 1 (not an old or ephemeral IP). **Save**.

**Step 4 — Cloudflare SSL (so origin works)**

1. Cloudflare → **SSL/TLS** → **Overview**.
2. Set to **Flexible** (Cloudflare talks HTTP to your origin on port 80). Use Full/Strict only if Caddy serves HTTPS on 443.

**Step 5 — Run the stack on the VM**

SSH into the VM and run the setup script (Section 1), then create/edit Caddyfile (Section 2) and start services (Section 3). After that, the API should be reachable at `https://bootstrap.ridechain.in/register` (or your hostname).

---

## 1. Run setup-bootstrap-server.sh first (fresh VM or full reinstall)

On a **new VM** or when you want to **install Redis, Caddy, and the bootstrap binary** from scratch, run the setup script before doing the Caddyfile and start steps.

**Option A — Script already on the VM (e.g. from metadata / startup):**  
If the VM was created with the startup script, it may have run at first boot. Skip to section 2 if Redis, Caddy, and the binary are already installed.

**Option B — Run the script manually:**

```bash
# Copy the script to the VM (or paste its contents into a file), then:
sudo nano /tmp/setup-bootstrap-server.sh
# Paste the contents of services/bootstrap/scripts/setup-bootstrap-server.sh, save (Ctrl+O, Enter, Ctrl+X)

sudo bash /tmp/setup-bootstrap-server.sh
```

**Option C — From repo URL (if public):**

```bash
curl -sL https://raw.githubusercontent.com/YOUR_ORG/chain/main/services/bootstrap/scripts/setup-bootstrap-server.sh | sudo bash
```

Ensure VM metadata **bootstrap_binary_url** (e.g. `gs://ridechain-bootstrap/bootstrap`) or **bootstrap_repo** is set so the script can download or build the binary. After the script finishes, proceed to section 2 to create or edit the Caddyfile, then section 3 to start/restart and check status.

---

## 2. Create or edit Caddyfile (with nano)

**Location:** `/etc/caddy/Caddyfile`

Edit with nano:

```bash
sudo nano /etc/caddy/Caddyfile
```

Paste or type the config below. Save: **Ctrl+O**, **Enter**. Exit: **Ctrl+X**.

**Use `:80`** when Cloudflare (or another proxy) terminates TLS and forwards HTTP to the VM on port 80.  
**Use `bootstrap.ridechain.in`** if Caddy does TLS itself (e.g. DNS-only, no Cloudflare).

### Minimal Caddyfile (HTTP on :80, WebSocket + API)

Do **not** add `encode zstd gzip` in this block — it can break WebSocket for `/rider` and `/driver`.

```caddyfile
# Bootstrap: /rider -> 4003 (WebSocket), /driver -> 4004 (WebSocket), rest -> 4005 (HTTP API)
# Use :80 when behind Cloudflare (origin HTTP). Use bootstrap.ridechain.in if Caddy does TLS.
:80 {
    handle /rider* {
        reverse_proxy localhost:4003
    }
    handle /driver* {
        reverse_proxy localhost:4004
    }
    handle {
        reverse_proxy localhost:4005
    }
}
```

After editing:

```bash
# Validate config (optional)
sudo caddy validate --config /etc/caddy/Caddyfile

# If Caddy is already running: reload (no downtime)
sudo systemctl reload caddy

# If Caddy is not running: start it instead (reload will fail with "cannot reload")
sudo systemctl start caddy
```

Full Caddyfile with global options, security headers, and logging is in **docs/rider-app-value-features.md** (search for “Caddyfile for bootstrap.ridechain.in”).

---

## 3. Start or restart services and check status

### Start services (first time or after reboot)

Run in order: Redis → Caddy → bootstrap.

```bash
# Enable on boot (optional; run once)
sudo systemctl enable redis-server
sudo systemctl enable caddy
sudo systemctl enable ridechain-bootstrap

# Start in order
sudo systemctl start redis-server
sudo systemctl start caddy
sudo systemctl start ridechain-bootstrap
```

### Restart all three

```bash
sudo systemctl restart redis-server
sudo systemctl restart caddy
sudo systemctl restart ridechain-bootstrap
```

### Restart only bootstrap (e.g. after binary or env change)

```bash
sudo systemctl restart ridechain-bootstrap
```

### Check status

```bash
sudo systemctl status redis-server caddy ridechain-bootstrap --no-pager
```

All three should show **active (running)**. If `ridechain-bootstrap` is **inactive** or **failed**:

- Check the binary exists and is executable:  
  `ls -la /opt/ridechain-bootstrap/bootstrap`
- Check env: `cat /etc/ridechain-bootstrap.env`
- View logs: `sudo journalctl -u ridechain-bootstrap.service -n 50 --no-pager`

### Bootstrap fails with status=203/EXEC ("Failed at step EXEC")

This means systemd could not **execute** the binary. Common cause: the file at `/opt/ridechain-bootstrap/bootstrap` is **missing or empty** (e.g. the setup script ran without `bootstrap_binary_url` and created a placeholder).

**Fix:**

1. **On the VM, check the file:**
   ```bash
   ls -la /opt/ridechain-bootstrap/bootstrap
   ```
   If it shows **0 bytes** or is missing, the binary was never installed.

2. **Install the binary.** Either:
   - **From GCS** (if you uploaded the binary to a bucket):
     ```bash
     sudo systemctl stop ridechain-bootstrap.service
     sudo gsutil -q cp gs://ridechain-bootstrap/bootstrap /opt/ridechain-bootstrap/bootstrap
     sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
     sudo chmod 755 /opt/ridechain-bootstrap/bootstrap
     sudo systemctl start ridechain-bootstrap.service
     ```
   - **From your machine** (build and SCP):
     ```bash
     # On your machine (monorepo root):
     GOOS=linux GOARCH=amd64 go build -o bootstrap ./services/bootstrap/cmd
     scp bootstrap USER@VM_IP:/tmp/bootstrap

     # On the VM:
     sudo mv /tmp/bootstrap /opt/ridechain-bootstrap/bootstrap
     sudo chown ridechain:ridechain /opt/ridechain-bootstrap/bootstrap
     sudo chmod 755 /opt/ridechain-bootstrap/bootstrap
     sudo systemctl start ridechain-bootstrap.service
     ```

3. **Verify:** `ls -la /opt/ridechain-bootstrap/bootstrap` should show a non-zero size (e.g. tens of MB). Then `sudo systemctl status ridechain-bootstrap.service --no-pager`.

### Reload Caddy only (after Caddyfile change)

```bash
# Use reload if Caddy is running; use start if it is not active
sudo systemctl reload caddy   # or: sudo systemctl start caddy
```

### Caddy won't start ("Job for caddy.service failed")

**Quick fix (most common):** The setup script writes a **global block** (`admin off`, `storage file_system`). Some Caddy builds (e.g. Debian package) don't support it and exit with status 1. Replace the Caddyfile with the **minimal config (no global block)** from section 2 above:

sudo mkdir -p /var/log/caddy
sudo chown -R caddy:caddy /var/log/caddy
sudo chmod 755 /var/log/caddy

```bash
sudo nano /etc/caddy/Caddyfile
```

Delete everything and paste only:

```caddyfile
:80 {
    handle /rider* {
        reverse_proxy localhost:4003
    }
    handle /driver* {
        reverse_proxy localhost:4004
    }
    handle {
        reverse_proxy localhost:4005
    }
}
```

Save (Ctrl+O, Enter) and exit (Ctrl+X), then:

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl start caddy
```

If it still fails, try `http://:80` instead of `:80` in the first line (some Caddy versions require the scheme).

---

1. **See why it failed:**
   ```bash
   sudo journalctl -xeu caddy.service --no-pager -n 40
   sudo systemctl status caddy.service --no-pager
   ```

2. **Check Caddyfile syntax:** Caddy often exits with an error code when the config is invalid.
   ```bash
   sudo caddy validate --config /etc/caddy/Caddyfile
   ```
   If it prints an error (e.g. "unrecognized directive", "invalid token"), fix the Caddyfile with `sudo nano /etc/caddy/Caddyfile` and validate again.

3. **Port 80 in use?** Another process may be bound to 80.
   ```bash
   sudo ss -tlnp | grep :80
   # or
   sudo lsof -i :80
   ```
   Stop the other service or change Caddy’s listen port in the Caddyfile.

4. **Config path:** Ensure the unit uses the right config. Default is often `/etc/caddy/Caddyfile`. Check:
   ```bash
   sudo systemctl cat caddy.service | grep -i caddyfile
   ```
   If the unit runs `caddy run --config /etc/caddy/Caddyfile`, that file must exist and be readable.

5. **Run Caddy in foreground** to see the exact error:
   ```bash
   sudo caddy run --config /etc/caddy/Caddyfile
   ```
   Press Ctrl+C to stop. Fix any error it prints, then start the service again.

---

## 4. Check if API is working (register returns 404?)

**1. On the VM — direct to bootstrap (port 4005):**  
Run this first. If you get **connection refused** or **empty response**, the API is not listening (bootstrap not running or Redis failed at startup).
```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST http://127.0.0.1:4005/register -H "Content-Type: application/json" -d '{"peerId":"test"}'
# Expect: {"status":"ok","peerId":"test"} and HTTP 200
```
If this is **200** but step 2 (localhost) is **404**, Caddy’s server block is not matching the request’s Host. **Fix:** use `:80` (or add `localhost`) in the Caddyfile so Caddy accepts both the domain and localhost — see “Caddy 404: localhost works, Caddy returns 404” below.

**2. On the VM — through Caddy (port 80):**
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost/register -H "Content-Type: application/json" -d '{"peerId":"test"}'
# Expect: 200
```
If 1 works but 2 fails with **404**, Caddy’s server block is bound to a hostname (e.g. `bootstrap.ridechain.in`) and does not match `Host: localhost`. **Fix:** edit the Caddyfile so the server address is **`:80`** (accept any host) or **`bootstrap.ridechain.in, localhost`**, then `sudo systemctl reload caddy`. See “Caddy 404” in section 4.

**3. From your machine — via Cloudflare (use HTTPS, not HTTP):**
```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST https://bootstrap.ridechain.in/register -H "Content-Type: application/json" -d '{"peerId":"test"}' --max-time 10
# Expect: {"status":"ok","peerId":"test"} and HTTP 200. If 404, see Cloudflare checklist. If hanging/timeout, see below.
```
**Important:** Use **https://** in the URL. If you use **http://**, Cloudflare may redirect to HTTPS and the POST can become a GET (body dropped), giving 405 or 404.

**If the request hangs or times out:** Cloudflare cannot reach your origin. Check (1) GCP firewall allows **tcp:80** from **0.0.0.0/0** (or Cloudflare IPs) to the VM, (2) Cloudflare **SSL/TLS** is **Flexible** (so CF connects to origin on port 80), (3) A record points to the VM’s **static** external IP (reserve it in GCP → VPC → IP addresses → Promote to static).

**Caddy 404: 200 on port 4005, 404 via localhost or https://bootstrap.ridechain.in**

If step 1 returns 200 but step 2 (localhost) or step 3 (public URL) returns 404, Caddy’s server block is not matching the request’s **Host**. A block like `bootstrap.ridechain.in { ... }` matches only that host; requests with `Host: localhost` (or some Cloudflare headers) get no block → 404.

**Fix on the VM:** Edit the Caddyfile so the server accepts all hosts:

```bash
sudo nano /etc/caddy/Caddyfile
```

Change the first line to **`:80`** (so Caddy accepts any Host). Use this **multiline** form (single-line `handle { ... }` can cause “Unexpected next token after '{'” on some Caddy versions):

```caddyfile
:80 {
    handle /rider* {
        reverse_proxy localhost:4003
    }
    handle /driver* {
        reverse_proxy localhost:4004
    }
    handle {
        reverse_proxy localhost:4005
    }
}
```

Save, then reload Caddy:

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

Retry step 2 and 3. Using `:80` is correct when Cloudflare terminates TLS and forwards HTTP to the VM.

**4. Cloudflare checklist (when register gives 404 from the app):**

| Check | What to do |
|-------|------------|
| **A record** | Cloudflare DNS → **A** record for your bootstrap host (e.g. `bootstrap`) → value = VM’s **static** external IP (reserve in GCP first: VPC → IP addresses → Promote to static). Remove any old IP. |
| **Proxy** | Orange cloud (Proxied) = traffic goes through Cloudflare. Grey (DNS only) = client hits VM directly. Both can work; if Proxied, origin must be reachable by Cloudflare. |
| **SSL/TLS** | If your Caddy serves **HTTP only on port 80**, set Cloudflare **SSL/TLS** → **Overview** → **Flexible**. (Cloudflare talks HTTP to origin:80.) Do **not** use Full/Strict unless Caddy is serving HTTPS on 443. |
| **Firewall** | GCP **VPC → Firewall**: allow **tcp:80** (and tcp:443 if Caddy uses TLS) from **0.0.0.0/0** (or Cloudflare IP ranges) to the VM. |
| **URL in app** | App must call **https://bootstrap.ridechain.in/register** (no trailing slash, correct host). Check `gradle.properties` / BuildConfig: `BOOTSTRAP_HTTP_URL` or equivalent. |

After changing DNS or SSL, wait a minute and retry. If still 404, run step 1 and 2 on the VM to see whether the failure is before or after Cloudflare.

**Still not working? Checklist:**

| Symptom | What to try |
|--------|--------------|
| **405 Method Not Allowed** | You may have used **http://** and Cloudflare redirected POST → GET. Use **https://** in the URL. |
| **404** | (1) **200 on 4005 but 404 via localhost or hostname** → Caddy server block doesn’t match Host. Fix: Caddyfile first line = **`:80`** (see “Caddy 404” above), then `sudo systemctl reload caddy`. (2) Otherwise: use **https://**, check A record and SSL **Flexible**, purge Cloudflare cache if needed. |
| **Timeout / hang** | Cloudflare cannot reach the VM. Check GCP firewall (tcp:80), Cloudflare SSL = **Flexible**, A record = VM’s **static** IP. Test from VM: `curl -s -X POST http://127.0.0.1:4005/register -H "Content-Type: application/json" -d '{"peerId":"test"}'`. |
| **Connection refused** | Bootstrap or Caddy not running on VM. SSH in: `sudo systemctl status ridechain-bootstrap caddy` and fix. |
| **200 on VM, fail via hostname** | DNS or Cloudflare. Confirm A record points to the VM’s current static IP; use **https** and **Flexible** SSL. |

**Exact curl that should work:**
```bash
curl -s -X POST https://bootstrap.ridechain.in/register -H "Content-Type: application/json" -d '{"peerId":"test"}'
# Expect: {"status":"ok","peerId":"test"}
```

**GCP Cloud Logging — reduce volume and cost**

GCP charges for log ingestion (first 50 GiB/month free, then ~$0.50/GiB). The bootstrap emits many **Debug**-level logs (conn_evt, relay, gossipsub_message, peer_stats, rider/driver connected, etc.) that can add up when sent to Cloud Logging.

- **Default (INFO):** Only important events are logged (startup, errors, register, FCM push, bridge listening). Keeps volume low.
- **LOG_LEVEL=debug:** Enables verbose logs (every connection, message relay, peer stats every 5s). Use only for troubleshooting.

To keep production logs (and cost) low, **do not set** `LOG_LEVEL` (or leave it unset). To add debug logs temporarily, set in `/etc/ridechain-bootstrap.env`:

```bash
LOG_LEVEL=debug
```

Then `sudo systemctl restart ridechain-bootstrap`. Remember to remove or comment out `LOG_LEVEL=debug` when done.

---

## 5. Quick reference

| Service              | Unit name               | Port(s)        | Config / binary                    |
|----------------------|-------------------------|----------------|------------------------------------|
| Redis                | `redis-server`          | 6379           | Default or `/etc/redis/redis.conf` |
| Caddy                | `caddy`                 | 80 (and 443 if TLS) | `/etc/caddy/Caddyfile`        |
| RideChain Bootstrap  | `ridechain-bootstrap`   | 4001–4005      | `/etc/ridechain-bootstrap.env`, `/opt/ridechain-bootstrap/bootstrap`. Optional: `LOG_LEVEL=debug` for verbose logs (default: info). |

| Action        | Command |
|---------------|---------|
| Start all     | `sudo systemctl start redis-server caddy ridechain-bootstrap` |
| Stop all      | `sudo systemctl stop ridechain-bootstrap caddy redis-server` |
| Status        | `sudo systemctl status redis-server caddy ridechain-bootstrap --no-pager` |
| Bootstrap logs| `sudo journalctl -u ridechain-bootstrap.service -f` |
| Reload Caddy  | `sudo systemctl reload caddy` |
